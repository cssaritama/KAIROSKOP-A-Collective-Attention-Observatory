"""
kairoskop.scripts.backfill_gdelt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Historical backfill script for GDELT data.

Downloads GDELT 2.0 event exports for a specified date range and loads
them directly into BigQuery, bypassing the Kafka/Spark streaming path.
Used for initial population of the data warehouse before the streaming
pipeline has accumulated sufficient history.

Usage:
    python scripts/backfill_gdelt.py --days 30
    python scripts/backfill_gdelt.py --start 2024-01-01 --end 2024-03-01
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
import structlog
from dotenv import load_dotenv
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

import os

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
)
log = structlog.get_logger("backfill.gdelt")

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET  = os.getenv("BQ_DATASET", "kairoskop")
BQ_TABLE    = "attention_events_raw"
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/sa-key.json")

GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

_CAMEO_CATEGORY_MAP: dict[str, str] = {
    "14": "conflict", "18": "conflict", "19": "conflict", "20": "conflict",
    "01": "culture",  "02": "culture",
    "03": "economics","07": "economics","08": "economics",
    "10": "health",
    "05": "science",
}

COL_GLOBALEVENTID = 0
COL_SQLDATE       = 1
COL_EVENTCODE     = 26
COL_ACTOR1NAME    = 6
COL_COUNTRY       = 37
COL_AVGTONE       = 34


def make_event_id(raw_id: str, ts: str) -> str:
    return hashlib.sha256(f"gdelt:{raw_id}:{ts}".encode()).hexdigest()[:32]


@retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5))
def fetch_master_list() -> list[tuple[str, str]]:
    """
    Download the GDELT master file list.
    Returns list of (datetime_str, url) tuples for export files.
    """
    log.info("fetching_master_list")
    response = requests.get(GDELT_MASTER_URL, timeout=30)
    response.raise_for_status()

    entries = []
    for line in response.text.strip().splitlines():
        parts = line.split()
        if len(parts) >= 3 and "export.CSV.zip" in parts[2]:
            # Extract datetime from filename: 20240315120000.export.CSV.zip
            filename = parts[2].split("/")[-1]
            dt_str   = filename[:14]
            entries.append((dt_str, parts[2]))
    return entries


def filter_by_date_range(
    entries: list[tuple[str, str]],
    start: datetime,
    end: datetime,
) -> list[tuple[str, str]]:
    """Filter master list entries to those within the date range."""
    result = []
    for dt_str, url in entries:
        try:
            dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            if start <= dt <= end:
                result.append((dt_str, url))
        except ValueError:
            continue
    return result


@retry(wait=wait_exponential(min=5, max=60), stop=stop_after_attempt(5))
def download_export(url: str) -> list[dict]:
    """Download and parse a GDELT export ZIP file."""
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()

    zipped = zipfile.ZipFile(io.BytesIO(response.content))
    csv_filename = [n for n in zipped.namelist() if n.endswith(".CSV")][0]

    rows = []
    with zipped.open(csv_filename) as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
        reader = csv.reader(text, delimiter="\t")
        for row in reader:
            if len(row) < 58:
                continue

            raw_date = row[COL_SQLDATE]
            try:
                ts = datetime.strptime(raw_date, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                ).isoformat()
                event_date = datetime.strptime(raw_date[:8], "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                continue

            try:
                tone   = float(row[COL_AVGTONE])
                signal = min(abs(tone) / 100.0, 1.0)
            except (ValueError, TypeError):
                signal = 0.0

            code   = row[COL_EVENTCODE]
            prefix = code[:2] if len(code) >= 2 else code
            cat    = _CAMEO_CATEGORY_MAP.get(prefix, "uncategorised")
            actor  = row[COL_ACTOR1NAME] or "Unknown"
            raw_id = row[COL_GLOBALEVENTID]

            rows.append({
                "event_id":         make_event_id(raw_id, ts),
                "source":           "gdelt",
                "event_timestamp":  ts,
                "event_date":       event_date,
                "topic":            actor,
                "topic_category":   cat,
                "language":         "en",
                "country_code":     row[COL_COUNTRY][:2] if row[COL_COUNTRY] else None,
                "signal_strength":  signal,
                "medium_type":      "institutional_media",
                "raw_payload":      json.dumps({"raw_id": raw_id, "code": code}),
                "ingested_at":      datetime.now(timezone.utc).isoformat(),
            })
    return rows


def load_to_bigquery(client: bigquery.Client, rows: list[dict]) -> int:
    """Insert a batch of rows into BigQuery."""
    if not rows:
        return 0

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors    = client.insert_rows_json(table_ref, rows)

    if errors:
        log.error("bq_insert_errors", count=len(errors), sample=errors[:2])
        return 0
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill GDELT data into BigQuery")
    parser.add_argument("--days",  type=int, help="Number of past days to backfill")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)

    if args.days:
        start = now - timedelta(days=args.days)
        end   = now
    elif args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end   = datetime.strptime(args.end,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        parser.error("Provide either --days or --start and --end")
        return

    log.info("backfill_starting", start=start.date(), end=end.date())

    client  = bigquery.Client(project=GCP_PROJECT)
    entries = fetch_master_list()
    entries = filter_by_date_range(entries, start, end)

    log.info("files_to_process", count=len(entries))

    total_loaded = 0
    for i, (dt_str, url) in enumerate(entries, 1):
        log.info("processing_file", file=i, total=len(entries), dt=dt_str)
        try:
            rows   = download_export(url)
            loaded = load_to_bigquery(client, rows)
            total_loaded += loaded
            log.info("file_loaded", rows=loaded, cumulative=total_loaded)
        except Exception as exc:
            log.error("file_failed", url=url, error=str(exc))

        # Respectful rate limiting
        time.sleep(1)

    log.info("backfill_complete", total_rows=total_loaded)


if __name__ == "__main__":
    main()
