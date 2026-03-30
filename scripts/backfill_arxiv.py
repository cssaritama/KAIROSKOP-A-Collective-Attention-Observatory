"""
kairoskop.scripts.backfill_arxiv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Historical backfill script for arXiv data.

Uses the arXiv API (ATOM feed) to retrieve papers submitted within a
specified date range and loads them directly into BigQuery.

Usage:
    python scripts/backfill_arxiv.py --days 30
    python scripts/backfill_arxiv.py --start 2024-01-01 --end 2024-03-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import feedparser
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
log = structlog.get_logger("backfill.arxiv")

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET  = os.getenv("BQ_DATASET", "kairoskop")
BQ_TABLE    = "attention_events_raw"

ARXIV_API_URL = "https://export.arxiv.org/api/query"

CATEGORIES = [
    "cs.AI", "cs.LG", "cs.CL", "cs.CV",
    "physics.gen-ph", "q-bio.NC", "q-bio.QM",
    "econ.GN", "stat.ML",
]

_CATEGORY_MAP: dict[str, str] = {
    "cs":      "science", "eess": "science", "math": "science",
    "physics": "science", "astro": "science", "cond": "science",
    "q-bio":   "health",  "q-fin": "economics",
    "econ":    "economics", "stat": "science",
}

BATCH_SIZE  = 100     # arXiv API max results per request
SLEEP_SECS  = 3       # Rate limit between API calls


def make_event_id(arxiv_id: str) -> str:
    return hashlib.sha256(f"arxiv:{arxiv_id}".encode()).hexdigest()[:32]


def _cat_to_topic(cat: str) -> str:
    prefix = cat.split(".")[0].lower()
    return _CATEGORY_MAP.get(prefix, "science")


@retry(wait=wait_exponential(min=3, max=30), stop=stop_after_attempt(5))
def fetch_arxiv_batch(
    category: str,
    start: datetime,
    end: datetime,
    offset: int = 0,
) -> list[dict]:
    """
    Fetch a batch of arXiv papers for a category within a date range.
    Returns a list of normalised row dicts ready for BigQuery.
    """
    params = {
        "search_query": (
            f"cat:{category} AND "
            f"submittedDate:[{start.strftime('%Y%m%d')}0000 "
            f"TO {end.strftime('%Y%m%d')}2359]"
        ),
        "start":         offset,
        "max_results":   BATCH_SIZE,
        "sortBy":        "submittedDate",
        "sortOrder":     "descending",
    }

    response = requests.get(ARXIV_API_URL, params=params, timeout=30)
    response.raise_for_status()

    feed  = feedparser.parse(response.content)
    rows  = []

    for entry in feed.entries:
        arxiv_id = getattr(entry, "id", "").split("/abs/")[-1]
        title    = getattr(entry, "title", "").strip().replace("\n", " ")

        if not arxiv_id or not title:
            continue

        published_str = getattr(entry, "published", None)
        if published_str:
            try:
                ts = datetime.fromisoformat(
                    published_str.replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        authors      = getattr(entry, "authors", [])
        author_count = len(authors) if authors else 1
        signal       = min(author_count / 20.0, 1.0)

        rows.append({
            "event_id":         make_event_id(arxiv_id),
            "source":           "arxiv",
            "event_timestamp":  ts.isoformat(),
            "event_date":       ts.strftime("%Y-%m-%d"),
            "topic":            title,
            "topic_category":   _cat_to_topic(category),
            "language":         "en",
            "country_code":     None,
            "signal_strength":  signal,
            "medium_type":      "formal_knowledge",
            "raw_payload":      json.dumps({
                "arxiv_id": arxiv_id,
                "category": category,
                "authors":  [a.get("name", "") for a in authors[:10]],
            }),
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
        })

    return rows


def load_to_bigquery(client: bigquery.Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors    = client.insert_rows_json(table_ref, rows)
    if errors:
        log.error("bq_insert_errors", count=len(errors))
        return 0
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill arXiv data into BigQuery")
    parser.add_argument("--days",  type=int)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end",   type=str)
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

    log.info("arxiv_backfill_starting", start=start.date(), end=end.date())

    client       = bigquery.Client(project=GCP_PROJECT)
    total_loaded = 0

    for category in CATEGORIES:
        log.info("processing_category", category=category)
        offset = 0

        while True:
            try:
                rows = fetch_arxiv_batch(category, start, end, offset)
                if not rows:
                    break

                loaded = load_to_bigquery(client, rows)
                total_loaded += loaded
                log.info("batch_loaded", category=category,
                          offset=offset, rows=loaded)

                if len(rows) < BATCH_SIZE:
                    break   # Last page

                offset += BATCH_SIZE
                time.sleep(SLEEP_SECS)

            except Exception as exc:
                log.error("batch_failed", category=category,
                           offset=offset, error=str(exc))
                break

    log.info("arxiv_backfill_complete", total_rows=total_loaded)


if __name__ == "__main__":
    main()
