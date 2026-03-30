"""
kairoskop.kafka.producer_gdelt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kafka producer for GDELT 2.0 attention signals.

GDELT (Global Database of Events, Language, and Tone) monitors print,
broadcast, and web news media in 100+ languages across every country,
updated every 15 minutes.  In KAIROSKOP's framework, GDELT represents
McLuhan's institutional media layer — what the global media apparatus
decides exists and therefore shapes public consciousness.

The producer polls the GDELT master file index every 15 minutes,
downloads the latest export, parses the TSV, and publishes events to
Kafka.  Each event is deduplicated via an in-memory LRU set.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import signal
import sys
import time
import zipfile
from collections import deque
from datetime import datetime, timezone
from typing import Iterator

import requests
import structlog
from confluent_kafka import Producer
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from schemas import (
    AttentionEvent,
    AttentionSource,
    MediumType,
    TopicCategory,
)

load_dotenv()

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
)
log = structlog.get_logger("producer.gdelt")

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_GDELT       = os.getenv("KAFKA_TOPIC_GDELT", "gdelt_events")
POLL_INTERVAL     = int(os.getenv("GDELT_POLL_INTERVAL_SECONDS", "900"))
GDELT_LASTUPDATE  = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# GDELT event codes that map to our topic categories
# Full codebook: https://www.gdeltproject.org/data/documentation/CAMEO.Manual.FINAL.pdf
_CAMEO_CATEGORY_MAP: dict[str, TopicCategory] = {
    "14": TopicCategory.CONFLICT_POLITICS,   # Protest
    "18": TopicCategory.CONFLICT_POLITICS,   # Assault
    "19": TopicCategory.CONFLICT_POLITICS,   # Fight
    "20": TopicCategory.CONFLICT_POLITICS,   # Use unconventional violence
    "01": TopicCategory.CULTURE_IDENTITY,    # Make public statement
    "02": TopicCategory.CULTURE_IDENTITY,    # Appeal
    "03": TopicCategory.ECONOMICS_FINANCE,   # Express intent to cooperate
    "07": TopicCategory.ECONOMICS_FINANCE,   # Provide aid
    "08": TopicCategory.ECONOMICS_FINANCE,   # Yield
    "10": TopicCategory.HEALTH_SOCIETY,      # Demand
    "05": TopicCategory.SCIENCE_TECHNOLOGY,  # Consult
}

# GDELT export TSV column indices (GKG export, event file)
# See: https://www.gdeltproject.org/data/documentation/GDELT-Event_Codebook-V2.0.pdf
COL_GLOBALEVENTID    = 0
COL_SQLDATE          = 1
COL_EVENTCODE        = 26
COL_ACTOR1NAME       = 6
COL_ACTOR2NAME       = 16
COL_COUNTRY          = 37
COL_AVGTONE          = 34
COL_NUMARTICLES      = 33

# Deduplication ring buffer — holds last 10,000 event IDs
_seen_ids: deque[str] = deque(maxlen=10_000)


def _cameo_to_category(code: str) -> TopicCategory:
    """Map a CAMEO event code prefix to a TopicCategory."""
    prefix = code[:2] if len(code) >= 2 else code
    return _CAMEO_CATEGORY_MAP.get(prefix, TopicCategory.UNCATEGORISED)


def make_event_id(raw_id: str) -> str:
    return hashlib.sha256(f"gdelt:{raw_id}".encode()).hexdigest()[:32]


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks":              "all",
        "compression.type":  "lz4",
        "linger.ms":         50,
        "enable.idempotence": True,
    })


def delivery_callback(err, msg) -> None:
    if err:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))


@retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5))
def fetch_latest_export_url() -> str | None:
    """
    Parse the GDELT lastupdate.txt file to get the URL of the most
    recent events export.  Returns None if the file is unavailable.
    """
    response = requests.get(GDELT_LASTUPDATE, timeout=15)
    response.raise_for_status()

    # Format: "size hash url" — one line per file type (events, mentions, gkg)
    for line in response.text.strip().splitlines():
        parts = line.split()
        if len(parts) >= 3 and "export.CSV.zip" in parts[2]:
            return parts[2]
    return None


@retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(5))
def download_and_parse_export(url: str) -> Iterator[dict]:
    """
    Download a GDELT export ZIP, decompress it in memory, and yield
    each row as a dict.  Skips malformed rows silently.
    """
    response = requests.get(url, timeout=60, stream=True)
    response.raise_for_status()

    zipped = zipfile.ZipFile(io.BytesIO(response.content))
    csv_filename = [n for n in zipped.namelist() if n.endswith(".CSV")][0]

    with zipped.open(csv_filename) as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
        reader = csv.reader(text, delimiter="\t")
        for row in reader:
            if len(row) < 58:   # GDELT V2 has 58+ columns
                continue
            yield {
                "id":       row[COL_GLOBALEVENTID],
                "date":     row[COL_SQLDATE],
                "code":     row[COL_EVENTCODE],
                "actor1":   row[COL_ACTOR1NAME],
                "actor2":   row[COL_ACTOR2NAME],
                "country":  row[COL_COUNTRY],
                "tone":     row[COL_AVGTONE],
                "articles": row[COL_NUMARTICLES],
            }


def run(producer: Producer) -> None:
    log.info("gdelt_producer_started", topic=TOPIC_GDELT, interval_s=POLL_INTERVAL)
    last_url: str | None = None

    while True:
        try:
            url = fetch_latest_export_url()
            if not url or url == last_url:
                log.debug("gdelt_no_new_data")
                time.sleep(POLL_INTERVAL)
                continue

            last_url  = url
            published = 0
            skipped   = 0

            for row in download_and_parse_export(url):
                event_id = make_event_id(row["id"])

                # Deduplicate
                if event_id in _seen_ids:
                    skipped += 1
                    continue
                _seen_ids.append(event_id)

                # Parse timestamp
                raw_date = row["date"]   # YYYYMMDDHHMMSS format
                try:
                    ts = datetime.strptime(raw_date, "%Y%m%d%H%M%S").replace(
                        tzinfo=timezone.utc
                    ).isoformat()
                except ValueError:
                    ts = datetime.now(timezone.utc).isoformat()

                # Normalise tone → signal strength (GDELT tone: -100 to +100)
                try:
                    tone = float(row["tone"])
                    signal = abs(tone) / 100.0   # magnitude, not sentiment
                except (ValueError, TypeError):
                    signal = 0.0

                actor = row["actor1"] or row["actor2"] or "Unknown"

                event = AttentionEvent(
                    event_id        = event_id,
                    source          = AttentionSource.GDELT,
                    event_timestamp = ts,
                    topic           = actor,
                    topic_category  = _cameo_to_category(row["code"]),
                    language        = "en",
                    country_code    = row["country"][:2] if row["country"] else None,
                    signal_strength = min(signal, 1.0),
                    medium_type     = MediumType.INSTITUTIONAL_MEDIA,
                    raw_payload     = row,
                )

                producer.produce(
                    TOPIC_GDELT,
                    key=actor.encode(),
                    value=event.to_json().encode(),
                    callback=delivery_callback,
                )
                published += 1
                if published % 1000 == 0:
                    producer.poll(0)

            producer.flush()
            log.info("gdelt_batch_published",
                     url=url.split("/")[-1],
                     published=published,
                     skipped=skipped)

        except Exception as exc:
            log.error("gdelt_poll_failed", error=str(exc))

        time.sleep(POLL_INTERVAL)


def main() -> None:
    producer = build_producer()

    def _shutdown(sig, frame):
        producer.flush(10)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    run(producer)


if __name__ == "__main__":
    main()
