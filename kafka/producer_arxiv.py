"""
kairoskop.kafka.producer_arxiv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kafka producer for arXiv attention signals.

arXiv publishes ~2,000 new papers per day across all scientific
disciplines.  In KAIROSKOP's framework, arXiv represents Teilhard de
Chardin's formal knowledge layer of the noosphere — the growing edge of
what humanity knows with rigour and precision.

The producer polls each configured arXiv RSS feed once per day (with a
configurable backfill window on first run) and publishes each paper as
an AttentionEvent to Kafka.
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
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
log = structlog.get_logger("producer.arxiv")

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_ARXIV      = os.getenv("KAFKA_TOPIC_ARXIV", "arxiv_papers")
CATEGORIES_STR   = os.getenv("ARXIV_CATEGORIES", "cs.AI,cs.LG,physics,q-bio,econ,stat")
CATEGORIES       = [c.strip() for c in CATEGORIES_STR.split(",")]
MAX_RESULTS      = int(os.getenv("ARXIV_MAX_RESULTS_PER_CATEGORY", "200"))
POLL_INTERVAL    = 86_400   # 24 hours in seconds

# arXiv category → TopicCategory mapping
_ARXIV_CATEGORY_MAP: dict[str, TopicCategory] = {
    "cs":      TopicCategory.SCIENCE_TECHNOLOGY,
    "eess":    TopicCategory.SCIENCE_TECHNOLOGY,
    "math":    TopicCategory.SCIENCE_TECHNOLOGY,
    "physics": TopicCategory.SCIENCE_TECHNOLOGY,
    "astro":   TopicCategory.SCIENCE_TECHNOLOGY,
    "cond":    TopicCategory.SCIENCE_TECHNOLOGY,
    "q-bio":   TopicCategory.HEALTH_SOCIETY,
    "q-fin":   TopicCategory.ECONOMICS_FINANCE,
    "econ":    TopicCategory.ECONOMICS_FINANCE,
    "stat":    TopicCategory.SCIENCE_TECHNOLOGY,
}


def _arxiv_to_category(arxiv_cat: str) -> TopicCategory:
    prefix = arxiv_cat.split(".")[0].lower()
    return _ARXIV_CATEGORY_MAP.get(prefix, TopicCategory.SCIENCE_TECHNOLOGY)


def make_event_id(arxiv_id: str) -> str:
    return hashlib.sha256(f"arxiv:{arxiv_id}".encode()).hexdigest()[:32]


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks":              "all",
        "compression.type":  "lz4",
        "linger.ms":         100,
        "enable.idempotence": True,
    })


def delivery_callback(err, msg) -> None:
    if err:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))


@retry(wait=wait_exponential(min=5, max=60), stop=stop_after_attempt(5))
def fetch_category_feed(category: str) -> list[feedparser.FeedParserDict]:
    """
    Fetch the arXiv RSS feed for a single category.
    Returns a list of entry objects from feedparser.
    """
    url = f"https://rss.arxiv.org/rss/{category}"
    feed = feedparser.parse(url)

    if feed.bozo and feed.bozo_exception:
        raise ValueError(f"Feed parse error for {category}: {feed.bozo_exception}")

    return feed.entries


def process_entry(entry: feedparser.FeedParserDict, category: str) -> AttentionEvent | None:
    """
    Convert a single arXiv RSS entry into an AttentionEvent.
    Returns None if the entry lacks required fields.
    """
    arxiv_id = getattr(entry, "id", None)
    title    = getattr(entry, "title", "").strip()

    if not arxiv_id or not title:
        return None

    # Parse publication date
    published = getattr(entry, "published", None)
    if published:
        try:
            ts = parsedate_to_datetime(published).astimezone(timezone.utc).isoformat()
        except Exception:
            ts = datetime.now(timezone.utc).isoformat()
    else:
        ts = datetime.now(timezone.utc).isoformat()

    # Use number of authors as a proxy for significance
    authors = getattr(entry, "authors", [])
    author_count = len(authors) if authors else 1
    signal = min(author_count / 20.0, 1.0)   # Normalised: 20+ authors → 1.0

    return AttentionEvent(
        event_id        = make_event_id(arxiv_id),
        source          = AttentionSource.ARXIV,
        event_timestamp = ts,
        topic           = title,
        topic_category  = _arxiv_to_category(category),
        language        = "en",
        country_code    = None,
        signal_strength = signal,
        medium_type     = MediumType.FORMAL_KNOWLEDGE,
        raw_payload     = {
            "arxiv_id": arxiv_id,
            "category": category,
            "authors":  [a.get("name", "") for a in authors[:10]],
            "summary":  getattr(entry, "summary", "")[:500],
        },
    )


def run(producer: Producer) -> None:
    log.info("arxiv_producer_started",
             topic=TOPIC_ARXIV,
             categories=CATEGORIES,
             poll_interval_h=POLL_INTERVAL // 3600)

    while True:
        total_published = 0

        for category in CATEGORIES:
            try:
                entries = fetch_category_feed(category)
                cat_published = 0

                for entry in entries[:MAX_RESULTS]:
                    event = process_entry(entry, category)
                    if event is None:
                        continue

                    producer.produce(
                        TOPIC_ARXIV,
                        key=category.encode(),
                        value=event.to_json().encode(),
                        callback=delivery_callback,
                    )
                    cat_published += 1
                    producer.poll(0)

                producer.flush()
                log.info("arxiv_category_published",
                         category=category,
                         count=cat_published)
                total_published += cat_published

                # Respectful rate limiting between category requests
                time.sleep(3)

            except Exception as exc:
                log.error("arxiv_category_failed",
                          category=category,
                          error=str(exc))
                time.sleep(10)

        log.info("arxiv_daily_run_complete", total=total_published)
        log.info("arxiv_sleeping", hours=POLL_INTERVAL // 3600)
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
