"""
kairoskop.kafka.producer_wikipedia
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Kafka producer for Wikipedia attention signals.

Consumes two complementary streams:

1. Wikipedia Recent Changes (SSE) — real-time feed of every edit made
   to any Wikimedia project.  Measures active collective memory: what
   humans are rewriting and debating right now.

2. Wikipedia Pageviews API — hourly aggregate of article view counts.
   Measures reactive attention: what people look up after something
   happens in the world.

Both streams publish to separate Kafka topics with a shared envelope
schema defined in `schemas.py`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Generator

import requests
import sseclient
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

# ─────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO)
)
log = structlog.get_logger("producer.wikipedia")

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_CHANGES    = os.getenv("KAFKA_TOPIC_WIKI_CHANGES", "wiki_changes")
TOPIC_PAGEVIEWS  = os.getenv("KAFKA_TOPIC_WIKI_PAGEVIEWS", "wiki_pageviews")
SSE_URL          = os.getenv("WIKI_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
PAGEVIEWS_PROJECT = os.getenv("WIKI_PAGEVIEWS_PROJECT", "en.wikipedia")

# Categories to track in pageviews (English Wikipedia categories)
TRACKED_CATEGORIES: list[str] = [
    "Artificial intelligence", "Climate change", "War", "Economics",
    "Public health", "Philosophy of mind", "Neuroscience", "Physics",
    "Geopolitics", "Social movement",
]

# ─────────────────────────────────────────────────────────────────────
# Topic classification — simple keyword lookup
# ─────────────────────────────────────────────────────────────────────
_CATEGORY_KEYWORDS: dict[TopicCategory, list[str]] = {
    TopicCategory.SCIENCE_TECHNOLOGY: ["science", "physics", "AI", "technology", "research", "biology"],
    TopicCategory.CONFLICT_POLITICS:  ["war", "conflict", "politics", "election", "protest", "military"],
    TopicCategory.HEALTH_SOCIETY:     ["health", "disease", "medicine", "pandemic", "mental"],
    TopicCategory.ENVIRONMENT:        ["climate", "environment", "ecology", "pollution", "energy"],
    TopicCategory.CULTURE_IDENTITY:   ["culture", "art", "religion", "history", "philosophy"],
    TopicCategory.ECONOMICS_FINANCE:  ["economy", "finance", "market", "trade", "inflation"],
}


def classify_topic(title: str) -> TopicCategory:
    """Assign a TopicCategory based on keyword presence in the title."""
    title_lower = title.lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw.lower() in title_lower for kw in keywords):
            return category
    return TopicCategory.UNCATEGORISED


def make_event_id(source: str, raw_id: str, ts: str) -> str:
    """Deterministic SHA-256 event ID — ensures idempotency on reprocessing."""
    payload = f"{source}:{raw_id}:{ts}".encode()
    return hashlib.sha256(payload).hexdigest()[:32]


# ─────────────────────────────────────────────────────────────────────
# Kafka producer setup
# ─────────────────────────────────────────────────────────────────────

def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":              KAFKA_BOOTSTRAP,
        "acks":                           "all",
        "retries":                        5,
        "retry.backoff.ms":               500,
        "compression.type":               "lz4",
        "linger.ms":                      20,       # small batch window
        "batch.size":                     65536,
        "enable.idempotence":             True,
    })


def delivery_callback(err, msg) -> None:
    if err:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))


# ─────────────────────────────────────────────────────────────────────
# Stream 1 — Wikipedia Recent Changes (SSE)
# ─────────────────────────────────────────────────────────────────────

def sse_events(url: str) -> Generator[dict, None, None]:
    """
    Generator that yields parsed SSE event payloads indefinitely.
    Reconnects automatically on network errors.
    """
    while True:
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            client = sseclient.SSEClient(response)
            for event in client.events():
                if event.data and event.data != "":
                    try:
                        yield json.loads(event.data)
                    except json.JSONDecodeError:
                        continue
        except Exception as exc:
            log.warning("sse_reconnecting", error=str(exc))
            time.sleep(5)


def run_changes_producer(producer: Producer) -> None:
    """
    Consume Wikipedia Recent Changes and publish to Kafka.

    Filters to mainspace edits (namespace=0) and skips bot edits to
    focus on genuine human collective activity.
    """
    log.info("changes_producer_started", topic=TOPIC_CHANGES)
    published = 0

    for raw in sse_events(SSE_URL):
        # Filter: mainspace articles only, no bots
        if raw.get("namespace") != 0:
            continue
        if raw.get("bot", False):
            continue

        title    = raw.get("title", "")
        wiki     = raw.get("wiki", "enwiki")
        ts_unix  = raw.get("timestamp", 0)
        revision = str(raw.get("revision", {}).get("new", ""))

        ts = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()

        event = AttentionEvent(
            event_id        = make_event_id("wiki_changes", revision, ts),
            source          = AttentionSource.WIKI_CHANGES,
            event_timestamp = ts,
            topic           = title,
            topic_category  = classify_topic(title),
            language        = wiki.replace("wiki", "") or "en",
            country_code    = None,
            signal_strength = min(abs(raw.get("length", {}).get("new", 0)
                                      - raw.get("length", {}).get("old", 0)) / 10_000, 1.0),
            medium_type     = MediumType.COLLECTIVE_MEMORY,
            raw_payload     = raw,
        )

        producer.produce(
            TOPIC_CHANGES,
            key=title.encode(),
            value=event.to_json().encode(),
            callback=delivery_callback,
        )
        producer.poll(0)

        published += 1
        if published % 500 == 0:
            producer.flush()
            log.info("changes_progress", published=published)


# ─────────────────────────────────────────────────────────────────────
# Stream 2 — Wikipedia Pageviews (hourly polling)
# ─────────────────────────────────────────────────────────────────────

@retry(wait=wait_exponential(min=2, max=60), stop=stop_after_attempt(6))
def fetch_pageviews(project: str, date: str, hour: str) -> list[dict]:
    """
    Fetch the top-1000 articles by pageview count for a given hour.
    Returns an empty list if no data is available yet.
    """
    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
        f"{project}/all-access/{date}/{hour}"
    )
    headers = {"User-Agent": "kairoskop-pipeline/1.0 (data-engineering-research)"}
    response = requests.get(url, headers=headers, timeout=15)

    if response.status_code == 404:
        return []  # Data not yet available for this hour
    response.raise_for_status()

    items = response.json().get("items", [])
    return items[0].get("articles", []) if items else []


def run_pageviews_producer(producer: Producer) -> None:
    """
    Poll the Wikipedia Pageviews API every hour and publish top articles.

    Polls the previous completed hour to ensure data availability.
    """
    log.info("pageviews_producer_started", topic=TOPIC_PAGEVIEWS)

    while True:
        now       = datetime.now(timezone.utc)
        prev_hour = now.replace(minute=0, second=0, microsecond=0)
        date_str  = prev_hour.strftime("%Y/%m/%d")
        hour_str  = prev_hour.strftime("%H")
        ts        = prev_hour.isoformat()

        try:
            articles = fetch_pageviews(PAGEVIEWS_PROJECT, date_str, hour_str)
            batch_published = 0

            for article in articles[:200]:      # Top 200 articles per hour
                title = article.get("article", "").replace("_", " ")
                views = article.get("views", 0)

                event = AttentionEvent(
                    event_id        = make_event_id("wiki_pageviews", title, ts),
                    source          = AttentionSource.WIKI_PAGEVIEWS,
                    event_timestamp = ts,
                    topic           = title,
                    topic_category  = classify_topic(title),
                    language        = "en",
                    country_code    = None,
                    signal_strength = min(views / 1_000_000, 1.0),
                    medium_type     = MediumType.COLLECTIVE_MEMORY,
                    raw_payload     = article,
                )

                producer.produce(
                    TOPIC_PAGEVIEWS,
                    key=title.encode(),
                    value=event.to_json().encode(),
                    callback=delivery_callback,
                )
                batch_published += 1

            producer.flush()
            log.info("pageviews_published", hour=f"{date_str}/{hour_str}", count=batch_published)

        except Exception as exc:
            log.error("pageviews_fetch_failed", error=str(exc))

        # Sleep until the next hour boundary (+90 seconds buffer for API lag)
        sleep_seconds = 3600 - (time.time() % 3600) + 90
        log.info("pageviews_sleeping", seconds=int(sleep_seconds))
        time.sleep(sleep_seconds)


# ─────────────────────────────────────────────────────────────────────
# Entry point — runs both producers in parallel threads
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    producer = build_producer()

    # Graceful shutdown on SIGTERM / SIGINT
    shutdown = threading.Event()

    def _handle_signal(sig, frame):
        log.info("shutdown_requested", signal=sig)
        producer.flush(timeout=10)
        shutdown.set()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    changes_thread = threading.Thread(
        target=run_changes_producer,
        args=(producer,),
        name="wiki-changes",
        daemon=True,
    )
    pageviews_thread = threading.Thread(
        target=run_pageviews_producer,
        args=(producer,),
        name="wiki-pageviews",
        daemon=True,
    )

    changes_thread.start()
    pageviews_thread.start()

    log.info("wikipedia_producer_running",
             topics=[TOPIC_CHANGES, TOPIC_PAGEVIEWS],
             broker=KAFKA_BOOTSTRAP)

    # Keep main thread alive until shutdown signal
    while not shutdown.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
