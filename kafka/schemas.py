"""
kairoskop.kafka.schemas
~~~~~~~~~~~~~~~~~~~~~~~
Canonical message schemas for the four Kafka topics.

Each dataclass represents the normalised envelope that producers write
to Kafka and Spark reads from it.  Using dataclasses (rather than bare
dicts) makes the contract between producers and consumers explicit and
catches structural regressions at import time.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────────────────────
# Shared enumerations
# ─────────────────────────────────────────────────────────────────────

class AttentionSource(str, Enum):
    WIKI_PAGEVIEWS = "wiki_pageviews"
    WIKI_CHANGES   = "wiki_changes"
    GDELT          = "gdelt"
    ARXIV          = "arxiv"


class MediumType(str, Enum):
    """McLuhan-inspired classification of the attention channel."""
    FORMAL_KNOWLEDGE    = "formal_knowledge"     # arXiv
    COLLECTIVE_MEMORY   = "collective_memory"    # Wikipedia
    INSTITUTIONAL_MEDIA = "institutional_media"  # GDELT


class TopicCategory(str, Enum):
    SCIENCE_TECHNOLOGY = "science"
    CONFLICT_POLITICS  = "conflict"
    HEALTH_SOCIETY     = "health"
    ENVIRONMENT        = "environment"
    CULTURE_IDENTITY   = "culture"
    ECONOMICS_FINANCE  = "economics"
    UNCATEGORISED      = "uncategorised"


# ─────────────────────────────────────────────────────────────────────
# Base envelope
# ─────────────────────────────────────────────────────────────────────

@dataclass
class AttentionEvent:
    """
    Normalised envelope for all attention events published to Kafka.

    Fields are kept minimal and source-agnostic so that Spark can apply
    a single schema across all four topics without branching logic.
    Source-specific data lives in `raw_payload`.
    """
    event_id:       str                      # SHA-256(source + raw_id + ts)
    source:         AttentionSource
    event_timestamp: str                     # ISO-8601, always UTC
    topic:          str                      # Article title / actor / paper title
    topic_category: TopicCategory
    language:       str                      # ISO 639-1, e.g. "en"
    country_code:   str | None               # ISO 3166-1 alpha-2, nullable
    signal_strength: float                   # Normalised 0–1
    medium_type:    MediumType
    raw_payload:    dict[str, Any] = field(default_factory=dict)
    ingested_at:    str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_json(self) -> str:
        """Serialise to a UTF-8 JSON string suitable for Kafka value bytes."""
        d = asdict(self)
        # Convert enum values to their string representation
        d["source"]         = self.source.value
        d["topic_category"] = self.topic_category.value
        d["medium_type"]    = self.medium_type.value
        return json.dumps(d, ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "AttentionEvent":
        """Deserialise from a Kafka message value."""
        d = json.loads(raw)
        d["source"]         = AttentionSource(d["source"])
        d["topic_category"] = TopicCategory(d["topic_category"])
        d["medium_type"]    = MediumType(d["medium_type"])
        return cls(**d)
