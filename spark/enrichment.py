"""
kairoskop.spark.enrichment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
UDFs and pure-Python functions for philosophical metric computation.

Each function here is the data-engineering implementation of one of the
theoretical frameworks that ground KAIROSKOP:

  - classify_consciousness_level  → Ken Wilber (Integral Theory)
  - detect_medium_dominance       → Marshall McLuhan
  - compute_signal_strength       → Normalisation + Bateson weighting

All UDFs are registered as PySpark user-defined functions and can be
applied column-wise inside Structured Streaming micro-batches.
"""

from __future__ import annotations

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType


# ─────────────────────────────────────────────────────────────────────
# Wilber — Consciousness Level Classification
# ─────────────────────────────────────────────────────────────────────

# Mapping from topic domain to Wilber's AQAL developmental levels.
# These are approximate heuristics, not authoritative classifications.
#
# Egocentric  — topics centred on tribal, national, or group identity
# Ethnocentric — topics with cross-group conflict or comparison
# Worldcentric — topics with global or universal scope
# Integral     — topics at the intersection of multiple disciplines
_TOPIC_TO_LEVEL: dict[str, str] = {
    "conflict":      "ethnocentric",
    "culture":       "ethnocentric",
    "economics":     "worldcentric",
    "health":        "worldcentric",
    "science":       "integral",
    "environment":   "worldcentric",
    "uncategorised": "egocentric",
}

# arXiv boosts everything one level toward integral
_SOURCE_BOOST: dict[str, int] = {
    "arxiv":          1,
    "wiki_pageviews": 0,
    "wiki_changes":   0,
    "gdelt":         -1,    # media often operates at ethnocentric level
}

_LEVELS     = ["egocentric", "ethnocentric", "worldcentric", "integral"]
_LEVEL_IDX  = {v: i for i, v in enumerate(_LEVELS)}


def _classify_consciousness_level(topic_category: str, source: str) -> str:
    """
    Assign a Wilber consciousness level to an attention event.

    The base level comes from the topic domain.  The source layer
    applies a directional boost (+1 for arXiv, -1 for GDELT).
    """
    if not topic_category:
        return "egocentric"

    base   = _TOPIC_TO_LEVEL.get(topic_category, "egocentric")
    boost  = _SOURCE_BOOST.get(source or "", 0)
    idx    = max(0, min(len(_LEVELS) - 1, _LEVEL_IDX[base] + boost))
    return _LEVELS[idx]


classify_consciousness_level_udf = udf(_classify_consciousness_level, StringType())


# ─────────────────────────────────────────────────────────────────────
# McLuhan — Medium Dominance Detection
# ─────────────────────────────────────────────────────────────────────

def _detect_medium_dominance(medium_type: str, signal_strength: float) -> str:
    """
    Classify which McLuhan medium layer is dominant for this event.

    A high-signal institutional media event (GDELT) overrides a
    weak formal knowledge signal.  This creates the 'medium_dominance'
    dimension used in the dashboard's categorical tile.
    """
    if not medium_type:
        return "unknown"

    strength = signal_strength or 0.0

    if medium_type == "institutional_media" and strength > 0.6:
        return "media_led"
    elif medium_type == "formal_knowledge" and strength > 0.4:
        return "knowledge_led"
    elif medium_type == "collective_memory":
        return "memory_led"
    else:
        return "ambient"


detect_medium_dominance_udf = udf(_detect_medium_dominance, StringType())


# ─────────────────────────────────────────────────────────────────────
# Bateson — Cross-Source Signal Strength Normalisation
# ─────────────────────────────────────────────────────────────────────

# Different sources produce signals on incompatible natural scales.
# These weights bring them onto a comparable 0–1 range, honouring
# Bateson's principle that "the unit of survival is organism + environment":
# no single source is privileged — the pattern across sources matters.
_SOURCE_WEIGHT: dict[str, float] = {
    "wiki_pageviews": 0.85,   # Pageviews can spike for trivial reasons
    "wiki_changes":   1.00,   # Active editing is stronger signal
    "gdelt":          0.75,   # Media can manufacture salience
    "arxiv":          1.10,   # Formal knowledge has high epistemic weight
}


def _compute_signal_strength(
    raw_strength: float,
    source: str,
    topic_category: str,
) -> float:
    """
    Apply source-specific weights and category boosts to normalise the
    raw signal into a comparable cross-source value (0.0 – 1.0).
    """
    strength = raw_strength or 0.0
    weight   = _SOURCE_WEIGHT.get(source or "", 1.0)

    # Environment and health topics get a salience boost — they are
    # systematically underweighted in media relative to their actual impact
    if topic_category in ("environment", "health"):
        weight *= 1.15

    return min(strength * weight, 1.0)


compute_signal_strength_udf = udf(_compute_signal_strength, FloatType())
