"""KAIROSKOP Collective Attention Observatory — Public Attention Snapshot Exporter.

Mathematical primitives and data exporter to normalize collective attention
velocity Z-scores across tracked topics, narrative assets, and collective
inquiries. Exports a standardized, zero-dependency JSON contract
(`attention_metrics.json`) for downstream research and quantitative models.

Standard:
- Zero Paid API Keys / Zero External Dependencies (Python 3.13 standard library)
- Z-score normalized attention velocity and acceleration
- Pure Public Attention Sensor (Strictly zero proprietary trading logic)
"""
from __future__ import annotations

from pathlib import Path
import datetime
import json
import math
from typing import Any


DEFAULT_TRACKED_TOPICS = [
    {
        "topic": "BTC",
        "current_volume": 14250.0,
        "baseline_mean": 9500.0,
        "baseline_std": 1500.0,
        "history": [9200.0, 9400.0, 9800.0, 11200.0, 14250.0],
        "lead_lag_hours": 18.5
    },
    {
        "topic": "ETH",
        "current_volume": 6800.0,
        "baseline_mean": 7000.0,
        "baseline_std": 1200.0,
        "history": [7100.0, 6900.0, 7050.0, 6950.0, 6800.0],
        "lead_lag_hours": 4.0
    },
    {
        "topic": "SOL",
        "current_volume": 8900.0,
        "baseline_mean": 5200.0,
        "baseline_std": 1100.0,
        "history": [4900.0, 5100.0, 5800.0, 7200.0, 8900.0],
        "lead_lag_hours": 24.0
    },
    {
        "topic": "NVDA",
        "current_volume": 12500.0,
        "baseline_mean": 8000.0,
        "baseline_std": 1600.0,
        "history": [7800.0, 8100.0, 8500.0, 10200.0, 12500.0],
        "lead_lag_hours": 12.0
    },
    {
        "topic": "SPY",
        "current_volume": 18000.0,
        "baseline_mean": 17500.0,
        "baseline_std": 2000.0,
        "history": [17200.0, 17400.0, 17300.0, 17600.0, 18000.0],
        "lead_lag_hours": 2.5
    },
    {
        "topic": "OPTIONS_GEX",
        "current_volume": 4200.0,
        "baseline_mean": 1800.0,
        "baseline_std": 600.0,
        "history": [1600.0, 1750.0, 2100.0, 3100.0, 4200.0],
        "lead_lag_hours": 36.0
    },
    {
        "topic": "AI_AGENTS",
        "current_volume": 21500.0,
        "baseline_mean": 11000.0,
        "baseline_std": 2500.0,
        "history": [10500.0, 11200.0, 13000.0, 16800.0, 21500.0],
        "lead_lag_hours": 48.0
    }
]


def compute_attention_velocity(history: list[float], eps: float = 1e-4) -> tuple[float, float]:
    """Calculate rate of change (velocity) and second derivative (acceleration)."""
    if not history or len(history) < 2:
        return 0.0, 0.0

    v1 = (history[-1] - history[-2]) / (abs(history[-2]) + eps)
    if len(history) < 3:
        return float(round(v1, 4)), 0.0

    v0 = (history[-2] - history[-3]) / (abs(history[-3]) + eps)
    acc = v1 - v0
    return float(round(v1, 4)), float(round(acc, 4))


def compute_attention_zscore(current: float, mean: float, std: float, eps: float = 1e-4) -> float:
    """Compute Gaussian Z-score of attention volume against historical baseline."""
    effective_std = std if std > eps else eps
    z = (current - mean) / effective_std
    if math.isnan(z) or math.isinf(z):
        return 0.0
    return float(round(z, 4))


def classify_attention_regime(zscore: float, velocity: float) -> str:
    """Classify collective attention regime based on Z-score and velocity."""
    if zscore >= 2.5 and velocity > 0.2:
        return "EXPONENTIAL_ATTENTION_SURGE"
    elif zscore >= 1.5:
        return "HIGH_VELOCITY_ATTENTION"
    elif zscore <= -1.5:
        return "ATTENTION_COLLAPSE"
    else:
        return "NEUTRAL_CONSENSUS"


def build_attention_snapshot(items: list[dict] | None = None) -> dict[str, Any]:
    """Build normalized attention snapshot schema across tracked topics."""
    source_items = items if items is not None else DEFAULT_TRACKED_TOPICS
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    metrics: dict[str, Any] = {}
    for entry in source_items:
        topic = entry.get("topic", "UNKNOWN")
        current = float(entry.get("current_volume", 0.0))
        mean = float(entry.get("baseline_mean", current))
        std = float(entry.get("baseline_std", 1.0))
        history = [float(x) for x in entry.get("history", [current])]
        lead_lag = float(entry.get("lead_lag_hours", 0.0))

        v, acc = compute_attention_velocity(history)
        z = compute_attention_zscore(current, mean, std)
        regime = classify_attention_regime(z, v)

        metrics[topic] = {
            "topic": topic,
            "current_volume": current,
            "baseline_mean": mean,
            "baseline_std": std,
            "attention_zscore": z,
            "velocity": v,
            "acceleration": acc,
            "regime": regime,
            "lead_lag_hours": lead_lag
        }

    return {
        "version": "1.0.0",
        "generated_at": timestamp,
        "sensor": "KAIROSKOP Collective Attention Observatory",
        "topic_count": len(metrics),
        "metrics": metrics
    }


def export_attention_snapshot(
    output_path: Path | str,
    items: list[dict] | None = None
) -> Path:
    """Export standardized attention JSON contract to disk."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_attention_snapshot(items)
    path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    return path
