"""Unit tests for KAIROSKOP public attention snapshot exporter."""
from __future__ import annotations

from pathlib import Path
import json
import pytest

from src.kairoskop.attention_snapshot import (
    DEFAULT_TRACKED_TOPICS,
    build_attention_snapshot,
    classify_attention_regime,
    compute_attention_velocity,
    compute_attention_zscore,
    export_attention_snapshot,
)


def test_compute_attention_velocity():
    # Empty or single point
    assert compute_attention_velocity([]) == (0.0, 0.0)
    assert compute_attention_velocity([100.0]) == (0.0, 0.0)

    # Steady increase
    v, acc = compute_attention_velocity([100.0, 150.0])
    assert v == 0.5
    assert acc == 0.0

    # Accelerating surge
    v, acc = compute_attention_velocity([100.0, 120.0, 180.0])
    assert v == 0.5  # (180 - 120) / 120 = 0.5
    assert acc > 0.0  # 0.5 - 0.2 = 0.3


def test_compute_attention_zscore():
    assert compute_attention_zscore(100.0, 100.0, 10.0) == 0.0
    assert compute_attention_zscore(130.0, 100.0, 10.0) == 3.0
    assert compute_attention_zscore(70.0, 100.0, 10.0) == -3.0

    # Zero std dev edge case
    z_zero = compute_attention_zscore(100.0, 100.0, 0.0)
    assert z_zero == 0.0


def test_classify_attention_regime():
    assert classify_attention_regime(3.0, 0.35) == "EXPONENTIAL_ATTENTION_SURGE"
    assert classify_attention_regime(1.8, 0.05) == "HIGH_VELOCITY_ATTENTION"
    assert classify_attention_regime(-2.0, -0.1) == "ATTENTION_COLLAPSE"
    assert classify_attention_regime(0.2, 0.01) == "NEUTRAL_CONSENSUS"


def test_build_attention_snapshot_schema():
    snapshot = build_attention_snapshot()
    assert snapshot["version"] == "1.0.0"
    assert "generated_at" in snapshot
    assert "metrics" in snapshot
    assert "BTC" in snapshot["metrics"]
    btc = snapshot["metrics"]["BTC"]
    assert btc["attention_zscore"] > 2.0
    assert "regime" in btc
    assert "velocity" in btc


def test_export_attention_snapshot(tmp_path: Path):
    target_path = tmp_path / "data" / "signals" / "attention_metrics.json"
    exported = export_attention_snapshot(target_path)
    assert exported.exists()

    payload = json.loads(target_path.read_text(encoding="utf-8"))
    assert payload["sensor"] == "KAIROSKOP Collective Attention Observatory"
    assert payload["topic_count"] == len(DEFAULT_TRACKED_TOPICS)
