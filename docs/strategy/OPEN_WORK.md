# Open Work — KAIROSKOP Collective Attention Observatory

Status: **Active & Governed by Rule #10**  
Maintainer: Carlos Saritama (`cssaritama`)

---

## Completed Tasks

- **`KAIROSKOP-001` (DONE 2026-11-14):** Created public attention snapshot exporter (`src/kairoskop/attention_snapshot.py`) with velocity/acceleration derivation, Z-score normalization, regime classification, and portable JSON contract (`data/signals/attention_metrics.json`). Passing 5 unit tests.

---

## Active & Pending Backlog

- **`KAIROSKOP-002` (NEXT):** Connect real-time streaming ingestion pipelines (Kafka / Spark) to continuously refresh `attention_metrics.json`.
- **`KAIROSKOP-003`:** Implement multi-language sentiment clustering for European macro events.
