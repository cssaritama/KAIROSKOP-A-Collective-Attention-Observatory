# Current State — KAIROSKOP Collective Attention Observatory

Status: **In development (2026-11-19). CI passes; the streaming source and the dashboard are not connected yet.**  
Maintainer: Carlos Saritama (`cssaritama`)  
Governing Rule: Pure Public Attention Sensor (Strictly zero proprietary trading logic)

---

## Active Operational Capabilities

1. **Public Attention Snapshot Exporter (`src/kairoskop/attention_snapshot.py`):**
   - Implemented Z-score normalization of collective attention volume against historical baselines.
   - Computes attention velocity ($\Delta \text{Attention}$) and acceleration ($\Delta \text{Velocity}$).
   - Classifies attention regimes (`EXPONENTIAL_ATTENTION_SURGE`, `HIGH_VELOCITY_ATTENTION`, `ATTENTION_COLLAPSE`, `NEUTRAL_CONSENSUS`).
   - Exports zero-dependency portable JSON contract (`data/signals/attention_metrics.json`) for downstream research and quantitative models.
2. **Deterministic Verification:**
   - Unit test suite (`tests/test_attention_snapshot.py`) passing 100% (5/5 tests in pytest).
   - Zero paid API keys, zero external runtime dependencies.
3. **CI/CD Reliability & GitHub Actions Parity:**
   - GitHub Actions workflow (`.github/workflows/ci.yml`) hardened with `PYTHONPATH=.:src` environment parity.
   - Quality checks configured with strict active sensor linting (`ruff check src/ tests/`) and resilient import fallbacks.
   - Unit test suite passing 100% across local and remote CI runners (5/5 tests).
