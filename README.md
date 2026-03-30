# KAIROSKOP
### A Collective Attention Observatory

> *"Kairos" (καιρός) — the ancient Greek concept of the right or critical moment, as opposed to Chronos, the sequential passing of time. "Skopos" (σκοπός) — the observer, the one who watches from the heights to see the whole pattern.*
>
> *KAIROSKOP is the instrument that watches civilization's collective attention — not to measure how time passes, but to detect the moments when something shifts.*

---

## What is KAIROSKOP?

Every second, thousands of humans are doing things that leave a data trail:

- Editing a Wikipedia article about a war that just started
- Searching for information about a disease they heard about on the news
- A scientist publishing a paper on a topic that will matter in ten years
- A journalist writing about an event that will define this decade

Each of these actions is a **signal**. Individually, each signal is noise. But when you collect thousands of them simultaneously across independent sources — Wikipedia, global news media, academic publishing — a pattern emerges.

That pattern is the closest thing we have to a **measurable collective mind**.

KAIROSKOP is a real-time data engineering platform that ingests these signals continuously, processes them with Apache Kafka and Spark Streaming, stores them in a cloud data warehouse, and transforms them into a set of metrics that answer one central question:

> **What do collective attention patterns reveal about the psychological state of a civilization — and can we detect the precise moments when that state changes?**

This is not a philosophical exercise. It is a data engineering problem. The pipeline is real. The data is live. The metrics are computable. The philosophy gives them meaning.

---

## The Philosophical Framework

KAIROSKOP is not just a data pipeline. Each metric is grounded in a specific theoretical tradition. This is what makes the project intellectually distinctive — and what makes the metrics interpretable rather than arbitrary.

### Carl Jung — Synchronicity & The Shadow

Jung described **synchronicity** as *"the simultaneous occurrence of events that appear significantly related but have no discernible causal connection."*

In KAIROSKOP's terms: when Wikipedia edit velocity, GDELT media events, and arXiv papers all spike on the same topic domain within 24 hours — with no single coordinating actor — that IS synchronicity, measurable in data for the first time in history.

The **Shadow Index** operationalises another Jungian concept: the collective shadow is what a society cannot consciously acknowledge but cannot stop being drawn to. The Shadow Index measures the gap between what people search privately (Wikipedia pageviews — no social pressure, no audience) and what media amplifies publicly (GDELT). A high Shadow Index means society is secretly obsessed with something it publicly refuses to discuss.

### Sigmund Freud & Jacques Lacan — The Return of the Repressed

Freud demonstrated that *what is repressed always returns* — not as the original content, but as a symptom. Lacan refined this: the unconscious is structured like a language, speaking through gaps and absences rather than through explicit content.

In data terms: the topics that people privately seek but that institutional media refuses to amplify follow patterns. They return cyclically. The **Repression Index** tracks those cycles — asking not what media covers, but what it systematically avoids while people silently seek it.

### Ken Wilber — Integral Theory & Levels of Consciousness

Wilber mapped the evolution of both individual and collective consciousness through developmental levels, each transcending and including the previous:

- **Egocentric** — attention centred on tribe, nation, or in-group survival
- **Ethnocentric** — attention defined by us-vs-them distinctions and conflict
- **Worldcentric** — attention that includes all humans and global systems
- **Integral** — attention that holds multiple perspectives simultaneously without collapsing them

KAIROSKOP classifies each attention event by its developmental altitude and computes a daily **Consciousness Level** score — essentially asking: *is civilization thinking at a larger or smaller scale today than last week?*

### Marshall McLuhan — The Medium is the Message

McLuhan's core insight: *"the medium through which a message travels shapes the message itself, regardless of content."* A war reported by global media (GDELT) is a fundamentally different phenomenon than the same war being rewritten in Wikipedia or studied in arXiv papers — even if the subject is identical.

KAIROSKOP tracks **Medium Dominance** — which attention layer is leading the narrative on each topic. When science leads, the topic is being understood. When media leads, it is being amplified. When collective memory (Wikipedia) leads, it is being contextualised and historicised. The dominance pattern reveals not just what civilization is paying attention to, but how it is processing it.

### Gregory Bateson — The Pattern That Connects

Bateson argued that *"the unit of survival is organism plus environment"* — intelligence is not located in any single node but in the **relationships between nodes**. His concept of the "pattern that connects" describes how meaning emerges from the structure of relationships rather than from individual elements.

This is the theoretical justification for KAIROSKOP's multi-source architecture: no single data source is sufficient. Wikipedia alone shows curiosity. GDELT alone shows media agenda. arXiv alone shows academic priorities. The insight lives in the **cross-source pattern** — in whether these independent systems are telling the same story or fundamentally different ones. The **Pattern Coherence Score** quantifies that relationship.

### Pierre Teilhard de Chardin — The Noosphere

Teilhard de Chardin proposed that the Earth is developing a new geological layer — the **noosphere** — a sphere of collective human thought that envelops the planet the way the biosphere envelops it biologically. He called its convergence point the **Omega Point**: the moment of maximum collective integration.

The internet, Wikipedia, academic publishing, and global media are the substrate of that noosphere made measurable. KAIROSKOP tracks its **density** (total volume of active collective thought per day across all sources) and its **trajectory** (is it converging toward coherence or fragmenting toward noise?). The Omega Trajectory metric asks: is the noosphere integrating or disintegrating this week?

---

## The Four Attention Layers

KAIROSKOP ingests four independent streams, each measuring a fundamentally different type of collective attention. There is zero redundancy between them — each source answers a question that none of the others can.

```
┌──────────────────────────────────────────────────────────────────┐
│              The Four Layers of Collective Attention             │
│                                                                  │
│  Wikipedia Pageviews    What humanity SEEKS                      │
│  ─────────────────────  Reactive attention — what people look    │
│  Source: Wikimedia API  up after something happens. No social    │
│  Frequency: Hourly      pressure. No performance. Pure private   │
│                         curiosity. The most honest signal.       │
│                                                                  │
│  Wikipedia Changes      What humanity REWRITES                   │
│  ─────────────────────  Active collective memory — what humans   │
│  Source: SSE Stream     are negotiating and debating right now.  │
│  Frequency: Real-time   Every edit is a vote on what reality     │
│                         means. The noosphere in motion.          │
│                                                                  │
│  GDELT 2.0              What humanity BROADCASTS                 │
│  ─────────────────────  Institutional attention — what the       │
│  Source: GDELT Project  global media apparatus (100,000+         │
│  Frequency: 15 minutes  sources, 65 languages) decides exists    │
│                         and therefore enters public consciousness.│
│                                                                  │
│  arXiv Papers           What humanity DISCOVERS                  │
│  ─────────────────────  Intellectual attention — where the       │
│  Source: arXiv RSS      formal knowledge frontier is moving.     │
│  Frequency: Daily       12,000+ papers/week. The slow signal     │
│                         that predicts what will matter next.     │
└──────────────────────────────────────────────────────────────────┘
```

**Why not X (Twitter), TikTok, or Instagram?**
These platforms would be philosophically richer for measuring emotional and viral attention. They were excluded for one reason: their APIs are either paywalled (X: $100+/month since 2023), closed to research (Instagram, Facebook since Cambridge Analytica), or non-existent (TikTok). The four sources used are entirely public, require no authentication, and can be reproduced by any evaluator without accounts or payment. KAIROSKOP is architecturally ready to add new streams as additional Kafka topics the moment access becomes available.

---

## The Metrics

| Metric | Framework | What it measures |
|---|---|---|
| `synchronicity_score` | Jung | Cross-source convergence on the same topic simultaneously (0–1) |
| `shadow_index` | Freud / Lacan | Gap between private search and public media coverage |
| `consciousness_level` | Ken Wilber | Developmental altitude of collective attention today |
| `medium_dominance` | McLuhan | Which layer is leading the narrative on each domain |
| `pattern_coherence` | Bateson | Agreement between all four independent signals |
| `noospheric_density` | Teilhard | Total volume of active collective thought per day |

A **Kairos Event** is flagged automatically when `synchronicity_score` ≥ 0.8 — meaning all four independent attention layers converge simultaneously on the same domain with no coordinating actor. These are the moments KAIROSKOP was built to detect.

---

## Streaming Pipeline — How It Works

KAIROSKOP implements a **full producer/consumer streaming architecture** using Apache Kafka and Apache Spark Structured Streaming — satisfying the stream processing criterion with active producers, Kafka topics as the message bus, and Spark as the continuous consumer.

```
PRODUCERS (3 active processes)          KAFKA TOPICS (4)
────────────────────────────            ────────────────
producer_wikipedia.py  ──────────────▶  wiki_pageviews
                       ──────────────▶  wiki_changes
producer_gdelt.py      ──────────────▶  gdelt_events
producer_arxiv.py      ──────────────▶  arxiv_papers
                                               │
                                               ▼
                                    CONSUMER: Spark Structured
                                    Streaming (streaming_job.py)
                                    ├── Reads all 4 topics
                                    ├── Parses + enriches events
                                    ├── Computes metric UDFs
                                    ├── Writes → GCS (Parquet)
                                    └── Writes → BigQuery
```

Each producer runs as an independent process. Wikipedia uses a native **Server-Sent Events (SSE)** stream — a continuous HTTP connection that pushes events in real time with no polling. GDELT is polled every 15 minutes and published as micro-batches. arXiv is published daily. All events share a canonical JSON schema (defined in `kafka/schemas.py`) so Spark reads them with a single unified job.

Spark processes the stream with a **5-minute trigger interval**, applying enrichment UDFs (philosophical metrics) before writing to both the GCS data lake and BigQuery simultaneously.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     KAIROSKOP — Architecture                         │
│                                                                       │
│  SOURCES              STREAM LAYER            STORAGE                │
│  ───────              ────────────            ───────                │
│  Wikipedia API   ───▶ Kafka                                          │
│  Wikipedia SSE   ───▶ 4 Topics          ───▶ GCS Data Lake           │
│  GDELT 2.0       ───▶                         (Parquet, partitioned  │
│  arXiv RSS       ───▶                          by event_date)        │
│                        │                                             │
│                   Spark Structured       ───▶ BigQuery               │
│                   Streaming                   (partitioned by date,  │
│                   (enrichment +               clustered by source    │
│                    metric UDFs)               + topic_category)      │
│                                                    │                 │
│  TRANSFORMATION       ORCHESTRATION          PRESENTATION            │
│  ──────────────       ─────────────          ────────────            │
│  Bruin SQL assets ◀── Bruin Pipeline    ───▶ Looker Studio           │
│  staging → marts      daily at 06:00 UTC      2-tile dashboard       │
│  → metrics            quality checks +        (categorical +         │
│                        lineage                 temporal)             │
│                                                                       │
│  INFRASTRUCTURE: Terraform (GCP)    CONTAINERS: Docker Compose       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Cloud | Google Cloud Platform | Primary cloud provider |
| IaC | Terraform ≥ 1.7 | Provisions GCS, BigQuery, IAM, service accounts |
| Streaming broker | Apache Kafka 3.6 | 4 topics, one per attention source |
| Stream processing | Apache Spark 3.5 Structured Streaming | Real-time enrichment + metric computation |
| Data Lake | Google Cloud Storage | Parquet files, partitioned by `event_date` |
| Data Warehouse | BigQuery | Partitioned + clustered analytical tables |
| Transformations | Bruin CLI (SQL + Python assets) | Staging, marts, philosophical metrics |
| Orchestration | Bruin Pipeline | Scheduling, quality checks, column-level lineage |
| Containers | Docker Compose | Local Kafka + Spark development environment |
| Dashboard | Looker Studio | Two-tile analytical dashboard |
| CI/CD | GitHub Actions | Lint + type check + Terraform validate + Bruin validate |

---

## Dashboard

**[View Live Dashboard → Looker Studio](https://lookerstudio.google.com)** *(URL updated after deployment — see Step 9 in How to Run)*

> **Screenshot:** The dashboard screenshot will be added after the first successful pipeline run. The two tiles below are fully specified and reproducible by following Step 9.

### Tile 1 — Collective Attention by Domain (Categorical Distribution)

A stacked bar chart showing the distribution of global attention across six knowledge domains for the trailing 30 days:

- Science & Technology
- Conflict & Politics
- Health & Society
- Environment & Climate
- Culture & Identity
- Economics & Finance

Each bar is broken down by **Medium Dominance** — revealing not just what civilization is focused on, but which layer (science, media, or collective memory) is leading that focus. This is McLuhan's insight made directly visible: the channel shapes the message.

### Tile 2 — Synchronicity Score Over Time (Temporal Distribution)

A time series chart for the trailing 90 days showing:

- **Primary line:** Weekly `synchronicity_score` (Jung) — cross-source convergence index (0–1)
- **Secondary line:** `shadow_index` (Freud) — gap between private search and public media
- **Annotations:** Kairos Events (synchronicity ≥ 0.8) marked directly on the chart

This tile answers the central question: *when did the collective mind synchronize — and what was it repressing at the same time?*

---

## Data Warehouse Design

All BigQuery tables are partitioned by `event_date` (DATE) and clustered by `source` and `topic_category`.

**Why date partitioning:** every analytical query in this project filters by date range. Partitioning eliminates full table scans — for a query like "show last 7 days of Kairos Events," BigQuery reads only 7 partitions instead of the entire table. This reduces bytes processed by 80–95% and costs proportionally.

**Why clustering on `source` + `topic_category`:** the two most common secondary filter dimensions are the attention source (to compare signals across philosophical layers) and topic category (to focus on specific domains). Clustering co-locates related rows within each partition, further reducing bytes read per query. These two choices together make interactive dashboard queries fast and cheap regardless of table size.

---

## Project Structure

```
kairoskop/
├── README.md
├── docker-compose.yml          # Kafka + Zookeeper + Spark (local dev)
├── requirements.txt
├── .env.example
├── .gitignore
│
├── terraform/
│   ├── main.tf                 # GCS bucket + BigQuery + IAM + SA key
│   ├── variables.tf
│   └── outputs.tf
│
├── kafka/
│   ├── schemas.py              # Canonical AttentionEvent envelope
│   ├── producer_wikipedia.py   # Wikipedia SSE + pageviews → Kafka
│   ├── producer_gdelt.py       # GDELT 15-min polling → Kafka
│   └── producer_arxiv.py       # arXiv RSS → Kafka
│
├── spark/
│   ├── streaming_job.py        # Unified Spark Structured Streaming job
│   ├── enrichment.py           # Philosophical metric UDFs
│   └── schemas.py              # PySpark StructType definitions
│
├── bruin/
│   ├── .bruin.yml              # Bruin project configuration
│   ├── pipeline.yml            # Orchestration + quality checks
│   └── assets/
│       ├── staging/            # stg_wikipedia · stg_gdelt · stg_arxiv
│       ├── marts/              # fact_attention · agg_daily · mart_dashboard
│       └── metrics/            # synchronicity · shadow_index · consciousness_level
│
├── scripts/
│   ├── backfill_gdelt.py       # Historical GDELT load (bypass Kafka)
│   └── backfill_arxiv.py       # Historical arXiv load (bypass Kafka)
│
└── .github/
    └── workflows/ci.yml        # Ruff + mypy + Terraform + Bruin validate
```

---

## How to Run — Complete Step by Step

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker + Docker Compose | ≥ 24.x | [docs.docker.com](https://docs.docker.com) |
| Python | ≥ 3.11 | [python.org](https://python.org) |
| Terraform | ≥ 1.7 | [terraform.io](https://terraform.io) |
| gcloud CLI | latest | [cloud.google.com/sdk](https://cloud.google.com/sdk) |
| Bruin CLI | latest | [github.com/bruin-data/bruin/releases](https://github.com/bruin-data/bruin/releases) |

### Step 1 — Clone and configure
```bash
git clone https://github.com/YOUR_USERNAME/kairoskop.git
cd kairoskop
mkdir credentials
cp .env.example .env
# Edit .env: set GCP_PROJECT_ID to your actual GCP project ID
```

### Step 2 — Authenticate with GCP
```bash
gcloud auth application-default login
gcloud config set project YOUR_GCP_PROJECT_ID
```

### Step 3 — Provision GCP infrastructure
```bash
cd terraform
terraform init
terraform apply -var="project_id=YOUR_GCP_PROJECT_ID"
# Type "yes" when prompted — takes ~2 minutes
cd ..
```

This creates: GCS bucket, BigQuery dataset + table (partitioned + clustered), service account, and writes `credentials/sa-key.json` automatically.

### Step 4 — Start local environment
```bash
docker-compose up -d
pip install -r requirements.txt
```

### Step 5 — Start data producers (3 terminals)
```bash
python kafka/producer_wikipedia.py  # Terminal 1
python kafka/producer_gdelt.py      # Terminal 2
python kafka/producer_arxiv.py      # Terminal 3
```

### Step 6 — Submit Spark Streaming job (Terminal 4)
```bash
docker exec kairoskop-spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  /opt/spark/jobs/streaming_job.py
```

### Step 7 — Load historical data
```bash
python scripts/backfill_gdelt.py --days 30
python scripts/backfill_arxiv.py --days 30
```

### Step 8 — Run Bruin transformations
```bash
cd bruin && bruin validate . && bruin run .
```

### Step 9 — Build Looker Studio dashboard
1. Open [lookerstudio.google.com](https://lookerstudio.google.com)
2. New Report → BigQuery → `mart_dashboard`
3. Tile 1: Bar chart — `topic_category` × `event_count`, breakdown `medium_dominance`, filter `tile_type = categorical`
4. Tile 2: Time series — `event_date` × `total_signal` + `shadow_index`, filter `tile_type = temporal`
5. Share → Anyone with link → copy URL → paste into README

---

## About Bruin

Bruin is an open-source (MIT license) data engineering CLI that unifies ingestion, transformation, orchestration, and quality checks in a single tool — without vendor lock-in. Unlike dbt (transformation only) or Airflow (orchestration only), Bruin handles the full pipeline lifecycle with column-level lineage out of the box. In KAIROSKOP, Bruin replaces a dbt + Kestra combination, reducing configuration overhead while adding native quality assertions on every asset.

---

## About the Name

**Kairos** (καιρός) is one of two Greek words for time. Chronos measures the time that passes — seconds, minutes, years. Kairos describes the time that *matters* — the right moment, the critical juncture, the point at which action or attention becomes decisive. Ancient Greek archers used *kairos* to describe the precise moment to release an arrow.

**Skopos** (σκοπός) is the observer — the scout on the hill who watches the entire battlefield and sees patterns that those in the middle of it cannot.

KAIROSKOP is the instrument that observes civilization's collective attention and detects its Kairos moments — the precise instants when the pattern shifts and history moves.

---

*Built for the DataTalksClub Data Engineering Zoomcamp 2026 · Bruin Sponsored Competition*
