terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─────────────────────────────────────────────────────────────────────
# GCS — Data Lake
# Stores Parquet files written by Spark Streaming, partitioned by date.
# ─────────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "datalake" {
  name          = "${var.gcs_bucket_prefix}-${var.project_id}"
  location      = var.region
  force_destroy = false

  storage_class = "STANDARD"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = var.data_retention_days
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true

  labels = {
    project     = "kairoskop"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ─────────────────────────────────────────────────────────────────────
# BigQuery — Dataset
# ─────────────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "kairoskop" {
  dataset_id    = var.bq_dataset
  friendly_name = "KAIROSKOP — Collective Attention Observatory"
  description   = "Attention signal data from Wikipedia, GDELT, and arXiv — processed by Spark Streaming and transformed by Bruin."
  location      = var.bq_location

  delete_contents_on_destroy = false

  labels = {
    project    = "kairoskop"
    managed_by = "terraform"
  }
}

# ─────────────────────────────────────────────────────────────────────
# BigQuery — Raw attention events table
# Partitioned by event_date, clustered by source + topic_category.
#
# Partitioning rationale: all downstream queries filter by date range.
# Clustering rationale: secondary filters are almost always on source
# (to compare signals across layers) and topic_category (to focus on
# specific domains). This combination minimises bytes read per query.
# ─────────────────────────────────────────────────────────────────────
resource "google_bigquery_table" "attention_events_raw" {
  dataset_id          = google_bigquery_dataset.kairoskop.dataset_id
  table_id            = "attention_events_raw"
  deletion_protection = false
  description         = "Raw enriched attention events written by Spark Structured Streaming."

  time_partitioning {
    type          = "DAY"
    field         = "event_date"
    expiration_ms = var.data_retention_days * 86400000
  }

  clustering = ["source", "topic_category"]

  schema = jsonencode([
    { name = "event_id",        type = "STRING",    mode = "REQUIRED",  description = "SHA-256 hash of source + raw_id + event_timestamp" },
    { name = "source",          type = "STRING",    mode = "REQUIRED",  description = "Attention layer: wiki_pageviews | wiki_changes | gdelt | arxiv" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "REQUIRED",  description = "UTC timestamp of the original event" },
    { name = "event_date",      type = "DATE",      mode = "REQUIRED",  description = "Partition key — calendar date of event" },
    { name = "topic",           type = "STRING",    mode = "NULLABLE",  description = "Article title, GDELT actor, or arXiv title" },
    { name = "topic_category",  type = "STRING",    mode = "NULLABLE",  description = "Domain: science | conflict | health | environment | culture | economics" },
    { name = "language",        type = "STRING",    mode = "NULLABLE",  description = "ISO 639-1 language code" },
    { name = "country_code",    type = "STRING",    mode = "NULLABLE",  description = "ISO 3166-1 alpha-2 country code (where available)" },
    { name = "signal_strength", type = "FLOAT64",   mode = "NULLABLE",  description = "Normalised attention signal (0–1) for cross-source comparison" },
    { name = "medium_type",     type = "STRING",    mode = "NULLABLE",  description = "McLuhan layer: formal_knowledge | collective_memory | institutional_media" },
    { name = "raw_payload",     type = "JSON",      mode = "NULLABLE",  description = "Original source payload for reprocessing" },
    { name = "ingested_at",     type = "TIMESTAMP", mode = "REQUIRED",  description = "Wall-clock timestamp when the record was written to BigQuery" }
  ])

  labels = { managed_by = "terraform" }
}

# ─────────────────────────────────────────────────────────────────────
# Service Account — pipeline identity (Spark + Bruin)
# ─────────────────────────────────────────────────────────────────────
resource "google_service_account" "pipeline_sa" {
  account_id   = "kairoskop-pipeline"
  display_name = "KAIROSKOP Pipeline Service Account"
  description  = "Used by Spark Streaming and Bruin to read/write GCS and BigQuery."
}

resource "google_project_iam_member" "sa_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

resource "local_sensitive_file" "sa_key_json" {
  content         = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename        = "${path.module}/../credentials/sa-key.json"
  file_permission = "0600"
}
