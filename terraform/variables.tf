variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "production"
}

variable "gcs_bucket_prefix" {
  description = "Prefix for the GCS data lake bucket (project ID is appended)"
  type        = string
  default     = "kairoskop-datalake"
}

variable "bq_dataset" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "kairoskop"
}

variable "bq_location" {
  description = "BigQuery dataset location (multi-region or single region)"
  type        = string
  default     = "US"
}

variable "data_retention_days" {
  description = "Number of days to retain data in GCS and BigQuery partitions"
  type        = number
  default     = 1825  # 5 years
}
