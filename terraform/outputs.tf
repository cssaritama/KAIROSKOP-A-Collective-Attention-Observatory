output "gcs_bucket_name" {
  description = "Name of the GCS data lake bucket"
  value       = google_storage_bucket.datalake.name
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.kairoskop.dataset_id
}

output "pipeline_sa_email" {
  description = "Service account email for the pipeline identity"
  value       = google_service_account.pipeline_sa.email
}
