# ─────────────────────────────────────────
# OUTPUTS
# Useful values printed after terraform apply
# ─────────────────────────────────────────

output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.skypulse.name
}

output "bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.skypulse.url
}

output "service_account_email" {
  description = "Ingestion service account email"
  value       = google_service_account.skypulse_ingestion.email
}

output "pubsub_topic_raw" {
  description = "Main Pub/Sub topic name"
  value       = google_pubsub_topic.flight_prices_raw.name
}

output "pubsub_topic_dlq" {
  description = "Dead letter Pub/Sub topic name"
  value       = google_pubsub_topic.flight_prices_dlq.name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription name"
  value       = google_pubsub_subscription.flight_prices_sub.name
}

output "bigquery_silver_dataset" {
  description = "BigQuery Silver dataset ID"
  value       = google_bigquery_dataset.skypulse_silver.dataset_id
}

output "bigquery_gold_dataset" {
  description = "BigQuery Gold dataset ID"
  value       = google_bigquery_dataset.skypulse_gold.dataset_id
}

output "project_number" {
  description = "GCP project number"
  value       = data.google_project.project.number
}
