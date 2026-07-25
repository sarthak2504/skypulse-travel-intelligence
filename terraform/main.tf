# ─────────────────────────────────────────
# GCS BUCKET
# ─────────────────────────────────────────

resource "google_storage_bucket" "skypulse" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  # Bronze files move to cheaper storage as they age
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# ─────────────────────────────────────────
# IAM SERVICE ACCOUNT
# ─────────────────────────────────────────

resource "google_service_account" "skypulse_ingestion" {
  account_id   = "skypulse-ingestion-sa"
  display_name = "SkyPulse Ingestion Service Account"
  project      = var.project_id
}

# ─────────────────────────────────────────
# IAM ROLE BINDINGS
# ─────────────────────────────────────────

locals {
  sa_email = "serviceAccount:${google_service_account.skypulse_ingestion.email}"

  roles = [
    "roles/pubsub.publisher",
    "roles/storage.objectAdmin",
    "roles/cloudfunctions.invoker",
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/dataproc.worker",
    "roles/bigquery.readSessionUser",
  ]
}

resource "google_project_iam_member" "skypulse_roles" {
  for_each = toset(local.roles)

  project = var.project_id
  role    = each.value
  member  = local.sa_email
}

# ─────────────────────────────────────────
# PUB/SUB TOPICS
# ─────────────────────────────────────────

resource "google_pubsub_topic" "flight_prices_raw" {
  name    = "flight-prices-raw"
  project = var.project_id
}

resource "google_pubsub_topic" "flight_prices_dlq" {
  name    = "flight-prices-dlq"
  project = var.project_id
}

# ─────────────────────────────────────────
# PUB/SUB SCHEMA
# ─────────────────────────────────────────

resource "google_pubsub_schema" "flight_price_schema" {
  name       = "flight-price-schema"
  type       = "AVRO"
  project    = var.project_id
  definition = file("${path.module}/../functions//flight_price.avsc")
}

# Attach schema to main topic
resource "google_pubsub_topic" "flight_prices_raw_with_schema" {
  name    = google_pubsub_topic.flight_prices_raw.name
  project = var.project_id

  schema_settings {
    schema   = google_pubsub_schema.flight_price_schema.id
    encoding = "JSON"
  }

  depends_on = [google_pubsub_schema.flight_price_schema]
}

# ─────────────────────────────────────────
# PUB/SUB SUBSCRIPTION
# ─────────────────────────────────────────

resource "google_pubsub_subscription" "flight_prices_sub" {
  name    = "flight-prices-sub"
  topic   = google_pubsub_topic.flight_prices_raw.name
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.flight_prices_dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "5s"
    maximum_backoff = "600s"
  }
}

# Grant Pub/Sub internal SA permissions for DLQ forwarding
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic   = google_pubsub_topic.flight_prices_dlq.name
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "sub_subscriber" {
  subscription = google_pubsub_subscription.flight_prices_sub.name
  project      = var.project_id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Data source to get project number
data "google_project" "project" {
  project_id = var.project_id
}

# ─────────────────────────────────────────
# BIGQUERY DATASETS
# ─────────────────────────────────────────

resource "google_bigquery_dataset" "skypulse_silver" {
  dataset_id  = "skypulse"
  location    = var.region
  description = "SkyPulse Silver layer — raw windowed aggregations"
  project     = var.project_id
}

resource "google_bigquery_dataset" "skypulse_gold" {
  dataset_id  = "skypulse_gold"
  location    = var.region
  description = "SkyPulse Gold layer — clean star schema"
  project     = var.project_id
}

# ─────────────────────────────────────────
# BIGQUERY SILVER TABLES
# ─────────────────────────────────────────

resource "google_bigquery_table" "price_5min_avg" {
  dataset_id          = google_bigquery_dataset.skypulse_silver.dataset_id
  table_id            = "price_5min_avg"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "flight_number",       type = "STRING" },
    { name = "route_id",            type = "STRING" },
    { name = "origin",              type = "STRING" },
    { name = "destination",         type = "STRING" },
    { name = "airline_code",        type = "STRING" },
    { name = "airline_name",        type = "STRING" },
    { name = "window_start",        type = "TIMESTAMP" },
    { name = "window_end",          type = "TIMESTAMP" },
    { name = "avg_price_usd",       type = "FLOAT" },
    { name = "min_price_usd",       type = "FLOAT" },
    { name = "max_price_usd",       type = "FLOAT" },
    { name = "message_count",       type = "INTEGER" },
    { name = "processing_timestamp",type = "TIMESTAMP" },
  ])
}

resource "google_bigquery_table" "price_1hr_trend" {
  dataset_id          = google_bigquery_dataset.skypulse_silver.dataset_id
  table_id            = "price_1hr_trend"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "flight_number",       type = "STRING" },
    { name = "route_id",            type = "STRING" },
    { name = "origin",              type = "STRING" },
    { name = "destination",         type = "STRING" },
    { name = "airline_code",        type = "STRING" },
    { name = "window_start",        type = "TIMESTAMP" },
    { name = "window_end",          type = "TIMESTAMP" },
    { name = "avg_price_usd",       type = "FLOAT" },
    { name = "message_count",       type = "INTEGER" },
    { name = "processing_timestamp",type = "TIMESTAMP" },
  ])
}

resource "google_bigquery_table" "late_arrivals" {
  dataset_id          = google_bigquery_dataset.skypulse_silver.dataset_id
  table_id            = "late_arrivals"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "route_id",             type = "STRING" },
    { name = "flight_number",        type = "STRING" },
    { name = "origin",               type = "STRING" },
    { name = "destination",          type = "STRING" },
    { name = "airline_code",         type = "STRING" },
    { name = "price_usd",            type = "FLOAT" },
    { name = "flight_date",          type = "STRING" },
    { name = "event_timestamp",      type = "TIMESTAMP" },
    { name = "ingestion_timestamp",  type = "TIMESTAMP" },
    { name = "lateness_seconds",     type = "INTEGER" },
    { name = "processing_timestamp", type = "TIMESTAMP" },
  ])
}

resource "google_bigquery_table" "pipeline_watermarks" {
  dataset_id          = google_bigquery_dataset.skypulse_silver.dataset_id
  table_id            = "pipeline_watermarks"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "pipeline_name",     type = "STRING" },
    { name = "last_processed_ts", type = "STRING" },
    { name = "updated_at",        type = "STRING" },
  ])
}

resource "google_bigquery_table" "pipeline_audit_log" {
  dataset_id          = google_bigquery_dataset.skypulse_silver.dataset_id
  table_id            = "pipeline_audit_log"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "run_date",         type = "STRING" },
    { name = "run_timestamp",    type = "STRING" },
    { name = "silver_rows",      type = "INTEGER" },
    { name = "gold_rows",        type = "INTEGER" },
    { name = "late_arrivals",    type = "INTEGER" },
    { name = "late_rate",        type = "FLOAT" },
    { name = "reconciled_count", type = "INTEGER" },
    { name = "status",           type = "STRING" },
  ])
}

# ─────────────────────────────────────────
# BIGQUERY GOLD TABLES
# ─────────────────────────────────────────

resource "google_bigquery_table" "dim_date" {
  dataset_id          = google_bigquery_dataset.skypulse_gold.dataset_id
  table_id            = "dim_date"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "date_key",      type = "INTEGER" },
    { name = "date",          type = "STRING" },
    { name = "day_of_week",   type = "STRING" },
    { name = "day_number",    type = "INTEGER" },
    { name = "is_weekend",    type = "BOOLEAN" },
    { name = "month",         type = "INTEGER" },
    { name = "quarter",       type = "INTEGER" },
    { name = "is_us_holiday", type = "BOOLEAN" },
    { name = "season",        type = "STRING" },
  ])
}

resource "google_bigquery_table" "dim_airlines" {
  dataset_id          = google_bigquery_dataset.skypulse_gold.dataset_id
  table_id            = "dim_airlines"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "airline_key",  type = "INTEGER" },
    { name = "airline_code", type = "STRING" },
    { name = "airline_name", type = "STRING" },
    { name = "alliance",     type = "STRING" },
    { name = "is_domestic",  type = "BOOLEAN" },
  ])
}

resource "google_bigquery_table" "dim_flights" {
  dataset_id          = google_bigquery_dataset.skypulse_gold.dataset_id
  table_id            = "dim_flights"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "flight_key",    type = "INTEGER" },
    { name = "flight_number", type = "STRING" },
    { name = "route_id",      type = "STRING" },
    { name = "origin",        type = "STRING" },
    { name = "destination",   type = "STRING" },
    { name = "airline_code",  type = "STRING" },
    { name = "effective_date",type = "STRING" },
    { name = "expiry_date",   type = "STRING" },
    { name = "is_current",    type = "BOOLEAN" },
  ])
}

resource "google_bigquery_table" "fact_flight_prices" {
  dataset_id          = google_bigquery_dataset.skypulse_gold.dataset_id
  table_id            = "fact_flight_prices"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "flight_key",   type = "INTEGER" },
    { name = "airline_key",  type = "INTEGER" },
    { name = "date_key",     type = "INTEGER" },
    { name = "window_start", type = "TIMESTAMP" },
    { name = "window_end",   type = "TIMESTAMP" },
    { name = "avg_price_usd",type = "FLOAT" },
    { name = "min_price_usd",type = "FLOAT" },
    { name = "max_price_usd",type = "FLOAT" },
    { name = "message_count",type = "INTEGER" },
    { name = "is_anomaly",   type = "BOOLEAN" },
    { name = "z_score",      type = "FLOAT" },
    { name = "created_at",   type = "STRING" },
  ])
}

# ─────────────────────────────────────────
# CLOUD SCHEDULER JOBS
# ─────────────────────────────────────────

resource "google_cloud_scheduler_job" "function_a_daily" {
  name        = "skypulse-function-a-daily"
  description = "Trigger Function A daily at midnight UTC"
  schedule    = "0 0 * * *"
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "GET"
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/function-a-route-refresh"

    oidc_token {
      service_account_email = google_service_account.skypulse_ingestion.email
    }
  }
}

resource "google_cloud_scheduler_job" "function_b_ticker" {
  name        = "skypulse-function-b-ticker"
  description = "Trigger Function B every minute"
  schedule    = "* * * * *"
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  http_target {
    http_method = "GET"
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/function-b-price-ticker"

    oidc_token {
      service_account_email = google_service_account.skypulse_ingestion.email
    }
  }
}

# ─────────────────────────────────────────
# SECRET MANAGER
# ─────────────────────────────────────────

resource "google_secret_manager_secret" "aviationstack_key" {
  secret_id = "aviationstack-api-key"
  project   = var.project_id

  replication {
    auto {}
  }
}

# Note: Secret value is managed outside Terraform
# Set manually: gcloud secrets versions add aviationstack-api-key --data-file=-
