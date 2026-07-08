# SkyPulse — GCP Commands Reference

---

## Project Setup

```bash
# Authenticate with GCP using your Google account
gcloud auth login

# Set active project so all subsequent commands target this project
gcloud config set project triptide-28062026

# Authenticate application default credentials — allows local Python code to use GCP services
gcloud auth application-default login

# Verify current config (account + project)
gcloud config list

# Get project number (numeric ID, different from project ID — needed for internal service account emails)
gcloud projects describe triptide-28062026 --format="value(projectNumber)"
# Output: 47109282086
```

---

## APIs Enabled

```bash
# Week 1 — Core infrastructure APIs
gcloud services enable pubsub.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com cloudscheduler.googleapis.com iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com --project=triptide-28062026

# Week 2 — Stream processing and analytics APIs
# dataflow: managed Apache Beam execution engine
# bigquery: analytical warehouse where windowed results land
gcloud services enable dataflow.googleapis.com bigquery.googleapis.com --project=triptide-28062026
```

---

## GCS (Google Cloud Storage)

```bash
# Create bucket in us-central1 with uniform bucket-level access
gsutil mb -p triptide-28062026 -l us-central1 -b on gs://skypulse-triptide

# Create Bronze/Silver/Gold folder structure
echo $null > .keep
gsutil cp .keep gs://skypulse-triptide/bronze/.keep
gsutil cp .keep gs://skypulse-triptide/silver/.keep
gsutil cp .keep gs://skypulse-triptide/gold/.keep
del .keep

# Apply lifecycle policy — Bronze: Nearline after 30d, Coldline after 90d
gsutil lifecycle set lifecycle.json gs://skypulse-triptide

# List files
gsutil ls gs://skypulse-triptide/bronze/routes/
gsutil ls gs://skypulse-triptide/silver/

# List Dataflow staging/temp files
gsutil ls gs://skypulse-triptide/dataflow/staging/
gsutil ls gs://skypulse-triptide/dataflow/temp/
```

---

## IAM (Identity and Access Management)

```bash
# Create service account
gcloud iam service-accounts create skypulse-ingestion-sa --display-name="SkyPulse Ingestion Service Account" --project=triptide-28062026

# Grant Pub/Sub Publisher — publish messages to topics
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/pubsub.publisher"

# Grant Storage Object Creator — create new GCS objects (cannot overwrite)
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/storage.objectCreator"

# Grant Storage Object Admin — create AND overwrite GCS objects
# Needed because Function A overwrites active_routes.json daily
# Lesson: objectCreator alone fails if file already exists
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"

# Grant Cloud Functions Invoker — trigger Cloud Functions via HTTP
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/cloudfunctions.invoker"

# Grant Dataflow Worker — run as a Dataflow worker, read Pub/Sub, write temp files to GCS
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/dataflow.worker"

# Grant BigQuery Data Editor — write rows to BigQuery tables
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User — run BigQuery jobs (required for streaming inserts)
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"

# Verify all roles on service account
gcloud projects get-iam-policy triptide-28062026 --flatten="bindings[].members" --filter="bindings.members:skypulse-ingestion-sa" --format="table(bindings.role)"

# List all service accounts
gcloud iam service-accounts list --project=triptide-28062026
```

---

## Pub/Sub

```bash
# Create main topic
gcloud pubsub topics create flight-prices-raw --project=triptide-28062026

# Create dead letter topic
gcloud pubsub topics create flight-prices-dlq --project=triptide-28062026

# Create subscription with DLQ routing
# --ack-deadline=60: Dataflow has 60 sec to ack before redelivery
# --message-retention-duration=7d: keep unacked messages 7 days
# --max-delivery-attempts=5: retry 5 times before routing to DLQ
gcloud pubsub subscriptions create flight-prices-sub --topic=flight-prices-raw --project=triptide-28062026 --ack-deadline=60 --message-retention-duration=7d --dead-letter-topic=flight-prices-dlq --max-delivery-attempts=5

# Grant Pub/Sub internal SA publish access on DLQ
# Required for Pub/Sub's internal machinery to forward failed messages to DLQ
# Without this, messages silently disappear after max retries
gcloud pubsub topics add-iam-policy-binding flight-prices-dlq --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.publisher"

# Grant Pub/Sub internal SA subscriber access on main subscription
# Required so Pub/Sub can read failed messages to forward them to DLQ
gcloud pubsub subscriptions add-iam-policy-binding flight-prices-sub --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.subscriber"

# Verify subscription config
gcloud pubsub subscriptions describe flight-prices-sub --project=triptide-28062026

# List topics
gcloud pubsub topics list --project=triptide-28062026

# Pull messages for debugging (auto-ack deletes them after pulling)
gcloud pubsub subscriptions pull flight-prices-sub --limit=5 --auto-ack --project=triptide-28062026
```

---

## Pub/Sub Schema Registry

```bash
# Register Avro schema in Schema Registry (not attached to topic yet)
gcloud pubsub schemas create flight-price-schema --type=AVRO --definition-file=flight_price.avsc --project=triptide-28062026

# Attach schema to topic — enforces validation on every incoming message
# --message-encoding=JSON: easier to debug than binary during development
gcloud pubsub topics update flight-prices-raw --schema=flight-price-schema --message-encoding=JSON --project=triptide-28062026

# Verify schema attached
gcloud pubsub topics describe flight-prices-raw --project=triptide-28062026
```

---

## Cloud Functions

```bash
# Deploy Function A — daily route refresh from AviationStack
# --gen2: 2nd gen (60 min timeout, runs on Cloud Run)
# --set-env-vars: inject API key — NEVER hardcode in source
# --timeout=120s: generous timeout for API call + GCS write
gcloud functions deploy function-a-route-refresh --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --set-env-vars=AVIATIONSTACK_API_KEY=YOUR_KEY_HERE --memory=256MB --timeout=120s --project=triptide-28062026

# Deploy Function B — price ticker every 60 seconds
# No --set-env-vars needed — never calls AviationStack
gcloud functions deploy function-b-price-ticker --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --memory=256MB --timeout=60s --project=triptide-28062026

# Call function manually for testing
gcloud functions call function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026
gcloud functions call function-b-price-ticker --gen2 --region=us-central1 --project=triptide-28062026

# Read logs — essential for debugging
gcloud functions logs read function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026 --limit=50
gcloud functions logs read function-b-price-ticker --gen2 --region=us-central1 --project=triptide-28062026 --limit=50

# Describe function — verify what's actually deployed
gcloud functions describe function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026
```

---

## Cloud Scheduler

```bash
# Schedule Function A — daily at midnight UTC
# OIDC token authenticates the HTTP call to the function
gcloud scheduler jobs create http skypulse-function-a-daily --location=us-central1 --schedule="0 0 * * *" --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-a-route-refresh" --http-method=GET --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --project=triptide-28062026

# Schedule Function B — every minute
# "* * * * *" = minimum Cloud Scheduler interval = every 60 seconds
gcloud scheduler jobs create http skypulse-function-b-ticker --location=us-central1 --schedule="* * * * *" --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-b-price-ticker" --http-method=GET --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --project=triptide-28062026

# Verify scheduler jobs
gcloud scheduler jobs describe skypulse-function-a-daily --location=us-central1 --project=triptide-28062026
gcloud scheduler jobs describe skypulse-function-b-ticker --location=us-central1 --project=triptide-28062026

# Manually trigger a scheduler job for testing
gcloud scheduler jobs run skypulse-function-a-daily --location=us-central1 --project=triptide-28062026
gcloud scheduler jobs run skypulse-function-b-ticker --location=us-central1 --project=triptide-28062026

# List all scheduler jobs
gcloud scheduler jobs list --location=us-central1 --project=triptide-28062026
```

---

## BigQuery

```bash
# Create dataset (container for tables, like a database schema)
bq mk --dataset --location=us-central1 --description="SkyPulse flight price analytics" triptide-28062026:skypulse

# Create tables manually
bq mk --table --description="5-minute windowed average price per route" triptide-28062026:skypulse.price_5min_avg route_id:STRING,origin:STRING,destination:STRING,airline_code:STRING,airline_name:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,avg_price_usd:FLOAT,min_price_usd:FLOAT,max_price_usd:FLOAT,message_count:INTEGER,processing_timestamp:TIMESTAMP
bq mk --table --description="1-hour sliding window average price per route" triptide-28062026:skypulse.price_1hr_trend route_id:STRING,origin:STRING,destination:STRING,airline_code:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,avg_price_usd:FLOAT,message_count:INTEGER,processing_timestamp:TIMESTAMP
bq mk --table --description="Messages that arrived more than 2 minutes late" triptide-28062026:skypulse.late_arrivals route_id:STRING,origin:STRING,destination:STRING,airline_code:STRING,price_usd:FLOAT,flight_date:STRING,event_timestamp:TIMESTAMP,ingestion_timestamp:TIMESTAMP,lateness_seconds:INTEGER,processing_timestamp:TIMESTAMP

# List tables in dataset
bq ls triptide-28062026:skypulse

# Verify row counts
bq query --use_legacy_sql=false "SELECT COUNT(*) as row_count FROM triptide-28062026.skypulse.price_5min_avg"
bq query --use_legacy_sql=false "SELECT COUNT(*) as row_count FROM triptide-28062026.skypulse.price_1hr_trend"
bq query --use_legacy_sql=false "SELECT COUNT(*) as late_count FROM triptide-28062026.skypulse.late_arrivals"

# Query recent 5-minute windowed data
bq query --use_legacy_sql=false "SELECT route_id, window_start, window_end, avg_price_usd, min_price_usd, max_price_usd, message_count FROM triptide-28062026.skypulse.price_5min_avg ORDER BY window_start DESC LIMIT 10"

# Query sliding window trend data
bq query --use_legacy_sql=false "SELECT route_id, window_start, window_end, avg_price_usd, message_count FROM triptide-28062026.skypulse.price_1hr_trend ORDER BY window_start DESC LIMIT 10"

# Query specific route price history
bq query --use_legacy_sql=false "SELECT window_start, avg_price_usd, message_count FROM triptide-28062026.skypulse.price_5min_avg WHERE route_id='ORD-LAX' ORDER BY window_start DESC LIMIT 20"

# Anomaly detection query — routes where price deviates more than 3 std devs from 7-day mean
bq query --use_legacy_sql=false "
WITH stats AS (
  SELECT
    route_id,
    AVG(avg_price_usd) as mean_price,
    STDDEV(avg_price_usd) as stddev_price
  FROM triptide-28062026.skypulse.price_5min_avg
  WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY route_id
)
SELECT
  p.route_id,
  p.avg_price_usd,
  s.mean_price,
  s.stddev_price,
  ABS(p.avg_price_usd - s.mean_price) / NULLIF(s.stddev_price, 0) as z_score
FROM triptide-28062026.skypulse.price_5min_avg p
JOIN stats s ON p.route_id = s.route_id
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
AND ABS(p.avg_price_usd - s.mean_price) / NULLIF(s.stddev_price, 0) > 3
ORDER BY z_score DESC"
```

---

## Dataflow

```bash
# Deploy pipeline — run from dataflow/ folder with venv activated
# Packages pipeline.py, uploads to GCS staging, starts Dataflow job on GCP
cd skypulse/dataflow
venv\Scripts\activate
python pipeline.py

# List running Dataflow jobs
gcloud dataflow jobs list --region=us-central1 --project=triptide-28062026

# Describe a specific job
gcloud dataflow jobs describe JOB_ID --region=us-central1 --project=triptide-28062026

# Cancel a running job — ALWAYS do this when done to avoid cost
# Dataflow charges per minute of worker time
gcloud dataflow jobs cancel JOB_ID --region=us-central1 --project=triptide-28062026

# Week 2 job ID for reference
# 2026-07-07_22_55_30-10937875646678629127

# View job in console
# https://console.cloud.google.com/dataflow/jobs/us-central1/JOB_ID?project=triptide-28062026

# Key metrics to watch in Dataflow console:
# - System lag: how far behind real time (healthy = under 60 sec)
# - Elements added: messages read from Pub/Sub (should grow at 0.33/sec)
# - Streaming mode: should say "Exactly once"
# - All nodes: should be green (no red = no errors)
```

---

## Debugging Reference

```bash
# Check active project and account
gcloud config list

# Get numeric project number
gcloud projects describe triptide-28062026 --format="value(projectNumber)"

# List all enabled APIs
gcloud services list --enabled --project=triptide-28062026

# Check IAM roles on service account
gcloud projects get-iam-policy triptide-28062026 --flatten="bindings[].members" --filter="bindings.members:skypulse-ingestion-sa" --format="table(bindings.role)"

# List service accounts
gcloud iam service-accounts list --project=triptide-28062026
```

---

## Cost Management

```bash
# CRITICAL: Always cancel Dataflow jobs when done testing
gcloud dataflow jobs cancel JOB_ID --region=us-central1 --project=triptide-28062026

# CRITICAL: Always delete Dataproc clusters after use (Week 3)
# gcloud dataproc clusters delete CLUSTER_NAME --region=us-central1 --project=triptide-28062026

# Check billing alerts
# console.cloud.google.com/billing/budgets
# Alert set at $50/month
```

---

## Lessons Learned

| Issue | Root Cause | Fix |
|---|---|---|
| Function got 401 from AviationStack | API key hardcoded, not read from env var | Use `os.environ.get("AVIATIONSTACK_API_KEY")` |
| Second deploy didn't update function | main.py unchanged so GCP skipped rebuild | Always change source file before redeploying |
| Function got 403 writing to GCS | `storage.objectCreator` can't overwrite existing files | Grant `storage.objectAdmin` instead |
| IAM change didn't take effect | IAM is eventually consistent — 30-60 sec propagation delay | Wait 60 seconds after IAM change before retesting |
| API key exposed in GitHub | Key hardcoded in source code | Rotate immediately, use env vars going forward |
| Beam install failed on Python 3.13 | Beam 2.61.0 doesn't support Python 3.13 | Use Beam 2.74.0 |
| Beam install failed with pkg_resources error | New venvs on Python 3.13 missing setuptools | Use `--system-site-packages` when creating venv |
| Duplicate rows in price_5min_avg | Accumulating mode fires update even without late data | Add deduplication in Week 4 Cloud Composer DAG |
| DLQ warning in Dataflow logs | Dataflow retry logic conflicts with Pub/Sub DLQ policy | Known issue — address in Week 5 Terraform cleanup |