# SkyPulse — GCP Commands Reference

---

## Project Setup

```bash
# Authenticate with GCP
gcloud auth login

# Set active project
gcloud config set project triptide-28062026

# Authenticate local Python code to use GCP services
gcloud auth application-default login

# Verify config
gcloud config list

# Get numeric project number (needed for internal service account emails)
gcloud projects describe triptide-28062026 --format="value(projectNumber)"
# Output: 47109282086
```

---

## APIs Enabled

```bash
# Week 1 — Core infrastructure
gcloud services enable pubsub.googleapis.com storage.googleapis.com \
  cloudfunctions.googleapis.com cloudscheduler.googleapis.com \
  iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com \
  --project=triptide-28062026

# Week 2 — Stream processing and analytics
gcloud services enable dataflow.googleapis.com bigquery.googleapis.com \
  --project=triptide-28062026

# Week 3 — Batch processing
gcloud services enable dataproc.googleapis.com --project=triptide-28062026
```

---

## GCS (Google Cloud Storage)

```bash
# Create bucket
gsutil mb -p triptide-28062026 -l us-central1 -b on gs://skypulse-triptide

# Create Bronze/Silver/Gold folder structure (Windows)
echo $null > .keep
gsutil cp .keep gs://skypulse-triptide/bronze/.keep
gsutil cp .keep gs://skypulse-triptide/silver/.keep
gsutil cp .keep gs://skypulse-triptide/gold/.keep
del .keep

# Apply lifecycle policy (Bronze: Nearline 30d, Coldline 90d)
gsutil lifecycle set lifecycle.json gs://skypulse-triptide

# List files
gsutil ls gs://skypulse-triptide/bronze/routes/
gsutil ls gs://skypulse-triptide/silver/

# Copy file from GCS to local
gsutil cp gs://skypulse-triptide/bronze/routes/2026-07-09.json C:\Users\sarth\Downloads\routes.json

# Upload file to GCS
gsutil cp spark/batch_job.py gs://skypulse-triptide/spark/batch_job.py
```

---

## IAM (Identity and Access Management)

```bash
# Create service account
gcloud iam service-accounts create skypulse-ingestion-sa \
  --display-name="SkyPulse Ingestion Service Account" \
  --project=triptide-28062026

# Grant roles
gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/cloudfunctions.invoker"

gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding triptide-28062026 \
  --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Verify roles on service account
gcloud projects get-iam-policy triptide-28062026 \
  --flatten="bindings[].members" \
  --filter="bindings.members:skypulse-ingestion-sa" \
  --format="table(bindings.role)"
```

---

## Pub/Sub

```bash
# Create topics
gcloud pubsub topics create flight-prices-raw --project=triptide-28062026
gcloud pubsub topics create flight-prices-dlq --project=triptide-28062026

# Create subscription with DLQ routing
gcloud pubsub subscriptions create flight-prices-sub \
  --topic=flight-prices-raw \
  --project=triptide-28062026 \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --dead-letter-topic=flight-prices-dlq \
  --max-delivery-attempts=5

# Grant Pub/Sub internal SA permissions for DLQ forwarding
# (project number 47109282086 is triptide-28062026's numeric ID)
gcloud pubsub topics add-iam-policy-binding flight-prices-dlq \
  --project=triptide-28062026 \
  --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

gcloud pubsub subscriptions add-iam-policy-binding flight-prices-sub \
  --project=triptide-28062026 \
  --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# Verify subscription
gcloud pubsub subscriptions describe flight-prices-sub --project=triptide-28062026

# Pull messages for debugging
gcloud pubsub subscriptions pull flight-prices-sub \
  --limit=5 --auto-ack --project=triptide-28062026

# Seek subscription to clear backlog (discard messages before timestamp)
gcloud pubsub subscriptions seek flight-prices-sub \
  --time=2026-07-12T21:00:00Z --project=triptide-28062026

# Delete and recreate subscription (nuclear option to clear all pending messages)
gcloud pubsub subscriptions delete flight-prices-sub --project=triptide-28062026
gcloud pubsub subscriptions create flight-prices-sub \
  --topic=flight-prices-raw --project=triptide-28062026 \
  --ack-deadline=60 --message-retention-duration=7d \
  --dead-letter-topic=flight-prices-dlq --max-delivery-attempts=5
```

---

## Pub/Sub Schema Registry

```bash
# Register Avro schema
gcloud pubsub schemas create flight-price-schema \
  --type=AVRO \
  --definition-file=flight_price.avsc \
  --project=triptide-28062026

# Attach schema to topic (JSON encoding for easier debugging)
gcloud pubsub topics update flight-prices-raw \
  --schema=flight-price-schema \
  --message-encoding=JSON \
  --project=triptide-28062026

# Verify schema attached
gcloud pubsub topics describe flight-prices-raw --project=triptide-28062026
```

---

## Cloud Functions

```bash
# Deploy Function A (daily route refresh)
cd functions/function_a
gcloud functions deploy function-a-route-refresh \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=. --entry-point=run --trigger-http \
  --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com \
  --set-env-vars=AVIATIONSTACK_API_KEY=YOUR_KEY_HERE \
  --memory=256MB --timeout=120s --project=triptide-28062026

# Deploy Function B (price ticker every 60 seconds)
cd functions/function_b
gcloud functions deploy function-b-price-ticker \
  --gen2 --runtime=python311 --region=us-central1 \
  --source=. --entry-point=run --trigger-http \
  --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com \
  --memory=256MB --timeout=60s --project=triptide-28062026

# Call function manually for testing
gcloud functions call function-a-route-refresh \
  --gen2 --region=us-central1 --project=triptide-28062026
gcloud functions call function-b-price-ticker \
  --gen2 --region=us-central1 --project=triptide-28062026

# Read function logs
gcloud functions logs read function-b-price-ticker \
  --gen2 --region=us-central1 --project=triptide-28062026 --limit=50

# Describe function (verify what's deployed)
gcloud functions describe function-b-price-ticker \
  --gen2 --region=us-central1 --project=triptide-28062026
```

---

## Cloud Scheduler

```bash
# Schedule Function A daily at midnight UTC
gcloud scheduler jobs create http skypulse-function-a-daily \
  --location=us-central1 \
  --schedule="0 0 * * *" \
  --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-a-route-refresh" \
  --http-method=GET \
  --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com \
  --project=triptide-28062026

# Schedule Function B every minute
gcloud scheduler jobs create http skypulse-function-b-ticker \
  --location=us-central1 \
  --schedule="* * * * *" \
  --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-b-price-ticker" \
  --http-method=GET \
  --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com \
  --project=triptide-28062026

# Manually trigger scheduler job
gcloud scheduler jobs run skypulse-function-b-ticker \
  --location=us-central1 --project=triptide-28062026

# List all jobs
gcloud scheduler jobs list --location=us-central1 --project=triptide-28062026
```

---

## BigQuery

```bash
# Create datasets
bq mk --dataset --location=us-central1 \
  --description="SkyPulse Silver layer" triptide-28062026:skypulse
bq mk --dataset --location=us-central1 \
  --description="SkyPulse Gold layer" triptide-28062026:skypulse_gold

# Add column to existing table
bq query --use_legacy_sql=false \
  "ALTER TABLE triptide-28062026.skypulse.price_5min_avg ADD COLUMN flight_number STRING"

# Create Gold tables (all dates as STRING for Spark compatibility)
bq mk --table triptide-28062026:skypulse_gold.dim_date \
  date_key:INTEGER,date:STRING,day_of_week:STRING,day_number:INTEGER,\
is_weekend:BOOLEAN,month:INTEGER,quarter:INTEGER,is_us_holiday:BOOLEAN,season:STRING

bq mk --table triptide-28062026:skypulse_gold.dim_airlines \
  airline_key:INTEGER,airline_code:STRING,airline_name:STRING,\
alliance:STRING,is_domestic:BOOLEAN

bq mk --table triptide-28062026:skypulse_gold.dim_flights \
  flight_key:INTEGER,flight_number:STRING,route_id:STRING,origin:STRING,\
destination:STRING,airline_code:STRING,effective_date:STRING,\
expiry_date:STRING,is_current:BOOLEAN

bq mk --table triptide-28062026:skypulse_gold.fact_flight_prices \
  flight_key:INTEGER,airline_key:INTEGER,date_key:INTEGER,\
window_start:TIMESTAMP,window_end:TIMESTAMP,avg_price_usd:FLOAT,\
min_price_usd:FLOAT,max_price_usd:FLOAT,message_count:INTEGER,\
is_anomaly:BOOLEAN,z_score:FLOAT,created_at:STRING

# List tables
bq ls triptide-28062026:skypulse
bq ls triptide-28062026:skypulse_gold

# Show table schema
bq show --schema triptide-28062026:skypulse.price_5min_avg

# Truncate table (delete all rows, keep schema)
bq query --use_legacy_sql=false \
  "TRUNCATE TABLE triptide-28062026.skypulse.price_5min_avg"

# Drop table
bq query --use_legacy_sql=false \
  "DROP TABLE IF EXISTS triptide-28062026.skypulse_gold.dim_date"
```

---

## BigQuery Queries (run in console)

```sql
-- Check row counts across Silver tables
SELECT 'price_5min_avg' as tbl, COUNT(*) as cnt
FROM triptide-28062026.skypulse.price_5min_avg
UNION ALL
SELECT 'price_1hr_trend', COUNT(*)
FROM triptide-28062026.skypulse.price_1hr_trend
UNION ALL
SELECT 'late_arrivals', COUNT(*)
FROM triptide-28062026.skypulse.late_arrivals

-- Check Silver data timestamp range
SELECT
    MIN(processing_timestamp) as earliest,
    MAX(processing_timestamp) as latest,
    COUNT(*) as row_count
FROM triptide-28062026.skypulse.price_5min_avg

-- Check duplicates in Silver
SELECT flight_number, window_start, COUNT(*) as cnt
FROM triptide-28062026.skypulse.price_5min_avg
GROUP BY flight_number, window_start
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 10

-- Recent Silver data (verify flight_number populated)
SELECT flight_number, route_id, airline_name, window_start, avg_price_usd
FROM triptide-28062026.skypulse.price_5min_avg
ORDER BY window_start DESC
LIMIT 10

-- Gold layer row counts
SELECT 'dim_date' as tbl, COUNT(*) as cnt
FROM triptide-28062026.skypulse_gold.dim_date
UNION ALL SELECT 'dim_airlines', COUNT(*)
FROM triptide-28062026.skypulse_gold.dim_airlines
UNION ALL SELECT 'dim_flights', COUNT(*)
FROM triptide-28062026.skypulse_gold.dim_flights
UNION ALL SELECT 'fact_flight_prices', COUNT(*)
FROM triptide-28062026.skypulse_gold.fact_flight_prices

-- Business query: top anomalies with dimension joins
SELECT
    f.flight_number,
    f.route_id,
    a.airline_name,
    a.alliance,
    fact.window_start,
    fact.avg_price_usd,
    ROUND(fact.z_score, 2) as z_score,
    fact.is_anomaly
FROM triptide-28062026.skypulse_gold.fact_flight_prices fact
JOIN triptide-28062026.skypulse_gold.dim_flights f ON fact.flight_key = f.flight_key
JOIN triptide-28062026.skypulse_gold.dim_airlines a ON fact.airline_key = a.airline_key
ORDER BY fact.z_score DESC
LIMIT 10

-- Reset watermark (force Spark to reprocess all data)
DELETE FROM triptide-28062026.skypulse.pipeline_watermarks
WHERE pipeline_name = 'daily_batch'

-- Check watermark
SELECT * FROM triptide-28062026.skypulse.pipeline_watermarks
ORDER BY updated_at DESC
```

---

## Dataflow

```bash
# Deploy pipeline (from dataflow/ folder with venv activated)
cd dataflow
venv\Scripts\activate  # Windows
python pipeline.py

# List running jobs (check region — we use us-east1 due to us-central1 capacity issues)
gcloud dataflow jobs list --region=us-east1 --project=triptide-28062026

# Cancel job (ALWAYS do this when done to avoid cost)
gcloud dataflow jobs cancel JOB_ID --region=us-east1 --project=triptide-28062026

# Job IDs for reference:
# Week 2: 2026-07-07_22_55_30-10937875646678629127 (us-central1)
# Week 3: check console for current job ID

# View job in console
# https://console.cloud.google.com/dataflow/jobs?project=triptide-28062026
```

---

## Dataproc Serverless (Managed Apache Spark)

```bash
# Upload Spark job to GCS first (Windows path issue with backslash)
gsutil cp spark/batch_job.py gs://skypulse-triptide/spark/batch_job.py

# Submit batch job (increment batch ID each run — must be unique)
gcloud dataproc batches submit pyspark gs://skypulse-triptide/spark/batch_job.py \
  --region=us-east1 \
  --project=triptide-28062026 \
  --deps-bucket=gs://skypulse-triptide \
  --batch=skypulse-batch-014

# Note: us-central1 frequently has zone capacity issues
# Use us-east1 for Dataproc Serverless

# View batch status
gcloud dataproc batches describe skypulse-batch-014 \
  --region=us-east1 --project=triptide-28062026

# List all batches
gcloud dataproc batches list --region=us-east1 --project=triptide-28062026
```

---

## Debugging Reference

```bash
# Check active config
gcloud config list

# Get project number
gcloud projects describe triptide-28062026 --format="value(projectNumber)"

# List enabled APIs
gcloud services list --enabled --project=triptide-28062026

# Check IAM roles on service account
gcloud projects get-iam-policy triptide-28062026 \
  --flatten="bindings[].members" \
  --filter="bindings.members:skypulse-ingestion-sa" \
  --format="table(bindings.role)"
```

---

## Cost Management

```bash
# CRITICAL: Cancel Dataflow when done testing (charges per minute)
gcloud dataflow jobs cancel JOB_ID --region=us-east1 --project=triptide-28062026

# Dataproc Serverless auto-terminates after job completes — no manual cleanup needed

# Check billing
# console.cloud.google.com/billing/budgets
# Alert set at $50/month — actual spend ~$0.10-3/day depending on Dataflow runtime
```

---

## Lessons Learned

| Issue | Root Cause | Fix |
|---|---|---|
| Function 401 from AviationStack | API key hardcoded not from env var | Use `os.environ.get("AVIATIONSTACK_API_KEY")` |
| Second deploy didn't update | main.py unchanged, GCP skipped rebuild | Always change source before redeploying |
| Function 403 writing to GCS | `storage.objectCreator` can't overwrite | Grant `storage.objectAdmin` |
| IAM change didn't take effect | IAM is eventually consistent (30-60s) | Wait 60 seconds after IAM change |
| API key exposed in GitHub | Hardcoded in source code | Rotate immediately, use env vars |
| Beam install failed on Python 3.13 | Beam 2.61.0 doesn't support Python 3.13 | Use Beam 2.74.0 |
| Beam install pkg_resources error | New venvs missing setuptools | Use `--system-site-packages` |
| Dataflow zone exhaustion | us-central1 out of capacity | Switch to us-east1 |
| Spark job found 0 rows | Watermark defaulted to after data timestamp | Hardcode watermark to before earliest data |
| Spark ErrorIfExists on dim tables | BigQuery connector default mode | Add `.mode("append")` to all writes |
| Spark schema type mismatch | dim tables created with DATE, Spark writes STRING | Recreate tables with STRING for date fields |
| Duplicate watermark entries | update_watermark uses WRITE_APPEND | Delete watermark before rerun to reset |
| High z-scores (100+) | Only 2 unique 1hr windows, stddev near zero | Add deduplication to read_silver_1hr(); resolves naturally after 7 days of data |
| price_5min_avg has duplicates | Dataflow accumulating mode fires window twice | Expected — Spark deduplication handles in Gold layer |
| AviationStack returns yesterday's flights | No flight_date filter, returns "scheduled" flights already departed | Add flight_date param; or use hardcoded schedule |