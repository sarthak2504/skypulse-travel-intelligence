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
# pubsub: message queue
# storage: GCS buckets
# cloudfunctions: serverless functions
# cloudscheduler: cron job scheduler
# iam: identity and access management
# cloudbuild: used internally by Cloud Functions during deployment
# run: Cloud Run, used by Cloud Functions gen2 under the hood
gcloud services enable pubsub.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com cloudscheduler.googleapis.com iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com --project=triptide-28062026
```

---

## GCS (Google Cloud Storage)

```bash
# Create bucket in us-central1 with uniform bucket-level access
# -p: project, -l: location/region, -b on: uniform bucket-level access (simpler IAM)
gsutil mb -p triptide-28062026 -l us-central1 -b on gs://skypulse-triptide

# Create Bronze/Silver/Gold folder structure
# GCS has no real folders — placeholder .keep files simulate the structure
echo $null > .keep
gsutil cp .keep gs://skypulse-triptide/bronze/.keep
gsutil cp .keep gs://skypulse-triptide/silver/.keep
gsutil cp .keep gs://skypulse-triptide/gold/.keep
del .keep

# Apply lifecycle policy from lifecycle.json
# Bronze files: move to Nearline after 30 days, Coldline after 90 days
# Silver/Gold stay as Standard (actively queried)
gsutil lifecycle set lifecycle.json gs://skypulse-triptide

# List files in a folder (for verification)
gsutil ls gs://skypulse-triptide/bronze/routes/
gsutil ls gs://skypulse-triptide/silver/
```

---

## IAM (Identity and Access Management)

```bash
# Create a dedicated service account for the ingestion pipeline
# Service accounts are non-human identities that code runs as
gcloud iam service-accounts create skypulse-ingestion-sa --display-name="SkyPulse Ingestion Service Account" --project=triptide-28062026

# Grant Pub/Sub Publisher role — allows SA to publish messages to any topic in the project
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/pubsub.publisher"

# Grant Storage Object Creator role — allows SA to create new GCS objects
# NOTE: this does NOT allow overwriting existing objects
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/storage.objectCreator"

# Grant Storage Object Admin role — allows SA to create AND overwrite existing GCS objects
# Needed because Function A overwrites active_routes.json daily
# Lesson learned: objectCreator alone is not enough if the file already exists
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"

# Grant Cloud Functions Invoker role — allows SA to trigger Cloud Functions via HTTP
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/cloudfunctions.invoker"

# Verify all roles attached to the service account
gcloud projects get-iam-policy triptide-28062026 --flatten="bindings[].members" --filter="bindings.members:skypulse-ingestion-sa" --format="table(bindings.role)"

# List all service accounts in the project
gcloud iam service-accounts list --project=triptide-28062026
```

---

## Pub/Sub

```bash
# Create main topic — flight price messages published here every 60 seconds
gcloud pubsub topics create flight-prices-raw --project=triptide-28062026

# Create dead letter topic — failed messages routed here after 5 delivery attempts
gcloud pubsub topics create flight-prices-dlq --project=triptide-28062026

# Create subscription on main topic with DLQ routing
# --ack-deadline=60: Dataflow has 60 seconds to ack before redelivery
# --message-retention-duration=7d: keep unacked messages for 7 days
# --dead-letter-topic: route failed messages to DLQ after max attempts
# --max-delivery-attempts=5: retry 5 times before sending to DLQ
gcloud pubsub subscriptions create flight-prices-sub --topic=flight-prices-raw --project=triptide-28062026 --ack-deadline=60 --message-retention-duration=7d --dead-letter-topic=flight-prices-dlq --max-delivery-attempts=5

# Grant Pub/Sub internal service account publish access on DLQ topic
# Required so Pub/Sub's own machinery can forward failed messages to the DLQ
# Without this, messages just disappear after max retries instead of landing in DLQ
# project number 47109282086 is the numeric ID of triptide-28062026
gcloud pubsub topics add-iam-policy-binding flight-prices-dlq --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.publisher"

# Grant Pub/Sub internal service account subscriber access on main subscription
# Required so Pub/Sub can read failed messages off the subscription to forward them
gcloud pubsub subscriptions add-iam-policy-binding flight-prices-sub --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.subscriber"

# Verify subscription config — confirms DLQ routing, ack deadline, retention
gcloud pubsub subscriptions describe flight-prices-sub --project=triptide-28062026

# List all topics in the project
gcloud pubsub topics list --project=triptide-28062026

# Pull messages from subscription for debugging (auto-ack deletes them after pulling)
gcloud pubsub subscriptions pull flight-prices-sub --limit=5 --auto-ack --project=triptide-28062026
```

---

## Pub/Sub Schema Registry

```bash
# Register Avro schema — stored in GCP Schema Registry, not tied to any topic yet
# --type=AVRO: schema format
# --definition-file: path to the .avsc schema file
gcloud pubsub schemas create flight-price-schema --type=AVRO --definition-file=flight_price.avsc --project=triptide-28062026

# Attach schema to topic — enforces validation on every incoming message
# --message-encoding=JSON: messages arrive as JSON (easier to debug than binary)
# After this, any message that doesn't match the schema is rejected at the topic level
gcloud pubsub topics update flight-prices-raw --schema=flight-price-schema --message-encoding=JSON --project=triptide-28062026

# Verify schema is attached to topic
gcloud pubsub topics describe flight-prices-raw --project=triptide-28062026
```

---

## Cloud Functions

```bash
# Deploy Function A — daily route refresh from AviationStack
# --gen2: 2nd generation (runs on Cloud Run, 60 min timeout vs 9 min for gen1)
# --runtime=python311: Python version
# --region=us-central1: same region as all other resources (no cross-region egress)
# --source=.: upload current directory (main.py + requirements.txt)
# --entry-point=run: which function inside main.py to call when triggered
# --trigger-http: expose as HTTP endpoint (Cloud Scheduler hits this URL)
# --service-account: run as skypulse-ingestion-sa, not the default compute SA
# --set-env-vars: inject API key as environment variable (never hardcode in source)
# --memory=256MB: RAM allocation
# --timeout=120s: max execution time before GCP kills the function
gcloud functions deploy function-a-route-refresh --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --set-env-vars=AVIATIONSTACK_API_KEY=YOUR_KEY_HERE --memory=256MB --timeout=120s --project=triptide-28062026

# Deploy Function B — price ticker (publishes simulated prices to Pub/Sub every minute)
# No --set-env-vars needed — Function B never calls AviationStack
# --timeout=60s: shorter timeout since this function runs fast (2-3 seconds)
gcloud functions deploy function-b-price-ticker --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --service-account=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --memory=256MB --timeout=60s --project=triptide-28062026

# Call a function manually to test it (without waiting for scheduler)
gcloud functions call function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026
gcloud functions call function-b-price-ticker --gen2 --region=us-central1 --project=triptide-28062026

# Read function logs — essential for debugging
# --limit=50: show last 50 log lines
gcloud functions logs read function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026 --limit=50
gcloud functions logs read function-b-price-ticker --gen2 --region=us-central1 --project=triptide-28062026 --limit=50

# Describe function — shows config, env vars, service account, revision, build ID
# Use this to verify what's actually deployed vs what you think is deployed
gcloud functions describe function-a-route-refresh --gen2 --region=us-central1 --project=triptide-28062026
```

---

## Cloud Scheduler

```bash
# Schedule Function A to run daily at midnight UTC
# schedule="0 0 * * *": cron expression — minute hour day month weekday
# --uri: URL of the Cloud Function to trigger
# --http-method=GET: how to call the function
# --oidc-service-account-email: identity used to authenticate the HTTP call to the function
gcloud scheduler jobs create http skypulse-function-a-daily --location=us-central1 --schedule="0 0 * * *" --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-a-route-refresh" --http-method=GET --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --project=triptide-28062026

# Schedule Function B to run every minute
# schedule="* * * * *": every minute — minimum Cloud Scheduler interval
gcloud scheduler jobs create http skypulse-function-b-ticker --location=us-central1 --schedule="* * * * *" --uri="https://us-central1-triptide-28062026.cloudfunctions.net/function-b-price-ticker" --http-method=GET --oidc-service-account-email=skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com --project=triptide-28062026

# Verify scheduler job config
gcloud scheduler jobs describe skypulse-function-a-daily --location=us-central1 --project=triptide-28062026
gcloud scheduler jobs describe skypulse-function-b-ticker --location=us-central1 --project=triptide-28062026

# Manually trigger a scheduler job (useful for testing without waiting for schedule)
gcloud scheduler jobs run skypulse-function-a-daily --location=us-central1 --project=triptide-28062026
gcloud scheduler jobs run skypulse-function-b-ticker --location=us-central1 --project=triptide-28062026

# List all scheduler jobs
gcloud scheduler jobs list --location=us-central1 --project=triptide-28062026
```

---

## Debugging Reference

```bash
# Check which project and account are active
gcloud config list

# Get numeric project number (needed for internal GCP service account emails)
gcloud projects describe triptide-28062026 --format="value(projectNumber)"

# List service accounts
gcloud iam service-accounts list --project=triptide-28062026

# Check IAM roles for a specific service account
gcloud projects get-iam-policy triptide-28062026 --flatten="bindings[].members" --filter="bindings.members:skypulse-ingestion-sa" --format="table(bindings.role)"
```

---

## Lessons Learned

| Issue | Root Cause | Fix |
|---|---|---|
| Function got 401 from AviationStack | API key was hardcoded in main.py, not read from env var | Replace hardcoded value with `os.environ.get("AVIATIONSTACK_API_KEY")` |
| Second deploy didn't update function | main.py hadn't changed so GCP skipped rebuild | Always change the source file before redeploying |
| Function got 403 writing to GCS | `storage.objectCreator` can't overwrite existing files | Grant `storage.objectAdmin` instead |
| IAM change didn't take effect immediately | IAM is eventually consistent — 30-60 second propagation delay | Wait 60 seconds after IAM change before retesting |
| API key exposed in GitHub | Key was hardcoded in source code that was committed | Rotate the key immediately, use env vars going forward |