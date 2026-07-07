# SkyPulse — GCP Commands Reference

## Project Setup
```bash
# Set active project
gcloud config set project triptide-28062026

# Verify config
gcloud config list
```

## APIs Enabled
```bash
# Week 1 — Core infrastructure
gcloud services enable pubsub.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com cloudscheduler.googleapis.com iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com --project=triptide-28062026
```

## GCS
```bash
# Create bucket
gsutil mb -p triptide-28062026 -l us-central1 -b on gs://skypulse-triptide

# Set lifecycle policy
gsutil lifecycle set lifecycle.json gs://skypulse-triptide

# List files
gsutil ls gs://skypulse-triptide/bronze/routes/
gsutil ls gs://skypulse-triptide/silver/
```

## IAM
```bash
# Create service account
gcloud iam service-accounts create skypulse-ingestion-sa --display-name="SkyPulse Ingestion Service Account" --project=triptide-28062026

# Grant roles
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/pubsub.publisher"
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/storage.objectCreator"
gcloud projects add-iam-policy-binding triptide-28062026 --member="serviceAccount:skypulse-ingestion-sa@triptide-28062026.iam.gserviceaccount.com" --role="roles/cloudfunctions.invoker"

# Verify roles
gcloud projects get-iam-policy triptide-28062026 --flatten="bindings[].members" --filter="bindings.members:skypulse-ingestion-sa" --format="table(bindings.role)"
```

## Pub/Sub
```bash
# Create topics
gcloud pubsub topics create flight-prices-raw --project=triptide-28062026
gcloud pubsub topics create flight-prices-dlq --project=triptide-28062026

# Create subscription with DLQ
gcloud pubsub subscriptions create flight-prices-sub --topic=flight-prices-raw --project=triptide-28062026 --ack-deadline=60 --message-retention-duration=7d --dead-letter-topic=flight-prices-dlq --max-delivery-attempts=5

# Grant Pub/Sub internal SA permissions for DLQ forwarding
gcloud pubsub topics add-iam-policy-binding flight-prices-dlq --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.publisher"
gcloud pubsub subscriptions add-iam-policy-binding flight-prices-sub --project=triptide-28062026 --member="serviceAccount:service-47109282086@gcp-sa-pubsub.iam.gserviceaccount.com" --role="roles/pubsub.subscriber"

# Verify subscription
gcloud pubsub subscriptions describe flight-prices-sub --project=triptide-28062026

# Pull messages (for debugging)
gcloud pubsub subscriptions pull flight-prices-sub --limit=5 --auto-ack --project=triptide-28062026

# List topics
gcloud pubsub topics list --project=triptide-28062026
```

## Pub/Sub Schema Registry
```bash
# Create schema
gcloud pubsub schemas create flight-price-schema --type=AVRO --definition-file=flight_price.avsc --project=triptide-28062026

# Attach schema to topic
gcloud pubsub topics update flight-prices-raw --schema=flight-price-schema --message-encoding=JSON --project=triptide-28062026

# Verify schema attached
gcloud pubsub topics describe flight-prices-raw --project=triptide-28062026
```

## Cloud Functions
```bash
# Deploy Function A (daily route refresh)


# Deploy Function B (price ticker every 60 seconds)

```

## Debugging
```bash
# Check project number
gcloud projects describe triptide-28062026 --format="value(projectNumber)"

# List service accounts
gcloud iam service-accounts list --project=triptide-28062026
```