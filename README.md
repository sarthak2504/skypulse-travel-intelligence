# SkyPulse — Real-Time Travel Intelligence Platform

A production-grade data engineering platform that ingests live flight data, streams pricing signals through a multi-layer GCP pipeline, and serves analytics via Snowflake and a natural language AI interface.

Built to demonstrate end-to-end data engineering depth across GCP, Snowflake, Apache Beam, Terraform, and the Claude API.

---

## Architecture

```
AviationStack API (real flight routes, ORD departures)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                  INGESTION LAYER                     │
│                                                      │
│  Cloud Function A (daily)                            │
│  └── Fetches real ORD flight schedules               │
│  └── Saves full dataset → GCS Bronze                 │
│  └── Derives top 20 routes → GCS Silver              │
│                                                      │
│  Cloud Function B (every 60 seconds)                 │
│  └── Reads active routes from GCS Silver             │
│  └── Generates simulated price per route             │
│  └── Publishes Avro message → Pub/Sub                │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│               MESSAGE QUEUE LAYER                    │
│                                                      │
│  Pub/Sub topic: flight-prices-raw                    │
│  └── Avro schema enforced at publish time            │
│  └── Dead letter topic: flight-prices-dlq            │
│  └── 7-day message retention                         │
│  └── 5 max delivery attempts before DLQ              │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│             STREAM PROCESSING LAYER                  │
│                   [Week 2 - In Progress]             │
│                                                      │
│  Dataflow (Apache Beam)                              │
│  └── Fixed window (5 min): avg price per route       │
│  └── Sliding window (1 hr / 15 min): trend detection │
│  └── Watermark: 2-min allowed lateness               │
│  └── Stream-table join: enrich with route metadata   │
│  └── Idempotent writes to BigQuery                   │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                 STORAGE LAYER                        │
│                   [Week 3 - Planned]                 │
│                                                      │
│  GCS (Bronze → Nearline 30d → Coldline 90d)          │
│  BigQuery Silver (cleaned events)                    │
│  BigQuery Gold (star schema)                         │
│  └── fact_flight_prices (grain: route × airline      │
│       × 5-min window)                               │
│  └── dim_routes (SCD Type 2)                         │
│  └── dim_airlines                                    │
│  └── dim_date                                        │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              ORCHESTRATION + DQ LAYER                │
│                   [Week 4 - Planned]                 │
│                                                      │
│  Cloud Composer (managed Airflow)                    │
│  └── Rule-based DQ (row counts, nulls, freshness)    │
│  └── Statistical anomaly detection (3-sigma)         │
│  └── Lineage tracking table                          │
│  └── Cloud Monitoring alerts                         │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│            INFRASTRUCTURE AS CODE                    │
│                   [Week 5 - Planned]                 │
│                                                      │
│  Terraform                                           │
│  └── GCS buckets + lifecycle policies                │
│  └── Pub/Sub topics + schemas                        │
│  └── BigQuery datasets + tables                      │
│  └── IAM service accounts + bindings                 │
│  └── Cloud Composer environment                      │
│  └── Secret Manager for API keys                     │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│               ANALYTICS SERVING LAYER                │
│                   [Weeks 6-7 - Planned]              │
│                                                      │
│  Snowflake                                           │
│  └── Snowpipe: continuous ingestion from GCS         │
│  └── Snowpark: Python transformations                │
│  └── Dynamic Tables: materialized hourly summary     │
│  └── Cortex Analyst: natural language queries        │
│  SnowPro Core certification                          │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                    AI LAYER                          │
│                   [Weeks 8-9 - Planned]              │
│                                                      │
│  Claude API + pgvector RAG                           │
│  └── Route embeddings in Cloud SQL PostgreSQL        │
│  └── Semantic similarity search                      │
│  Multi-step agent with three tools:                  │
│  └── search_routes (vector search)                   │
│  └── query_bigquery (historical analysis)            │
│  └── query_snowflake (analytics queries)             │
│  Streamlit UI deployed on Cloud Run                  │
└─────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Cloud Functions (Python), Cloud Scheduler, AviationStack API |
| Messaging | GCP Pub/Sub, Avro Schema Registry |
| Stream Processing | Apache Beam, Google Dataflow |
| Batch Processing | Apache Spark, Google Dataproc |
| Storage | GCS (Bronze/Silver/Gold), BigQuery |
| Orchestration | Cloud Composer (managed Airflow) |
| Infrastructure | Terraform |
| Analytics Serving | Snowflake (Snowpipe, Snowpark, Dynamic Tables, Cortex AI) |
| AI Layer | Claude API, pgvector, Cloud SQL PostgreSQL |
| Frontend | Streamlit, Cloud Run |
| Language | Python |

---

## What's Built (Week 1 — Complete)

### Ingestion Pipeline

Two Cloud Functions running on automated schedules:

**Function A — Route Refresher (daily, midnight UTC)**
- Calls AviationStack API to fetch all scheduled ORD departures
- Filters valid flights (removes records with null airline, flight number, or IATA codes)
- Saves full raw dataset to GCS Bronze (`bronze/routes/{date}.json`)
- Derives top 20 routes by flight frequency
- Saves curated route list to GCS Silver (`silver/active_routes.json`)
- Uses 1 of 100 free monthly API calls — designed for sustainability within free tier

**Function B — Price Ticker (every 60 seconds)**
- Reads active route list from GCS Silver — no API call required
- Generates realistic simulated prices per route (base price + ±20% variance + time-of-day factor)
- Publishes Avro-encoded message to Pub/Sub for each route
- 20 messages published per execution, every minute, 24/7

### Message Queue Infrastructure

- **Pub/Sub topic** `flight-prices-raw` with Avro schema enforcement
- **Avro Schema Registry** — messages validated at publish time, malformed records rejected before entering pipeline
- **Dead letter topic** `flight-prices-dlq` — messages that fail 5 delivery attempts are routed here for investigation
- **Subscription** `flight-prices-sub` with 60-second ack deadline and 7-day retention
- **IAM** — least-privilege service account `skypulse-ingestion-sa` with only required permissions

### GCS Storage

- Bucket `skypulse-triptide` in `us-central1`
- Bronze/Silver/Gold folder structure
- Lifecycle policy: Bronze → Nearline after 30 days → Coldline after 90 days

---

## Architectural Decisions

### Why Pub/Sub over Kafka?

Pub/Sub is the right choice for SkyPulse for three reasons. First, it's fully managed with zero operational overhead — no brokers, no partition management, no ZooKeeper. Second, it integrates natively with Dataflow, eliminating the need for connectors. Third, replay capability — the main reason to choose Kafka — is handled at the GCS layer instead. Raw Avro files in Bronze serve as the reprocessing source if needed, which is a cleaner separation of concerns than relying on Kafka's log retention.

Kafka would be the right choice if the architecture required message-level replay, strict cross-partition ordering, or existing Kafka investment to leverage.

### Why separate Function A and Function B?

Flight schedules are essentially static throughout a given day — they're set in advance and don't change minute to minute. Prices, on the other hand, are the genuinely volatile signal. Refreshing schedule data once daily from a live API and streaming price ticks every 60 seconds reflects the actual rate of change of each data type. This design also stays within the 100 requests/month free tier constraint by making exactly one API call per day.

### Why fixed windows AND sliding windows?

They answer different business questions. Fixed windows (5-minute) produce a clean time-series of average prices suitable for storage and trend charts — one row per route per 5-minute interval. Sliding windows (1-hour, advancing every 15 minutes) produce a rolling average that smooths short-term spikes and is better suited for anomaly detection — comparing today's rolling average against the 7-day baseline.

### Why BigQuery for processing and Snowflake for serving?

BigQuery is optimized for large-scale analytical processing — partitioned tables, slot-based compute, native integration with Dataflow. Snowflake is optimized for serving — virtual warehouses that auto-suspend, result cache for repeated queries, Cortex AI for natural language. Keeping processing in BigQuery and serving in Snowflake separates concerns and avoids the cost of running heavy transformations in Snowflake compute credits.

### Why SCD Type 2 for the route dimension?

Routes change over time — airlines add and drop routes, new carriers enter markets. SCD Type 2 preserves history by closing the old row (setting `expiry_date` and `is_current = false`) and inserting a new row rather than overwriting. This means historical price facts can always be joined back to the route as it existed at the time — which is essential for accurate trend analysis. SCD Type 1 (overwrite) would corrupt historical analysis.

### Why Avro over JSON for Pub/Sub messages?

Avro enforces schema at publish time via the Schema Registry — malformed messages are rejected before entering the pipeline, not discovered after landing in BigQuery. It also produces smaller messages (binary format, field names stored in schema not in every message). The trade-off is human readability — we use JSON encoding in development for easier debugging, with the option to switch to binary encoding in production for throughput.

---

## Data Model (BigQuery Gold Layer)

**Grain:** One row in `fact_flight_prices` = average price for one origin-destination pair operated by one airline in one 5-minute window.

```
fact_flight_prices
├── route_key (FK → dim_routes)
├── airline_key (FK → dim_airlines)
├── date_key (FK → dim_date)
├── window_start_timestamp
├── window_end_timestamp
├── avg_price_usd
├── min_price_usd
├── max_price_usd
├── price_update_count
└── is_anomaly (bool)

dim_routes (SCD Type 2)
├── route_key (surrogate key)
├── route_id (natural key, e.g. ORD-LAX)
├── origin
├── destination
├── distance_km
├── effective_date
├── expiry_date
└── is_current

dim_airlines
├── airline_key
├── airline_code
├── airline_name
└── alliance

dim_date
├── date_key
├── date
├── day_of_week
├── is_weekend
└── is_holiday
```

---

## Active Routes (derived from real AviationStack data)

Top 20 ORD departure routes by daily flight frequency:

| Route | Destination | Example Airline |
|---|---|---|
| ORD-SAN | San Diego | United |
| ORD-PHX | Phoenix | American |
| ORD-BOS | Boston | Delta |
| ORD-SFO | San Francisco | United |
| ORD-DFW | Dallas/Fort Worth | American |
| ORD-LAX | Los Angeles | American |
| ORD-JFK | New York JFK | Delta |
| ORD-MIA | Miami | American |
| ORD-SEA | Seattle | Alaska |
| ORD-DEN | Denver | United |
| ORD-LGA | New York LaGuardia | Delta |
| ORD-DCA | Washington DC | American |
| ORD-STL | St. Louis | United |
| ORD-CLT | Charlotte | American |
| ORD-CLE | Cleveland | United |
| ORD-CUN | Cancun | Frontier |
| ORD-MEX | Mexico City | Aeromexico |
| ORD-YYC | Calgary | Air Canada |
| ORD-PVG | Shanghai | United |
| ORD-CDG | Paris | Air France |

Routes derived empirically from real AviationStack traffic volume, not hardcoded assumptions.

---

## What's Coming

### Week 2 — Dataflow Stream Processing
Apache Beam pipeline reading from `flight-prices-sub`:
- Fixed window (5-min) price aggregations per route
- Sliding window (1-hr / 15-min) trend detection
- Watermark-based late data handling (2-min allowed lateness)
- Stream-table join for route metadata enrichment
- Idempotent BigQuery writes via insertId deduplication

### Week 3 — BigQuery Modeling + Dataproc Batch
- Star schema in BigQuery Gold layer (fact + 3 dimensions)
- SCD Type 2 implementation on dim_routes
- Dataproc PySpark job for historical batch processing
- Watermark table for incremental processing
- Partition on price_date, cluster on route_id + airline_code

### Week 4 — Cloud Composer + Data Quality
- Cloud Composer DAG orchestrating the full pipeline
- Rule-based DQ checks (row counts, null percentages, freshness)
- Statistical anomaly detection (3-sigma price deviation from 7-day rolling mean)
- Pipeline lineage tracking table
- Cloud Monitoring alerts (Pub/Sub lag, Dataflow errors, DAG failures)

### Week 5 — Terraform
- Full GCP infrastructure as code
- GCS, Pub/Sub, BigQuery, IAM, Cloud Composer all provisioned via Terraform
- Secret Manager for API key management
- Reproducible dev and prod environments

### Weeks 6-7 — Snowflake Integration
- Snowpipe continuous ingestion from GCS Bronze
- Snowpark Python transformations
- Dynamic Tables for materialized hourly price summary
- Cortex Analyst for natural language querying
- SnowPro Core certification

### Weeks 8-9 — AI Layer
- pgvector on Cloud SQL PostgreSQL for route embeddings
- RAG pipeline: question → embed → vector search → context injection
- Multi-step Claude API agent with three tools (route search, BigQuery query, Snowflake query)
- Streamlit chat UI deployed on Cloud Run

### Week 10 — Polish
- Architecture diagram
- Cost analysis document
- Demo video

---

## Project Structure

```
skypulse-travel-intelligence/
├── README.md
├── commands.md                    # All GCP CLI commands used, with explanations
├── lifecycle.json                 # GCS lifecycle policy
├── schemas/
│   └── flight_price.avsc          # Avro schema registered in Pub/Sub
└── functions/
    ├── function_a/
    │   ├── main.py                # Daily route refresh from AviationStack
    │   └── requirements.txt
    └── function_b/
        ├── main.py                # 60-second price ticker → Pub/Sub
        └── requirements.txt
```

---

## Setup

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.11+
- AviationStack free API key (aviationstack.com)

### Deploy

```bash
# Clone the repo
git clone https://github.com/sarthak2504/skypulse-travel-intelligence.git
cd skypulse-travel-intelligence

# Authenticate with GCP
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable required APIs
gcloud services enable pubsub.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com cloudscheduler.googleapis.com iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com

# Deploy Function A
cd functions/function_a
gcloud functions deploy function-a-route-refresh --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --set-env-vars=AVIATIONSTACK_API_KEY=YOUR_KEY --memory=256MB --timeout=120s

# Deploy Function B
cd ../function_b
gcloud functions deploy function-b-price-ticker --gen2 --runtime=python311 --region=us-central1 --source=. --entry-point=run --trigger-http --memory=256MB --timeout=60s
```

Full setup instructions including IAM, Pub/Sub, and schema configuration are documented in `commands.md`.

---

## Cost

This project runs within GCP's free tier for Week 1:
- Cloud Functions: free (first 2M invocations/month)
- Pub/Sub: free (first 10 GB/month — we use ~100 MB)
- GCS: free (first 5 GB)
- Cloud Scheduler: free (first 3 jobs/month)

Dataflow (Week 2) and Dataproc (Week 3) will consume GCP free trial credits ($300 available). Total estimated project cost: $70-90 over 10 weeks, fully covered by free credits.

---

## Author

**Sarthak Bhingarde**
Data Engineer III → Senior Data Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, University of Illinois Urbana-Champaign (4.0 GPA)
9 years experience: Teradata, AGCO, Expedia Group
