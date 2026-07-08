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
│                  INGESTION LAYER          ✅ Week 1  │
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
│               MESSAGE QUEUE LAYER         ✅ Week 1  │
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
│             STREAM PROCESSING LAYER       ✅ Week 2  │
│                                                      │
│  Dataflow (Apache Beam)                              │
│  └── Fixed window (5 min): avg price per route       │
│  └── Sliding window (1 hr / 15 min): trend detection │
│  └── Watermark: 2-min allowed lateness               │
│  └── Late data side output → late_arrivals table     │
│  └── Exactly-once processing via Streaming Engine    │
│  └── Idempotent writes to BigQuery                   │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                 STORAGE LAYER             ✅ Week 2  │
│                                                      │
│  GCS (Bronze → Nearline 30d → Coldline 90d)          │
│  BigQuery Silver:                                    │
│  └── price_5min_avg (11,610+ rows)                   │
│  └── price_1hr_trend                                 │
│  └── late_arrivals                                   │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              BATCH + MODELING LAYER  🔜 Week 3       │
│                                                      │
│  Dataproc (managed Spark)                            │
│  └── Historical price analysis PySpark job           │
│  └── SCD Type 2 updates on dim_routes                │
│  └── Incremental watermark-based processing          │
│  BigQuery Gold (star schema):                        │
│  └── fact_flight_prices                              │
│  └── dim_routes (SCD Type 2)                         │
│  └── dim_airlines                                    │
│  └── dim_date                                        │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              ORCHESTRATION + DQ LAYER  🔜 Week 4     │
│                                                      │
│  Cloud Composer (managed Airflow)                    │
│  └── Rule-based DQ (row counts, nulls, freshness)    │
│  └── Statistical anomaly detection (3-sigma)         │
│  └── Late arrivals reconciliation DAG                │
│  └── Lineage tracking table                          │
│  └── Cloud Monitoring alerts                         │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────┐
│            INFRASTRUCTURE AS CODE      🔜 Week 5     │
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
│               ANALYTICS SERVING LAYER  🔜 Weeks 6-7 │
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
│                    AI LAYER            🔜 Weeks 8-9  │
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
| Stream Processing | Apache Beam 2.74.0, Google Dataflow |
| Batch Processing | Apache Spark, Google Dataproc |
| Storage | GCS (Bronze/Silver/Gold), BigQuery |
| Orchestration | Cloud Composer (managed Airflow) |
| Infrastructure | Terraform |
| Analytics Serving | Snowflake (Snowpipe, Snowpark, Dynamic Tables, Cortex AI) |
| AI Layer | Claude API, pgvector, Cloud SQL PostgreSQL |
| Frontend | Streamlit, Cloud Run |
| Language | Python |

---

## What's Built

### ✅ Week 1 — Ingestion Pipeline

**Function A — Route Refresher (daily, midnight UTC)**
- Calls AviationStack API to fetch all scheduled ORD departures
- Filters valid flights (removes records with null airline, flight number, or IATA codes)
- Saves full raw dataset to GCS Bronze (`bronze/routes/{date}.json`)
- Derives top 20 routes by flight frequency empirically from real traffic volume
- Saves curated route list to GCS Silver (`silver/active_routes.json`)
- Uses 1 of 100 free monthly API calls — designed for sustainability within free tier

**Function B — Price Ticker (every 60 seconds)**
- Reads active route list from GCS Silver — no API call required
- Generates realistic simulated prices per route:
  - Base price per route (domestic short-haul cheap, international expensive)
  - ±20% random variance per tick
  - 10% time-of-day factor during peak hours (7-9am, 5-7pm UTC)
- Publishes Avro-encoded message to Pub/Sub for each route
- 20 messages published per execution, every minute, 24/7

**Message Queue Infrastructure**
- Pub/Sub topic `flight-prices-raw` with Avro schema enforcement
- Schema Registry — messages validated at publish time, malformed records rejected before entering pipeline
- Dead letter topic `flight-prices-dlq` — messages failing 5 delivery attempts routed here
- Subscription with 60-second ack deadline and 7-day retention
- Least-privilege IAM service account `skypulse-ingestion-sa`

**GCS Storage**
- Bucket `skypulse-triptide` in `us-central1`
- Bronze/Silver/Gold folder structure
- Lifecycle policy: Bronze → Nearline after 30 days → Coldline after 90 days

---

### ✅ Week 2 — Stream Processing Pipeline

**Apache Beam pipeline on Google Dataflow:**

```
Pub/Sub (flight-prices-sub)
        ↓ ReadFromPubSub
Raw bytes
        ↓ ParseMessage (DoFn)
Python dicts — validates required fields, drops malformed
        ↓ AssignTimestamp (DoFn)
Timestamped by event_timestamp — windows by when price happened, not when published
        ↓ SplitLateMessages (DoFn)
        ├── on_time → Fixed Window (5 min) → GroupByKey → Aggregate → BQ price_5min_avg
        ├── on_time → Sliding Window (1hr/15min) → GroupByKey → Aggregate → BQ price_1hr_trend
        └── late (>2 min) → BQ late_arrivals
```

**Key pipeline decisions:**
- Event time windowing (not processing time) for correctness
- Allowed lateness: 2 minutes — window state held open for late arrivals
- Side output for messages beyond allowed lateness — zero data loss
- Accumulating mode — updated results include all data, not just deltas
- Exactly-once processing via Dataflow Streaming Engine
- Tables auto-created by Beam on first write (`CREATE_IF_NEEDED`)

**BigQuery tables (Silver layer):**

| Table | Description | Rows |
|---|---|---|
| `price_5min_avg` | 5-minute windowed avg price per route | 11,610+ |
| `price_1hr_trend` | 1-hour sliding window avg for trend detection | Growing |
| `late_arrivals` | Messages arriving >2 min late — investigation queue | 0 (healthy) |

**Sample query results:**
```sql
SELECT route_id, window_start, window_end, avg_price_usd, message_count
FROM skypulse.price_5min_avg
ORDER BY window_start DESC LIMIT 5

-- ORD-LGA  | 06:05 | 06:10 | $159.48 | 5 messages
-- ORD-MIA  | 06:05 | 06:10 | $192.68 | 5 messages
-- ORD-LAX  | 06:05 | 06:10 | $185.20 | 5 messages
```

---

## Architectural Decisions

### Why Pub/Sub over Kafka?
Pub/Sub is fully managed with zero operational overhead — no brokers, no partition management, no ZooKeeper. It integrates natively with Dataflow, eliminating connectors. Replay capability — the main reason to choose Kafka — is handled at the GCS layer instead. Raw Avro files in Bronze serve as the reprocessing source, which is a cleaner separation of concerns than relying on Kafka's log retention. Kafka would be the right choice if the architecture required message-level replay, strict cross-partition ordering, or existing Kafka investment.

### Why separate Function A and Function B?
Flight schedules are essentially static throughout a given day — set in advance, don't change minute to minute. Prices are the genuinely volatile signal. Refreshing schedule data once daily from a live API and streaming price ticks every 60 seconds reflects the actual rate of change of each data type. This design also stays within the 100 requests/month free tier by making exactly one API call per day.

### Why fixed windows AND sliding windows?
They answer different business questions. Fixed windows (5-minute) produce a clean time-series of average prices suitable for storage and trend charts — one row per route per 5-minute interval. Sliding windows (1-hour, advancing every 15 minutes) produce a rolling average that smooths short-term spikes and is better suited for anomaly detection — comparing today's rolling average against a 7-day baseline.

### Why event time over processing time?
Windowing by processing time would assign messages to windows based on when Dataflow received them — which is always slightly later than when the price event occurred. A message delayed by network or retry would land in the wrong window, producing incorrect aggregations. Windowing by event time ensures every price tick is assigned to the window when the price actually existed, regardless of delivery delay.

### Why side outputs for late data instead of dropping?
No data is truly lost with side outputs — late messages land in `late_arrivals` where they can be investigated and reconciled. A daily Cloud Composer DAG (Week 4) will check `late_arrivals` volume, alert if above threshold, and reprocess significant volumes back into the main aggregation. Dropping late data silently is never acceptable in a production analytics system.

### Why BigQuery for processing and Snowflake for serving?
BigQuery is optimized for large-scale analytical processing — partitioned tables, slot-based compute, native Dataflow integration. Snowflake is optimized for serving — virtual warehouses that auto-suspend, result cache for repeated queries, Cortex AI for natural language. Separating processing and serving avoids running heavy transformations in Snowflake compute credits.

### Why SCD Type 2 for the route dimension?
Routes change over time — airlines add and drop routes, new carriers enter markets. SCD Type 2 preserves history by closing the old row and inserting a new one rather than overwriting. Historical price facts can always be joined back to the route as it existed at the time — essential for accurate trend analysis.

### Why Avro over JSON for Pub/Sub messages?
Avro enforces schema at publish time via the Schema Registry — malformed messages are rejected before entering the pipeline. It also produces smaller messages (binary format, field names in schema not in every message). JSON encoding used in development for easier debugging, with the option to switch to binary encoding in production.

---

## Data Model (BigQuery Gold Layer — Week 3)

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

## Active Routes

Top 20 ORD departure routes by daily flight frequency (derived from real AviationStack data):

| Route | Destination | Category |
|---|---|---|
| ORD-SAN | San Diego | Domestic |
| ORD-PHX | Phoenix | Domestic |
| ORD-BOS | Boston | Domestic |
| ORD-SFO | San Francisco | Domestic |
| ORD-DFW | Dallas/Fort Worth | Domestic |
| ORD-LAX | Los Angeles | Domestic |
| ORD-JFK | New York JFK | Domestic |
| ORD-MIA | Miami | Domestic |
| ORD-SEA | Seattle | Domestic |
| ORD-DEN | Denver | Domestic |
| ORD-LGA | New York LaGuardia | Domestic |
| ORD-DCA | Washington DC | Domestic |
| ORD-STL | St. Louis | Domestic |
| ORD-CLT | Charlotte | Domestic |
| ORD-CLE | Cleveland | Domestic |
| ORD-CUN | Cancun | International |
| ORD-MEX | Mexico City | International |
| ORD-YYC | Calgary | International |
| ORD-PVG | Shanghai | International |
| ORD-CDG | Paris | International |

Routes derived empirically from real AviationStack traffic volume, not hardcoded assumptions.

---

## What's Coming

### 🔜 Week 3 — BigQuery Modeling + Dataproc Batch
- Star schema in BigQuery Gold layer (fact + 3 dimensions)
- SCD Type 2 implementation on dim_routes
- Dataproc PySpark job for historical batch processing
- Watermark table for incremental processing
- Partition on price_date, cluster on route_id + airline_code
- Broadcast joins for small dimension tables

### 🔜 Week 4 — Cloud Composer + Data Quality
- Cloud Composer DAG orchestrating the full pipeline
- Rule-based DQ checks (row counts, null percentages, freshness)
- Statistical anomaly detection (3-sigma price deviation from 7-day rolling mean)
- Late arrivals reconciliation — reprocess into main aggregation daily
- Pipeline lineage tracking table
- Cloud Monitoring alerts (Pub/Sub lag, Dataflow errors, DAG failures, BQ slot utilization)

### 🔜 Week 5 — Terraform
- Full GCP infrastructure as code
- GCS, Pub/Sub, BigQuery, IAM, Cloud Composer all provisioned via Terraform
- Secret Manager for API key management
- Reproducible dev and prod environments

### 🔜 Weeks 6-7 — Snowflake Integration
- Snowpipe continuous ingestion from GCS Bronze
- Snowpark Python transformations
- Dynamic Tables for materialized hourly price summary
- Cortex Analyst for natural language querying
- SnowPro Core certification

### 🔜 Weeks 8-9 — AI Layer
- pgvector on Cloud SQL PostgreSQL for route embeddings
- RAG pipeline: question → embed → vector search → context injection
- Multi-step Claude API agent (route search + BigQuery query + Snowflake query)
- Streamlit chat UI deployed on Cloud Run

### 🔜 Week 10 — Polish
- Architecture diagram (Excalidraw)
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
├── functions/
│   ├── function_a/
│   │   ├── main.py                # Daily route refresh from AviationStack
│   │   └── requirements.txt
│   └── function_b/
│       ├── main.py                # 60-second price ticker → Pub/Sub
│       └── requirements.txt
└── dataflow/
    ├── pipeline.py                # Apache Beam streaming pipeline
    └── requirements.txt
```

---

## Setup

### Prerequisites
- GCP account with billing enabled and $300 free credits activated
- `gcloud` CLI installed and authenticated
- Python 3.11+ (Beam 2.74.0 requires Python 3.11-3.13)
- AviationStack free API key (aviationstack.com)

### Deploy Ingestion Layer (Week 1)

```bash
git clone https://github.com/sarthak2504/skypulse-travel-intelligence.git
cd skypulse-travel-intelligence

gcloud auth login
gcloud config set project YOUR_PROJECT_ID

gcloud services enable pubsub.googleapis.com storage.googleapis.com \
  cloudfunctions.googleapis.com cloudscheduler.googleapis.com \
  iam.googleapis.com cloudbuild.googleapis.com run.googleapis.com

cd functions/function_a
gcloud functions deploy function-a-route-refresh --gen2 --runtime=python311 \
  --region=us-central1 --source=. --entry-point=run --trigger-http \
  --set-env-vars=AVIATIONSTACK_API_KEY=YOUR_KEY --memory=256MB --timeout=120s

cd ../function_b
gcloud functions deploy function-b-price-ticker --gen2 --runtime=python311 \
  --region=us-central1 --source=. --entry-point=run --trigger-http \
  --memory=256MB --timeout=60s
```

### Deploy Streaming Pipeline (Week 2)

```bash
gcloud services enable dataflow.googleapis.com bigquery.googleapis.com

cd dataflow
python -m venv venv --system-site-packages
venv\Scripts\activate  # Windows
pip install apache-beam[gcp]==2.74.0

python pipeline.py
```

Full setup including IAM, Pub/Sub, schema configuration documented in `commands.md`.

---

## Cost

| Week | Services | Estimated cost |
|---|---|---|
| 1 | Cloud Functions, Pub/Sub, GCS, Scheduler | $0 (free tier) |
| 2 | + Dataflow (~15 min testing) | ~$0.10 |
| 3 | + Dataproc | ~$10-15 |
| 4 | + Cloud Composer | ~$20-25 |
| 5 | Terraform only | ~$5 |
| 6-7 | + Snowflake (free trial) | ~$10-15 GCP |
| 8-9 | + Cloud Run + Cloud SQL | ~$10-15 |
| 10 | Polish, mostly idle | ~$5 |

**Total estimated: $70-90 over 10 weeks — fully covered by GCP $300 free trial credits.**

---

## Author

**Sarthak Bhingarde**
Data Engineer III → Solutions Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, University of Illinois Urbana-Champaign (4.0 GPA)
9 years experience: Teradata, AGCO, Expedia Group