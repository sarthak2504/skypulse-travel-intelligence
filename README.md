# SkyPulse ✈️

**A real-time flight price intelligence platform built on GCP.**

SkyPulse ingests live flight data, streams pricing signals through a production-grade pipeline, detects price anomalies using statistical analysis, and serves analytics through both SQL and natural language interfaces.

Built to demonstrate end-to-end data engineering across GCP, Apache Beam, PySpark, Snowflake, Terraform, and the Claude API.

---

## What It Does

SkyPulse monitors flight prices across 36 ORD departure routes — domestic and international — and answers questions like:

- Is ORD-LAX cheaper right now than usual?
- Which airline is consistently cheapest on this route?
- Did something unusual just happen with ORD-JFK prices?
- Which routes are cheapest on weekend mornings in July?

Prices are streamed every 60 seconds. Anomalies are detected by comparing each 5-minute price snapshot against a 1-hour rolling baseline. The full day's data lands in a star schema queryable by any BI tool or natural language interface.

---

## What's Built

### ✅ Ingestion Layer
Two Cloud Functions running on automated schedules:

**Route Refresher (daily)** calls AviationStack to fetch real ORD flight schedules, extracts operating carriers from codeshare data, and saves a clean 36-flight schedule to GCS.

**Price Ticker (every 60 seconds)** reads that schedule, generates a simulated price for every active flight using distance-based pricing with ±20% variance, and publishes to Pub/Sub. Once a flight departs, prices stop. At midnight the schedule refreshes.

5% of messages are deliberately backdated to simulate late arrivals and exercise the late data handling infrastructure.

### ✅ Message Queue
Pub/Sub sits between the producer and Dataflow with Avro schema enforcement — malformed messages are rejected before they enter the pipeline. A dead letter topic catches messages that fail processing after 5 retries.

### ✅ Streaming Pipeline
An Apache Beam pipeline on Google Dataflow reads continuously from Pub/Sub and produces two aggregations:

**5-minute fixed windows** — average price per flight per 5-minute interval. The primary price signal, volatile, used for anomaly detection.

**1-hour sliding windows** — rolling average advancing every 15 minutes. Smooth baseline that dampens short-term noise, used as the comparison point.

The pipeline windows by event time (when the price happened) not processing time (when Dataflow received it). Late messages route to a side output rather than being dropped. Exactly-once processing via Dataflow Streaming Engine.

### ✅ Batch Processing + Gold Layer
A daily PySpark job on Dataproc Serverless transforms the raw Silver data into a clean star schema:

- Deduplicates Silver (accumulating mode produces two rows per window — batch keeps the best one)
- Populates `dim_date`, `dim_airlines`, `dim_flights` (SCD Type 2)
- Computes z-scores by comparing 5-minute prices against 1-hour rolling stats
- Flags windows where z-score > 3 as anomalies
- Writes clean rows to `fact_flight_prices`
- Updates a watermark table for incremental processing

### 🔜 Orchestration + Data Quality (Week 4)
Cloud Composer DAG triggering the daily Spark job, running rule-based DQ checks, reconciling late arrivals, and sending pipeline health alerts.

### 🔜 Infrastructure as Code (Week 5)
Terraform provisioning the entire GCP stack. Secret Manager for API keys.

### 🔜 Snowflake Integration (Weeks 6-7)
Snowpipe continuous ingestion, Snowpark transformations, Dynamic Tables for hourly summaries, Cortex Analyst for natural language SQL.

### 🔜 AI Layer (Weeks 8-9)
Claude API agent with vector search over routes, BigQuery and Snowflake query tools, and a Streamlit chat interface on Cloud Run.

---

## Architecture

```
AviationStack API
        │ (1 call/day — real ORD schedules)
        ▼
Cloud Function A ──────────────────────► GCS Bronze
(daily, 6am UTC)                         (raw schedules)
                                              │
                                         GCS Silver
                                         (active_routes)
                                              │
Cloud Function B ◄────────────────────────────┘
(every 60 sec)
        │ Avro messages
        ▼
   Pub/Sub ──────────────────────────────► Dead Letter Queue
(flight-prices-raw)                      (flight-prices-dlq)
        │
        ▼
   Dataflow (Apache Beam)
        ├── Fixed Window 5min ──────────► BigQuery Silver
        ├── Sliding Window 1hr ─────────► price_5min_avg
        └── Late Arrivals ──────────────► price_1hr_trend
                                          late_arrivals
                                              │
                                    PySpark (Dataproc Serverless)
                                    (daily batch job)
                                              │
                                              ▼
                                    BigQuery Gold (star schema)
                                    ├── fact_flight_prices
                                    ├── dim_flights (SCD Type 2)
                                    ├── dim_airlines
                                    └── dim_date
```

---

## Data Model

**Grain:** one row in `fact_flight_prices` = one flight × one 5-minute window

```
fact_flight_prices
├── flight_key    → dim_flights  (which flight, SCD Type 2)
├── airline_key   → dim_airlines (which airline, which alliance)
├── date_key      → dim_date     (weekend? holiday? season?)
├── window_start / window_end
├── avg_price_usd / min_price_usd / max_price_usd
├── message_count (how many ticks in this window)
├── z_score       (how many std devs from 1hr rolling mean)
└── is_anomaly    (z_score > 3)
```

---

## Active Routes

36 real ORD departures covering 6am-11:30pm CT across 5 airlines:

| Airline | Alliance | Routes |
|---|---|---|
| United Airlines | Star Alliance | ORD-LAX, ORD-SFO, ORD-SEA, ORD-DEN, ORD-LGA, ORD-NRT, ORD-LHR, ORD-PVG |
| American Airlines | Oneworld | ORD-JFK, ORD-MIA, ORD-DFW, ORD-PHX, ORD-BOS, ORD-SEA, ORD-CDG, ORD-GRU |
| Delta Air Lines | SkyTeam | ORD-ATL, ORD-BOS, ORD-ATL, ORD-AMS |
| Southwest Airlines | None | ORD-DEN, ORD-DFW, ORD-STL |
| Lufthansa | Star Alliance | ORD-FRA |

Domestic prices range $80-250. International $600-1000. All with ±20% random variance and 10% peak-hour markup.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Cloud Functions (Python), Cloud Scheduler |
| Data source | AviationStack API (real schedules), simulated prices |
| Messaging | GCP Pub/Sub, Avro Schema Registry |
| Stream Processing | Apache Beam 2.74.0, Google Dataflow |
| Batch Processing | PySpark, Dataproc Serverless |
| Storage | GCS (Bronze/Silver/Gold), BigQuery |
| Orchestration | Cloud Composer — Week 4 |
| Infrastructure | Terraform — Week 5 |
| Analytics | Snowflake (Snowpipe, Snowpark, Cortex AI) — Weeks 6-7 |
| AI | Claude API, pgvector, Cloud Run, Streamlit — Weeks 8-9 |

---

## Key Design Decisions

**Why event time windowing?**
Flight prices should be assigned to the window when they occurred, not when Dataflow received them. A message delayed by 90 seconds belongs in the 08:00-08:05 window, not the 08:05-08:10 window.

**Why two Silver tables (5min + 1hr)?**
They answer different questions. 5-minute windows show what the price is right now. 1-hour sliding windows show what the price typically is. Anomaly detection needs both — compare the volatile signal against the stable baseline.

**Why Silver has duplicates?**
BigQuery streaming inserts are append-only. Dataflow's accumulating mode writes a second row when late data updates a window. The daily Spark job deduplicates when writing to Gold. Silver is raw and immutable; Gold is clean and analytical.

**Why SCD Type 2 for flights?**
If an airline changes which routes it flies, historical price facts should still join to the correct airline for their time period. SCD Type 2 closes the old record and inserts a new one rather than overwriting.

**Why Pub/Sub over Kafka?**
Fully managed, native Dataflow integration, replay handled at GCS layer. Kafka would be chosen if message-level replay or strict ordering were required.

---

## Repository

```
skypulse-travel-intelligence/
├── README.md                         ← this file
├── README_TECHNICAL.md               ← architecture diagrams, setup instructions
├── commands.md                       ← every GCP CLI command used, with explanations
├── docs/
│   └── streaming_writes_reference.md ← BigQuery vs Iceberg, upserts, z-scores
├── schemas/
│   └── flight_price.avsc             ← Avro schema registered in Pub/Sub
├── functions/
│   ├── function_a/                   ← daily route refresh
│   └── function_b/                   ← 60-second price ticker
├── dataflow/
│   └── pipeline.py                   ← Apache Beam streaming pipeline
└── spark/
    └── batch_job.py                  ← daily PySpark Gold layer job
```

---

## Author

**Sarthak Bhingarde** — Data Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, UIUC (4.0 GPA) · 9 years experience · Teradata · AGCO · Expedia Group