# SkyPulse ✈️ &nbsp; <img src="/docs/favicon.ico" height="40"/> &nbsp; <img src="/docs/snowflake_computing_logo.jpeg" height="40"/>

**A real-time flight price intelligence platform built on GCP + Snowflake.**

SkyPulse ingests live flight data, streams pricing signals through a production-grade pipeline, detects price anomalies using statistical analysis, and serves analytics through both SQL and natural language interfaces.

Built to demonstrate end-to-end data engineering across GCP, Apache Beam, PySpark, Airflow, Snowflake, Terraform, and the Claude API.

---

## What It Does

SkyPulse monitors flight prices across 36 ORD departure routes — domestic and international — and answers questions like:

- Is ORD-LAX cheaper right now than usual?
- Which airline is consistently cheapest on this route?
- Did something unusual just happen with ORD-JFK prices?
- Which routes are cheapest on weekend mornings in July?
- Show me flights from Chicago to Austin this summer

Prices are streamed every 60 seconds. Anomalies are detected by comparing each 5-minute price snapshot against a 1-hour rolling baseline. The full day's data lands in a star schema queryable by any BI tool or in plain English via Cortex Analyst.

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

### ✅ Orchestration + Data Quality
A Cloud Composer (managed Airflow) DAG runs daily at 6am UTC:

```
check_silver_freshness     → Silver must have data within last 2 hours
        ↓
check_silver_row_counts    → minimum 100 new rows expected daily
        ↓
check_null_rates           → critical fields must be <1% null
        ↓
run_spark_batch_job        → triggers Dataproc Serverless batch_job.py
        ↓
check_gold_freshness       → Gold must receive new rows after Spark
        ↓
check_late_arrivals_volume → alerts if late rate exceeds 10%
        ↓
reconcile_late_arrivals    → reprocesses late data if threshold exceeded
        ↓
write_audit_log            → records run summary to pipeline_audit_log
```

First successful run: 21,641 Silver rows processed → 11,191 Gold rows written → 4,131 late arrivals reconciled.

### ✅ Infrastructure as Code
Terraform provisions the entire GCP stack — GCS bucket, Pub/Sub topics, IAM service accounts, BigQuery datasets and tables, Cloud Scheduler jobs, and Secret Manager. All resources defined as code for reproducible deployments.

### ✅ Snowflake Analytics Layer
Snowflake serves as the analytics serving layer on top of the GCP processing pipeline:

- **Snowpipe** continuously ingests daily AviationStack schedule files from GCS Bronze
- **Silver table** (`raw_flights`) — 1,897 rows of real flight schedule data across 19 days
- **Gold tables** — denormalized price summaries by flight and route
- **Dynamic Table** (`hourly_route_summary`) — auto-refreshes every hour, always current
- **Semantic View** — defines business vocabulary (routes, prices, seasons, alliances)
- **Cortex Analyst** — natural language queries: "show me flights from Chicago to Austin" → SQL → answer

### 🔜 AI Layer (Weeks 8-9)
Claude API agent with vector search over routes, BigQuery and Snowflake query tools, and a Streamlit chat interface on Cloud Run.

---

## Architecture

```
AviationStack API
        │ (1 call/day — real ORD schedules)
        ▼
Cloud Function A ──────────────────────► GCS Bronze ──────────────────► Snowflake
(daily)                                  (raw schedules)    Snowpipe    SILVER.raw_flights
                                                                              │
Cloud Function B ◄────────────────────────────┘                    SQL transforms
(every 60 sec)                                                            │
        │ Avro messages                                          Snowflake GOLD
        ▼                                                        Dynamic Tables
   Pub/Sub ──────────────────────────────► Dead Letter Queue     Semantic View
(flight-prices-raw)                      (flight-prices-dlq)     Cortex Analyst
        │                                                              ↑
        ▼                                                    Natural language queries
   Dataflow (Apache Beam)
        ├── Fixed Window 5min ──────────► BigQuery Silver
        ├── Sliding Window 1hr ─────────► price_5min_avg
        └── Late Arrivals ──────────────► price_1hr_trend
                                          late_arrivals
                                              │
                              Cloud Composer (Airflow DAG)
                              ├── DQ checks
                              ├── Trigger Dataproc Serverless
                              ├── Late arrival reconciliation
                              └── Audit log
                                              │
                                    PySpark (Dataproc Serverless)
                                              │
                                              ▼
                                    BigQuery Gold (star schema)
                                    ├── fact_flight_prices (11,191+ rows)
                                    ├── dim_flights (36 rows, SCD Type 2)
                                    ├── dim_airlines (5 rows)
                                    └── dim_date (3 rows)
```

---

## Data Model

### BigQuery Gold (GCP)
**Grain:** one row = one flight × one 5-minute window

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

### Snowflake (Analytics Serving)
```
SILVER.raw_flights          ← real AviationStack schedule data
GOLD.flight_price_summary   ← per flight per day with prices
GOLD.route_stats            ← per route per day aggregated
ANALYTICS.hourly_route_summary ← Dynamic Table, auto-refreshes hourly
ANALYTICS.flight_price_semantic_view ← powers Cortex Analyst
```

---

## Sample Business Queries

```sql
-- BigQuery: Which airline is cheapest on ORD-LAX?
SELECT a.airline_name, AVG(f.avg_price_usd) as avg_price
FROM fact_flight_prices f
JOIN dim_flights fl ON f.flight_key = fl.flight_key
JOIN dim_airlines a ON f.airline_key = a.airline_key
WHERE fl.route_id = 'ORD-LAX'
GROUP BY a.airline_name ORDER BY avg_price ASC

-- Snowflake: Price anomalies today
SELECT route_id, avg_price, total_anomalies
FROM SKYPULSE.ANALYTICS.hourly_route_summary
WHERE total_anomalies > 0
ORDER BY total_anomalies DESC

-- Snowflake Cortex Analyst: plain English
-- "Show me the cheapest flights from Chicago to Asia this summer"
-- "Which routes are most expensive on weekends?"
-- "How many airlines fly from ORD to London?"
```

---

## Active Routes

36 real ORD departures covering 6am-11:30pm CT across 5 airlines:

| Airline | Alliance | Sample Routes |
|---|---|---|
| United Airlines | Star Alliance | ORD-LAX, ORD-SFO, ORD-NRT, ORD-LHR, ORD-PVG |
| American Airlines | Oneworld | ORD-JFK, ORD-MIA, ORD-CDG, ORD-GRU |
| Delta Air Lines | SkyTeam | ORD-ATL, ORD-BOS, ORD-AMS |
| Southwest Airlines | None | ORD-DEN, ORD-DFW, ORD-STL |
| Lufthansa | Star Alliance | ORD-FRA |

---

## Tech Stack

| Layer | Technology | Status |
|---|---|---|
| Ingestion | Cloud Functions (Python), Cloud Scheduler | ✅ |
| Data source | AviationStack API + simulated prices | ✅ |
| Messaging | GCP Pub/Sub, Avro Schema Registry | ✅ |
| Stream Processing | Apache Beam 2.74.0, Google Dataflow | ✅ |
| Batch Processing | PySpark, Dataproc Serverless | ✅ |
| Orchestration | Cloud Composer (managed Airflow) | ✅ |
| Storage | GCS Bronze/Silver/Gold, BigQuery | ✅ |
| Infrastructure | Terraform | ✅ |
| Analytics Serving | Snowflake (Snowpipe, Dynamic Tables, Cortex Analyst) | ✅ |
| AI | Claude API, pgvector, Cloud Run, Streamlit | 🔜 Weeks 8-9 |

---

## Key Design Decisions

**Why event time windowing?**
Flight prices should be assigned to the window when they occurred, not when Dataflow received them.

**Why two Silver tables (5min + 1hr)?**
5-minute windows show what the price is right now. 1-hour sliding windows show the typical price. Anomaly detection needs both.

**Why Silver has duplicates?**
BigQuery streaming inserts are append-only. Accumulating mode writes a second row when late data updates a window. Spark deduplicates when writing to Gold.

**Why SCD Type 2 for flights?**
If an airline changes routes, historical price facts should join to the correct airline for their time period.

**Why both BigQuery AND Snowflake?**
BigQuery handles heavy streaming processing and anomaly detection. Snowflake handles analytics serving and natural language queries via Cortex Analyst. In production you'd choose one — for a portfolio project demonstrating both platforms is the goal.

**Why Pub/Sub over Kafka?**
Fully managed, native Dataflow integration, replay handled at GCS layer.

---

## Repository

```
skypulse-travel-intelligence/
├── README.md                         ← this file
├── README_TECHNICAL.md               ← architecture diagrams, setup
├── commands.md                       ← every CLI command used
├── docs/
│   └── streaming_writes_reference.md ← BigQuery vs Iceberg, upserts, z-scores
├── schemas/
│   └── flight_price.avsc             ← Avro schema
├── functions/
│   ├── function_a/                   ← daily route refresh
│   └── function_b/                   ← 60-second price ticker
├── dataflow/
│   └── pipeline.py                   ← Apache Beam streaming pipeline
├── spark/
│   └── batch_job.py                  ← daily PySpark Gold layer job
├── dags/
│   └── skypulse_daily_dag.py         ← Cloud Composer Airflow DAG
├── terraform/
│   ├── main.tf                       ← all GCP resources as IaC
│   ├── variables.tf
│   ├── outputs.tf
│   └── provider.tf
└── snowflake/
    └── setup.sql                     ← all Snowflake SQL in order
```

---

## Author

**Sarthak Bhingarde** — Data Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, UIUC (4.0 GPA) · 9 years experience · Teradata · AGCO · Expedia Group