# SkyPulse ✈️ GCP [<img alt="Google Cloud Logo" src="/docs/favicon.ico" height="50" align="right"/>](https://cloud.google.com/icons)



**A real-time flight price intelligence platform built on GCP.**

SkyPulse ingests live flight data, streams pricing signals through a production-grade pipeline, detects price anomalies using statistical analysis, and serves analytics through both SQL and natural language interfaces.

Built to demonstrate end-to-end data engineering across GCP, Apache Beam, PySpark, Airflow, Snowflake, Terraform, and the Claude API.

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
(daily)                                  (raw schedules)
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
                              Cloud Composer (Airflow DAG)
                              ├── DQ checks (freshness, counts, nulls)
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

## Sample Business Queries

```sql
-- Which airline is cheapest on ORD-LAX?
SELECT a.airline_name, AVG(f.avg_price_usd) as avg_price
FROM fact_flight_prices f
JOIN dim_flights fl ON f.flight_key = fl.flight_key
JOIN dim_airlines a ON f.airline_key = a.airline_key
WHERE fl.route_id = 'ORD-LAX'
GROUP BY a.airline_name
ORDER BY avg_price ASC

-- Price anomalies today
SELECT fl.flight_number, fl.route_id, f.window_start, f.avg_price_usd, f.z_score
FROM fact_flight_prices f
JOIN dim_flights fl ON f.flight_key = fl.flight_key
WHERE f.is_anomaly = true
AND DATE(f.window_start) = CURRENT_DATE()
ORDER BY f.z_score DESC

-- Are prices higher on weekends?
SELECT d.is_weekend, AVG(f.avg_price_usd) as avg_price
FROM fact_flight_prices f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
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
| Infrastructure | Terraform | 🔜 Week 5 |
| Analytics | Snowflake (Snowpipe, Snowpark, Cortex AI) | 🔜 Weeks 6-7 |
| AI | Claude API, pgvector, Cloud Run, Streamlit | 🔜 Weeks 8-9 |

---

## Key Design Decisions

**Why event time windowing?**
Flight prices should be assigned to the window when they occurred, not when Dataflow received them. A message delayed by 90 seconds belongs in the 08:00-08:05 window, not 08:05-08:10.

**Why two Silver tables (5min + 1hr)?**
5-minute windows show what the price is right now. 1-hour sliding windows show the typical price. Anomaly detection needs both — compare the volatile signal against the stable baseline.

**Why Silver has duplicates?**
BigQuery streaming inserts are append-only. Dataflow's accumulating mode writes a second row when late data updates a window. The daily Spark job deduplicates when writing to Gold.

**Why SCD Type 2 for flights?**
If an airline changes routes, historical price facts should join to the correct airline for their time period. SCD Type 2 preserves this history.

**Why Cloud Composer for orchestration?**
Airflow provides dependency management between tasks, retry logic, branching (late arrival reconciliation only triggers when threshold exceeded), and a visual DAG graph for monitoring. All more robust than cron + shell scripts.

**Why Pub/Sub over Kafka?**
Fully managed, native Dataflow integration, replay handled at GCS layer.

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
├── spark/
│   └── batch_job.py                  ← daily PySpark Gold layer job
└── dags/
    └── skypulse_daily_dag.py         ← Cloud Composer Airflow DAG
```

---

## Author

**Sarthak Bhingarde** — Data Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, UIUC (4.0 GPA) · 9 years experience · Teradata · AGCO · Expedia Group