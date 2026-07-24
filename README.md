# SkyPulse — Real-Time Travel Intelligence Platform

> *A production-grade data engineering platform built from the ground up to demonstrate end-to-end expertise across GCP, Apache Beam, Spark, Snowflake, Terraform, and the Claude API.*

---

## The Problem Worth Solving

Flight prices are one of the most volatile datasets in the world. A ticket for ORD-LAX can swing from $150 to $450 within hours based on seat inventory, competitor pricing, demand signals, and dozens of other factors. Travel companies like Expedia track these signals continuously — but most data engineers have never built the infrastructure to do it themselves.

SkyPulse is that infrastructure. Built from scratch on GCP, it ingests real flight schedule data, streams simulated pricing signals through a multi-layer pipeline, detects anomalies in near real time, and serves analytics through both SQL and natural language interfaces.

Every architectural decision in this project maps to a real business need. Nothing was added for complexity's sake.

---

## How It Started — The Data Source

The first challenge was getting real data. AviationStack's free tier returns live flight schedules — real airlines, real routes, real departure times — but only 100 API calls per month and no pricing data.

Rather than pretend this wasn't a constraint, the architecture was designed around it. A daily Cloud Function (Function A) calls AviationStack once per day to fetch all scheduled ORD departures, extracts operating carriers from codeshare data, and saves a clean flight schedule to GCS. A second function (Function B) runs every 60 seconds, reads that schedule, and generates realistic simulated prices for every active flight — stopping when a flight's departure time passes.

This split reflects how real travel systems actually work. Flight schedules are static throughout a day — they're set in advance. Prices are volatile — they change every few seconds. Refreshing schedule data once and streaming price signals continuously matches the actual rate of change of each data type.

The price simulation uses distance-based pricing (domestic short-haul $80-150, transatlantic $600-1000) with ±20% random variance and a 10% peak-hour markup. 5% of messages are deliberately backdated by 3-5 minutes to simulate late arrivals — generating real data for the late arrival handling infrastructure.

---

## The Ingestion Layer — Making Data Move

Before any processing can happen, data needs to flow reliably from Function B into the pipeline. That's Pub/Sub's job.

Pub/Sub sits between the producer (Function B) and consumer (Dataflow) as a durable message queue. Messages are published with Avro schema enforcement — malformed messages are rejected at the topic level before they enter the pipeline. A dead letter topic catches messages that fail processing after 5 retries.

The choice of Pub/Sub over Kafka was deliberate. Pub/Sub is fully managed with zero operational overhead, integrates natively with Dataflow, and replay capability — the main reason to choose Kafka — is handled at the GCS layer instead. Raw data saved to Bronze serves as the reprocessing source if needed.

Every message published contains: flight number, route, airline, simulated price, scheduled departure time, and two timestamps — when the price event occurred (`event_timestamp`) and when it was published (`ingestion_timestamp`). The difference between these two timestamps is what the late arrival detection uses.

---

## The Streaming Layer — Processing at Speed

Dataflow runs an Apache Beam pipeline that reads continuously from Pub/Sub and produces two types of aggregations in near real time.

**Fixed windows (5 minutes)** answer: "what was the average price for this flight in the last 5 minutes?" Every flight active in a given 5-minute period produces one aggregated row in BigQuery Silver. This is the primary price signal — volatile, changes every 5 minutes, used for anomaly detection.

**Sliding windows (1 hour, advancing every 15 minutes)** answer: "what has the typical price been over the last hour?" Each flight produces a rolling average that smooths out the natural ±20% variance in individual price ticks. This is the baseline — stable, changes slowly, used as the comparison point for anomaly detection.

The pipeline windows by **event time**, not processing time. This is the critical distinction. A price event that happened at 8:02 belongs in the 8:00→8:05 window regardless of when Dataflow received it. Windowing by processing time would silently assign late-arriving messages to the wrong windows, producing incorrect aggregations.

Late messages — those arriving more than 2 minutes after the watermark passes their window — are routed to a side output instead of being dropped. They land in a `late_arrivals` table where they can be investigated and reconciled by the daily batch job. No data is ever silently lost.

Dataflow's Streaming Engine provides exactly-once processing guarantees. The same price message will never be counted twice in an aggregation, even if Pub/Sub delivers it multiple times.

---

## The Storage Layer — Three Tiers for Three Purposes

GCS holds raw data in a Bronze/Silver/Gold folder structure with lifecycle policies that automatically move Bronze files to cheaper storage tiers as they age.

BigQuery Silver holds the streaming output — windowed price aggregations and late arrivals. Silver is append-only and intentionally allows duplicates. When a late message triggers a window update, Dataflow writes a second row rather than modifying the first. BigQuery streaming inserts are append-only by design — upserts are only possible in batch SQL.

BigQuery Gold holds the clean analytical data — a star schema built by the daily Spark batch job. The Gold layer has no duplicates, proper dimension keys, and anomaly flags. This is what business queries run against.

---

## The Batch Layer — Making Data Clean

Every night, a PySpark job runs on Dataproc Serverless and transforms Silver into Gold.

It starts by reading only new Silver data since the last run, tracked via a watermark table. This makes each run incremental — processing only what's new rather than reprocessing the entire history every day.

Deduplication comes first. The Beam pipeline's accumulating mode means each 5-minute window can appear twice in Silver — once when the window fires and again if a late message triggers an update. The Spark job keeps only the row with the highest `message_count` per flight per window, using a window function that partitions by `(flight_number, window_start)` and orders by `message_count DESC`.

Dimension tables are populated next. `dim_date` gets one row per unique date with weekend, holiday, and season flags. `dim_airlines` gets one row per airline with alliance membership and domestic/international classification. `dim_flights` uses SCD Type 2 — when a flight's attributes change, the old record is closed and a new one inserted, preserving the full history of which airline flew which route at any given time.

Anomaly detection joins the deduplicated 5-minute windows against the rolling stats computed from the 1-hour trend data. For each flight, it computes the mean and standard deviation of all 1-hour window averages, then calculates a z-score for each 5-minute window:

```
z_score = |avg_price - rolling_mean| / rolling_stddev
```

Windows with z-score > 3 are flagged as anomalies. In production with 7+ days of rolling data, this threshold catches genuine price spikes — flash sales, demand surges, data quality issues. In the early days of the pipeline, z-scores are inflated because standard deviation is computed from too few data points. This is expected and resolves naturally as historical data accumulates.

Finally, the fact table is populated by joining price data with dimension keys. Each row in `fact_flight_prices` represents one flight, one 5-minute window, and carries the anomaly flag and z-score alongside the price metrics.

---

## The Star Schema — Answering Business Questions

The Gold layer is a Kimball-style star schema with a clear grain: one row in `fact_flight_prices` represents the average price for one flight in one 5-minute window.

Four tables enable rich analytical queries:

**dim_flights** tracks which physical flights exist, with SCD Type 2 history. If American Airlines takes over a United route, historical price facts still join to the correct airline for their time period.

**dim_airlines** carries alliance membership. "Which alliance has the cheapest transatlantic prices?" becomes a simple join.

**dim_date** carries weekend, holiday, and season flags. "Are prices higher on holiday weekends?" becomes a simple filter.

**fact_flight_prices** is the center. Every analytical question starts here and joins outward.

```sql
-- Which airline is cheapest on ORD-LAX on weekends?
SELECT a.airline_name, AVG(f.avg_price_usd)
FROM fact_flight_prices f
JOIN dim_flights fl ON f.flight_key = fl.flight_key
JOIN dim_airlines a ON f.airline_key = a.airline_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE fl.route_id = 'ORD-LAX'
AND d.is_weekend = true
GROUP BY a.airline_name
ORDER BY 2 ASC
```

---

## What's Being Built Next

### Orchestration and Data Quality (Week 4)
Cloud Composer (managed Airflow) will orchestrate the full pipeline — triggering the daily Spark job, running data quality checks (row counts, null rates, freshness), reconciling late arrivals back into the main aggregation, and alerting when anomalies spike above threshold. Cloud Monitoring will watch Pub/Sub lag, Dataflow errors, and BigQuery slot utilization.

### Infrastructure as Code (Week 5)
Terraform will provision the entire GCP stack — every bucket, topic, dataset, service account, and IAM binding — from a single `terraform apply`. Secret Manager will replace environment variable API keys. This makes the environment reproducible across dev and prod with a single command.

### Snowflake Integration (Weeks 6-7)
Snowflake will serve as the analytics layer on top of the GCP processing layer. Snowpipe will continuously ingest Gold data from GCS into Snowflake. Snowpark Python transformations will replicate the key Spark logic in Snowflake's compute engine. Dynamic Tables will materialize hourly price summaries that auto-refresh as new data arrives. Cortex Analyst will enable natural language queries — "which routes are cheapest on Tuesday mornings?" answered in plain English by Snowflake's built-in LLM.

### AI Layer (Weeks 8-9)
A multi-step Claude API agent will tie everything together. Route descriptions and historical price summaries will be embedded using Claude's embeddings API and stored in pgvector on Cloud SQL. When a user asks a question in plain English, the agent will search for relevant routes via vector similarity, query BigQuery for historical analysis, query Snowflake for trend data, and synthesize a plain English answer with specific prices and recommendations. The full application will be deployed on Cloud Run with a Streamlit chat interface.

---

## The Active Routes

36 real ORD departure routes covering the full day, based on published timetables:

| Time (CT) | Flight | Route | Airline |
|---|---|---|---|
| 06:00 | UA500 | ORD-LAX | United |
| 06:30 | AA100 | ORD-JFK | American |
| 07:00 | DL200 | ORD-ATL | Delta |
| 08:30 | WN300 | ORD-DEN | Southwest |
| ... | ... | ... | ... |
| 20:30 | UA600 | ORD-LHR | United |
| 21:00 | AA200 | ORD-CDG | American |
| 21:30 | LH400 | ORD-FRA | Lufthansa |
| 22:00 | UA601 | ORD-NRT | United |
| 23:00 | UA602 | ORD-PVG | United |
| 23:30 | DL300 | ORD-AMS | Delta |

Prices stop being generated for each flight once its departure time passes. The pipeline naturally winds down through the day and resets at midnight when the schedule refreshes.

---

## Technical Stack

| Layer | Technology |
|---|---|
| Ingestion | Cloud Functions (Python), Cloud Scheduler, AviationStack API |
| Messaging | GCP Pub/Sub, Avro Schema Registry |
| Stream Processing | Apache Beam 2.74.0, Google Dataflow (us-east1) |
| Batch Processing | PySpark, Dataproc Serverless |
| Storage | GCS (Bronze/Silver/Gold), BigQuery Silver + Gold |
| Orchestration | Cloud Composer (Week 4) |
| Infrastructure | Terraform (Week 5) |
| Analytics Serving | Snowflake (Weeks 6-7) |
| AI Layer | Claude API, pgvector, Cloud Run, Streamlit (Weeks 8-9) |

---

## Project Structure

```
skypulse-travel-intelligence/
├── README.md                  # This file — project story
├── README_TECHNICAL.md        # Technical reference with architecture diagram
├── commands.md                # Every GCP CLI command used, with explanations
├── lifecycle.json             # GCS lifecycle policy
├── schemas/
│   └── flight_price.avsc      # Avro schema registered in Pub/Sub
├── functions/
│   ├── function_a/            # Daily route refresh (AviationStack)
│   └── function_b/            # 60-second price ticker → Pub/Sub
├── dataflow/
│   └── pipeline.py            # Apache Beam streaming pipeline
└── spark/
    └── batch_job.py           # Daily PySpark Gold layer job
```

---

## Author

**Sarthak Bhingarde**
Data Engineer → Solutions Engineer
[LinkedIn](https://linkedin.com/in/sarthakbhingarde) | [GitHub](https://github.com/sarthak2504)

MS Information Management, University of Illinois Urbana-Champaign (4.0 GPA)
9 years experience across Teradata, AGCO, and Expedia Group