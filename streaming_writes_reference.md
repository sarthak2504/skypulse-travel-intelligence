# Streaming Writes, Duplicates, and Upserts — Reference Guide

---

## The Core Problem

When you write data from a streaming pipeline to a sink (BigQuery, Iceberg, Delta Lake), two things can cause duplicate rows:

1. **At-least-once delivery** — Pub/Sub and Dataflow guarantee every message is delivered at least once. Retries cause duplicates.
2. **Accumulating mode** — When a late message arrives after a window fires, the window fires again with updated results. Two rows for the same window land in the sink.

---

## BigQuery — Append Only

BigQuery streaming inserts are append-only. You cannot update or delete rows in real time.

```python
# Dataflow/Beam — streaming insert
beam.io.WriteToBigQuery(TABLE,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
```

**Result:** Every window fire = new row. Late message = second row. Duplicates inevitable.

**MERGE is available but only in batch SQL:**
```sql
-- This runs as a batch job, not in real time
MERGE INTO skypulse.price_5min_avg target
USING updates source
ON target.flight_number = source.flight_number
AND target.window_start = source.window_start
WHEN MATCHED AND source.message_count > target.message_count
    THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**When to use BigQuery streaming:**
- You need data available for queries within seconds
- Append-only is acceptable (logs, events, raw telemetry)
- You handle deduplication downstream (batch job)

---

## Apache Iceberg — Real-Time Upserts

Iceberg supports ACID transactions and MERGE natively. No duplicates if you use foreachBatch.

```python
def write_to_iceberg(df, epoch_id):
    df.createOrReplaceTempView("updates")
    df.sparkSession.sql("""
        MERGE INTO iceberg_catalog.skypulse.price_5min_avg target
        USING updates source
        ON target.flight_number = source.flight_number
        AND target.window_start = source.window_start
        WHEN MATCHED AND source.message_count > target.message_count
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = streaming_df.writeStream \
    .foreachBatch(write_to_iceberg) \
    .trigger(processingTime="30 seconds") \
    .outputMode("update") \
    .start()
```

**Result:** Each window has exactly one row. Late messages update the existing row. No duplicates.

**How Iceberg handles this under the hood:**
Iceberg is still immutable underneath. MERGE creates a new snapshot that replaces the old row — the old data is preserved in snapshot history. This is how it achieves ACID while remaining immutable.

**When to use Iceberg:**
- You need clean data without downstream deduplication
- You already use Iceberg (migrating from Hive)
- You need time travel (query data as of any past snapshot)
- Schema evolution without breaking existing queries

---

## Delta Lake — Real-Time Upserts (Databricks)

Delta Lake (created by Databricks, now open source) has the same MERGE capability as Iceberg.

```python
from delta.tables import DeltaTable

def write_to_delta(df, epoch_id):
    delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.flight_number = source.flight_number AND target.window_start = source.window_start"
    ).whenMatchedUpdate(
        condition="source.message_count > target.message_count",
        set={"avg_price_usd": "source.avg_price_usd", "message_count": "source.message_count"}
    ).whenNotMatchedInsertAll().execute()

query = streaming_df.writeStream \
    .foreachBatch(write_to_delta) \
    .start()
```

**Delta Lake vs Iceberg:**

| Feature | Delta Lake | Apache Iceberg |
|---|---|---|
| Creator | Databricks | Netflix |
| Open source | Yes (since 2019) | Yes |
| MERGE support | Yes | Yes |
| Time travel | Yes | Yes |
| Schema evolution | Yes | Yes |
| Multi-engine | Limited | Strong (Spark, Flink, Trino, Hive) |
| Best with | Databricks / Spark | Multi-engine environments |
| Used at | Databricks customers | Netflix, Apple, Expedia, LinkedIn |

---

## SkyPulse Architecture Decision

SkyPulse uses BigQuery as the streaming sink because:
- Native Dataflow integration (no connectors needed)
- Familiar to Google CE/SA interviewers
- Cheap at dev scale

The trade-off is duplicates in Silver. We handle them with a Spark batch deduplication job that writes clean data to Gold.

**Future enhancement:** Replace BigQuery Silver with Iceberg tables on GCS. Dataflow would write to Iceberg via `foreachBatch` + MERGE, eliminating duplicates at the source and simplifying the Spark job.

---

## The Two-Layer Pattern (Lambda Architecture)

This is the standard pattern when your streaming sink doesn't support upserts:

```
Streaming layer (Speed layer):
    Dataflow → BigQuery Silver
    Raw, may have duplicates, available in seconds

Batch layer:
    Spark job (daily) → BigQuery Gold
    Clean, deduplicated, available after batch runs
```

**Pros:**
- Streaming layer is simple and fast
- Batch layer is reliable and cheap
- Clear separation of raw vs clean data

**Cons:**
- Data in Gold is always slightly stale (last batch run)
- Two layers to maintain
- Deduplication logic in batch job

**When to use Iceberg/Delta instead:**
- You need real-time clean data (not T+1)
- Your team already uses Iceberg/Delta
- You want to eliminate the batch deduplication layer

---

## Standard Deviation and Z-Score — Quick Reference

**Standard deviation** measures how spread out values are from the mean.

```
Prices: $170, $190, $210, $180, $200
Mean = $190
Stddev ≈ $15  (prices typically wander $15 from $190)
```

**Z-score** measures how many standard deviations a value is from the mean.

```
z = |price - mean| / stddev

Price $211 → z = |211 - 190| / 15 = 1.4  → normal (< 3)
Price $280 → z = |280 - 190| / 15 = 6.0  → anomaly (> 3)
Price $450 → z = |450 - 190| / 15 = 17.3 → clear anomaly
```

**Why SkyPulse z-scores are currently inflated:**
Only 2 unique 1hr windows exist per flight. With almost no variance in those 2 values, stddev ≈ $0.10. Even a $15 price difference produces z = 150. After 7 days of data, stddev will be $15-25 and z-scores will reflect real anomalies.

---

## Sink Comparison Matrix

| Sink | Real-time upsert | Append only | MERGE support | Best use case |
|---|---|---|---|---|
| BigQuery streaming | ❌ | ✅ | Batch only | Real-time ingestion, raw logs |
| BigQuery batch | ✅ | ✅ | ✅ | Daily ETL, Gold layer |
| Apache Iceberg | ✅ | ✅ | ✅ | Multi-engine, no duplicates |
| Delta Lake | ✅ | ✅ | ✅ | Databricks-heavy environments |
| PostgreSQL | ✅ | ✅ | ✅ | Transactional workloads |
| Apache Kafka | ❌ | ✅ | ❌ | Message streaming, replay |
| Elasticsearch | ✅ | ✅ | ✅ | Search and analytics |

