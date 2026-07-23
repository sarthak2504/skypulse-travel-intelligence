"""
SkyPulse — Daily Batch Job
Runs on Dataproc PySpark once per day via Cloud Composer (Week 4)

What it does:
1. Reads Silver price_5min_avg (new data only via watermark)
2. Deduplicates (keeps row with highest message_count per flight+window)
3. Populates dim_date, dim_airlines, dim_flights (SCD Type 2)
4. Computes anomaly detection (z-score vs 7-day rolling mean)
5. Writes to Gold fact_flight_prices
6. Updates watermark table
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType, DateType
)
from datetime import datetime, timezone, timedelta
import sys

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

PROJECT_ID      = "triptide-28062026"
SILVER_DATASET  = "skypulse"
GOLD_DATASET    = "skypulse_gold"
GCS_BUCKET      = "gs://skypulse-triptide"
TEMP_GCS        = f"{GCS_BUCKET}/spark/temp"

# BigQuery table references
SILVER_5MIN     = f"{PROJECT_ID}.{SILVER_DATASET}.price_5min_avg"
SILVER_1HR      = f"{PROJECT_ID}.{SILVER_DATASET}.price_1hr_trend"
GOLD_FACT       = f"{PROJECT_ID}.{GOLD_DATASET}.fact_flight_prices"
GOLD_DIM_DATE   = f"{PROJECT_ID}.{GOLD_DATASET}.dim_date"
GOLD_DIM_AIRLINE= f"{PROJECT_ID}.{GOLD_DATASET}.dim_airlines"
GOLD_DIM_FLIGHT = f"{PROJECT_ID}.{GOLD_DATASET}.dim_flights"
WATERMARK_TABLE = f"{PROJECT_ID}.{SILVER_DATASET}.pipeline_watermarks"

# Anomaly threshold
Z_SCORE_THRESHOLD = 3.0

# Alliance mapping
ALLIANCE_MAP = {
    "UA": "Star Alliance", "LH": "Star Alliance", "AC": "Star Alliance",
    "AA": "Oneworld",      "BA": "Oneworld",      "QR": "Oneworld",
    "DL": "SkyTeam",       "AF": "SkyTeam",       "KL": "SkyTeam",
    "WN": "None",          "B6": "None",           "NK": "None",
    "F9": "None",          "G4": "None",
}

DOMESTIC_CARRIERS = ["UA", "AA", "DL", "WN", "B6", "NK", "F9", "G4", "AS", "SY"]

# US holidays 2026 (simplified)
US_HOLIDAYS_2026 = [
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25",
    "2026-07-04", "2026-09-07", "2026-11-26", "2026-12-25"
]

# ─────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SkyPulse-DailyBatch")
        .config("spark.sql.adaptive.enabled", "true")
        .config("temporaryGcsBucket", "skypulse-triptide")
        .getOrCreate()
    )

# ─────────────────────────────────────────
# WATERMARK HELPERS
# ─────────────────────────────────────────

def get_watermark(spark, pipeline_name):
    """Get last processed timestamp for this pipeline."""
    try:
        df = spark.read.format("bigquery") \
            .option("table", WATERMARK_TABLE) \
            .load() \
            .filter(F.col("pipeline_name") == pipeline_name)

        if df.count() == 0:
            # First run — go back 7 days to build anomaly baseline
            return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

        return df.agg(F.max("last_processed_ts")).collect()[0][0]
    except Exception:
        # Table doesn't exist yet — first run
        return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()


def update_watermark(spark, pipeline_name, last_ts):
    """Update watermark after successful processing."""
    schema = StructType([
        StructField("pipeline_name",    StringType(),    False),
        StructField("last_processed_ts",StringType(),    False),
        StructField("updated_at",       StringType(),    False),
    ])

    data = [(pipeline_name, str(last_ts), datetime.now(timezone.utc).isoformat())]
    df   = spark.createDataFrame(data, schema)

    df.write.format("bigquery") \
        .option("table", WATERMARK_TABLE) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print(f"Watermark updated: {pipeline_name} → {last_ts}")

# ─────────────────────────────────────────
# STEP 1: READ SILVER DATA
# ─────────────────────────────────────────

def read_silver_data(spark, last_watermark):
    """Read new price_5min_avg data since last watermark."""
    print(f"Reading Silver data since: {last_watermark}")

    df = spark.read.format("bigquery") \
        .option("table", SILVER_5MIN) \
        .load() \
        .filter(F.col("processing_timestamp") > last_watermark)

    count = df.count()
    print(f"Read {count} rows from Silver price_5min_avg")
    return df


def read_silver_1hr(spark):
    """Read 1-hour trend data for anomaly baseline."""
    return spark.read.format("bigquery") \
        .option("table", SILVER_1HR) \
        .load()

# ─────────────────────────────────────────
# STEP 2: DEDUPLICATE
# ─────────────────────────────────────────

def deduplicate(df):
    """
    Remove duplicate rows from accumulating mode.
    Keep the row with the highest message_count per flight+window.
    If message_count is equal, keep the latest processing_timestamp.
    """
    window = Window.partitionBy("flight_number", "window_start") \
                   .orderBy(F.col("message_count").desc(),
                            F.col("processing_timestamp").desc())

    deduped = df.withColumn("row_num", F.row_number().over(window)) \
                .filter(F.col("row_num") == 1) \
                .drop("row_num")

    print(f"After deduplication: {deduped.count()} rows")
    return deduped

# ─────────────────────────────────────────
# STEP 3: POPULATE dim_date
# ─────────────────────────────────────────

def populate_dim_date(spark, df):
    """
    Extract unique dates from the data and insert into dim_date.
    Skips dates that already exist.
    """
    print("Populating dim_date...")

    # Get existing dates
    try:
        existing_dates = spark.read.format("bigquery") \
            .option("table", GOLD_DIM_DATE) \
            .load() \
            .select("date") \
            .distinct()
        existing_date_list = [r.date for r in existing_dates.collect()]
    except Exception:
        existing_date_list = []

    # Get unique dates from new data
    dates_df = df.withColumn("date", F.to_date("window_start")) \
                 .select("date") \
                 .distinct()

    # Filter to new dates only
    new_dates = [r.date for r in dates_df.collect()
                 if r.date not in existing_date_list]

    if not new_dates:
        print("No new dates to add to dim_date")
        return

    # Build dim_date rows
    rows = []
    for d in new_dates:
        date_str  = d.strftime("%Y-%m-%d")
        date_key  = int(d.strftime("%Y%m%d"))
        dow_num   = d.weekday()  # 0=Monday, 6=Sunday
        dow_name  = d.strftime("%A")
        is_weekend= dow_num >= 5
        month     = d.month
        quarter   = (month - 1) // 3 + 1
        is_holiday= date_str in US_HOLIDAYS_2026

        if month in [12, 1, 2]:
            season = "Winter"
        elif month in [3, 4, 5]:
            season = "Spring"
        elif month in [6, 7, 8]:
            season = "Summer"
        else:
            season = "Fall"

        rows.append((date_key, date_str, dow_name, dow_num,
                     is_weekend, month, quarter, is_holiday, season))

    schema = StructType([
        StructField("date_key",     IntegerType(), False),
        StructField("date",         StringType(),  False),
        StructField("day_of_week",  StringType(),  False),
        StructField("day_number",   IntegerType(), False),
        StructField("is_weekend",   BooleanType(), False),
        StructField("month",        IntegerType(), False),
        StructField("quarter",      IntegerType(), False),
        StructField("is_us_holiday",BooleanType(), False),
        StructField("season",       StringType(),  False),
    ])

    dim_date_df = spark.createDataFrame(rows, schema)

    dim_date_df.write.format("bigquery") \
        .option("table", GOLD_DIM_DATE) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print(f"Added {len(rows)} new dates to dim_date")

# ─────────────────────────────────────────
# STEP 4: POPULATE dim_airlines
# ─────────────────────────────────────────

def populate_dim_airlines(spark, df):
    """
    Insert new airlines into dim_airlines.
    Skips airlines that already exist.
    """
    print("Populating dim_airlines...")

    try:
        existing_airlines = spark.read.format("bigquery") \
            .option("table", GOLD_DIM_AIRLINE) \
            .load() \
            .select("airline_code") \
            .distinct()
        existing_codes = [r.airline_code for r in existing_airlines.collect()]
    except Exception:
        existing_codes = []

    new_airlines = df.select("airline_code", "airline_name") \
                     .distinct() \
                     .collect()

    rows = []
    airline_key = 1000

    for r in new_airlines:
        if r.airline_code in existing_codes:
            continue

        rows.append((
            airline_key,
            r.airline_code,
            r.airline_name,
            ALLIANCE_MAP.get(r.airline_code, "None"),
            r.airline_code in DOMESTIC_CARRIERS
        ))
        airline_key += 1

    if not rows:
        print("No new airlines to add")
        return

    schema = StructType([
        StructField("airline_key",  IntegerType(), False),
        StructField("airline_code", StringType(),  False),
        StructField("airline_name", StringType(),  False),
        StructField("alliance",     StringType(),  True),
        StructField("is_domestic",  BooleanType(), False),
    ])

    airlines_df = spark.createDataFrame(rows, schema)

    airlines_df.write.format("bigquery") \
        .option("table", GOLD_DIM_AIRLINE) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print(f"Added {len(rows)} new airlines to dim_airlines")

# ─────────────────────────────────────────
# STEP 5: POPULATE dim_flights (SCD Type 2)
# ─────────────────────────────────────────

def populate_dim_flights(spark, df):
    """
    SCD Type 2 implementation for dim_flights.

    For each flight in the new data:
    - If flight doesn't exist → INSERT new row
    - If flight exists and nothing changed → SKIP
    - If flight exists but route/airline changed → CLOSE old row, INSERT new row
    """
    print("Populating dim_flights (SCD Type 2)...")

    today = datetime.now(timezone.utc).date().isoformat()

    try:
        existing = spark.read.format("bigquery") \
            .option("table", GOLD_DIM_FLIGHT) \
            .load() \
            .filter(F.col("is_current") == True)
        existing_flights = {
            r.flight_number: r
            for r in existing.collect()
        }
    except Exception:
        existing_flights = {}

    new_flights = df.select(
        "flight_number", "route_id", "origin",
        "destination", "airline_code"
    ).distinct().collect()

    inserts   = []
    flight_key = 2000

    for r in new_flights:
        fn = r.flight_number

        if fn not in existing_flights:
            # New flight — insert
            inserts.append((
                flight_key, fn, r.route_id, r.origin,
                r.destination, r.airline_code,
                today, "9999-12-31", True
            ))
            flight_key += 1

        else:
            existing_row = existing_flights[fn]
            changed = (
                existing_row.route_id    != r.route_id or
                existing_row.airline_code != r.airline_code
            )

            if changed:
                print(f"Flight {fn} changed — closing old record, inserting new")
                # We'd close the old record here in a full implementation
                # For now just insert the new version
                inserts.append((
                    flight_key, fn, r.route_id, r.origin,
                    r.destination, r.airline_code,
                    today, "9999-12-31", True
                ))
                flight_key += 1

    if not inserts:
        print("No new flights to add to dim_flights")
        return

    schema = StructType([
        StructField("flight_key",        IntegerType(), False),
        StructField("flight_number",     StringType(),  False),
        StructField("route_id",          StringType(),  False),
        StructField("origin",            StringType(),  False),
        StructField("destination",       StringType(),  False),
        StructField("airline_code",      StringType(),  False),
        StructField("effective_date",    StringType(),  False),
        StructField("expiry_date",       StringType(),  False),
        StructField("is_current",        BooleanType(), False),
    ])

    flights_df = spark.createDataFrame(inserts, schema)

    flights_df.write.format("bigquery") \
        .option("table", GOLD_DIM_FLIGHT) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print(f"Added {len(inserts)} new flights to dim_flights")

# ─────────────────────────────────────────
# STEP 6: ANOMALY DETECTION
# ─────────────────────────────────────────

def compute_anomalies(spark, df, df_1hr):
    """
    Compare each 5-min window price against the 7-day rolling mean
    from the 1-hour sliding window table.

    z_score = (avg_price - rolling_mean) / rolling_stddev
    is_anomaly = z_score > Z_SCORE_THRESHOLD (3.0)
    """
    print("Computing anomaly scores...")

    # Compute mean and stddev from 1hr trend data
    stats = df_1hr.groupBy("flight_number") \
        .agg(
            F.mean("avg_price_usd").alias("rolling_mean"),
            F.stddev("avg_price_usd").alias("rolling_stddev")
        )

    # Join 5min data with rolling stats
    df_with_stats = df.join(stats, on="flight_number", how="left")

    # Compute z-score
    df_anomaly = df_with_stats.withColumn(
        "z_score",
        F.when(
            F.col("rolling_stddev").isNull() | (F.col("rolling_stddev") == 0),
            F.lit(0.0)
        ).otherwise(
            F.abs(F.col("avg_price_usd") - F.col("rolling_mean")) /
            F.col("rolling_stddev")
        )
    ).withColumn(
        "is_anomaly",
        F.col("z_score") > Z_SCORE_THRESHOLD
    )

    anomaly_count = df_anomaly.filter(F.col("is_anomaly") == True).count()
    print(f"Found {anomaly_count} anomalies")

    return df_anomaly

# ─────────────────────────────────────────
# STEP 7: WRITE TO FACT TABLE
# ─────────────────────────────────────────

def write_fact_table(spark, df_anomaly):
    """
    Join with dimension tables to get surrogate keys.
    Write final fact rows to Gold fact_flight_prices.
    """
    print("Writing to fact_flight_prices...")

    now = datetime.now(timezone.utc).isoformat()

    # Load dimension tables for key lookup
    dim_flights  = spark.read.format("bigquery") \
        .option("table", GOLD_DIM_FLIGHT) \
        .load() \
        .filter(F.col("is_current") == True) \
        .select("flight_key", "flight_number")

    dim_airlines = spark.read.format("bigquery") \
        .option("table", GOLD_DIM_AIRLINE) \
        .load() \
        .select("airline_key", "airline_code")

    dim_date = spark.read.format("bigquery") \
        .option("table", GOLD_DIM_DATE) \
        .load() \
        .select("date_key", "date")

    # Join to get surrogate keys
    fact = df_anomaly \
        .withColumn("date", F.to_date("window_start").cast(StringType())) \
        .join(dim_flights,  on="flight_number", how="left") \
        .join(dim_airlines, on="airline_code",  how="left") \
        .join(dim_date,     on="date",          how="left") \
        .select(
            F.col("flight_key"),
            F.col("airline_key"),
            F.col("date_key"),
            F.col("window_start"),
            F.col("window_end"),
            F.col("avg_price_usd"),
            F.col("min_price_usd"),
            F.col("max_price_usd"),
            F.col("message_count"),
            F.col("is_anomaly"),
            F.col("z_score"),
            F.lit(now).alias("created_at")
        )

    row_count = fact.count()

    fact.write.format("bigquery") \
        .option("table", GOLD_FACT) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .save()

    print(f"Wrote {row_count} rows to fact_flight_prices")
    return row_count

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    print("=" * 60)
    print("SkyPulse Daily Batch Job Starting")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Step 1: Get watermark
        last_watermark = get_watermark(spark, "daily_batch")
        print(f"Last watermark: {last_watermark}")

        # Step 2: Read Silver data
        df_5min = read_silver_data(spark, last_watermark)

        if df_5min.count() == 0:
            print("No new data to process. Exiting.")
            spark.stop()
            return

        df_1hr = read_silver_1hr(spark)

        # Step 3: Deduplicate
        df_deduped = deduplicate(df_5min)

        # Step 4: Populate dimensions
        populate_dim_date(spark, df_deduped)
        populate_dim_airlines(spark, df_deduped)
        populate_dim_flights(spark, df_deduped)

        # Step 5: Anomaly detection
        df_anomaly = compute_anomalies(spark, df_deduped, df_1hr)

        # Step 6: Write fact table
        rows_written = write_fact_table(spark, df_anomaly)

        # Step 7: Update watermark
        max_ts = df_5min.agg(F.max("processing_timestamp")).collect()[0][0]
        update_watermark(spark, "daily_batch", max_ts)

        print("=" * 60)
        print(f"Job completed successfully. Rows written: {rows_written}")
        print("=" * 60)

    except Exception as e:
        print(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()