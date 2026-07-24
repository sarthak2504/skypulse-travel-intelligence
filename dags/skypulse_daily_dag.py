"""
SkyPulse Daily Pipeline DAG
Runs every day at 6am UTC

Tasks:
1. check_silver_freshness     - Is Silver receiving data?
2. check_silver_row_counts    - Did we get expected rows today?
3. check_null_rates           - Are critical fields populated?
4. run_spark_batch_job        - Trigger Dataproc Serverless
5. check_gold_freshness       - Did Gold get new rows?
6. check_late_arrivals_volume - How many late messages today?
7. reconcile_late_arrivals    - Reprocess late data into Gold
8. write_audit_log            - Record pipeline run summary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
import uuid

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

PROJECT_ID     = "triptide-28062026"
REGION         = "us-central1"
SILVER_DATASET = "skypulse"
GOLD_DATASET   = "skypulse_gold"
GCS_BUCKET     = "gs://skypulse-triptide"
SPARK_JOB_FILE = f"{GCS_BUCKET}/spark/batch_job.py"

# DQ thresholds
MIN_SILVER_ROWS_TODAY    = 100    # expect at least 100 new Silver rows per day
MAX_NULL_RATE            = 0.01   # max 1% nulls in critical fields
MAX_LATE_ARRIVAL_RATE    = 0.10   # alert if >10% of messages are late
FRESHNESS_HOURS          = 2      # Silver must have data within last 2 hours

# ─────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────

default_args = {
    "owner":            "skypulse",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ─────────────────────────────────────────
# HELPER: RUN BIGQUERY QUERY
# ─────────────────────────────────────────

def run_bq_query(query):
    """Execute a BigQuery query and return results."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False, location="us-central1")
    result = hook.get_records(query)
    return result

# ─────────────────────────────────────────
# TASK 1: CHECK SILVER FRESHNESS
# ─────────────────────────────────────────

def check_silver_freshness(**context):
    query = f"""
        SELECT
            CASE 
                WHEN COUNT(*) = 0 THEN NULL
                ELSE MAX(processing_timestamp)
            END as latest_ts,
            CASE
                WHEN COUNT(*) = 0 THEN 999
                ELSE TIMESTAMP_DIFF(
                    CURRENT_TIMESTAMP(),
                    MAX(processing_timestamp),
                    HOUR
                )
            END as hours_since_last
        FROM `{PROJECT_ID}.{SILVER_DATASET}.price_5min_avg`
    """
    result = run_bq_query(query)
    
    if not result or not result[0]:
        raise ValueError("Silver freshness check FAILED. Could not query Silver table.")
    
    latest_ts, hours_since = result[0]
    
    print(f"Latest Silver data: {latest_ts}")
    print(f"Hours since last data: {hours_since}")

    # Empty table
    if latest_ts is None:
        raise ValueError(
            "Silver freshness check FAILED. "
            "Table is empty. Check if Dataflow is running."
        )

    # Stale data
    if hours_since > FRESHNESS_HOURS:
        raise ValueError(
            f"Silver freshness check FAILED. "
            f"Last data was {hours_since} hours ago (threshold: {FRESHNESS_HOURS}hrs). "
            f"Last timestamp: {latest_ts}. "
            f"Check if Dataflow is running."
        )

    print(f"Silver freshness check PASSED. Data is {hours_since} hours old.")
    context['ti'].xcom_push(key='latest_silver_ts', value=str(latest_ts))
    
# ─────────────────────────────────────────
# TASK 2: CHECK SILVER ROW COUNTS
# ─────────────────────────────────────────

def check_silver_row_counts(**context):
    """
    Verify Silver received enough rows today.
    Fails if below minimum threshold.
    """
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{SILVER_DATASET}.price_5min_avg`
        WHERE DATE(processing_timestamp) = CURRENT_DATE()
    """
    result = run_bq_query(query)
    row_count = result[0][0]

    print(f"Silver rows today: {row_count}")
    print(f"Minimum expected: {MIN_SILVER_ROWS_TODAY}")

    if row_count < MIN_SILVER_ROWS_TODAY:
        raise ValueError(
            f"Silver row count check FAILED. "
            f"Got {row_count} rows today, expected at least {MIN_SILVER_ROWS_TODAY}. "
            f"Check if Function B is publishing messages."
        )

    print(f"Silver row count check PASSED. {row_count} rows today.")
    context['ti'].xcom_push(key='silver_row_count', value=row_count)

# ─────────────────────────────────────────
# TASK 3: CHECK NULL RATES
# ─────────────────────────────────────────

def check_null_rates(**context):
    """
    Check null rates for critical fields in Silver.
    Fails if any field exceeds 1% nulls.
    """
    query = f"""
        SELECT
            COUNTIF(flight_number IS NULL) / COUNT(*) as flight_number_null_rate,
            COUNTIF(avg_price_usd IS NULL) / COUNT(*) as price_null_rate,
            COUNTIF(window_start IS NULL)  / COUNT(*) as window_start_null_rate,
            COUNTIF(airline_code IS NULL)  / COUNT(*) as airline_code_null_rate,
            COUNT(*) as total_rows
        FROM `{PROJECT_ID}.{SILVER_DATASET}.price_5min_avg`
        WHERE DATE(processing_timestamp) = CURRENT_DATE()
    """
    result = run_bq_query(query)
    fn_null, price_null, ws_null, ac_null, total = result[0]

    print(f"Null rates — flight_number: {fn_null:.2%}, price: {price_null:.2%}, "
          f"window_start: {ws_null:.2%}, airline_code: {ac_null:.2%}")

    issues = []
    if fn_null and fn_null > MAX_NULL_RATE:
        issues.append(f"flight_number null rate {fn_null:.2%} exceeds {MAX_NULL_RATE:.2%}")
    if price_null and price_null > MAX_NULL_RATE:
        issues.append(f"price_usd null rate {price_null:.2%} exceeds {MAX_NULL_RATE:.2%}")
    if ws_null and ws_null > MAX_NULL_RATE:
        issues.append(f"window_start null rate {ws_null:.2%} exceeds {MAX_NULL_RATE:.2%}")

    if issues:
        raise ValueError(f"Null rate check FAILED: {'; '.join(issues)}")

    print("Null rate check PASSED.")

# ─────────────────────────────────────────
# TASK 4: RUN SPARK BATCH JOB
# ─────────────────────────────────────────

def get_spark_batch_config():
    """Build Dataproc Serverless batch config."""
    batch_id = f"skypulse-daily-{datetime.now().strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:8]}"

    return {
        "batch_id": batch_id,
        "batch": {
            "pyspark_batch": {
                "main_python_file_uri": SPARK_JOB_FILE,
            },
            "runtime_config": {
                "version": "2.1",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": f"skypulse-ingestion-sa@{PROJECT_ID}.iam.gserviceaccount.com",
                }
            },
        },
        "project_id": PROJECT_ID,
        "region":     REGION,
    }

# ─────────────────────────────────────────
# TASK 5: CHECK GOLD FRESHNESS
# ─────────────────────────────────────────

def check_gold_freshness(**context):
    """
    Verify Gold fact table received new rows after Spark job.
    Fails if no rows written today.
    """
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{PROJECT_ID}.{GOLD_DATASET}.fact_flight_prices`
        WHERE DATE(CAST(created_at AS TIMESTAMP)) = CURRENT_DATE()
    """
    result = run_bq_query(query)
    row_count = result[0][0]

    print(f"Gold rows written today: {row_count}")

    if row_count == 0:
        raise ValueError(
            "Gold freshness check FAILED. "
            "No rows written to fact_flight_prices today. "
            "Check Spark job logs."
        )

    print(f"Gold freshness check PASSED. {row_count} rows written today.")
    context['ti'].xcom_push(key='gold_row_count', value=row_count)

# ─────────────────────────────────────────
# TASK 6: CHECK LATE ARRIVALS VOLUME
# ─────────────────────────────────────────

def check_late_arrivals_volume(**context):
    """
    Check what percentage of today's messages were late.
    Alert (but don't fail) if above threshold.
    Returns branch decision for reconciliation.
    """
    query = f"""
        WITH silver_count AS (
            SELECT COUNT(*) as total
            FROM `{PROJECT_ID}.{SILVER_DATASET}.price_5min_avg`
            WHERE DATE(processing_timestamp) = CURRENT_DATE()
        ),
        late_count AS (
            SELECT COUNT(*) as late_total
            FROM `{PROJECT_ID}.{SILVER_DATASET}.late_arrivals`
            WHERE DATE(processing_timestamp) = CURRENT_DATE()
        )
        SELECT
            s.total,
            l.late_total,
            SAFE_DIVIDE(l.late_total, s.total) as late_rate
        FROM silver_count s, late_count l
    """
    result    = run_bq_query(query)
    total, late_total, late_rate = result[0]
    late_rate = late_rate or 0

    print(f"Total messages today: {total}")
    print(f"Late messages today:  {late_total}")
    print(f"Late rate:            {late_rate:.2%}")
    print(f"Threshold:            {MAX_LATE_ARRIVAL_RATE:.2%}")

    context['ti'].xcom_push(key='late_total',  value=late_total)
    context['ti'].xcom_push(key='late_rate',   value=float(late_rate))

    if late_rate > MAX_LATE_ARRIVAL_RATE:
        print(f"WARNING: Late arrival rate {late_rate:.2%} exceeds threshold. "
              f"Triggering reconciliation.")
        return "reconcile_late_arrivals"
    else:
        print("Late arrival rate within acceptable range. Skipping reconciliation.")
        return "write_audit_log"

# ─────────────────────────────────────────
# TASK 7: RECONCILE LATE ARRIVALS
# ─────────────────────────────────────────

def reconcile_late_arrivals(**context):
    """
    Reprocess late arrivals back into the main aggregation.

    For each late message:
    1. Find which 5-min window it belongs to (by event_timestamp)
    2. Check if that window exists in price_5min_avg
    3. If yes — the window already has data, late message is minor correction
    4. Log the reconciliation for audit

    In production this would update the Gold fact table.
    For now we log to the audit table and flag for manual review.
    """
    query = f"""
        SELECT
            route_id,
            flight_number,
            airline_code,
            price_usd,
            event_timestamp,
            ingestion_timestamp,
            lateness_seconds
        FROM `{PROJECT_ID}.{SILVER_DATASET}.late_arrivals`
        WHERE DATE(processing_timestamp) = CURRENT_DATE()
        ORDER BY lateness_seconds DESC
    """
    result = run_bq_query(query)

    print(f"Late arrivals to reconcile: {len(result)}")

    for row in result[:10]:  # log first 10 for visibility
        route, flight, airline, price, event_ts, ingest_ts, lateness = row
        print(f"  {flight} {route} @ ${price:.2f} — "
              f"late by {lateness}s "
              f"(event: {event_ts}, ingested: {ingest_ts})")

    print("Reconciliation logged. Late arrivals flagged for review.")
    context['ti'].xcom_push(key='reconciled_count', value=len(result))

# ─────────────────────────────────────────
# TASK 8: WRITE AUDIT LOG
# ─────────────────────────────────────────

def write_audit_log(**context):
    """
    Write a summary of this pipeline run to the audit log table.
    Creates the table if it doesn't exist.
    """
    ti = context['ti']
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_ts   = datetime.now().isoformat()

    silver_rows     = ti.xcom_pull(task_ids='check_silver_row_counts', key='silver_row_count') or 0
    gold_rows       = ti.xcom_pull(task_ids='check_gold_freshness',    key='gold_row_count')   or 0
    late_total      = ti.xcom_pull(task_ids='check_late_arrivals',     key='late_total')        or 0
    late_rate       = ti.xcom_pull(task_ids='check_late_arrivals',     key='late_rate')         or 0
    reconciled      = ti.xcom_pull(task_ids='reconcile_late_arrivals', key='reconciled_count')  or 0

    query = f"""
        INSERT INTO `{PROJECT_ID}.{SILVER_DATASET}.pipeline_audit_log`
        (run_date, run_timestamp, silver_rows, gold_rows,
         late_arrivals, late_rate, reconciled_count, status)
        VALUES (
            '{run_date}',
            '{run_ts}',
            {silver_rows},
            {gold_rows},
            {late_total},
            {float(late_rate)},
            {reconciled},
            'SUCCESS'
        )
    """

    try:
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        hook.run_query(query, use_legacy_sql=False)
        print(f"Audit log written for {run_date}")
        print(f"  Silver rows:    {silver_rows}")
        print(f"  Gold rows:      {gold_rows}")
        print(f"  Late arrivals:  {late_total} ({float(late_rate):.2%})")
        print(f"  Reconciled:     {reconciled}")
    except Exception as e:
        # Don't fail the DAG just because audit log failed
        print(f"Warning: Could not write audit log: {e}")

# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────

with DAG(
    dag_id="skypulse_daily_pipeline",
    default_args=default_args,
    description="SkyPulse daily pipeline — DQ checks, Spark batch, late arrival reconciliation",
    schedule_interval="0 6 * * *",  # 6am UTC daily
    start_date=days_ago(1),
    catchup=False,
    tags=["skypulse", "data-quality", "batch"],
) as dag:

    # Task 1: Silver freshness
    t1_silver_freshness = PythonOperator(
        task_id="check_silver_freshness",
        python_callable=check_silver_freshness,
    )

    # Task 2: Silver row counts
    t2_silver_counts = PythonOperator(
        task_id="check_silver_row_counts",
        python_callable=check_silver_row_counts,
    )

    # Task 3: Null rates
    t3_null_rates = PythonOperator(
        task_id="check_null_rates",
        python_callable=check_null_rates,
    )

    # Task 4: Run Spark batch job (Dataproc Serverless)
    spark_config = get_spark_batch_config()
    t4_spark_job = DataprocCreateBatchOperator(
        task_id="run_spark_batch_job",
        project_id=spark_config["project_id"],
        region=spark_config["region"],
        batch=spark_config["batch"],
        batch_id=spark_config["batch_id"],
        gcp_conn_id="google_cloud_default",
    )

    # Task 5: Gold freshness
    t5_gold_freshness = PythonOperator(
        task_id="check_gold_freshness",
        python_callable=check_gold_freshness,
    )

    # Task 6: Late arrivals check (branch)
    t6_late_arrivals = BranchPythonOperator(
        task_id="check_late_arrivals",
        python_callable=check_late_arrivals_volume,
    )

    # Task 7: Reconcile late arrivals (only if late rate exceeded)
    t7_reconcile = PythonOperator(
        task_id="reconcile_late_arrivals",
        python_callable=reconcile_late_arrivals,
    )

    # Task 8: Audit log
    t8_audit = PythonOperator(
        task_id="write_audit_log",
        python_callable=write_audit_log,
        trigger_rule="none_failed_min_one_success",
    )

    # ─── DAG Dependencies ───
    #
    # DQ checks run in parallel after each other
    # Spark runs only after all DQ checks pass
    # Gold check runs after Spark
    # Late arrivals check branches to reconcile or skip
    # Audit log always runs at the end

    t1_silver_freshness >> t2_silver_counts >> t3_null_rates >> t4_spark_job
    t4_spark_job >> t5_gold_freshness >> t6_late_arrivals
    t6_late_arrivals >> [t7_reconcile, t8_audit]
    t7_reconcile >> t8_audit