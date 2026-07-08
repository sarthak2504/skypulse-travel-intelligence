import json
import logging
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode
from apache_beam.utils.timestamp import Duration
from datetime import datetime, timezone

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
PROJECT_ID = "triptide-28062026"
REGION = "us-central1"
SUBSCRIPTION = "projects/triptide-28062026/subscriptions/flight-prices-sub"
BUCKET = "gs://skypulse-triptide"

# BigQuery tables
BQ_DATASET = "skypulse"
BQ_5MIN_TABLE = f"{PROJECT_ID}:{BQ_DATASET}.price_5min_avg"
BQ_1HR_TABLE = f"{PROJECT_ID}:{BQ_DATASET}.price_1hr_trend"
BQ_LATE_TABLE = f"{PROJECT_ID}:{BQ_DATASET}.late_arrivals"

# Window config
FIXED_WINDOW_SIZE = 300        # 5 minutes in seconds
SLIDING_WINDOW_SIZE = 3600     # 1 hour in seconds
SLIDING_WINDOW_PERIOD = 900    # every 15 minutes in seconds
ALLOWED_LATENESS = 120         # 2 minutes in seconds

# Tag for side output (late messages)
LATE_TAG = "late"

# ─────────────────────────────────────────
# BIGQUERY SCHEMAS
# ─────────────────────────────────────────
SCHEMA_5MIN = {
    "fields": [
        {"name": "route_id",              "type": "STRING"},
        {"name": "origin",                "type": "STRING"},
        {"name": "destination",           "type": "STRING"},
        {"name": "airline_code",          "type": "STRING"},
        {"name": "airline_name",          "type": "STRING"},
        {"name": "window_start",          "type": "TIMESTAMP"},
        {"name": "window_end",            "type": "TIMESTAMP"},
        {"name": "avg_price_usd",         "type": "FLOAT"},
        {"name": "min_price_usd",         "type": "FLOAT"},
        {"name": "max_price_usd",         "type": "FLOAT"},
        {"name": "message_count",         "type": "INTEGER"},
        {"name": "processing_timestamp",  "type": "TIMESTAMP"},
    ]
}

SCHEMA_1HR = {
    "fields": [
        {"name": "route_id",              "type": "STRING"},
        {"name": "origin",                "type": "STRING"},
        {"name": "destination",           "type": "STRING"},
        {"name": "airline_code",          "type": "STRING"},
        {"name": "window_start",          "type": "TIMESTAMP"},
        {"name": "window_end",            "type": "TIMESTAMP"},
        {"name": "avg_price_usd",         "type": "FLOAT"},
        {"name": "message_count",         "type": "INTEGER"},
        {"name": "processing_timestamp",  "type": "TIMESTAMP"},
    ]
}

SCHEMA_LATE = {
    "fields": [
        {"name": "route_id",              "type": "STRING"},
        {"name": "origin",                "type": "STRING"},
        {"name": "destination",           "type": "STRING"},
        {"name": "airline_code",          "type": "STRING"},
        {"name": "price_usd",             "type": "FLOAT"},
        {"name": "flight_date",           "type": "STRING"},
        {"name": "event_timestamp",       "type": "TIMESTAMP"},
        {"name": "ingestion_timestamp",   "type": "TIMESTAMP"},
        {"name": "lateness_seconds",      "type": "INTEGER"},
        {"name": "processing_timestamp",  "type": "TIMESTAMP"},
    ]
}

# ─────────────────────────────────────────
# STEP 1: PARSE MESSAGE
# ─────────────────────────────────────────
class ParseMessage(beam.DoFn):
    """
    Parse raw Pub/Sub JSON bytes into a Python dict.
    Emit valid records to main output.
    Skip unparseable messages with a log warning.
    """
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))

            # Validate required fields are present
            required = ["route_id", "origin", "destination", "airline_code",
                       "airline_name", "price_usd", "event_timestamp", "ingestion_timestamp"]
            if not all(field in record for field in required):
                logging.warning(f"Missing required fields in message: {record}")
                return

            yield record

        except Exception as e:
            logging.error(f"Failed to parse message: {e}")


# ─────────────────────────────────────────
# STEP 2: ASSIGN EVENT TIMESTAMP
# ─────────────────────────────────────────
class AssignTimestamp(beam.DoFn):
    """
    Tell Beam to use the message's event_timestamp for windowing,
    not Pub/Sub's publish timestamp.

    This is critical for correctness — we want to window by when
    the price event happened, not when it was published.
    """
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        event_ts = element.get("event_timestamp")
        if event_ts:
            yield beam.window.TimestampedValue(element, event_ts)
        else:
            # Fall back to processing time if no event timestamp
            yield element


# ─────────────────────────────────────────
# STEP 3: DETECT AND SPLIT LATE MESSAGES
# ─────────────────────────────────────────
class SplitLateMessages(beam.DoFn):
    """
    Compare event_timestamp to ingestion_timestamp.
    If the message was ingested more than ALLOWED_LATENESS seconds
    after the event occurred, route it to the late side output.

    Main output  → on-time messages → windowing pipeline
    Late output  → late messages    → late_arrivals table
    """
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        event_ts = element.get("event_timestamp", 0)
        ingestion_ts = element.get("ingestion_timestamp", 0)
        lateness = ingestion_ts - event_ts

        if lateness > ALLOWED_LATENESS:
            # Route to late side output
            late_record = {
                "route_id":             element["route_id"],
                "origin":               element["origin"],
                "destination":          element["destination"],
                "airline_code":         element["airline_code"],
                "price_usd":            element["price_usd"],
                "flight_date":          element.get("flight_date", ""),
                "event_timestamp":      datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat(),
                "ingestion_timestamp":  datetime.fromtimestamp(ingestion_ts, tz=timezone.utc).isoformat(),
                "lateness_seconds":     int(lateness),
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield pvalue.TaggedOutput(LATE_TAG, late_record)
        else:
            # On-time — pass through to main output
            yield element


# ─────────────────────────────────────────
# STEP 4: AGGREGATE PRICES PER WINDOW
# ─────────────────────────────────────────
class AggregatePrice(beam.DoFn):
    """
    Takes a (route_id, [prices]) tuple produced by GroupByKey
    and computes avg, min, max, count.

    Also formats window start/end timestamps for BigQuery.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        route_id, prices = element
        prices = list(prices)

        if not prices:
            return

        # Extract route metadata from first price record
        # prices is a list of dicts at this point
        first = prices[0] if isinstance(prices[0], dict) else {}

        avg_price = sum(p if isinstance(p, float) else p.get("price_usd", 0)
                       for p in prices) / len(prices)
        min_price = min(p if isinstance(p, float) else p.get("price_usd", 0)
                       for p in prices)
        max_price = max(p if isinstance(p, float) else p.get("price_usd", 0)
                       for p in prices)

        window_start = datetime.fromtimestamp(
            window.start.micros / 1e6, tz=timezone.utc
        ).isoformat()
        window_end = datetime.fromtimestamp(
            window.end.micros / 1e6, tz=timezone.utc
        ).isoformat()

        yield {
            "route_id":             route_id,
            "origin":               first.get("origin", ""),
            "destination":          first.get("destination", ""),
            "airline_code":         first.get("airline_code", ""),
            "airline_name":         first.get("airline_name", ""),
            "window_start":         window_start,
            "window_end":           window_end,
            "avg_price_usd":        round(avg_price, 2),
            "min_price_usd":        round(min_price, 2),
            "max_price_usd":        round(max_price, 2),
            "message_count":        len(prices),
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        }


class AggregatePriceTrend(beam.DoFn):
    """
    Same as AggregatePrice but for sliding windows.
    Omits min/max/airline_name since this is for trend detection.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        route_id, prices = element
        prices = list(prices)

        if not prices:
            return

        first = prices[0] if isinstance(prices[0], dict) else {}

        avg_price = sum(p if isinstance(p, float) else p.get("price_usd", 0)
                       for p in prices) / len(prices)

        window_start = datetime.fromtimestamp(
            window.start.micros / 1e6, tz=timezone.utc
        ).isoformat()
        window_end = datetime.fromtimestamp(
            window.end.micros / 1e6, tz=timezone.utc
        ).isoformat()

        yield {
            "route_id":             route_id,
            "origin":               first.get("origin", ""),
            "destination":          first.get("destination", ""),
            "airline_code":         first.get("airline_code", ""),
            "window_start":         window_start,
            "window_end":           window_end,
            "avg_price_usd":        round(avg_price, 2),
            "message_count":        len(prices),
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ─────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────
def run():
    # Pipeline options
    options = PipelineOptions()

    # Standard options — streaming mode
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = True
    standard_options.runner = "DataflowRunner"

    # GCP options
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = REGION
    gcp_options.staging_location = f"{BUCKET}/dataflow/staging"
    gcp_options.temp_location = f"{BUCKET}/dataflow/temp"
    gcp_options.job_name = "skypulse-price-pipeline"

    # Setup options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    with beam.Pipeline(options=options) as pipeline:

        # ── STEP 1: Read from Pub/Sub ──
        raw_messages = (
            pipeline
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                subscription=SUBSCRIPTION
            )
        )

        # ── STEP 2: Parse JSON messages ──
        parsed = (
            raw_messages
            | "Parse Messages" >> beam.ParDo(ParseMessage())
        )

        # ── STEP 3: Assign event timestamps ──
        timestamped = (
            parsed
            | "Assign Timestamps" >> beam.ParDo(AssignTimestamp())
        )

        # ── STEP 4: Split late messages ──
        split = (
            timestamped
            | "Split Late Messages" >> beam.ParDo(
                SplitLateMessages()
            ).with_outputs(LATE_TAG, main="on_time")
        )

        on_time_messages = split["on_time"]
        late_messages = split[LATE_TAG]

        # ── STEP 5a: Fixed window (5 min) ──
        fixed_windowed = (
            on_time_messages
            | "Fixed Window" >> beam.WindowInto(
                FixedWindows(FIXED_WINDOW_SIZE),
                trigger=AfterWatermark(
                    late=AfterCount(1)
                ),
                allowed_lateness=Duration(seconds=ALLOWED_LATENESS),
                accumulation_mode=AccumulationMode.ACCUMULATING
            )
        )

        fixed_keyed = (
            fixed_windowed
            | "Key by Route (5min)" >> beam.Map(
                lambda x: (x["route_id"], x)
            )
            | "Group by Route (5min)" >> beam.GroupByKey()
            | "Aggregate (5min)" >> beam.ParDo(AggregatePrice())
        )

        fixed_keyed | "Write 5min to BQ" >> beam.io.WriteToBigQuery(
            BQ_5MIN_TABLE,
            schema=SCHEMA_5MIN,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # ── STEP 5b: Sliding window (1hr / 15min) ──
        sliding_windowed = (
            on_time_messages
            | "Sliding Window" >> beam.WindowInto(
                SlidingWindows(
                    size=SLIDING_WINDOW_SIZE,
                    period=SLIDING_WINDOW_PERIOD
                ),
                trigger=AfterWatermark(
                    late=AfterCount(1)
                ),
                allowed_lateness=Duration(seconds=ALLOWED_LATENESS),
                accumulation_mode=AccumulationMode.ACCUMULATING
            )
        )

        sliding_keyed = (
            sliding_windowed
            | "Key by Route (1hr)" >> beam.Map(
                lambda x: (x["route_id"], x)
            )
            | "Group by Route (1hr)" >> beam.GroupByKey()
            | "Aggregate (1hr)" >> beam.ParDo(AggregatePriceTrend())
        )

        sliding_keyed | "Write 1hr to BQ" >> beam.io.WriteToBigQuery(
            BQ_1HR_TABLE,
            schema=SCHEMA_1HR,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # ── STEP 5c: Write late messages ──
        late_messages | "Write Late to BQ" >> beam.io.WriteToBigQuery(
            BQ_LATE_TABLE,
            schema=SCHEMA_LATE,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()