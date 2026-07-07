import io
import json
import random
import time
from datetime import datetime, timezone

import fastavro
from google.cloud import pubsub_v1, storage

# Configuration
PROJECT_ID = "triptide-28062026"
BUCKET_NAME = "skypulse-triptide"
TOPIC_ID = "flight-prices-raw"
ACTIVE_ROUTES_PATH = "silver/active_routes.json"

# Avro schema — must match what we registered in Pub/Sub
AVRO_SCHEMA = {
    "type": "record",
    "name": "FlightPrice",
    "namespace": "com.skypulse",
    "fields": [
        {"name": "route_id",            "type": "string"},
        {"name": "origin",              "type": "string"},
        {"name": "destination",         "type": "string"},
        {"name": "airline_code",        "type": "string"},
        {"name": "airline_name",        "type": "string"},
        {"name": "flight_number",       "type": "string"},
        {"name": "price_usd",           "type": "double"},
        {"name": "flight_date",         "type": "string"},
        {"name": "scheduled_departure", "type": "string"},
        {"name": "event_timestamp",     "type": "long"},
        {"name": "ingestion_timestamp", "type": "long"}
    ]
}

# Base prices per route for realistic simulation
BASE_PRICES = {
    "ORD-SAN": 180, "ORD-PHX": 150, "ORD-BOS": 200,
    "ORD-PVG": 850, "ORD-CDG": 750, "ORD-SFO": 220,
    "ORD-DFW": 140, "ORD-STL": 90,  "ORD-LGA": 160,
    "ORD-MEX": 300, "ORD-YYC": 250, "ORD-DEN": 130,
    "ORD-LAX": 190, "ORD-DCA": 155, "ORD-JFK": 170,
    "ORD-SEA": 210, "ORD-BJX": 280, "ORD-MIA": 200,
    "ORD-NKG": 800, "ORD-CUN": 320
}
DEFAULT_BASE_PRICE = 200

def read_active_routes():
    """Read active routes from GCS Silver."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(ACTIVE_ROUTES_PATH)
    content = blob.download_as_text()
    routes = json.loads(content)
    print(f"Loaded {len(routes)} active routes from GCS")
    return routes

def simulate_price(route_id):
    """Generate a realistic simulated price for a route."""
    base = BASE_PRICES.get(route_id, DEFAULT_BASE_PRICE)
    # Add ±20% random variance
    variance = random.uniform(0.80, 1.20)
    # Add time-of-day effect — slightly higher in morning/evening
    hour = datetime.now(timezone.utc).hour
    time_factor = 1.10 if hour in [7, 8, 9, 17, 18, 19] else 1.0
    price = round(base * variance * time_factor, 2)
    return price

def serialize_avro(record, schema):
    """Serialize a record to Avro bytes."""
    parsed_schema = fastavro.parse_schema(schema)
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed_schema, record)
    return buffer.getvalue()

def publish_message(publisher, topic_path, record):
    """Publish a single Avro message to Pub/Sub."""
    # Since we set message-encoding=JSON on the topic,
    # publish as JSON bytes not binary Avro
    data = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()

def run(request=None):
    """Main entry point for Cloud Function."""
    now = datetime.now(timezone.utc)
    ingestion_ts = int(now.timestamp())

    # Step 1: Read active routes from GCS
    routes = read_active_routes()

    # Step 2: Set up Pub/Sub publisher
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # Step 3: For each route, generate price and publish
    published = 0
    for route in routes:
        record = {
            "route_id":            route["route_id"],
            "origin":              route["origin"],
            "destination":         route["destination"],
            "airline_code":        route["airline_code"],
            "airline_name":        route["airline_name"],
            "flight_number":       route["flight_number"],
            "price_usd":           simulate_price(route["route_id"]),
            "flight_date":         route["flight_date"],
            "scheduled_departure": route["scheduled_departure"],
            "event_timestamp":     ingestion_ts,
            "ingestion_timestamp": ingestion_ts
        }

        publish_message(publisher, topic_path, record)
        print(f"Published: {record['route_id']} @ ${record['price_usd']}")
        published += 1

    print(f"Function B complete. Published {published} messages.")
    return f"Published {published} messages.", 200

if __name__ == "__main__":
    run()