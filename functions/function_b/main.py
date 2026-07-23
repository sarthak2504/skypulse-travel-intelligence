import json
import time
import random
from datetime import datetime, timezone
import pytz

from google.cloud import pubsub_v1

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

PROJECT_ID = "triptide-28062026"
TOPIC_ID   = "flight-prices-raw"

# ─────────────────────────────────────────
# HARDCODED FLIGHT SCHEDULE (ORD departures)
# Departure times in Central Time (CT)
# Based on real published ORD timetables
# ─────────────────────────────────────────

FLIGHTS = [
    # Early morning
    {"flight_number": "UA500", "route_id": "ORD-LAX", "origin": "ORD", "destination": "LAX", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "06:00"},
    {"flight_number": "AA100", "route_id": "ORD-JFK", "origin": "ORD", "destination": "JFK", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "06:30"},
    {"flight_number": "DL200", "route_id": "ORD-ATL", "origin": "ORD", "destination": "ATL", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "07:00"},
    {"flight_number": "UA501", "route_id": "ORD-SFO", "origin": "ORD", "destination": "SFO", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "07:30"},
    {"flight_number": "AA101", "route_id": "ORD-MIA", "origin": "ORD", "destination": "MIA", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "08:00"},
    {"flight_number": "WN300", "route_id": "ORD-DEN", "origin": "ORD", "destination": "DEN", "airline_code": "WN", "airline_name": "Southwest Airlines", "departure_time": "08:30"},
    {"flight_number": "UA502", "route_id": "ORD-SEA", "origin": "ORD", "destination": "SEA", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "09:00"},
    {"flight_number": "AA102", "route_id": "ORD-DFW", "origin": "ORD", "destination": "DFW", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "09:30"},

    # Mid morning
    {"flight_number": "DL201", "route_id": "ORD-BOS", "origin": "ORD", "destination": "BOS", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "10:00"},
    {"flight_number": "UA503", "route_id": "ORD-LGA", "origin": "ORD", "destination": "LGA", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "10:30"},
    {"flight_number": "AA103", "route_id": "ORD-PHX", "origin": "ORD", "destination": "PHX", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "11:00"},
    {"flight_number": "WN301", "route_id": "ORD-STL", "origin": "ORD", "destination": "STL", "airline_code": "WN", "airline_name": "Southwest Airlines", "departure_time": "11:30"},

    # Afternoon
    {"flight_number": "UA504", "route_id": "ORD-LAX", "origin": "ORD", "destination": "LAX", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "12:00"},
    {"flight_number": "AA104", "route_id": "ORD-JFK", "origin": "ORD", "destination": "JFK", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "12:30"},
    {"flight_number": "DL202", "route_id": "ORD-ATL", "origin": "ORD", "destination": "ATL", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "13:00"},
    {"flight_number": "UA505", "route_id": "ORD-DEN", "origin": "ORD", "destination": "DEN", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "13:30"},
    {"flight_number": "AA105", "route_id": "ORD-MIA", "origin": "ORD", "destination": "MIA", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "14:00"},
    {"flight_number": "WN302", "route_id": "ORD-DFW", "origin": "ORD", "destination": "DFW", "airline_code": "WN", "airline_name": "Southwest Airlines", "departure_time": "14:30"},
    {"flight_number": "UA506", "route_id": "ORD-SFO", "origin": "ORD", "destination": "SFO", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "15:00"},
    {"flight_number": "AA106", "route_id": "ORD-SEA", "origin": "ORD", "destination": "SEA", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "15:30"},

    # Evening
    {"flight_number": "DL203", "route_id": "ORD-BOS", "origin": "ORD", "destination": "BOS", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "16:00"},
    {"flight_number": "UA507", "route_id": "ORD-LGA", "origin": "ORD", "destination": "LGA", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "16:30"},
    {"flight_number": "AA107", "route_id": "ORD-PHX", "origin": "ORD", "destination": "PHX", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "17:00"},
    {"flight_number": "WN303", "route_id": "ORD-STL", "origin": "ORD", "destination": "STL", "airline_code": "WN", "airline_name": "Southwest Airlines", "departure_time": "17:30"},
    {"flight_number": "UA508", "route_id": "ORD-LAX", "origin": "ORD", "destination": "LAX", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "18:00"},
    {"flight_number": "AA108", "route_id": "ORD-JFK", "origin": "ORD", "destination": "JFK", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "18:30"},
    {"flight_number": "DL204", "route_id": "ORD-ATL", "origin": "ORD", "destination": "ATL", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "19:00"},
    {"flight_number": "UA509", "route_id": "ORD-DEN", "origin": "ORD", "destination": "DEN", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "19:30"},

    # Night — international departures
    {"flight_number": "AA109", "route_id": "ORD-MIA", "origin": "ORD", "destination": "MIA", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "20:00"},
    {"flight_number": "UA600", "route_id": "ORD-LHR", "origin": "ORD", "destination": "LHR", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "20:30"},
    {"flight_number": "AA200", "route_id": "ORD-CDG", "origin": "ORD", "destination": "CDG", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "21:00"},
    {"flight_number": "LH400", "route_id": "ORD-FRA", "origin": "ORD", "destination": "FRA", "airline_code": "LH", "airline_name": "Lufthansa",          "departure_time": "21:30"},
    {"flight_number": "UA601", "route_id": "ORD-NRT", "origin": "ORD", "destination": "NRT", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "22:00"},
    {"flight_number": "AA201", "route_id": "ORD-GRU", "origin": "ORD", "destination": "GRU", "airline_code": "AA", "airline_name": "American Airlines",  "departure_time": "22:30"},
    {"flight_number": "UA602", "route_id": "ORD-PVG", "origin": "ORD", "destination": "PVG", "airline_code": "UA", "airline_name": "United Airlines",    "departure_time": "23:00"},
    {"flight_number": "DL300", "route_id": "ORD-AMS", "origin": "ORD", "destination": "AMS", "airline_code": "DL", "airline_name": "Delta Air Lines",    "departure_time": "23:30"},
]

# ─────────────────────────────────────────
# PRICE SIMULATION
# ─────────────────────────────────────────

SHORT_HAUL  = ["STL", "DFW", "ATL", "PHX", "LGA", "BOS", "IND", "MKE"]
MEDIUM_HAUL = ["LAX", "SFO", "SEA", "DEN", "MIA", "JFK"]
INTL        = ["LHR", "CDG", "FRA", "NRT", "PVG", "AMS", "GRU"]

def simulate_price(destination):
    if destination in SHORT_HAUL:
        base = random.randint(80, 150)
    elif destination in MEDIUM_HAUL:
        base = random.randint(150, 250)
    elif destination in INTL:
        base = random.randint(600, 1000)
    else:
        base = random.randint(150, 300)

    variance    = random.uniform(0.80, 1.20)
    hour        = datetime.now(timezone.utc).hour
    time_factor = 1.10 if hour in [7, 8, 9, 17, 18, 19] else 1.0
    return round(base * variance * time_factor, 2)

# ─────────────────────────────────────────
# DEPARTURE CHECK
# Departure times stored in CT, compare against UTC
# ─────────────────────────────────────────

CT = pytz.timezone("America/Chicago")

def is_active(flight):
    today_ct      = datetime.now(CT).strftime("%Y-%m-%d")
    departure_str = f"{today_ct} {flight['departure_time']}"
    departure_ct  = CT.localize(datetime.strptime(departure_str, "%Y-%m-%d %H:%M"))
    departure_utc = departure_ct.astimezone(timezone.utc)
    return datetime.now(timezone.utc) < departure_utc

# ─────────────────────────────────────────
# PUBLISH TO PUB/SUB
# ─────────────────────────────────────────

def run(request=None):
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    today     = datetime.now(CT).strftime("%Y-%m-%d")
    futures   = []
    skipped   = 0

    for flight in FLIGHTS:
        # Skip departed flights
        if not is_active(flight):
            skipped += 1
            continue

        now = int(time.time())

        # 5% chance of simulating a late message
        # Backdates event_timestamp by 3-5 minutes
        # ingestion_timestamp stays as now
        # lateness = now - event_ts > 120s → routes to late_arrivals in Dataflow
        if random.random() < 0.05:
            delay_seconds = random.randint(180, 300)
            event_ts = now - delay_seconds
        else:
            event_ts = now

        message = {
            "route_id":            flight["route_id"],
            "origin":              flight["origin"],
            "destination":         flight["destination"],
            "airline_code":        flight["airline_code"],
            "airline_name":        flight["airline_name"],
            "flight_number":       flight["flight_number"],
            "price_usd":           simulate_price(flight["destination"]),
            "flight_date":         today,
            "scheduled_departure": f"{today}T{flight['departure_time']}:00",
            "event_timestamp":     event_ts,
            "ingestion_timestamp": now
        }

        data   = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic_path, data)
        futures.append((future, flight["flight_number"]))

    # Wait for all acks
    published = 0
    failed    = 0

    for future, flight_number in futures:
        try:
            future.result()
            published += 1
        except Exception as e:
            print(f"Failed to publish {flight_number}: {e}")
            failed += 1

    print(f"Published: {published} | Skipped (departed): {skipped} | Failed: {failed}")
    return f"Published {published} messages.", 200


if __name__ == "__main__":
    run()