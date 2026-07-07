import os
import json
import requests
from datetime import datetime, timezone
from collections import Counter
from google.cloud import storage

# Configuration
AVIATIONSTACK_API_KEY = "0a8b77b40fa7430d95bb60ffc6c75462"
BUCKET_NAME = "skypulse-triptide"
DEP_IATA = "ORD"
TOP_ROUTES_COUNT = 20

def fetch_ord_flights():
    """Call AviationStack and return all scheduled ORD departures."""
    url = "http://api.aviationstack.com/v1/flights"
    params = {
        "access_key": AVIATIONSTACK_API_KEY,
        "dep_iata": DEP_IATA,
        "flight_status": "scheduled",
        "limit": 100
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    flights = data.get("data", [])
    print(f"Fetched {len(flights)} flights from AviationStack")
    return flights

def filter_valid_flights(flights):
    """Remove flights with missing critical fields."""
    valid = []
    for f in flights:
        origin = f.get("departure", {}).get("iata")
        destination = f.get("arrival", {}).get("iata")
        airline_iata = f.get("airline", {}).get("iata")
        airline_name = f.get("airline", {}).get("name")
        flight_iata = f.get("flight", {}).get("iata")
        scheduled = f.get("departure", {}).get("scheduled")
        
        # Only keep flights with all critical fields present
        if all([origin, destination, airline_iata, airline_name, 
                flight_iata, scheduled]):
            valid.append(f)
    
    print(f"{len(valid)} valid flights after filtering nulls")
    return valid

def get_top_routes(flights, top_n=TOP_ROUTES_COUNT):
    """Find top N routes by flight frequency."""
    route_counts = Counter(
        f"{f['departure']['iata']}-{f['arrival']['iata']}" 
        for f in flights
    )
    top = route_counts.most_common(top_n)
    print(f"Top {top_n} routes: {top}")
    return [route for route, count in top]

def build_active_routes(flights, top_routes):
    """Build active routes list from top routes."""
    active_routes = []
    seen_routes = set()
    
    for f in flights:
        origin = f["departure"]["iata"]
        destination = f["arrival"]["iata"]
        route_id = f"{origin}-{destination}"
        
        # Only include top routes, one entry per route
        if route_id in top_routes and route_id not in seen_routes:
            active_routes.append({
                "route_id": route_id,
                "origin": origin,
                "destination": destination,
                "airline_code": f["airline"]["iata"],
                "airline_name": f["airline"]["name"],
                "flight_number": f["flight"]["iata"],
                "scheduled_departure": f["departure"]["scheduled"],
                "flight_date": f.get("flight_date")
            })
            seen_routes.add(route_id)
    
    print(f"Built {len(active_routes)} active routes")
    return active_routes

def save_to_gcs(data, blob_path):
    """Save JSON data to GCS."""
    client = storage.Client(project="triptide-28062026")
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"Saved to gs://{BUCKET_NAME}/{blob_path}")

def run(request=None):
    """Main entry point for Cloud Function."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    print(f"Starting Function A for date: {today}")
    
    # Step 1: Fetch from AviationStack
    flights = fetch_ord_flights()
    
    # Step 2: Filter out incomplete records
    valid_flights = filter_valid_flights(flights)
    
    # Step 3: Save full raw response to Bronze
    save_to_gcs(
        valid_flights,
        f"bronze/routes/{today}.json"
    )
    
    # Step 4: Find top routes by frequency
    top_routes = get_top_routes(valid_flights)
    
    # Step 5: Build active routes list
    active_routes = build_active_routes(valid_flights, set(top_routes))
    
    # Step 6: Save active routes to Silver
    save_to_gcs(
        active_routes,
        "silver/active_routes.json"
    )
    
    return f"Function A complete. {len(active_routes)} active routes saved.", 200

if __name__ == "__main__":
    run()