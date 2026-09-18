import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

# Load environment variables
load_dotenv()

api_key = os.getenv("DELHI_OTD_API_KEY")
base_url = os.getenv("DELHI_OTD_BASE_URL")

if not api_key or not base_url:
    raise ValueError("Missing API key or base URL. Check your .env file.")

def fetch_vehicle_positions():
    """Fetch and decode the live GTFS-RT feed into a list of dicts."""
    url = f"{base_url}?key={api_key}"
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            record = {
                "entity_id": entity.id,
                "vehicle_id": v.vehicle.id,
                "route_id": v.trip.route_id if v.HasField("trip") else None,
                "trip_id": v.trip.trip_id if v.HasField("trip") else None,
                "latitude": v.position.latitude if v.HasField("position") else None,
                "longitude": v.position.longitude if v.HasField("position") else None,
                "speed": v.position.speed if v.HasField("position") and v.position.HasField("speed") else None,
                "vehicle_timestamp": v.timestamp,
                "fetched_at": fetched_at,
            }
            records.append(record)

    return records

def save_to_local_json(records):
    """Save the fetched records locally, timestamped, as a stand-in for S3 landing."""
    os.makedirs("raw_data", exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("vehicle_positions_%Y%m%dT%H%M%SZ.json")
    filepath = os.path.join("raw_data", filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    print(f"Saved {len(records)} records to {filepath}")
    return filepath

if __name__ == "__main__":
    records = fetch_vehicle_positions()
    print(f"Fetched {len(records)} vehicle records.")
    save_to_local_json(records)