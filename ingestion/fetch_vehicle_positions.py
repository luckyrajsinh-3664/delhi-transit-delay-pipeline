import os
import json
import boto3
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

# Load environment variables
load_dotenv()

api_key = os.getenv("DELHI_OTD_API_KEY")
base_url = os.getenv("DELHI_OTD_BASE_URL")
aws_region = os.getenv("AWS_REGION")
bucket_name = os.getenv("S3_BUCKET_NAME")

if not api_key or not base_url:
    raise ValueError("Missing API key or base URL. Check your .env file.")

if not bucket_name:
    raise ValueError("Missing S3_BUCKET_NAME. Check your .env file.")

# boto3 automatically picks up AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from .env-loaded environment
s3_client = boto3.client("s3", region_name=aws_region)

def fetch_vehicle_positions():
    """Fetch and decode the live GTFS-RT feed into a list of dicts."""
    url = f"{base_url}?key={api_key}"
    response = requests.get(url, timeout=30)
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

def save_locally(records):
    """Keep a local backup copy too, for debugging/offline testing."""
    os.makedirs("raw_data", exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("vehicle_positions_%Y%m%dT%H%M%SZ.json")
    filepath = os.path.join("raw_data", filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    return filepath, filename

def upload_to_s3(filepath, filename, records):
    """Upload the JSON file to S3, organized by date for easy partitioning later."""
    now = datetime.now(timezone.utc)
    # Partition path: raw/year=2026/month=09/day=18/filename.json
    s3_key = f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/{filename}"

    s3_client.upload_file(filepath, bucket_name, s3_key)

    print(f"Uploaded {len(records)} records to s3://{bucket_name}/{s3_key}")
    return s3_key

if __name__ == "__main__":
    records = fetch_vehicle_positions()
    print(f"Fetched {len(records)} vehicle records.")

    filepath, filename = save_locally(records)
    print(f"Saved locally to {filepath}")

    upload_to_s3(filepath, filename, records)