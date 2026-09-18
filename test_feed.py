import os
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

# Load variables from .env
load_dotenv()

api_key = os.getenv("DELHI_OTD_API_KEY")
base_url = os.getenv("DELHI_OTD_BASE_URL")

if not api_key or not base_url:
    raise ValueError("Missing API key or base URL. Check your .env file.")

url = f"{base_url}?key={api_key}"

print(f"Fetching from: {base_url}?key=***HIDDEN***")

response = requests.get(url)

if response.status_code != 200:
    print(f"Request failed. Status code: {response.status_code}")
    print(response.text[:500])
else:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    print(f"Feed fetched successfully!")
    print(f"Number of entities (vehicles): {len(feed.entity)}")

    for entity in feed.entity[:3]:
        if entity.HasField('vehicle'):
            v = entity.vehicle
            print("---")
            print(f"Vehicle ID: {v.vehicle.id}")
            print(f"Route ID: {v.trip.route_id}")
            print(f"Lat/Lon: {v.position.latitude}, {v.position.longitude}")
            print(f"Timestamp: {v.timestamp}")