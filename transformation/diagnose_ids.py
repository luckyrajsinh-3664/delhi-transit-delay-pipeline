import csv, json, glob

feed_trips, feed_routes = set(), set()
for f in glob.glob("raw_data/*.json"):
    for r in json.load(open(f, encoding="utf-8")):
        feed_trips.add(str(r["trip_id"]).strip())
        feed_routes.add(str(r["route_id"]).strip())

with open("static_gtfs/trips.txt", encoding="utf-8-sig", newline="") as fh:
    rows = list(csv.DictReader(fh))

static_trips = {r["trip_id"].strip() for r in rows}
static_routes = {r["route_id"].strip() for r in rows}

print("trips.txt columns      :", list(rows[0].keys()))
print("feed distinct trip_ids :", len(feed_trips))
print("feed distinct route_ids:", len(feed_routes))
print("static trips           :", len(static_trips))
print("trip_id matches        :", len(feed_trips & static_trips))
print("route_id matches       :", len(feed_routes & static_routes))
print("sample feed trip_ids   :", sorted(feed_trips)[:8])
print("sample static trip_ids :", sorted(static_trips)[:8])

common = sorted(feed_routes & static_routes)
if common:
    r0 = common[0]
    print(f"\nroute {r0} in feed  :", sorted(t for t in feed_trips if t.startswith(r0 + "_"))[:8])
    print(f"route {r0} in static:", sorted(x["trip_id"] for x in rows if x["route_id"].strip() == r0)[:8])