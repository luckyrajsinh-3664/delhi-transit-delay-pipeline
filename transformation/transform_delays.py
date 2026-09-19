"""
transform_delays.py - Delhi Transit Delay Pipeline (PySpark stage)

Reads raw GTFS-RT vehicle-position JSON snapshots + static GTFS, estimates the
schedule delay for every vehicle observation, and writes clean Parquet.

Raw file format (flat JSON array): entity_id, vehicle_id, route_id, trip_id,
latitude, longitude, speed, vehicle_timestamp (epoch seconds), fetched_at.

Delay logic (snapshot-friendly, because Airflow polls every ~10 min, not continuously):
  1. Join each ping to every scheduled stop on its trip (via trip_id).
  2. Pick the stop physically nearest to the GPS position (haversine).
  3. delay_sec = observed time - scheduled arrival time at that stop,
     folded to the nearest day so after-midnight trips are handled.
  4. Flag confidence: is_near_stop (<= NEAR_STOP_M) and is_outlier (|delay| > 3h).

Run (dev, local files):
  python transformation/transform_delays.py --input "raw_data/*.json" \
      --static static_gtfs --output processed_data/vehicle_delays
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F

NEAR_STOP_M = 150          # "at the stop" threshold in metres
OUTLIER_SEC = 3 * 3600     # delays beyond +/- 3h are flagged, not dropped


def build_spark():
    return (
        SparkSession.builder.appName("delhi-transit-delays")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ---------------------------------------------------------------- readers
def read_positions(spark, path):
    """Raw files are a flat JSON array of records:
    entity_id, vehicle_id, route_id, trip_id, latitude, longitude, speed,
    vehicle_timestamp (epoch seconds), fetched_at."""
    raw = spark.read.option("multiLine", True).json(path)
    return raw.select(
        F.col("vehicle_id").cast("string").alias("vehicle_id"),
        F.trim(F.col("trip_id").cast("string")).alias("trip_id"),
        F.col("route_id").cast("string").alias("route_id_rt"),
        F.col("latitude").cast("double").alias("lat"),
        F.col("longitude").cast("double").alias("lon"),
        F.col("speed").cast("double").alias("speed"),
        F.col("vehicle_timestamp").cast("long").alias("obs_epoch"),
    )


def gtfs_sec(c):
    """'HH:MM:SS' -> seconds since midnight (GTFS allows HH >= 24)."""
    p = F.split(c, ":")
    return p[0].cast("int") * 3600 + p[1].cast("int") * 60 + p[2].cast("int")


def read_static(spark, d):
    rd = lambda n: spark.read.option("header", True).csv(f"{d}/{n}.txt")
    stop_times = rd("stop_times").select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("stop_id").alias("stop_id"),
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        gtfs_sec(F.trim(F.col("arrival_time"))).alias("sched_arrival_sec"),
    )
    stops = rd("stops").select(
        F.trim("stop_id").alias("stop_id"),
        "stop_name",
        F.col("stop_lat").cast("double").alias("stop_lat"),
        F.col("stop_lon").cast("double").alias("stop_lon"),
    )
    trips = rd("trips").select(F.trim("trip_id").alias("trip_id"), "route_id")
    routes = rd("routes").select("route_id", "route_short_name", "route_long_name")
    return stop_times, stops, trips, routes


# ---------------------------------------------------------------- transform
def haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (F.col(c) for c in (lat1, lon1, lat2, lon2))
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * 6371000.0 * F.asin(F.sqrt(a))


def build_delays(pos, stop_times, stops, trips, routes):
    pos = (
        pos.filter(
            F.col("trip_id").isNotNull()
            & F.col("lat").isNotNull()
            & F.col("lon").isNotNull()
            & F.col("obs_epoch").isNotNull()
        )
        .dropDuplicates(["vehicle_id", "obs_epoch"])
        .withColumn("obs_ts", F.col("obs_epoch").cast("timestamp"))
        .withColumn("obs_date", F.to_date("obs_ts"))
    )

    # every ping x every scheduled stop on its trip, then keep the nearest stop
    cand = (
        stop_times.join(F.broadcast(pos), "trip_id")
        .join(F.broadcast(stops), "stop_id")
        .withColumn("dist_to_stop_m", haversine_m("lat", "lon", "stop_lat", "stop_lon"))
    )
    w = Window.partitionBy("vehicle_id", "obs_epoch").orderBy(F.col("dist_to_stop_m"))
    nearest = cand.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    # delay vs. schedule, folded to the nearest day so after-midnight trips work
    midnight = F.col("obs_date").cast("timestamp").cast("long")
    raw_delay = F.col("obs_epoch") - (midnight + F.col("sched_arrival_sec"))

    return (
        nearest.withColumn("day_shift", F.round(raw_delay / 86400.0).cast("int"))
        .withColumn("delay_sec", raw_delay - F.col("day_shift") * 86400)
        .withColumn("service_date", F.expr("date_sub(obs_date, day_shift)"))
        .withColumn("delay_min", F.round(F.col("delay_sec") / 60.0, 2))
        .withColumn("is_near_stop", F.col("dist_to_stop_m") <= NEAR_STOP_M)
        .withColumn("is_outlier", F.abs(F.col("delay_sec")) > OUTLIER_SEC)
        .withColumn("hour_of_day", F.hour("obs_ts"))
        .withColumn("day_of_week", F.dayofweek("obs_ts"))
        .join(trips, "trip_id", "left")
        .join(routes, "route_id", "left")
        .select(
            "vehicle_id", "trip_id", "route_id", "route_short_name", "route_long_name",
            "stop_id", "stop_name", "stop_sequence",
            "lat", "lon", "speed", "dist_to_stop_m",
            "obs_ts", "service_date", "hour_of_day", "day_of_week",
            "delay_sec", "delay_min", "is_near_stop", "is_outlier",
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="raw_data/*.json")
    ap.add_argument("--static", default="static_gtfs")
    ap.add_argument("--output", default="processed_data/vehicle_delays")
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    pos = read_positions(spark, args.input)
    print(f"Raw vehicle pings read: {pos.count()}")

    st, stops, trips, routes = read_static(spark, args.static)
    out = build_delays(pos, st, stops, trips, routes).cache()
    print(f"Rows with a computed delay: {out.count()}")

    out.select("delay_min", "dist_to_stop_m").summary("min", "25%", "50%", "75%", "max").show()
    out.show(10, truncate=False)

    out.write.mode("overwrite").partitionBy("service_date").parquet(args.output)
    print(f"Written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()