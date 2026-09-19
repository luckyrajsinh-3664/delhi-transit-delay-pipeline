"""
transform_speed_analytics.py - Delhi Transit Pipeline (PySpark stage)

Builds analytics from the live GTFS-RT vehicle positions only (no static join needed):
  1. vehicle_pings_clean : cleaned pings + derived speed, moving flag, bunching flag
  2. route_hour_speed    : per route x date x hour metrics (ML-ready)

Raw format (flat JSON array): entity_id, vehicle_id, route_id, trip_id,
latitude, longitude, speed, vehicle_timestamp (epoch s), fetched_at.

IMPORTANT: the feed's own `speed` field is always 0.0, so it is ignored.
Speed is DERIVED from each vehicle's consecutive pings:
  speed_kmph = straight-line distance between two pings / time between them
It is a net-progress speed (lower than the true speed on winding roads), so it is a
congestion proxy, not a speedometer reading. It needs snapshots close together in time.

Definitions:
  derived speed valid : gap between the two pings is 60 s .. 1500 s
  is_speed_valid      : derived speed is present and <= 80 km/h (higher = GPS jump)
  is_moving           : derived speed > 3 km/h
  is_bunched          : moving bus with another moving bus of the SAME route within
                        300 m in the SAME snapshot (fetched_at)

Run:
  python transformation/transform_speed_analytics.py --input "raw_data/*.json" --output processed_data
"""
import argparse

from pyspark.sql import SparkSession, Window, functions as F

MIN_GAP_S = 60
MAX_GAP_S = 1500
MAX_KMPH = 80.0
MOVING_KMPH = 3.0
BUNCH_M = 300
# rough bounding box around Delhi NCR, drops GPS junk like (0, 0)
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 28.0, 29.0, 76.5, 77.8


def build_spark():
    return (
        SparkSession.builder.appName("delhi-transit-speed-analytics")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_positions(spark, path):
    raw = spark.read.option("multiLine", True).json(path)
    return raw.select(
        F.col("vehicle_id").cast("string").alias("vehicle_id"),
        F.trim(F.col("route_id").cast("string")).alias("route_id"),
        F.trim(F.col("trip_id").cast("string")).alias("trip_id"),
        F.col("latitude").cast("double").alias("lat"),
        F.col("longitude").cast("double").alias("lon"),
        F.col("vehicle_timestamp").cast("long").alias("obs_epoch"),
        F.col("fetched_at").cast("string").alias("snapshot_id"),
    )


def haversine_m(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * 6371000.0 * F.asin(F.sqrt(a))


def clean(pos):
    parts = F.split(F.col("trip_id"), "_")
    return (
        pos.filter(
            F.col("route_id").isNotNull()
            & F.col("vehicle_id").isNotNull()
            & F.col("lat").between(LAT_MIN, LAT_MAX)
            & F.col("lon").between(LON_MIN, LON_MAX)
            & F.col("obs_epoch").isNotNull()
        )
        .dropDuplicates(["vehicle_id", "obs_epoch"])
        .withColumn("obs_ts", F.col("obs_epoch").cast("timestamp"))
        .withColumn("service_date", F.to_date("obs_ts"))
        .withColumn("hour_of_day", F.hour("obs_ts"))
        .withColumn("day_of_week", F.dayofweek("obs_ts"))
        # dispatch time is encoded in the trip_id: route_HH_MM[_seq]
        .withColumn("dispatch_hour", F.get(parts, F.lit(1)).try_cast("int"))
        .withColumn("dispatch_minute", F.get(parts, F.lit(2)).try_cast("int"))
    )


def add_derived_speed(df):
    w = Window.partitionBy("vehicle_id").orderBy("obs_epoch")
    df = (
        df.withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("gap_s", F.col("obs_epoch") - F.lag("obs_epoch").over(w))
        .withColumn("dist_m", haversine_m(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")))
    )
    gap_ok = F.col("gap_s").between(MIN_GAP_S, MAX_GAP_S)
    speed = F.when(gap_ok, F.try_divide(F.col("dist_m"), F.col("gap_s")) * 3.6)
    return (
        df.withColumn("speed_kmph", F.round(speed, 2))
        .withColumn("is_speed_valid", F.coalesce(F.col("speed_kmph") <= MAX_KMPH, F.lit(False)))
        .withColumn(
            "is_moving",
            F.coalesce(F.col("is_speed_valid") & (F.col("speed_kmph") > MOVING_KMPH), F.lit(False)),
        )
        .drop("prev_lat", "prev_lon")
    )


def flag_bunching(df):
    m = df.filter("is_moving")
    a = m.select(
        F.col("snapshot_id").alias("s"), F.col("route_id").alias("r"),
        F.col("vehicle_id").alias("v1"), F.col("lat").alias("lat1"), F.col("lon").alias("lon1"),
    )
    b = m.select(
        F.col("snapshot_id").alias("s"), F.col("route_id").alias("r"),
        F.col("vehicle_id").alias("v2"), F.col("lat").alias("lat2"), F.col("lon").alias("lon2"),
    )
    close = (
        a.join(b, ["s", "r"])
        .filter(F.col("v1") < F.col("v2"))
        .filter(haversine_m(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")) < BUNCH_M)
    )
    bunched = (
        close.select("s", "r", F.col("v1").alias("vehicle_id"))
        .union(close.select("s", "r", F.col("v2").alias("vehicle_id")))
        .distinct()
        .withColumnRenamed("s", "snapshot_id")
        .withColumnRenamed("r", "route_id")
        .withColumn("is_bunched", F.lit(True))
    )
    return (
        df.join(bunched, ["snapshot_id", "route_id", "vehicle_id"], "left")
        .withColumn("is_bunched", F.coalesce(F.col("is_bunched"), F.lit(False)))
    )


def route_hour(df):
    valid = F.when(F.col("is_speed_valid"), F.col("speed_kmph"))
    moving = F.when(F.col("is_moving"), F.col("speed_kmph"))
    return (
        df.groupBy("route_id", "service_date", "hour_of_day", "day_of_week")
        .agg(
            F.count("*").alias("n_pings"),
            F.countDistinct("vehicle_id").alias("n_vehicles"),
            F.sum(F.col("is_speed_valid").cast("int")).alias("n_speed_obs"),
            F.sum(F.col("is_moving").cast("int")).alias("n_moving"),
            F.round(F.avg(valid), 2).alias("avg_speed_kmph"),
            F.round(F.avg(moving), 2).alias("avg_moving_speed_kmph"),
            F.sum(F.col("is_bunched").cast("int")).alias("n_bunched"),
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="raw_data/*.json")
    ap.add_argument("--output", default="processed_data")
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    pos = read_positions(spark, args.input)
    print(f"Raw pings read: {pos.count()}")

    pings = flag_bunching(add_derived_speed(clean(pos))).cache()
    print(f"Clean pings: {pings.count()}")
    print(f"Pings with a valid derived speed: {pings.filter('is_speed_valid').count()}")

    rh = route_hour(pings).cache()
    print(f"Route-hour rows: {rh.count()}")

    print("Fleet state (pings with a valid derived speed):")
    pings.filter("is_speed_valid").agg(
        F.count("*").alias("n"),
        F.round(F.avg(F.col("is_moving").cast("double")) * 100, 1).alias("pct_moving"),
        F.round(F.avg("speed_kmph"), 2).alias("avg_speed_kmph"),
        F.round(F.avg(F.col("is_bunched").cast("double")) * 100, 1).alias("pct_bunched"),
    ).show()

    print("Slowest routes by speed of moving buses (>= 3 moving pings):")
    rh.filter("n_moving >= 3").orderBy("avg_moving_speed_kmph").show(10, truncate=False)

    pings.write.mode("overwrite").partitionBy("service_date").parquet(f"{args.output}/vehicle_pings_clean")
    rh.write.mode("overwrite").partitionBy("service_date").parquet(f"{args.output}/route_hour_speed")
    print(f"Written to {args.output}/vehicle_pings_clean and {args.output}/route_hour_speed")
    spark.stop()


if __name__ == "__main__":
    main()