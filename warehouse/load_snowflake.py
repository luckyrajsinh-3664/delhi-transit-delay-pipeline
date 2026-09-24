import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


def connect():
    load_dotenv()
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/snowflake_rsa_key.pem")
    if not account:
        sys.exit("SNOWFLAKE_ACCOUNT is missing - add it to .env")
    if not Path(key_path).exists():
        sys.exit(f"Private key not found at {key_path} - run warehouse/snowflake_keygen.py first")
    return snowflake.connector.connect(
        account=account,
        user=os.getenv("SNOWFLAKE_USER", "ROUTERADAR_LOADER_SVC"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=key_path,
        role=os.getenv("SNOWFLAKE_ROLE", "ROUTERADAR_LOADER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ROUTERADAR_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ROUTERADAR"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "DW"),
        # staged timestamps are UTC wall-clock values, so the session must read them as UTC
        session_parameters={"TIMEZONE": "UTC"},
    )


def read_parquet(path):
    df = pd.read_parquet(path)
    if "service_date" in df.columns:  # Spark partition column comes back as 'category'
        df["service_date"] = pd.to_datetime(df["service_date"].astype(str))
    return df


def stage(conn, table, ddl_cols, df):
    """Create a temporary staging table and bulk-upload df into it."""
    conn.cursor().execute(f"CREATE OR REPLACE TEMPORARY TABLE {table} ({ddl_cols})")
    ok, _, nrows, _ = write_pandas(conn, df, table, quote_identifiers=False,
                                   auto_create_table=False, use_logical_type=True)
    if not ok or nrows != len(df):
        sys.exit(f"Upload to {table} failed ({nrows} of {len(df)} rows)")


def merge_dim(conn, dim, id_col, ids):
    ids = sorted({str(i) for i in ids if pd.notna(i)})
    stage(conn, f"stg_{dim}", f"{id_col} TEXT", pd.DataFrame({id_col: ids}))
    conn.cursor().execute(f"""
        MERGE INTO {dim} t USING stg_{dim} s ON t.{id_col} = s.{id_col}
        WHEN NOT MATCHED THEN INSERT ({id_col}) VALUES (s.{id_col})""")


def load_route_hour(conn, rh):
    rh = rh.copy()
    rh["date_key"] = rh["service_date"].dt.strftime("%Y%m%d").astype(int)
    rh["hour_key"] = rh["hour_of_day"].astype(int)
    rh["route_id"] = rh["route_id"].astype(str)
    cols = ["date_key", "hour_key", "route_id", "n_pings", "n_vehicles", "n_speed_obs",
            "n_moving", "avg_speed_kmph", "avg_moving_speed_kmph", "n_bunched"]
    stage(conn, "stg_route_hour", """
        date_key INTEGER, hour_key SMALLINT, route_id TEXT, n_pings INTEGER,
        n_vehicles INTEGER, n_speed_obs INTEGER, n_moving INTEGER,
        avg_speed_kmph NUMERIC(7,2), avg_moving_speed_kmph NUMERIC(7,2), n_bunched INTEGER""",
        rh[cols])
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO fact_route_hour_speed t
        USING (SELECT s.*, r.route_key
               FROM stg_route_hour s JOIN dim_route r ON r.route_id = s.route_id) s
        ON t.date_key = s.date_key AND t.hour_key = s.hour_key AND t.route_key = s.route_key
        WHEN MATCHED THEN UPDATE SET
            n_pings = s.n_pings, n_vehicles = s.n_vehicles, n_speed_obs = s.n_speed_obs,
            n_moving = s.n_moving, avg_speed_kmph = s.avg_speed_kmph,
            avg_moving_speed_kmph = s.avg_moving_speed_kmph, n_bunched = s.n_bunched,
            loaded_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (date_key, hour_key, route_key, n_pings, n_vehicles, n_speed_obs, n_moving,
             avg_speed_kmph, avg_moving_speed_kmph, n_bunched)
        VALUES
            (s.date_key, s.hour_key, s.route_key, s.n_pings, s.n_vehicles, s.n_speed_obs,
             s.n_moving, s.avg_speed_kmph, s.avg_moving_speed_kmph, s.n_bunched)""")
    return len(rh)


def load_pings(conn, pings):
    p = pings.copy()
    ts = pd.to_datetime(p["obs_ts"])
    if ts.dt.tz is not None:              # store as UTC wall-clock (session TIMEZONE is UTC)
        ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    p["obs_ts"] = ts.astype("datetime64[us]")
    p["date_key"] = p["service_date"].dt.strftime("%Y%m%d").astype(int)
    p["hour_key"] = p["hour_of_day"].astype(int)
    p["route_id"] = p["route_id"].astype(str)
    p["vehicle_id"] = p["vehicle_id"].astype(str)
    p = p.drop_duplicates(["vehicle_id", "obs_ts"])  # MERGE needs one source row per key
    for c in ["dispatch_hour", "dispatch_minute", "gap_s"]:
        p[c] = p[c].astype("Int64")
    cols = ["obs_ts", "date_key", "hour_key", "route_id", "vehicle_id", "trip_id",
            "dispatch_hour", "dispatch_minute", "lat", "lon", "gap_s", "dist_m",
            "speed_kmph", "is_speed_valid", "is_moving", "is_bunched"]
    stage(conn, "stg_ping", """
        obs_ts TIMESTAMP_NTZ, date_key INTEGER, hour_key SMALLINT, route_id TEXT,
        vehicle_id TEXT, trip_id TEXT, dispatch_hour SMALLINT, dispatch_minute SMALLINT,
        lat FLOAT, lon FLOAT, gap_s INTEGER, dist_m FLOAT, speed_kmph FLOAT,
        is_speed_valid BOOLEAN, is_moving BOOLEAN, is_bunched BOOLEAN""", p[cols])
    cur = conn.cursor()
    cur.execute("""
        MERGE INTO fact_vehicle_ping t
        USING (SELECT s.* EXCLUDE (obs_ts), s.obs_ts::TIMESTAMP_TZ AS obs_ts,
                      r.route_key, v.vehicle_key
               FROM stg_ping s
               JOIN dim_route   r ON r.route_id   = s.route_id
               JOIN dim_vehicle v ON v.vehicle_id = s.vehicle_id) s
        ON t.vehicle_key = s.vehicle_key AND t.obs_ts = s.obs_ts
        WHEN MATCHED THEN UPDATE SET
            trip_id = s.trip_id, gap_s = s.gap_s, dist_m = s.dist_m,
            speed_kmph = s.speed_kmph, is_speed_valid = s.is_speed_valid,
            is_moving = s.is_moving, is_bunched = s.is_bunched
        WHEN NOT MATCHED THEN INSERT
            (obs_ts, date_key, hour_key, route_key, vehicle_key, trip_id,
             dispatch_hour, dispatch_minute, lat, lon, gap_s, dist_m,
             speed_kmph, is_speed_valid, is_moving, is_bunched)
        VALUES
            (s.obs_ts, s.date_key, s.hour_key, s.route_key, s.vehicle_key, s.trip_id,
             s.dispatch_hour, s.dispatch_minute, s.lat, s.lon, s.gap_s, s.dist_m,
             s.speed_kmph, s.is_speed_valid, s.is_moving, s.is_bunched)""")
    return len(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="processed_data")
    args = ap.parse_args()
    root = Path(args.input)

    pings = read_parquet(root / "vehicle_pings_clean")
    rh = read_parquet(root / "route_hour_speed")
    print(f"Read {len(pings)} pings and {len(rh)} route-hour rows from {root}")

    conn = connect()
    try:
        merge_dim(conn, "dim_route", "route_id", list(pings["route_id"]) + list(rh["route_id"]))
        merge_dim(conn, "dim_vehicle", "vehicle_id", pings["vehicle_id"])
        n_rh = load_route_hour(conn, rh)
        n_p = load_pings(conn, pings)
        print(f"Merged {n_rh} route-hour rows and {n_p} pings")

        cur = conn.cursor()
        for t in ["dim_route", "dim_vehicle", "fact_route_hour_speed", "fact_vehicle_ping"]:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  DW.{t}: {cur.fetchone()[0]} rows")
        # Snowflake does not enforce UNIQUE, so check the natural keys explicitly
        cur.execute("""SELECT
            (SELECT COUNT(*) - COUNT(DISTINCT date_key, hour_key, route_key) FROM fact_route_hour_speed),
            (SELECT COUNT(*) - COUNT(DISTINCT vehicle_key, obs_ts) FROM fact_vehicle_ping),
            (SELECT COUNT(*) - COUNT(DISTINCT route_id) FROM dim_route),
            (SELECT COUNT(*) - COUNT(DISTINCT vehicle_id) FROM dim_vehicle)""")
        dups = cur.fetchone()
        print(f"  duplicate natural keys (route-hour, ping, route, vehicle): {dups}")
        if any(dups):
            sys.exit("Duplicates found - investigate before trusting this load")
    finally:
        conn.close()


if __name__ == "__main__":
    main()