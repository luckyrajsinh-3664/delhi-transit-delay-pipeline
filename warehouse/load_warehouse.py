"""
load_warehouse.py - load the Spark outputs (Parquet) into the PostgreSQL star schema.

Reads : processed_data/vehicle_pings_clean, processed_data/route_hour_speed
Writes: dw.dim_route, dw.dim_vehicle, dw.fact_route_hour_speed, dw.fact_vehicle_ping

Safe to re-run: dimensions are inserted once, route-hour rows and pings are upserted,
so the newest Spark run always wins and nothing is duplicated.

Connection (defaults match docker-compose.yaml; the password comes from .env):
  WAREHOUSE_HOST=localhost  WAREHOUSE_PORT=5433  WAREHOUSE_DB=transit_dw
  WAREHOUSE_USER=dw_admin   WAREHOUSE_PASSWORD=...

Run from the project root (venv-dq environment):
  python warehouse/load_warehouse.py --input processed_data
"""
import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values


def connect():
    load_dotenv()
    password = os.getenv("WAREHOUSE_PASSWORD")
    if not password:
        sys.exit("WAREHOUSE_PASSWORD is missing - add it to .env")
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "transit_dw"),
        user=os.getenv("WAREHOUSE_USER", "dw_admin"),
        password=password,
    )


def py(v):
    """pandas/numpy value -> plain Python value (NaN/NaT/NA -> None)."""
    if v is None or (not isinstance(v, (str, bool)) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if hasattr(v, "item"):
        return v.item()
    return v


def rows(df, cols):
    return [tuple(py(v) for v in r) for r in df[cols].itertuples(index=False, name=None)]


def read_parquet(path):
    df = pd.read_parquet(path)
    if "service_date" in df.columns:  # Spark partition column comes back as 'category'
        df["service_date"] = pd.to_datetime(df["service_date"].astype(str))
    return df


def upsert_dim(cur, table, key_col, id_col, ids):
    ids = sorted({str(i) for i in ids if pd.notna(i)})
    execute_values(
        cur,
        f"INSERT INTO dw.{table} ({id_col}) VALUES %s ON CONFLICT ({id_col}) DO NOTHING",
        [(i,) for i in ids],
    )
    cur.execute(f"SELECT {id_col}, {key_col} FROM dw.{table}")
    return dict(cur.fetchall())


def load_route_hour(cur, rh, route_map):
    rh = rh.copy()
    rh["date_key"] = rh["service_date"].dt.strftime("%Y%m%d").astype(int)
    rh["hour_key"] = rh["hour_of_day"].astype(int)
    rh["route_key"] = rh["route_id"].astype(str).map(route_map)
    cols = ["date_key", "hour_key", "route_key", "n_pings", "n_vehicles", "n_speed_obs",
            "n_moving", "avg_speed_kmph", "avg_moving_speed_kmph", "n_bunched"]
    execute_values(
        cur,
        """
        INSERT INTO dw.fact_route_hour_speed
            (date_key, hour_key, route_key, n_pings, n_vehicles, n_speed_obs, n_moving,
             avg_speed_kmph, avg_moving_speed_kmph, n_bunched)
        VALUES %s
        ON CONFLICT (date_key, hour_key, route_key) DO UPDATE SET
            n_pings = EXCLUDED.n_pings, n_vehicles = EXCLUDED.n_vehicles,
            n_speed_obs = EXCLUDED.n_speed_obs, n_moving = EXCLUDED.n_moving,
            avg_speed_kmph = EXCLUDED.avg_speed_kmph,
            avg_moving_speed_kmph = EXCLUDED.avg_moving_speed_kmph,
            n_bunched = EXCLUDED.n_bunched, loaded_at = now()
        """,
        rows(rh, cols),
        page_size=5000,
    )
    return len(rh)


def load_pings(cur, pings, route_map, vehicle_map):
    p = pings.copy()
    ts = pd.to_datetime(p["obs_ts"])
    p["obs_ts"] = ts.dt.tz_localize("UTC") if ts.dt.tz is None else ts
    p["date_key"] = p["service_date"].dt.strftime("%Y%m%d").astype(int)
    p["hour_key"] = p["hour_of_day"].astype(int)
    p["route_key"] = p["route_id"].astype(str).map(route_map)
    p["vehicle_key"] = p["vehicle_id"].astype(str).map(vehicle_map)
    p = p.drop_duplicates(["vehicle_key", "obs_ts"])
    cols = ["obs_ts", "date_key", "hour_key", "route_key", "vehicle_key", "trip_id",
            "dispatch_hour", "dispatch_minute", "lat", "lon", "gap_s", "dist_m",
            "speed_kmph", "is_speed_valid", "is_moving", "is_bunched"]
    execute_values(
        cur,
        """
        INSERT INTO dw.fact_vehicle_ping
            (obs_ts, date_key, hour_key, route_key, vehicle_key, trip_id,
             dispatch_hour, dispatch_minute, lat, lon, gap_s, dist_m,
             speed_kmph, is_speed_valid, is_moving, is_bunched)
        VALUES %s
        ON CONFLICT (vehicle_key, obs_ts) DO UPDATE SET
            trip_id = EXCLUDED.trip_id, gap_s = EXCLUDED.gap_s, dist_m = EXCLUDED.dist_m,
            speed_kmph = EXCLUDED.speed_kmph, is_speed_valid = EXCLUDED.is_speed_valid,
            is_moving = EXCLUDED.is_moving, is_bunched = EXCLUDED.is_bunched
        """,
        rows(p, cols),
        page_size=5000,
    )
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
        with conn, conn.cursor() as cur:
            route_map = upsert_dim(cur, "dim_route", "route_key", "route_id",
                                   list(pings["route_id"]) + list(rh["route_id"]))
            vehicle_map = upsert_dim(cur, "dim_vehicle", "vehicle_key", "vehicle_id", pings["vehicle_id"])
            n_rh = load_route_hour(cur, rh, route_map)
            n_p = load_pings(cur, pings, route_map, vehicle_map)
        print(f"Upserted {n_rh} route-hour rows and {n_p} pings")

        with conn, conn.cursor() as cur:
            for t in ["dim_route", "dim_vehicle", "fact_route_hour_speed", "fact_vehicle_ping"]:
                cur.execute(f"SELECT count(*) FROM dw.{t}")
                print(f"  dw.{t}: {cur.fetchone()[0]} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()