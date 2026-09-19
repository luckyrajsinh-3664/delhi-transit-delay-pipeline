-- 01_schema.sql - star schema for the Delhi transit warehouse (PostgreSQL)
-- Safe to run more than once (CREATE ... IF NOT EXISTS / ON CONFLICT DO NOTHING).

CREATE SCHEMA IF NOT EXISTS dw;
SET search_path TO dw;

-- ---------- dimensions ----------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER  PRIMARY KEY,            -- yyyymmdd, e.g. 20260919
    full_date    DATE     NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,               -- 1=Sunday .. 7=Saturday (same as Spark dayofweek)
    day_name     TEXT     NOT NULL,
    is_weekend   BOOLEAN  NOT NULL
);

INSERT INTO dim_date
SELECT to_char(g.d, 'YYYYMMDD')::int,
       g.d::date,
       extract(year  FROM g.d)::smallint,
       extract(month FROM g.d)::smallint,
       extract(day   FROM g.d)::smallint,
       (extract(dow  FROM g.d) + 1)::smallint,
       trim(to_char(g.d, 'Day')),
       extract(dow FROM g.d) IN (0, 6)
FROM generate_series(DATE '2026-01-01', DATE '2028-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_hour (
    hour_key    SMALLINT PRIMARY KEY CHECK (hour_key BETWEEN 0 AND 23),
    hour_label  TEXT NOT NULL,                    -- '18:00-18:59'
    day_part    TEXT NOT NULL                     -- our own grouping, see CASE below
);

INSERT INTO dim_hour
SELECT h,
       lpad(h::text, 2, '0') || ':00-' || lpad(h::text, 2, '0') || ':59',
       CASE WHEN h BETWEEN 0  AND 5  THEN 'Night'
            WHEN h BETWEEN 6  AND 10 THEN 'Morning peak'
            WHEN h BETWEEN 11 AND 15 THEN 'Midday'
            WHEN h BETWEEN 16 AND 20 THEN 'Evening peak'
            ELSE 'Late evening' END
FROM generate_series(0, 23) AS h
ON CONFLICT (hour_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_route (
    route_key        SERIAL PRIMARY KEY,
    route_id         TEXT NOT NULL UNIQUE,        -- as reported by the live feed
    route_long_name  TEXT,                        -- fill in once a matching static timetable is available
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key   SERIAL PRIMARY KEY,
    vehicle_id    TEXT NOT NULL UNIQUE,           -- e.g. DL1PD8776
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- facts ----------

-- grain: one row per route x date x hour
CREATE TABLE IF NOT EXISTS fact_route_hour_speed (
    date_key               INTEGER  NOT NULL REFERENCES dim_date (date_key),
    hour_key               SMALLINT NOT NULL REFERENCES dim_hour (hour_key),
    route_key              INTEGER  NOT NULL REFERENCES dim_route (route_key),
    n_pings                INTEGER  NOT NULL,
    n_vehicles             INTEGER  NOT NULL,
    n_speed_obs            INTEGER  NOT NULL,
    n_moving               INTEGER  NOT NULL,
    avg_speed_kmph         NUMERIC(7,2),
    avg_moving_speed_kmph  NUMERIC(7,2),
    n_bunched              INTEGER  NOT NULL,
    loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date_key, hour_key, route_key)
);

-- grain: one row per vehicle position report
CREATE TABLE IF NOT EXISTS fact_vehicle_ping (
    ping_id          BIGSERIAL PRIMARY KEY,
    obs_ts           TIMESTAMPTZ NOT NULL,
    date_key         INTEGER  NOT NULL REFERENCES dim_date (date_key),
    hour_key         SMALLINT NOT NULL REFERENCES dim_hour (hour_key),
    route_key        INTEGER  NOT NULL REFERENCES dim_route (route_key),
    vehicle_key      INTEGER  NOT NULL REFERENCES dim_vehicle (vehicle_key),
    trip_id          TEXT,
    dispatch_hour    SMALLINT,
    dispatch_minute  SMALLINT,
    lat              NUMERIC(9,6) NOT NULL,
    lon              NUMERIC(9,6) NOT NULL,
    gap_s            INTEGER,
    dist_m           NUMERIC(10,1),
    speed_kmph       NUMERIC(8,2),
    is_speed_valid   BOOLEAN NOT NULL,
    is_moving        BOOLEAN NOT NULL,
    is_bunched       BOOLEAN NOT NULL,
    UNIQUE (vehicle_key, obs_ts)
);

CREATE INDEX IF NOT EXISTS ix_ping_date_hour_route ON fact_vehicle_ping (date_key, hour_key, route_key);