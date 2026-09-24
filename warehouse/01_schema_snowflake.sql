USE ROLE SYSADMIN;

-- Small dedicated compute warehouse: suspends after 60 s idle so it barely uses trial credits.
CREATE WAREHOUSE IF NOT EXISTS ROUTERADAR_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE ROUTERADAR_WH;

CREATE DATABASE IF NOT EXISTS ROUTERADAR;
CREATE SCHEMA IF NOT EXISTS ROUTERADAR.DW;
USE SCHEMA ROUTERADAR.DW;

-- ---------- dimensions ----------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER  NOT NULL PRIMARY KEY,    -- yyyymmdd, e.g. 20260919
    full_date    DATE     NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,                -- 1=Sunday .. 7=Saturday (same as Spark dayofweek)
    day_name     TEXT     NOT NULL,
    is_weekend   BOOLEAN  NOT NULL
);


MERGE INTO dim_date t
USING (
    SELECT YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)  AS date_key,
           d                                          AS full_date,
           YEAR(d)                                    AS year,
           MONTH(d)                                   AS month,
           DAY(d)                                     AS day,
           MOD(DAYOFWEEKISO(d), 7) + 1                AS day_of_week,
           DECODE(DAYOFWEEKISO(d), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday',
                  4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7, 'Sunday') AS day_name,
           DAYOFWEEKISO(d) IN (6, 7)                  AS is_weekend
    FROM (
        SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2026-01-01'::DATE) AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 1096))
    )
) s
ON t.date_key = s.date_key
WHEN NOT MATCHED THEN INSERT
    (date_key, full_date, year, month, day, day_of_week, day_name, is_weekend)
VALUES
    (s.date_key, s.full_date, s.year, s.month, s.day, s.day_of_week, s.day_name, s.is_weekend);

CREATE TABLE IF NOT EXISTS dim_hour (
    hour_key    SMALLINT NOT NULL PRIMARY KEY,     -- 0..23
    hour_label  TEXT     NOT NULL,                 -- '18:00-18:59'
    day_part    TEXT     NOT NULL                  -- same grouping as the PostgreSQL version
);

MERGE INTO dim_hour t
USING (
    SELECT h AS hour_key,
           LPAD(h::TEXT, 2, '0') || ':00-' || LPAD(h::TEXT, 2, '0') || ':59' AS hour_label,
           CASE WHEN h BETWEEN 0  AND 5  THEN 'Night'
                WHEN h BETWEEN 6  AND 10 THEN 'Morning peak'
                WHEN h BETWEEN 11 AND 15 THEN 'Midday'
                WHEN h BETWEEN 16 AND 20 THEN 'Evening peak'
                ELSE 'Late evening' END AS day_part
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS h
        FROM TABLE(GENERATOR(ROWCOUNT => 24))
    )
) s
ON t.hour_key = s.hour_key
WHEN NOT MATCHED THEN INSERT (hour_key, hour_label, day_part)
VALUES (s.hour_key, s.hour_label, s.day_part);

CREATE TABLE IF NOT EXISTS dim_route (
    route_key        INTEGER IDENTITY(1,1) PRIMARY KEY,
    route_id         TEXT NOT NULL UNIQUE,         -- as reported by the live feed
    route_long_name  TEXT,                         -- fill in once a matching static timetable is available
    first_seen_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key   INTEGER IDENTITY(1,1) PRIMARY KEY,
    vehicle_id    TEXT NOT NULL UNIQUE,            -- e.g. DL1PD8776
    first_seen_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
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
    n_moving                INTEGER  NOT NULL,
    avg_speed_kmph         NUMERIC(7,2),
    avg_moving_speed_kmph  NUMERIC(7,2),
    n_bunched              INTEGER  NOT NULL,
    loaded_at              TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date_key, hour_key, route_key)
);

-- grain: one row per vehicle position report
CREATE TABLE IF NOT EXISTS fact_vehicle_ping (
    ping_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    obs_ts           TIMESTAMP_TZ NOT NULL,
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

SELECT 'dim_date' AS table_name, COUNT(*) AS n_rows FROM dim_date
UNION ALL SELECT 'dim_hour',              COUNT(*) FROM dim_hour
UNION ALL SELECT 'dim_route',             COUNT(*) FROM dim_route
UNION ALL SELECT 'dim_vehicle',           COUNT(*) FROM dim_vehicle
UNION ALL SELECT 'fact_route_hour_speed', COUNT(*) FROM fact_route_hour_speed
UNION ALL SELECT 'fact_vehicle_ping',     COUNT(*) FROM fact_vehicle_ping;