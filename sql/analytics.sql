-- analytics.sql - SQL analytics on the RouteRadar warehouse (dw schema)
-- Demonstrates: CTEs, window functions, ranking, joins, aggregation, CASE.

SET search_path TO dw;

-- =====================================================================
-- Q1. Top 10 slowest route-hours overall (minimum sample size enforced)
-- =====================================================================
SELECT
    r.route_id,
    d.full_date,
    h.hour_label,
    f.n_moving,
    f.avg_moving_speed_kmph,
    f.n_bunched
FROM fact_route_hour_speed f
JOIN dim_route r USING (route_key)
JOIN dim_date  d USING (date_key)
JOIN dim_hour  h USING (hour_key)
WHERE f.n_moving >= 3
ORDER BY f.avg_moving_speed_kmph ASC
LIMIT 10;


-- =====================================================================
-- Q2. Rank routes by average speed WITHIN each hour (window function)
--     Shows, for every hour, which route was slowest that hour.
-- =====================================================================
WITH ranked AS (
    SELECT
        h.hour_label,
        r.route_id,
        f.avg_moving_speed_kmph,
        RANK() OVER (
            PARTITION BY f.hour_key
            ORDER BY f.avg_moving_speed_kmph ASC
        ) AS slow_rank_in_hour
    FROM fact_route_hour_speed f
    JOIN dim_route r USING (route_key)
    JOIN dim_hour  h USING (hour_key)
    WHERE f.n_moving >= 3
)
SELECT hour_label, route_id, avg_moving_speed_kmph, slow_rank_in_hour
FROM ranked
WHERE slow_rank_in_hour = 1
ORDER BY hour_label;


-- =====================================================================
-- Q3. Congestion by time-of-day bucket (CASE + aggregation)
-- =====================================================================
SELECT
    CASE
        WHEN h.hour_key BETWEEN 0  AND 5  THEN 'Night'
        WHEN h.hour_key BETWEEN 6  AND 10 THEN 'Morning peak'
        WHEN h.hour_key BETWEEN 11 AND 15 THEN 'Midday'
        WHEN h.hour_key BETWEEN 16 AND 20 THEN 'Evening peak'
        ELSE 'Late evening'
    END AS day_part,
    COUNT(*)                              AS route_hour_rows,
    ROUND(AVG(f.avg_moving_speed_kmph), 2) AS avg_speed_kmph,
    SUM(f.n_bunched)                      AS total_bunched_pings
FROM fact_route_hour_speed f
JOIN dim_hour h USING (hour_key)
GROUP BY 1
ORDER BY avg_speed_kmph ASC;


-- =====================================================================
-- Q4. Routes with the highest bunching RATE (bunched pings / moving pings)
--     Only routes with a meaningful sample size.
-- =====================================================================
SELECT
    r.route_id,
    SUM(f.n_moving)   AS total_moving_pings,
    SUM(f.n_bunched)  AS total_bunched_pings,
    ROUND(100.0 * SUM(f.n_bunched) / NULLIF(SUM(f.n_moving), 0), 1) AS bunching_pct
FROM fact_route_hour_speed f
JOIN dim_route r USING (route_key)
GROUP BY r.route_id
HAVING SUM(f.n_moving) >= 10
ORDER BY bunching_pct DESC
LIMIT 10;


-- =====================================================================
-- Q5. Day-over-day trend for one route (window function: LAG)
--     Shows how average speed changed vs. the previous day, per route.
-- =====================================================================
WITH daily AS (
    SELECT
        r.route_id,
        d.full_date,
        ROUND(AVG(f.avg_moving_speed_kmph), 2) AS avg_speed_kmph
    FROM fact_route_hour_speed f
    JOIN dim_route r USING (route_key)
    JOIN dim_date  d USING (date_key)
    WHERE f.n_moving >= 3
    GROUP BY r.route_id, d.full_date
)
SELECT
    route_id,
    full_date,
    avg_speed_kmph,
    LAG(avg_speed_kmph) OVER (PARTITION BY route_id ORDER BY full_date) AS prev_day_speed,
    ROUND(
        avg_speed_kmph - LAG(avg_speed_kmph) OVER (PARTITION BY route_id ORDER BY full_date),
        2
    ) AS change_vs_prev_day
FROM daily
ORDER BY route_id, full_date
LIMIT 30;