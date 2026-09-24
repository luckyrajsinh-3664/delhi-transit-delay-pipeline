# RouteRadar — Delhi Bus Congestion Analytics Pipeline

A data engineering pipeline that collects Delhi's live bus GPS feed, cleans and processes it, validates its quality, stores it in a warehouse, and generates SQL analytics, a GenAI-powered plain-English insight summary, a speed-prediction model, an API, and a Power BI dashboard — running end to end on free-tier tools, with the processing stages fully automated in Airflow.

## What it does

Delhi's official live bus feed (GTFS-Realtime) only shows the current moment — once the next update arrives, earlier positions are gone, and the feed itself has real data problems (its speed field is always zero, and its trip IDs don't match the published timetable). RouteRadar solves the underlying **data problem**: it collects the feed continuously, works out real vehicle speed from consecutive GPS positions, checks the data's quality, and stores it in a proper warehouse — so questions like *"which routes are slowest, and when?"* and *"where are buses bunching together?"* can actually be answered with SQL, on a schedule, with no manual steps. On top of that warehouse sit a GenAI insight generator, a speed-prediction ML model, a FastAPI service, and a Power BI dashboard.

## Architecture

```mermaid
flowchart LR
    A[Delhi OTD Live Feed<br/>GTFS-Realtime / Protobuf] -->|every 10 min| B[Airflow<br/>Docker]
    B --> C[AWS S3<br/>date-partitioned raw JSON]
    subgraph Hourly, automated in Airflow
    C --> D[PySpark<br/>clean, derive speed,<br/>detect bunching]
    D --> E[Parquet<br/>vehicle_pings_clean<br/>route_hour_speed]
    E --> F[Great Expectations<br/>26 data quality checks]
    F --> G[PostgreSQL<br/>Star Schema Warehouse]
    end
    G -->|secondary target| G2[Snowflake<br/>Star Schema Warehouse]
    G --> H[SQL Analytics<br/>CTEs, window functions]
    G --> I[Gemini API<br/>plain-English insights]
    G --> J[ML Model<br/>speed prediction]
    G --> K[FastAPI]
    G --> L[Power BI Dashboard]
```

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker, LocalExecutor) |
| Ingestion | Python, Protobuf (GTFS-Realtime) |
| Storage (raw) | AWS S3 (date-partitioned) |
| Processing | PySpark (DataFrames, window functions), run inside the Airflow container |
| Data quality | Great Expectations |
| Warehouse | PostgreSQL (star schema, primary) + Snowflake (star schema, secondary, key-pair auth) |
| Analytics | SQL (CTEs, window functions, ranking) |
| Insights | Gemini API |
| ML | scikit-learn (RandomForestRegressor) |
| API | FastAPI |
| Dashboard | Power BI (live connection to PostgreSQL) |
| Infra | Docker Compose |

## Key findings

Two things I discovered while building this, which shaped the design:

1. **The feed's `speed` field is always `0.0`.** Every vehicle report includes a speed value, but it never varies — confirmed across every raw file collected. Speed is instead **derived** from each bus's consecutive GPS positions (haversine distance / time gap), which gives a net-progress speed rather than a speedometer reading — a congestion proxy, not an exact figure.
2. **The live feed's trip IDs don't match the official static timetable.** Cross-checking against Delhi OTD's published GTFS schedule found only 24 of 2,326 live trip IDs matched. This ruled out computing "schedule delay" directly, so the project pivoted to speed and bunching analytics, which the live feed can support reliably on its own.

## Pipeline stages

1. **Ingestion** (`ingestion/`, `dags/transit_ingestion_dag.py`) — an Airflow DAG fetches the live feed every 10 minutes, decodes Protobuf, and uploads to S3 (`raw/year=X/month=X/day=X/`). It fails and retries if the feed returns zero records, instead of silently saving an empty file.
2. **Transformation** (`transformation/`) — PySpark reads raw JSON, cleans and de-duplicates pings, filters to Delhi's bounding box, derives speed and a moving/bunching flag per ping, and aggregates to route-hour metrics. Writes partitioned Parquet.
3. **Data quality** (`quality/`) — Great Expectations runs 26 checks (nulls, valid ranges, uniqueness, cross-column consistency) across both datasets before they reach the warehouse.
4. **Warehouse** (`warehouse/`) — a star schema loaded into two targets from the same Parquet:
   - **PostgreSQL** (primary) — two fact tables (`fact_route_hour_speed`, `fact_vehicle_ping`) and four dimensions (`dim_date`, `dim_hour`, `dim_route`, `dim_vehicle`). Loaded by `load_warehouse.py`, an idempotent script (upserts, safe to re-run or run concurrently).
   - **Snowflake** (secondary) — the same star schema (`01_schema_snowflake.sql`), loaded by `load_snowflake.py` via staging tables + `MERGE`, also idempotent. Authenticates as a dedicated service user with key-pair login; one-time setup via `snowflake_keygen.py`. Verified row-for-row identical to PostgreSQL, both in row counts and in query output (`sql/analytics.sql` Q1).
5. **Automation** (`dags/processing_pipeline_dag.py`) — a second Airflow DAG runs stages 2–4 hourly with no manual steps: Spark transform → quality checks → warehouse load, in that order, so a failed quality check stops the load rather than letting bad data through. Java 17 and PySpark are installed directly in the Airflow Docker image so Spark runs natively inside the container. (Currently loads PostgreSQL; Snowflake is loaded as a manual secondary step via `load_snowflake.py`.)
6. **SQL analytics** (`sql/analytics.sql`) — five queries using CTEs, window functions (`RANK()`, `LAG()`), `CASE`, and aggregation: slowest route-hours, per-hour ranking, congestion by time of day, bunching rate per route, and day-over-day speed trends.
7. **GenAI insights** (`genai/`) — pulls the slowest routes and worst-bunching routes from the warehouse and asks the Gemini API to write a short, plain-English summary for a transit planner.
8. **ML model** (`ml/train_speed_model.py`) — a RandomForestRegressor predicting `avg_moving_speed_kmph` from route/hour/day-of-week, benchmarked against a route's own historical average speed as baseline. Model MAE 2.51 km/h vs. baseline MAE 2.65 km/h (5.0% improvement), R² = 0.161. Feature importance is dominated by `route_avg_speed` (92.4%) — with only a few distinct collection dates so far, hour/day features carry little signal yet and should strengthen as the automated DAG accumulates more data. The trained model (`ml/model/speed_model.joblib`) is gitignored; only the training script is committed.
9. **FastAPI** (`api/main.py`) — four endpoints: `/health`, `/routes/{route_id}/speed`, `/routes/slowest`, `/predict` (loads the saved ML model). Tested live via Swagger UI against the real warehouse, including edge cases (404 for unknown routes, 422 for bad input).
10. **Power BI dashboard** (`dashboard/RouteRadar_Dashboard.pbix`) — connected live to the PostgreSQL warehouse. Four visuals: Top 15 Slowest Routes, Top 10 Routes by Bus-Bunching Rate (custom DAX measure), Average Speed: Midday vs. Evening Peak, and fleet-wide KPI cards (pings, distinct routes, average moving speed).

## Sample results (from real collected data)

- Fleet-wide: ~71% of tracked buses were moving at any given snapshot; average moving speed ~8.8 km/h.
- Slowest observed: Route 1914 at 20:00–20:59, averaging 3.87 km/h across 3 buses.
- Worst bunching: Route 2657, where 80% of its moving GPS pings showed another bus on the same route within 300 metres.
- Evening peak (16:00–20:59) averaged slower speeds than midday across the collected sample.
- ML model beats the route-average baseline by 5.0% (MAE 2.51 vs. 2.65 km/h) on real held-out data.

## Running it

```bash
# 1. Start everything: ingestion, the hourly processing DAG, and both databases
docker compose up -d

# Ingestion runs every 10 minutes on its own; the processing DAG
# (Spark -> Great Expectations -> warehouse load) runs hourly on its own too.
# Both can also be triggered manually from the Airflow UI at localhost:8081.

# The rest of these are optional, for running a stage by hand or exploring the data:

# SQL analytics
docker compose exec -T warehouse psql -U dw_admin -d transit_dw < sql/analytics.sql

# GenAI insight summary (venv-dq)
python genai/generate_insights.py

# Train the ML model (venv-dq)
python ml/train_speed_model.py

# Run the API locally (venv-dq) -> Swagger UI at localhost:8000/docs
uvicorn api.main:app --reload

# Running the Spark/quality/load stages manually outside Airflow, e.g. for local
# debugging (venv-dq, or venv with JAVA_HOME/HADOOP_HOME set on Windows):
python transformation/transform_speed_analytics.py --input "raw_data/*.json" --output processed_data
python quality/validate_data.py --input processed_data
python warehouse/load_warehouse.py --input processed_data

# Load the same processed data into Snowflake (secondary warehouse):
# one-time setup: python warehouse/snowflake_keygen.py, then run keys/snowflake_user_setup.sql in Snowsight
python warehouse/load_snowflake.py --input processed_data
```

Requires a `.env` file (not committed) with: `DELHI_OTD_API_KEY`, `DELHI_OTD_BASE_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME`, `WAREHOUSE_PASSWORD`, `GEMINI_API_KEY`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`.

## Known limitations

- Derived speed is net progress between two GPS points, not true road speed — it reads lower than reality on winding routes, and is a congestion indicator rather than an exact measurement.
- Because the live feed's trip IDs don't match the official static timetable, schedule-delay (actual vs. scheduled arrival) is not computed — this pipeline measures observed congestion and bunching instead.
- `route_long_name` in the warehouse is currently blank, since no matching static timetable is available yet to join against.
- ML model features beyond route identity (hour, day-of-week) carry little signal yet, since only a few distinct collection dates have accumulated so far; expected to improve as the automated DAG collects more data over time.
- Runs on a single machine; Airflow's webserver container needs a `docker compose up -d --force-recreate airflow-webserver` after an unclean host shutdown.
- Snowflake is loaded as a manual secondary step (`load_snowflake.py`), not yet wired into the automated Airflow DAG alongside the PostgreSQL load.

## Roadmap

- Wire Snowflake into the automated hourly DAG (currently a manual step)
- Join a matching static GTFS timetable once trip IDs align, to populate `route_long_name` and enable true schedule-delay analytics
- Retrain the ML model as more collection days accumulate, to see if hour/day-of-week features gain predictive signal