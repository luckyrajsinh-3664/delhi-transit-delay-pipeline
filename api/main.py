import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

MODEL_PATH = Path("ml/model/speed_model.joblib")
PEAK_HOURS = [8, 9, 18, 19, 20]
WEEKEND_DAYS = [1, 7]  # 1=Sunday, 7=Saturday, matches dim_date.day_of_week

state = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    state["model"] = joblib.load(MODEL_PATH) if MODEL_PATH.exists() else None
    yield
    state.clear()


app = FastAPI(
    title="RouteRadar API",
    description="Read-only API over the Delhi bus congestion warehouse.",
    lifespan=lifespan,
)


def connect_db():
    password = os.getenv("WAREHOUSE_PASSWORD")
    if not password:
        raise HTTPException(500, "WAREHOUSE_PASSWORD is missing - check .env")
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "transit_dw"),
        user=os.getenv("WAREHOUSE_USER", "dw_admin"),
        password=password,
    )


def run_query(sql, params=()):
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------- schemas

class RouteHourRow(BaseModel):
    route_id: str
    full_date: str
    hour_label: str
    n_moving: int
    avg_moving_speed_kmph: Optional[float]
    n_bunched: int


class PredictionResponse(BaseModel):
    route_id: str
    hour_key: int
    day_of_week: int
    predicted_speed_kmph: float
    route_historical_avg_kmph: Optional[float]
    note: str


# ---------------------------------------------------------------- routes

@app.get("/health")
def health():
    try:
        run_query("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "database_reachable": db_ok,
        "model_loaded": state.get("model") is not None,
    }


@app.get("/routes/{route_id}/speed", response_model=list[RouteHourRow])
def route_speed(
    route_id: str,
    hour: Optional[int] = Query(None, ge=0, le=23),
    min_moving: int = Query(3, ge=1),
):
    sql = """
        SELECT r.route_id, d.full_date::text AS full_date, h.hour_label,
               f.n_moving, f.avg_moving_speed_kmph, f.n_bunched
        FROM dw.fact_route_hour_speed f
        JOIN dw.dim_route r USING (route_key)
        JOIN dw.dim_date  d USING (date_key)
        JOIN dw.dim_hour  h USING (hour_key)
        WHERE r.route_id = %s AND f.n_moving >= %s
    """
    params = [route_id, min_moving]
    if hour is not None:
        sql += " AND h.hour_key = %s"
        params.append(hour)
    sql += " ORDER BY d.full_date, h.hour_key"

    rows = run_query(sql, tuple(params))
    if not rows:
        raise HTTPException(404, f"No data found for route '{route_id}' with n_moving >= {min_moving}")
    return rows


@app.get("/routes/slowest", response_model=list[RouteHourRow])
def slowest_routes(
    min_moving: int = Query(3, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    sql = """
        SELECT r.route_id, d.full_date::text AS full_date, h.hour_label,
               f.n_moving, f.avg_moving_speed_kmph, f.n_bunched
        FROM dw.fact_route_hour_speed f
        JOIN dw.dim_route r USING (route_key)
        JOIN dw.dim_date  d USING (date_key)
        JOIN dw.dim_hour  h USING (hour_key)
        WHERE f.n_moving >= %s
        ORDER BY f.avg_moving_speed_kmph ASC
        LIMIT %s
    """
    return run_query(sql, (min_moving, limit))


@app.get("/predict", response_model=PredictionResponse)
def predict(
    route_id: str,
    hour: int = Query(..., ge=0, le=23),
    day_of_week: int = Query(..., ge=1, le=7, description="1=Sunday .. 7=Saturday"),
):
    model = state.get("model")
    if model is None:
        raise HTTPException(503, "Model not loaded - run ml/train_speed_model.py first")

    rows = run_query(
        """
        SELECT AVG(f.avg_moving_speed_kmph) AS route_avg
        FROM dw.fact_route_hour_speed f
        JOIN dw.dim_route r USING (route_key)
        WHERE r.route_id = %s AND f.n_moving >= 3
        """,
        (route_id,),
    )
    route_avg = rows[0]["route_avg"] if rows else None
    note = "using this route's historical average speed as a feature"
    if route_avg is None:
        route_avg = run_query(
            "SELECT AVG(avg_moving_speed_kmph) AS m FROM dw.fact_route_hour_speed WHERE n_moving >= 3"
        )[0]["m"]
        note = "route not seen before - using the fleet-wide average speed instead"

    features = pd.DataFrame([{
        "hour_key": hour,
        "day_of_week": day_of_week,
        "is_peak_hour": int(hour in PEAK_HOURS),
        "is_weekend": int(day_of_week in WEEKEND_DAYS),
        "route_avg_speed": float(route_avg),
    }])
    pred = float(model.predict(features)[0])

    return PredictionResponse(
        route_id=route_id,
        hour_key=hour,
        day_of_week=day_of_week,
        predicted_speed_kmph=round(pred, 2),
        route_historical_avg_kmph=round(float(route_avg), 2) if route_avg is not None else None,
        note=note,
    )