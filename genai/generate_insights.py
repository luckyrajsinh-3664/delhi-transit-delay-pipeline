"""
generate_insights.py - GenAI insight summary for the RouteRadar warehouse.

Reads the slowest routes and bus-bunching stats straight from PostgreSQL
(same underlying data as sql/analytics.sql), sends them to Gemini, and
prints/saves a short plain-English summary a transit planner could read.

Uses the Gemini free tier (no billing). Requires GEMINI_API_KEY in .env.

Run from the project root (venv-dq environment):
  python genai/generate_insights.py
"""
import os
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from google import genai

SLOWEST_ROUTES_SQL = """
SELECT r.route_id, d.full_date, h.hour_label, f.n_moving, f.avg_moving_speed_kmph
FROM dw.fact_route_hour_speed f
JOIN dw.dim_route r USING (route_key)
JOIN dw.dim_date  d USING (date_key)
JOIN dw.dim_hour  h USING (hour_key)
WHERE f.n_moving >= 3
ORDER BY f.avg_moving_speed_kmph ASC
LIMIT 10;
"""

BUNCHING_SQL = """
SELECT r.route_id,
       SUM(f.n_moving)  AS total_moving_pings,
       SUM(f.n_bunched) AS total_bunched_pings,
       ROUND(100.0 * SUM(f.n_bunched) / NULLIF(SUM(f.n_moving), 0), 1) AS bunching_pct
FROM dw.fact_route_hour_speed f
JOIN dw.dim_route r USING (route_key)
GROUP BY r.route_id
HAVING SUM(f.n_moving) >= 10
ORDER BY bunching_pct DESC
LIMIT 5;
"""


def connect_db():
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


def fetch_rows(cur, sql):
    cur.execute(sql)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_prompt(slow_routes, bunching):
    lines = ["Slowest bus routes (route_id, date, hour, moving buses, avg speed km/h):"]
    for r in slow_routes:
        lines.append(
            f"- Route {r['route_id']} on {r['full_date']} at {r['hour_label']}: "
            f"{r['n_moving']} buses, {r['avg_moving_speed_kmph']} km/h"
        )
    lines.append("\nRoutes with the highest bus-bunching rate (same route, buses within 300m):")
    for r in bunching:
        lines.append(
            f"- Route {r['route_id']}: {r['bunching_pct']}% bunched "
            f"({r['total_bunched_pings']} of {r['total_moving_pings']} moving pings)"
        )
    lines.append(
        "\nWrite a short summary (4-6 sentences, plain English, no jargon) for a "
        "transit planner. Call out the worst congestion pattern and the worst "
        "bunching pattern. Do not invent numbers not shown above. Do not give "
        "traffic-management advice beyond one short suggestion."
    )
    return "\n".join(lines)


def generate_with_fallback(client, prompt):
    """Call Gemini with retries for temporary server overload (503)."""
    model = "gemini-3.6-flash"
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            return model, response.text
        except Exception as e:
            msg = str(e)
            is_503 = "503" in msg or "UNAVAILABLE" in msg
            print(f"  attempt {attempt}/{max_attempts} failed: {type(e).__name__}: {msg}")
            if is_503 and attempt < max_attempts:
                wait = 10 * attempt
                print(f"  server busy, waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            raise

def main():
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        sys.exit("GEMINI_API_KEY is missing - add it to .env")

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            slow_routes = fetch_rows(cur, SLOWEST_ROUTES_SQL)
            bunching = fetch_rows(cur, BUNCHING_SQL)
    finally:
        conn.close()

    if not slow_routes:
        sys.exit("No route-hour data found. Run the warehouse loader first.")

    print(f"Pulled {len(slow_routes)} slow-route rows and {len(bunching)} bunching rows from the warehouse.\n")

    client = genai.Client(api_key=api_key)
    prompt = build_prompt(slow_routes, bunching)

    model, summary = generate_with_fallback(client, prompt)

    print(f"\n--- Insight summary (model: {model}) ---\n")
    print(summary)

    out_dir = Path("genai/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "latest_insight.txt"
    out_path.write_text(summary, encoding="utf-8")
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()