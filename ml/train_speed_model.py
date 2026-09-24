import os
import sys
import warnings
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

warnings.filterwarnings("ignore")

MIN_MOVING = 3  # same sample-size floor used throughout the project
PEAK_HOURS = [8, 9, 18, 19, 20]
WEEKEND_DAYS = [1, 7]  # 1=Sunday, 7=Saturday (matches dim_date.day_of_week)

QUERY = """
SELECT r.route_id, d.full_date, h.hour_key, d.day_of_week,
       f.n_moving, f.avg_moving_speed_kmph
FROM dw.fact_route_hour_speed f
JOIN dw.dim_route r USING (route_key)
JOIN dw.dim_date  d USING (date_key)
JOIN dw.dim_hour  h USING (hour_key)
WHERE f.n_moving >= %s;
"""


def connect_db():
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


def load_data():
    conn = connect_db()
    try:
        df = pd.read_sql(QUERY, conn, params=(MIN_MOVING,))
    finally:
        conn.close()
    return df


def add_time_features(df):
    df = df.copy()
    df["is_peak_hour"] = df["hour_key"].isin(PEAK_HOURS).astype(int)
    df["is_weekend"] = df["day_of_week"].isin(WEEKEND_DAYS).astype(int)
    return df


def main():
    print("Reading route-hour data from the warehouse...")
    raw = load_data()
    print(f"  {len(raw)} rows with n_moving >= {MIN_MOVING}")
    print(f"  distinct dates in this data: {raw['full_date'].nunique()}")

    if len(raw) < 50:
        sys.exit(
            f"Only {len(raw)} usable rows - too few to train/evaluate a model "
            f"meaningfully. Let more data collect and try again."
        )

    df = add_time_features(raw)
    feature_cols = ["hour_key", "day_of_week", "is_peak_hour", "is_weekend", "route_avg_speed"]

    # Split FIRST, then compute each route's average speed from the TRAINING
    # rows only - this avoids leaking a test row's own value into its feature.
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    train_df = train_df.copy()
    test_df = test_df.copy()

    route_avg_train = train_df.groupby("route_id")["avg_moving_speed_kmph"].mean()
    global_mean_train = train_df["avg_moving_speed_kmph"].mean()
    train_df["route_avg_speed"] = train_df["route_id"].map(route_avg_train)
    test_df["route_avg_speed"] = test_df["route_id"].map(route_avg_train).fillna(global_mean_train)

    X_train, y_train = train_df[feature_cols], train_df["avg_moving_speed_kmph"]
    X_test, y_test = test_df[feature_cols], test_df["avg_moving_speed_kmph"]
    print(f"  train: {len(X_train)} rows, test: {len(X_test)} rows")

    model = RandomForestRegressor(
        n_estimators=200, max_depth=8, min_samples_leaf=3, random_state=42
    )
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    # Baseline: naive prediction = that route's training-set mean speed
    baseline_preds = test_df["route_avg_speed"].values
    baseline_mae = mean_absolute_error(y_test, baseline_preds)
    baseline_r2 = r2_score(y_test, baseline_preds)

    print("\n--- Results (held-out test set, no target leakage) ---")
    print(f"  Model     MAE: {mae:.2f} km/h   R^2: {r2:.3f}")
    print(f"  Baseline  MAE: {baseline_mae:.2f} km/h   R^2: {baseline_r2:.3f}")
    improvement = (baseline_mae - mae) / baseline_mae * 100
    print(f"  Model improves on the baseline by {improvement:.1f}% (MAE)")

    print("\n--- Feature importance ---")
    for name, imp in sorted(zip(feature_cols, model.feature_importances_), key=lambda x: -x[1]):
        print(f"  {name:20s} {imp:.3f}")

    out_dir = Path("ml/model")
    out_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out_dir / "speed_model.joblib")
    print(f"\nSaved model to {out_dir / 'speed_model.joblib'}")


if __name__ == "__main__":
    main()