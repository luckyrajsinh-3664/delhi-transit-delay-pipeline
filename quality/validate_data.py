"""
validate_data.py - Great Expectations data-quality checks for the Spark outputs.

Checks both datasets written by transform_speed_analytics.py:
  processed_data/vehicle_pings_clean
  processed_data/route_hour_speed

Run:
  python quality/validate_data.py --input processed_data

Exit code 0 = every check passed, 1 = at least one failed
(so Airflow can later fail the task and stop bad data reaching the warehouse).
A JSON report per dataset is saved in quality/reports/.
"""
import argparse
import sys
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd


def load(path):
    df = pd.read_parquet(path)
    if "service_date" in df.columns:  # Spark partition column comes back as 'category'
        df["service_date"] = df["service_date"].astype(str)
    return df


def pings_expectations():
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="vehicle_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="route_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="obs_ts"),
        gxe.ExpectColumnValuesToBeBetween(column="lat", min_value=28.0, max_value=29.0),
        gxe.ExpectColumnValuesToBeBetween(column="lon", min_value=76.5, max_value=77.8),
        gxe.ExpectColumnValuesToBeBetween(column="hour_of_day", min_value=0, max_value=23),
        gxe.ExpectColumnValuesToBeBetween(column="day_of_week", min_value=1, max_value=7),
        # GPS jumps are flagged, not dropped, so allow a small share above 80 km/h
        gxe.ExpectColumnValuesToBeBetween(column="speed_kmph", min_value=0, max_value=80, mostly=0.95),
        gxe.ExpectColumnValuesToBeInSet(column="is_moving", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="is_bunched", value_set=[True, False]),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["vehicle_id", "obs_ts"]),
    ]


def route_hour_expectations():
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="route_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="service_date"),
        gxe.ExpectColumnValuesToNotBeNull(column="hour_of_day"),
        gxe.ExpectColumnValuesToBeBetween(column="hour_of_day", min_value=0, max_value=23),
        gxe.ExpectColumnValuesToBeBetween(column="day_of_week", min_value=1, max_value=7),
        gxe.ExpectColumnValuesToBeBetween(column="n_pings", min_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="n_vehicles", min_value=1),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="n_pings", column_B="n_vehicles", or_equal=True),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="n_pings", column_B="n_speed_obs", or_equal=True),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="n_speed_obs", column_B="n_moving", or_equal=True),
        gxe.ExpectColumnValuesToBeBetween(column="avg_speed_kmph", min_value=0, max_value=80),
        gxe.ExpectColumnValuesToBeBetween(column="avg_moving_speed_kmph", min_value=3, max_value=80),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["route_id", "service_date", "hour_of_day"]),
    ]


def validate(context, name, df, expectations, report_dir):
    source = context.data_sources.add_pandas(f"{name}_source")
    asset = source.add_dataframe_asset(name=f"{name}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{name}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{name}_suite", expectations=expectations))
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{name}_validation", data=batch_def, suite=suite)
    )
    result = validation.run(batch_parameters={"dataframe": df})

    print(f"\n=== {name}: {len(df)} rows ===")
    for r in result.results:
        cfg = r.expectation_config
        kwargs = {k: v for k, v in cfg.kwargs.items() if k != "batch_id"}
        flag = "PASS" if r.success else "FAIL"
        extra = ""
        if not r.success:
            pct = r.result.get("unexpected_percent")
            extra = f"  (unexpected: {pct:.2f}%)" if pct is not None else f"  {r.result}"
        print(f"  [{flag}] {cfg.type} {kwargs}{extra}")
    print(f"  -> {'ALL PASSED' if result.success else 'SOME CHECKS FAILED'}")

    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"{name}.json").write_text(result.describe())
    return result.success


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="processed_data")
    ap.add_argument("--reports", default="quality/reports")
    args = ap.parse_args()

    context = gx.get_context(mode="ephemeral")
    root, reports = Path(args.input), Path(args.reports)

    ok_pings = validate(context, "vehicle_pings_clean", load(root / "vehicle_pings_clean"),
                        pings_expectations(), reports)
    ok_rh = validate(context, "route_hour_speed", load(root / "route_hour_speed"),
                     route_hour_expectations(), reports)

    sys.exit(0 if (ok_pings and ok_rh) else 1)


if __name__ == "__main__":
    main()