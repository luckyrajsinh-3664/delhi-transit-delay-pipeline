from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the ingestion script importable
sys.path.append('/opt/airflow/ingestion')

from fetch_vehicle_positions import fetch_vehicle_positions, save_locally, upload_to_s3

default_args = {
    "owner": "luckyrajsinh",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def run_ingestion():
    records = fetch_vehicle_positions()
    print(f"Fetched {len(records)} vehicle records.")

    if not records:
        raise ValueError("Feed returned 0 vehicle records - failing so Airflow retries")

    filepath, filename = save_locally(records)
    print(f"Saved locally to {filepath}")

    upload_to_s3(filepath, filename, records)

with DAG(
    dag_id="delhi_transit_ingestion",
    default_args=default_args,
    description="Fetch live Delhi bus GPS data and upload to S3",
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 9, 18),
    catchup=False,
    max_active_runs=1,
    tags=["transit", "ingestion"],
) as dag:

    fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload_vehicle_positions",
        python_callable=run_ingestion,
    )