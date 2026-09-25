from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "luckyrajsinh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Paths and environment inside the Airflow containers (see docker-compose.yaml mounts)
RAW_DATA = "/opt/airflow/raw_data/*.json"
PROCESSED_DATA = "/opt/airflow/processed_data"

# The Spark task needs JAVA_HOME set (added by the Dockerfile) and a writable
# HADOOP tmp dir; the load task needs the warehouse env vars set below to
# reach the "warehouse" service over the Docker network, not localhost.
SPARK_ENV = {
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
}
WAREHOUSE_ENV = {}

with DAG(
    dag_id="delhi_transit_processing",
    default_args=default_args,
    description="Spark transform -> Great Expectations -> warehouse load",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 9, 24),
    catchup=False,
    max_active_runs=1,
    tags=["transit", "processing"],
) as dag:

    run_spark_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command=(
            f"cd /opt/airflow && "
            f"python transformation/transform_speed_analytics.py "
            f"--input '{RAW_DATA}' --output {PROCESSED_DATA}"
        ),
        env=SPARK_ENV,
        append_env=True,
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            f"cd /opt/airflow && "
            f"python quality/validate_data.py --input {PROCESSED_DATA}"
        ),
    )

    run_warehouse_load = BashOperator(
        task_id="run_warehouse_load",
        bash_command=(
            f"cd /opt/airflow && "
            f"python warehouse/load_warehouse.py --input {PROCESSED_DATA}"
        ),
        env=WAREHOUSE_ENV,
        append_env=True,
    )

    run_spark_transform >> run_quality_checks >> run_warehouse_load