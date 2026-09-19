FROM apache/airflow:2.10.3

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    gtfs-realtime-bindings \
    requests \
    python-dotenv