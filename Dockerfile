FROM apache/airflow:2.10.3

USER root

# Java 17 for PySpark - Linux inside the container, so no winutils.exe needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    gtfs-realtime-bindings \
    requests \
    python-dotenv \
    pyspark==4.2.0 \
    great_expectations \
    psycopg2-binary \
    pandas \
    pyarrow