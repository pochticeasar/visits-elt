# Airflow поверх образа Spark: задачи запускают load.py и spark-submit тем же
# Python и Spark, что и Jupyter. Сам Airflow — в отдельном venv, чтобы его
# зависимости не меняли версии pyarrow/pandas.
FROM lab/spark:latest
ARG AIRFLOW_VERSION=2.10.5
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-venv && rm -rf /var/lib/apt/lists/* && \
    python3 -m venv /opt/airflow-venv && \
    /opt/airflow-venv/bin/pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt" && \
    mkdir -p /opt/airflow && chown spark /opt/airflow
# venv в конце PATH: команда airflow находится, а python3 остаётся системным (с pyarrow).
ENV AIRFLOW_HOME=/opt/airflow \
    PATH=$PATH:/opt/airflow-venv/bin
USER spark
