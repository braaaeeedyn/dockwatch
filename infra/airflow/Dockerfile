# Airflow 2.x (M4a): LocalExecutor, metadata in compose `airflow-db`. Tag dockwatch-airflow:2.11.2.
# DAG files are mounted from airflow/dags and the dockwatch package from src (PYTHONPATH), so code changes need no
# rebuild. Extra packages are pinned through Airflow's own constraints file; psycopg 3 is not in it, so it is pinned
# here to the host's uv.lock version. The `docker` SDK (memory guard, Connect keepalive) is already in the image.
FROM apache/airflow:2.11.2-python3.12

RUN pip install --no-cache-dir \
      "apache-airflow==2.11.2" \
      "trino" \
      "pydantic-settings" \
      "pytest" \
      "psycopg[binary]==3.3.6" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt"

ENV PYTHONPATH=/opt/dockwatch/src \
    AIRFLOW__OPENLINEAGE__DISABLED=true
