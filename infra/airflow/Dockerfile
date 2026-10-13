# Airflow 2.x (M4a): LocalExecutor, metadata in compose `airflow-db`. Tag dockwatch-airflow:2.11.2.
# DAG files are mounted from airflow/dags and the dockwatch package from src (PYTHONPATH), so code changes need no
# rebuild. Extra packages are pinned through Airflow's own constraints file; psycopg 3 is not in it, so it is pinned
# here to the host's uv.lock version. The `docker` SDK (memory guard, Connect keepalive) is already in the image, and
# so are pyarrow 22.0.0 and pyiceberg 0.11.1 (M4b trip loads; pinned by the same constraints file).
FROM apache/airflow:2.11.2-python3.12

RUN pip install --no-cache-dir \
      "apache-airflow==2.11.2" \
      "trino" \
      "pydantic-settings" \
      "pytest" \
      "psycopg[binary]==3.3.6" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt"

# M4b: dbt in its own venv (dbt needs protobuf>=6, Airflow's constraints pin 5.29); every package pinned.
COPY dbt-requirements.txt /opt/dbt-requirements.txt
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --disable-pip-version-check -r /opt/dbt-requirements.txt
USER airflow

# host.ensure_image() rebuilds when this hash of the Dockerfile + dbt-requirements.txt changes
ARG DOCKWATCH_BUILD_HASH=unset
LABEL dockwatch.build-hash=$DOCKWATCH_BUILD_HASH

ENV PYTHONPATH=/opt/dockwatch/src \
    AIRFLOW__OPENLINEAGE__DISABLED=true \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
