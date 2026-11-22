# Airflow 3.4 with dbt in its own virtual environment, so dbt's dependencies never clash with Airflow's.
FROM apache/airflow:3.4.0-python3.12

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/venvs/dbt \
 && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
