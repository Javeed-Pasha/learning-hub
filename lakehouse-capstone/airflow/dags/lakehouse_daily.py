"""Daily lakehouse pipeline: Spark ingestion to bronze, dbt to silver and gold, Trino check."""
from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

VENV = "/opt/airflow/venvs/dbt/bin"
DBT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="lakehouse_daily",
    schedule="30 1 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["lakehouse", "spark", "dbt", "iceberg"],
)
def lakehouse_daily():
    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{VENV}/python /opt/airflow/jobs/ingest_bronze.py",
    )
    deps = BashOperator(task_id="dbt_deps", bash_command=f"{VENV}/dbt deps", cwd=DBT_DIR)
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{VENV}/dbt source freshness --target prod",
        cwd=DBT_DIR,
    )
    build = BashOperator(
        task_id="dbt_build",
        # unit tests belong in CI, not in the production run
        bash_command=f"{VENV}/dbt build --target prod --exclude-resource-type unit_test",
        cwd=DBT_DIR,
    )
    check = BashOperator(
        task_id="trino_reads_gold",
        bash_command=f"{VENV}/python /opt/airflow/jobs/check_trino.py",
    )

    ingest >> deps >> freshness >> build >> check


lakehouse_daily()
