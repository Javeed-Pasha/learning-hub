"""EL step: copy the landing-zone Parquet extracts into Iceberg bronze tables.

The SQL runs on the Spark cluster through the Thrift Server, so Spark does the work;
Airflow only sends statements. Each table is replaced atomically (Iceberg RTAS) and
stamped with _loaded_at and _batch_id for freshness checks and lineage.

In production you would usually submit a PySpark job instead (spark-submit, Spark Connect
or the Spark Kubernetes operator); the target tables and audit columns stay the same.
"""
import os
import sys
import time

from pyhive import hive

TABLES = ["users", "products", "orders", "order_items", "events"]
LANDING = os.environ.get("LANDING_PATH", "/landing/thelook")
HOST = os.environ.get("SPARK_THRIFT_HOST", "spark-thrift")
PORT = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))


def main() -> int:
    batch_id = os.environ.get("BATCH_ID") or time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    conn = hive.connect(host=HOST, port=PORT, username="airflow")
    cur = conn.cursor()
    cur.execute("create namespace if not exists lakehouse.bronze")
    for table in TABLES:
        started = time.time()
        cur.execute(f"""
            create or replace table lakehouse.bronze.{table}
            using iceberg
            as
            select
                src.*,
                current_timestamp() as _loaded_at,
                '{batch_id}'        as _batch_id
            from parquet.`file://{LANDING}/{table}` as src
        """)
        cur.execute(f"select count(*) from lakehouse.bronze.{table}")
        rows = cur.fetchone()[0]
        print(f"bronze.{table:<12} {rows:>10,} rows  {time.time() - started:5.1f}s  batch {batch_id}")
        if rows == 0:
            print(f"bronze.{table} is empty: check {LANDING}/{table}", file=sys.stderr)
            return 1
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
