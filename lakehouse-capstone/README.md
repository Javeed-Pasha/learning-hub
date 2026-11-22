# Lakehouse capstone: Spark, dbt, Iceberg, MinIO, Trino and Airflow

The final capstone of the [dbt: Zero to Client-Ready](../pages/dbt-client-ready.html) course. You run a small on-prem lakehouse on your laptop. It has the same moving parts as a real Spark and Starburst platform:

```mermaid
flowchart LR
  L[Landing zone<br/>Parquet files] -->|Spark SQL via Thrift| B[(bronze)]
  subgraph Iceberg tables in MinIO, one REST catalog
    B --> S[(silver)]
    S --> G[(gold)]
  end
  D[dbt Core + dbt-spark] -->|Spark Thrift Server| S
  D --> G
  G --> T[Trino / Starburst<br/>analysts]
  A[Airflow] -.runs.-> L
  A -.runs.-> D
  A -.checks.-> T
```

| Service | Image | Host port | What it does |
|---|---|---|---|
| `minio` | `cgr.dev/chainguard/minio` | 9000, console 9001 | S3-compatible storage for every Iceberg table |
| `iceberg-rest` | `apache/iceberg-rest-fixture:1.10.1` | 8181 | One Iceberg catalog shared by Spark and Trino |
| `spark-master`, `spark-worker` | Spark 3.5.8 + Iceberg 1.12.0 (built here) | UI 8090 | The compute cluster |
| `spark-thrift` | same | 10000, UI 4040 | The SQL endpoint dbt-spark and the ingestion job use |
| `trino` | `trinodb/trino:480` | 8085 | Analyst SQL; the engine inside Starburst Enterprise |
| `airflow` | Airflow 3.4.0 + dbt in its own venv (built here) | 8080 | Runs ingestion, dbt and the Trino check daily |
| `postgres` | `postgres:17` | none | Airflow's metadata database |

MinIO stopped publishing community Docker images in October 2025 and archived the project in 2026. This lab uses Chainguard's free MinIO build. For a real platform, choose a maintained S3 store such as Ceph RGW, SeaweedFS, Garage, RustFS or MinIO's paid AIStor.

## Before you start

- Docker Desktop with at least **10 GB memory** and 4 CPUs (Settings, Resources). The stack uses about 8 GB.
- About 8 GB of free disk for images.
- Internet on the first build. It downloads the Iceberg jars from Maven Central and pip packages.

## Run it

```bash
cd lakehouse-capstone
cp .env.example .env              # lab credentials; change them if others use this machine
docker compose build              # about 5-10 minutes the first time
docker compose up -d
docker compose ps                 # wait until spark-thrift shows "healthy" (1-2 minutes)
```

**1. Put source files in the landing zone.** Either generate a realistic extract:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master "local[2]" \
  /opt/spark/jobs/generate_thelook.py --batch 1
```

or copy the thelook Parquet export from course module 3. You need the `users`, `products`, `orders`, `order_items` and `events` folders in `landing/thelook/`. On Linux, run `chmod 777 landing` first so the containers can write there.

**2. Run the pipeline.** Open Airflow at http://localhost:8080 (no login in this lab). Unpause `lakehouse_daily` and trigger it. The tasks run in this order:

| Task | What happens |
|---|---|
| `ingest_bronze` | Spark copies each landing folder into `lakehouse.bronze.<table>` as an Iceberg table, adding `_loaded_at` and `_batch_id` |
| `dbt_deps` | Installs `dbt_utils` |
| `dbt_source_freshness` | Fails the run if bronze is more than 3 days old |
| `dbt_build` | Seeds, snapshot, silver and gold models, and every data test |
| `trino_reads_gold` | Trino counts the gold tables and prints revenue by region |

**3. Query it like an analyst.**

```bash
docker compose exec trino trino --catalog lakehouse --schema gold
```
```sql
show tables;
select c.region, count(*) as orders, sum(o.order_revenue) as revenue
from fct_orders o join dim_customers c using (customer_id)
group by 1 order by 3 desc;

-- Iceberg metadata, straight from Trino
select * from "fct_events$snapshots";
select * from "fct_order_items$partitions";
```

**4. Run a second day.** Batch 2 adds a week of orders and events, moves orders from processing to complete, and relocates about 3% of customers.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master "local[2]" \
  /opt/spark/jobs/generate_thelook.py --batch 2
```

Trigger the DAG again, then check what changed:

```sql
-- incremental model: only new and late events were merged
select count(*) from fct_events;
select * from "fct_events$snapshots" order by committed_at;

-- SCD Type 2 from the snapshot
select customer_id, city, valid_from, valid_to, is_current
from dim_customers_history
where customer_id in (select customer_id from dim_customers_history where not is_current)
order by customer_id, valid_from;
```

## Develop dbt from your laptop

```bash
pip install "dbt-core==1.12.7" "dbt-spark[PyHive]==1.11.1"
cd lakehouse-capstone/dbt
export DBT_PROFILES_DIR=$PWD      # PowerShell: $env:DBT_PROFILES_DIR = $PWD
dbt deps
dbt debug                         # target dev connects to localhost:10000
dbt build --exclude-resource-type unit_test
```

The `dev` target builds into `dbt_jp_silver` and `dbt_jp_gold`. The `prod` target, which only Airflow uses, builds into `silver` and `gold`.

## What changes when dbt runs on Spark and Iceberg

- **Every model is `file_format: iceberg`.** Merge-based incremental models and snapshots need Iceberg (or Delta or Hudi). Plain Parquet tables only support `append` and `insert_overwrite`.
- **No views.** Spark stores its views in Spark SQL, which Trino cannot read. Staging is materialized as tables so analysts in Starburst can query silver too.
- **`spark.sql.defaultCatalog=lakehouse`.** dbt writes two-part names (`gold.fct_orders`). This setting points them at the Iceberg catalog instead of Spark's built-in Hive catalog. Spark 3.5 cannot run `SHOW TABLE EXTENDED` on Iceberg tables, so dbt-spark falls back to `SHOW TABLES` plus `DESCRIBE`. You will see that fallback in `logs/dbt.log`.
- **Schemas are Iceberg namespaces.** `generate_schema_name` gives production clean names (`silver`, `gold`) and gives developers prefixed ones.
- **Hidden partitioning.** `partition_by=['months(order_date)']` uses an Iceberg transform: analysts never filter on a partition column, and Trino still prunes.
- **Unit tests run only on a Spark session connection.** Over the Thrift Server, dbt-spark 1.11.1 fails them with `'NoneType' object is not iterable`. The unit test creates a temporary view through a query call, and Thrift returns no result set for it. The same test passes with `method: session`. The production DAG excludes unit tests, as any production run should.

## What was tested before publishing

Tested with dbt-core 1.12.7, dbt-spark 1.11.1, PySpark 3.5.8 and Airflow 3.4.0:

- The data generator ran for both batches.
- The ingestion script ran through a real Spark Thrift Server.
- `dbt build` passed all 46 seeds, models and data tests over the Thrift method, with unit tests excluded. The unit test passed with the session method.
- `dbt source freshness` and `dbt docs generate` ran.
- The DAG loaded in Airflow 3.4 with no import errors.
- `docker compose config` validated the stack.

These runs used Parquet tables, because the test machine could not download the Iceberg jars.

Not tested: the full Docker stack, Iceberg-specific behaviour (RTAS, MERGE, partition transforms, the snapshot), MinIO, the REST catalog, Trino and the Airflow container. Expect to fix a detail or two on the first run; the table below covers the likely ones.

## Troubleshooting

| You see | Likely cause | Fix |
|---|---|---|
| `spark-thrift` never healthy | Worker has no free cores or memory | `docker compose logs spark-thrift`; give Docker more memory |
| `NoSuchNamespaceException: bronze` | Ingestion has not run yet | Run `ingest_bronze` first |
| `Path does not exist: file:/landing/thelook/...` | Landing folder empty or not shared | Generate data (step 1); on Linux `chmod 777 landing` |
| S3 `403` or `UnknownHost warehouse.minio` | Credentials or path-style access | Check `.env`; keep `s3.path-style-access` true in Spark and Trino |
| Trino `Catalog 'lakehouse' not found` | Catalog file not mounted or invalid | `docker compose logs trino`; on Trino 481+ rename `fs.native-s3.enabled` to `fs.s3.enabled` |
| Airflow task `dbt_deps` fails | No internet from the container | Allow access to hub.getdbt.com, or vendor `dbt_utils` with a `local:` package |
| `pull access denied` for MinIO images | Registry change | Swap to another S3 store (see the note above) and keep `s3.path-style-access` |
| dbt `relation not found` after a catalog restart | Catalog volume removed | `docker compose down -v` wipes everything: re-run batch 1 and the DAG |

## Taking it to a real on-prem platform

| Lab | Production |
|---|---|
| Iceberg REST fixture with SQLite | Apache Polaris, Lakekeeper, Nessie or Hive Metastore, backed by Postgres, with HA |
| MinIO in one container | Ceph RGW, SeaweedFS or a commercial S3 store, replicated |
| Trino 480 | Starburst Enterprise with built-in access control or Ranger, LDAP or OAuth, resource groups |
| Spark standalone, 1 worker | Spark on Kubernetes or YARN with dynamic allocation; separate pools for ingestion and dbt |
| Thrift Server without auth | Thrift Server with Kerberos or LDAP (dbt-spark supports both) |
| `airflow standalone`, no login | Airflow with a proper deployment, CeleryExecutor or KubernetesExecutor, real authentication |
| One `dbt build` task | Astronomer Cosmos to render each model as a task, or one build per domain |
| Credentials in `.env` | A secrets manager; per-service identities |

## Challenges

1. **Table maintenance.** Add a weekly DAG that runs Iceberg maintenance through the Thrift Server: `CALL lakehouse.system.rewrite_data_files('gold.fct_events')`, `CALL lakehouse.system.expire_snapshots('gold.fct_events', TIMESTAMP '...')` and `CALL lakehouse.system.remove_orphan_files(table => 'gold.fct_events')`.
2. **Unit tests in CI.** Make the unit test run in GitHub Actions with a `method: session` target and a local PySpark.
3. **Per-model tasks.** Replace the single `dbt_build` task with Astronomer Cosmos.
4. **Spark Connect ingestion.** Rewrite `ingest_bronze.py` as a PySpark job submitted through Spark Connect.
5. **Time travel.** Query `fct_events` as of batch 1 in both Spark (`VERSION AS OF`) and Trino (`FOR VERSION AS OF`).
6. **Starburst.** If you have a Starburst Enterprise license, swap the `trino` image and reuse `lakehouse.properties` unchanged.
