# Spark-Iceberg Compose Notes

This repository already includes the full Docker Compose file needed for the `learning-hub` Spark and Iceberg demo:

- [../docker-compose.learning-hub.yml](../docker-compose.learning-hub.yml)

Use that file directly. There is no need to add extra mounts manually.

## What The Saved Compose File Does

When you start [../docker-compose.learning-hub.yml](../docker-compose.learning-hub.yml), it brings up:

- `spark-iceberg`
- `iceberg-rest`
- `minio`
- `mc`

It also mounts the repo folders needed by the demo:

- `./warehouse` to `/home/iceberg/warehouse`
- `./data` to `/home/iceberg/data`
- `./notebooks` to `/home/iceberg/notebooks/notebooks`
- `./ddl` to `/home/iceberg/work/learning-hub/ddl`
- `./spark` to `/home/iceberg/work/learning-hub/spark`
- `./notebooks` to `/home/iceberg/work/learning-hub/notebooks`

That is why `.gitkeep` files were added under `warehouse/` and `data/`: to make those repo-local mount folders explicit.

## How To Run It

From the repository root:

```powershell
docker compose -f docker-compose.learning-hub.yml up -d
```

Then open Jupyter at `http://localhost:8888` and run:

- [../notebooks/learning_hub_demo.ipynb](../notebooks/learning_hub_demo.ipynb)

## What The Notebook Does

1. Resets old demo tables if they already exist.
2. Creates `demo.ctl`, `demo.staging`, `demo.bronze`, `demo.silver`, and `demo.gold`.
3. Seeds batch 1 demo source data.
4. Runs the pipeline for `customer`, `orders`, `product`, `invoice`, and `shipment`.
5. Seeds batch 2 changed source data.
6. Runs the second batch.
7. Builds the final joined gold serving table.
8. Shows bronze, silver, and gold outputs.