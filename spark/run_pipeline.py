from __future__ import annotations

import argparse

from runtime_framework import (
    CATALOG_DEFAULT,
    build_spark_session,
    ensure_catalog_namespaces,
    list_active_sources,
    run_pipeline_for_source,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the metadata-driven Spark pipeline.")
    parser.add_argument("--catalog", default=CATALOG_DEFAULT, help="Iceberg catalog name. Defaults to demo.")
    parser.add_argument("--source", help="Single source to run. If omitted, all active sources run.")
    parser.add_argument("--requested-by", default="runtime", help="Audit value written to control tables.")
    parser.add_argument("--forced-batch-id", type=int, help="Optional fixed batch id for repeatable demos.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session("metadata-driven-pipeline")
    try:
        ensure_catalog_namespaces(spark, args.catalog)
        sources = [args.source] if args.source else list_active_sources(spark, args.catalog)
        if not sources:
            raise ValueError("No active sources were found in demo.ctl.source_registration.")
        for source_name in sources:
            run_pipeline_for_source(
                spark,
                source_name=source_name,
                catalog=args.catalog,
                forced_batch_id=args.forced_batch_id,
                requested_by=args.requested_by,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()