from __future__ import annotations

import argparse

from runtime_framework import (
    CATALOG_DEFAULT,
    build_spark_session,
    ensure_catalog_namespaces,
    list_requested_replays,
    run_replay_request,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run requested metadata-driven replay jobs.")
    parser.add_argument("--catalog", default=CATALOG_DEFAULT, help="Iceberg catalog name. Defaults to demo.")
    parser.add_argument("--replay-request-id", type=int, help="Specific replay request id to execute.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session("metadata-driven-replay")
    try:
        ensure_catalog_namespaces(spark, args.catalog)
        request_ids = [args.replay_request_id] if args.replay_request_id is not None else list_requested_replays(spark, args.catalog)
        if not request_ids:
            raise ValueError("No replay requests with status REQUESTED were found.")
        for replay_request_id in request_ids:
            run_replay_request(spark, replay_request_id=replay_request_id, catalog=args.catalog)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()