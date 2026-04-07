from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

try:
    from pyspark.sql import DataFrame, Row, SparkSession
    from pyspark.sql import Window
    from pyspark.sql import functions as F
except ModuleNotFoundError:
    DataFrame = Any  # type: ignore[assignment]
    Row = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    Window = None
    F = None


CATALOG_DEFAULT = "demo"
SYSTEM_COLUMNS = {
    "pipeline_batch_id",
    "bronze_load_ts",
    "source_name",
    "load_mode",
    "record_hash",
}
INCREMENTAL_MODES = {"INCREMENTAL_TS", "CDC", "EVENT"}


def _require_pyspark() -> None:
    if F is None or Window is None:
        raise ModuleNotFoundError("pyspark is required to run the metadata-driven runtime.")


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _sql_string(value: str | None) -> str:
    if value is None:
        return "NULL"
    return f"'{_escape(value)}'"


def _stringify_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _sql_literal(value: str, value_type: str | None) -> str:
    normalized = (value_type or "STRING").upper()
    if normalized == "TIMESTAMP":
        return f"TIMESTAMP '{_escape(value)}'"
    if normalized == "DATE":
        return f"DATE '{_escape(value)}'"
    if normalized in {"INT", "INTEGER", "BIGINT", "LONG", "DOUBLE", "FLOAT", "DECIMAL"}:
        return value
    return _sql_string(value)


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(value)]


def _parse_options(options_json: str | None) -> dict[str, Any]:
    if not options_json:
        return {}
    try:
        parsed = json.loads(options_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _control_table(catalog: str, table_name: str) -> str:
    return f"{catalog}.ctl.{table_name}"


@dataclass
class SourceRegistration:
    source_name: str
    domain_name: str | None
    source_type: str | None
    source_object: str | None
    enabled: bool
    load_mode: str
    base_extract_sql: str | None
    watermark_column: str | None
    watermark_type: str | None
    primary_keys: list[str]
    sequence_column: str | None
    delete_indicator_column: str | None
    bronze_table: str
    silver_current_table: str | None
    silver_history_table: str | None
    gold_table: str | None
    silver_transform_name: str
    gold_publish_name: str
    quality_rule_set: str | None
    replay_mode_default: str | None
    options: dict[str, Any]
    last_successful_watermark: str | None
    last_batch_id: int | None

    @property
    def ordering_column(self) -> str | None:
        return self.sequence_column or self.watermark_column

    @classmethod
    def from_row(cls, row: Row) -> "SourceRegistration":
        record = row.asDict(recursive=True)
        return cls(
            source_name=str(record["source_name"]),
            domain_name=record.get("domain_name"),
            source_type=record.get("source_type"),
            source_object=record.get("source_object"),
            enabled=bool(record.get("enabled", True)),
            load_mode=str(record.get("load_mode", "FULL") or "FULL").upper(),
            base_extract_sql=record.get("base_extract_sql"),
            watermark_column=record.get("watermark_column"),
            watermark_type=record.get("watermark_type"),
            primary_keys=_normalize_list(record.get("primary_keys")),
            sequence_column=record.get("sequence_column"),
            delete_indicator_column=record.get("delete_indicator_column"),
            bronze_table=str(record["bronze_table"]),
            silver_current_table=record.get("silver_current_table"),
            silver_history_table=record.get("silver_history_table"),
            gold_table=record.get("gold_table"),
            silver_transform_name=str(record.get("silver_transform_name", "scd2_current_history") or "scd2_current_history"),
            gold_publish_name=str(record.get("gold_publish_name", "publish_current_snapshot") or "publish_current_snapshot"),
            quality_rule_set=record.get("quality_rule_set"),
            replay_mode_default=record.get("replay_mode_default"),
            options=_parse_options(record.get("options_json")),
            last_successful_watermark=record.get("last_successful_watermark"),
            last_batch_id=record.get("last_batch_id"),
        )


@dataclass
class ReplayRequest:
    replay_request_id: int
    source_name: str
    replay_mode: str
    start_watermark: str | None
    end_watermark: str | None
    entity_predicate: str | None
    snapshot_id: int | None
    target_table: str | None
    requested_by: str | None
    request_status: str

    @classmethod
    def from_row(cls, row: Row) -> "ReplayRequest":
        record = row.asDict(recursive=True)
        return cls(
            replay_request_id=int(record["replay_request_id"]),
            source_name=str(record["source_name"]),
            replay_mode=str(record.get("replay_mode", "FULL_REBUILD") or "FULL_REBUILD").upper(),
            start_watermark=record.get("start_watermark"),
            end_watermark=record.get("end_watermark"),
            entity_predicate=record.get("entity_predicate"),
            snapshot_id=record.get("snapshot_id"),
            target_table=record.get("target_table"),
            requested_by=record.get("requested_by"),
            request_status=str(record.get("request_status", "REQUESTED") or "REQUESTED").upper(),
        )


def build_spark_session(app_name: str) -> SparkSession:
    _require_pyspark()
    return SparkSession.builder.appName(app_name).getOrCreate()


def ensure_catalog_namespaces(spark: SparkSession, catalog: str = CATALOG_DEFAULT) -> None:
    for namespace in ("ctl", "bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def list_active_sources(spark: SparkSession, catalog: str = CATALOG_DEFAULT) -> list[str]:
    rows = spark.sql(
        f"SELECT source_name FROM {_control_table(catalog, 'source_registration')} WHERE enabled = true ORDER BY source_name"
    ).collect()
    return [str(row.source_name) for row in rows]


def list_requested_replays(spark: SparkSession, catalog: str = CATALOG_DEFAULT) -> list[int]:
    rows = spark.sql(
        f"SELECT replay_request_id FROM {_control_table(catalog, 'replay_request')} "
        "WHERE upper(request_status) = 'REQUESTED' ORDER BY replay_request_id"
    ).collect()
    return [int(row.replay_request_id) for row in rows]


def get_source_registration(
    spark: SparkSession,
    source_name: str,
    catalog: str = CATALOG_DEFAULT,
) -> SourceRegistration:
    row = spark.sql(
        f"SELECT * FROM {_control_table(catalog, 'source_registration')} "
        f"WHERE source_name = {_sql_string(source_name)} AND enabled = true"
    ).first()
    if row is None:
        raise ValueError(f"No active source registration found for {source_name}.")
    return SourceRegistration.from_row(row)


def get_replay_request(
    spark: SparkSession,
    replay_request_id: int,
    catalog: str = CATALOG_DEFAULT,
) -> ReplayRequest:
    row = spark.sql(
        f"SELECT * FROM {_control_table(catalog, 'replay_request')} WHERE replay_request_id = {replay_request_id}"
    ).first()
    if row is None:
        raise ValueError(f"Replay request {replay_request_id} was not found.")
    return ReplayRequest.from_row(row)


def _next_batch_id(spark: SparkSession, catalog: str) -> int:
    row = spark.sql(
        f"SELECT COALESCE(MAX(batch_id), 0) + 1 AS next_batch_id FROM {_control_table(catalog, 'pipeline_run')}"
    ).first()
    return int(row.next_batch_id)


def create_batch_run(
    spark: SparkSession,
    source_name: str,
    run_mode: str,
    requested_by: str,
    catalog: str = CATALOG_DEFAULT,
    replay_request_id: int | None = None,
    forced_batch_id: int | None = None,
) -> int:
    batch_id = forced_batch_id or _next_batch_id(spark, catalog)
    spark.sql(
        f"""
        INSERT INTO {_control_table(catalog, 'pipeline_run')}
        SELECT
            {batch_id},
            {_sql_string(source_name)},
            {_sql_string(run_mode)},
            'RUNNING',
            {_sql_string(requested_by)},
            {replay_request_id if replay_request_id is not None else 'NULL'},
            current_timestamp(),
            CAST(NULL AS TIMESTAMP),
            CAST(NULL AS BIGINT),
            CAST(NULL AS BIGINT),
            CAST(NULL AS BIGINT),
            CAST(NULL AS BIGINT),
            CAST(NULL AS STRING),
            CAST(NULL AS STRING),
            CAST(NULL AS STRING)
        """
    )
    return batch_id


def finalize_batch_run(
    spark: SparkSession,
    batch_id: int,
    source_name: str,
    catalog: str,
    status: str,
    rows_read: int = 0,
    bronze_rows_written: int = 0,
    silver_rows_written: int = 0,
    gold_rows_written: int = 0,
    quality_status: str | None = None,
    watermark_value: str | None = None,
    error_message: str | None = None,
) -> None:
    spark.sql(
        f"""
        UPDATE {_control_table(catalog, 'pipeline_run')}
        SET
            run_status = {_sql_string(status)},
            run_end_ts = current_timestamp(),
            rows_read = {rows_read},
            bronze_rows_written = {bronze_rows_written},
            silver_rows_written = {silver_rows_written},
            gold_rows_written = {gold_rows_written},
            quality_status = {_sql_string(quality_status)},
            watermark_value = {_sql_string(watermark_value)},
            error_message = {_sql_string(error_message)}
        WHERE batch_id = {batch_id}
          AND source_name = {_sql_string(source_name)}
        """
    )


def update_source_watermark(
    spark: SparkSession,
    source_name: str,
    batch_id: int,
    watermark_value: str | None,
    catalog: str = CATALOG_DEFAULT,
) -> None:
    spark.sql(
        f"""
        UPDATE {_control_table(catalog, 'source_registration')}
        SET
            last_successful_watermark = {_sql_string(watermark_value)},
            last_batch_id = {batch_id},
            updated_ts = current_timestamp()
        WHERE source_name = {_sql_string(source_name)}
        """
    )


def mark_replay_request(
    spark: SparkSession,
    replay_request_id: int,
    status: str,
    catalog: str = CATALOG_DEFAULT,
) -> None:
    started_ts = "current_timestamp()" if status == "RUNNING" else "started_ts"
    completed_ts = "current_timestamp()" if status in {"COMPLETED", "FAILED"} else "completed_ts"
    spark.sql(
        f"""
        UPDATE {_control_table(catalog, 'replay_request')}
        SET
            request_status = {_sql_string(status)},
            started_ts = {started_ts},
            completed_ts = {completed_ts}
        WHERE replay_request_id = {replay_request_id}
        """
    )


def insert_quality_result(
    spark: SparkSession,
    batch_id: int,
    source_name: str,
    rule_name: str,
    rule_status: str,
    observed_value: str,
    expected_value: str,
    details: str,
    catalog: str = CATALOG_DEFAULT,
) -> None:
    spark.sql(
        f"""
        INSERT INTO {_control_table(catalog, 'quality_result')}
        SELECT
            {batch_id},
            {_sql_string(source_name)},
            {_sql_string(rule_name)},
            {_sql_string(rule_status)},
            {_sql_string(observed_value)},
            {_sql_string(expected_value)},
            {_sql_string(details)},
            current_timestamp()
        """
    )


def _resolve_upper_bound(spark: SparkSession, config: SourceRegistration) -> Any:
    if config.load_mode not in INCREMENTAL_MODES or not config.ordering_column:
        return None
    row = spark.sql("SELECT current_timestamp() AS upper_bound").first()
    return row.upper_bound


def _build_extract_df(
    spark: SparkSession,
    config: SourceRegistration,
    lower_bound: str | None,
    upper_bound: Any,
) -> DataFrame:
    base_df = spark.sql(config.base_extract_sql) if config.base_extract_sql else spark.table(config.source_object)
    if config.load_mode not in INCREMENTAL_MODES or not config.ordering_column:
        return base_df

    predicates: list[str] = []
    if lower_bound:
        predicates.append(f"{config.ordering_column} > {_sql_literal(lower_bound, config.watermark_type)}")
    if upper_bound is not None:
        upper_bound_text = _stringify_value(upper_bound)
        if upper_bound_text:
            predicates.append(f"{config.ordering_column} <= {_sql_literal(upper_bound_text, config.watermark_type)}")
    if predicates:
        return base_df.where(" AND ".join(predicates))
    return base_df


def _max_observed_watermark(df: DataFrame, column_name: str | None) -> str | None:
    if not column_name or column_name not in df.columns:
        return None
    row = df.agg(F.max(F.col(column_name)).alias("max_value")).first()
    return _stringify_value(row.max_value)


def _required_columns(config: SourceRegistration) -> list[str]:
    raw_columns = config.options.get("required_columns", [])
    return [str(value) for value in raw_columns if str(value).strip()]


def run_quality_checks(
    spark: SparkSession,
    batch_id: int,
    config: SourceRegistration,
    bronze_df: DataFrame,
    catalog: str = CATALOG_DEFAULT,
) -> str:
    row_count = bronze_df.count()
    row_status = "PASSED" if row_count > 0 else "NO_DATA"
    insert_quality_result(
        spark,
        batch_id,
        config.source_name,
        "bronze_row_count",
        row_status,
        str(row_count),
        ">= 0",
        "Basic completeness check for the bronze landing set.",
        catalog,
    )

    required = _required_columns(config)
    missing = [column for column in required if column not in bronze_df.columns]
    schema_status = "PASSED" if not missing else "FAILED"
    insert_quality_result(
        spark,
        batch_id,
        config.source_name,
        "required_columns_present",
        schema_status,
        ", ".join(bronze_df.columns),
        ", ".join(required) if required else "configured per source",
        "Missing columns: " + ", ".join(missing) if missing else "All required columns were present.",
        catalog,
    )
    return "FAILED" if schema_status == "FAILED" else row_status


def _append_bronze(
    spark: SparkSession,
    source_df: DataFrame,
    config: SourceRegistration,
    batch_id: int,
    run_mode: str,
) -> DataFrame:
    record_struct = F.struct(*[F.col(column) for column in source_df.columns])
    bronze_df = (
        source_df.withColumn("pipeline_batch_id", F.lit(batch_id))
        .withColumn("bronze_load_ts", F.current_timestamp())
        .withColumn("source_name", F.lit(config.source_name))
        .withColumn("load_mode", F.lit(run_mode))
        .withColumn("record_hash", F.sha2(F.to_json(record_struct), 256))
    )
    if spark.catalog.tableExists(config.bronze_table):
        bronze_df.writeTo(config.bronze_table).append()
    else:
        bronze_df.writeTo(config.bronze_table).using("iceberg").create()
    return bronze_df


def _business_columns(bronze_df: DataFrame) -> list[str]:
    return [column for column in bronze_df.columns if column not in SYSTEM_COLUMNS]


def _deleted_values(config: SourceRegistration) -> list[str]:
    values = config.options.get("deleted_values", ["Y", "YES", "TRUE", "1", "D"])
    return [str(value).upper() for value in values]


def build_scd2_frames(bronze_df: DataFrame, config: SourceRegistration) -> tuple[DataFrame, DataFrame]:
    _require_pyspark()
    if not config.primary_keys:
        raise ValueError(f"Source {config.source_name} requires primary_keys metadata for scd2_current_history.")
    if not config.ordering_column:
        raise ValueError(f"Source {config.source_name} requires a watermark_column or sequence_column.")

    order_desc = [F.col(config.ordering_column).desc_nulls_last(), F.col("bronze_load_ts").desc_nulls_last()]
    order_asc = [F.col(config.ordering_column).asc_nulls_last(), F.col("bronze_load_ts").asc_nulls_last()]
    current_window = Window.partitionBy(*config.primary_keys).orderBy(*order_desc)
    history_window = Window.partitionBy(*config.primary_keys).orderBy(*order_asc)
    payload_columns = _business_columns(bronze_df)

    ranked_df = (
        bronze_df.withColumn("_row_rank", F.row_number().over(current_window))
        .withColumn("_next_effective_from", F.lead(F.col(config.ordering_column)).over(history_window))
    )

    history_df = ranked_df.select(
        *[F.col(column) for column in payload_columns],
        F.col(config.ordering_column).alias("effective_from"),
        F.col("_next_effective_from").alias("effective_to"),
        (F.col("_row_rank") == F.lit(1)).alias("is_current"),
        F.col("pipeline_batch_id"),
        F.col("bronze_load_ts"),
        F.col("record_hash"),
    )

    current_df = history_df.where(F.col("is_current") == F.lit(True))
    if config.delete_indicator_column and config.delete_indicator_column in history_df.columns:
        current_df = current_df.where(
            ~F.upper(F.col(config.delete_indicator_column).cast("string")).isin(_deleted_values(config))
        )
    return history_df, current_df


def _replace_table(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    if spark.catalog.tableExists(table_name):
        df.writeTo(table_name).replace()
    else:
        df.writeTo(table_name).using("iceberg").create()


def _rewrite_impacted_scope(
    spark: SparkSession,
    table_name: str,
    rebuilt_df: DataFrame,
    impacted_keys_df: DataFrame | None,
    key_columns: list[str],
) -> None:
    if impacted_keys_df is None or not spark.catalog.tableExists(table_name):
        _replace_table(spark, rebuilt_df, table_name)
        return
    remaining_df = spark.table(table_name).join(impacted_keys_df, key_columns, "left_anti")
    combined_df = remaining_df.unionByName(rebuilt_df, allowMissingColumns=True)
    _replace_table(spark, combined_df, table_name)


def _publish_current_snapshot(spark: SparkSession, config: SourceRegistration, current_df: DataFrame) -> DataFrame:
    if not config.gold_table:
        return current_df
    _replace_table(spark, current_df, config.gold_table)
    return spark.table(config.gold_table)


def _run_transform(
    spark: SparkSession,
    config: SourceRegistration,
    bronze_scope_df: DataFrame,
    impacted_keys_df: DataFrame | None,
) -> tuple[DataFrame, DataFrame, DataFrame | None]:
    transform_name = config.silver_transform_name.upper()
    if transform_name != "SCD2_CURRENT_HISTORY":
        raise ValueError(f"Unsupported silver transform: {config.silver_transform_name}")
    if not config.silver_history_table or not config.silver_current_table:
        raise ValueError(f"Source {config.source_name} must define both silver tables.")

    history_df, current_df = build_scd2_frames(bronze_scope_df, config)
    _rewrite_impacted_scope(spark, config.silver_history_table, history_df, impacted_keys_df, config.primary_keys)
    _rewrite_impacted_scope(spark, config.silver_current_table, current_df, impacted_keys_df, config.primary_keys)

    current_materialized = spark.table(config.silver_current_table)
    history_materialized = spark.table(config.silver_history_table)

    gold_df: DataFrame | None = None
    if config.gold_publish_name.upper() == "PUBLISH_CURRENT_SNAPSHOT" and config.gold_table:
        gold_df = _publish_current_snapshot(spark, config, current_materialized)
    return history_materialized, current_materialized, gold_df


def run_pipeline_for_source(
    spark: SparkSession,
    source_name: str,
    catalog: str = CATALOG_DEFAULT,
    forced_batch_id: int | None = None,
    requested_by: str = "runtime",
) -> int:
    config = get_source_registration(spark, source_name, catalog)
    run_mode = f"PIPELINE_{config.load_mode}"
    batch_id = create_batch_run(spark, source_name, run_mode, requested_by, catalog, forced_batch_id=forced_batch_id)
    try:
        upper_bound = _resolve_upper_bound(spark, config)
        source_df = _build_extract_df(spark, config, config.last_successful_watermark, upper_bound)
        rows_read = source_df.count()
        if rows_read == 0:
            finalize_batch_run(
                spark,
                batch_id,
                source_name,
                catalog,
                status="SUCCESS",
                quality_status="NO_DATA",
                watermark_value=config.last_successful_watermark,
            )
            return batch_id

        bronze_df = _append_bronze(spark, source_df, config, batch_id, config.load_mode)
        bronze_rows_written = bronze_df.count()
        quality_status = run_quality_checks(spark, batch_id, config, bronze_df, catalog)
        if quality_status == "FAILED":
            raise ValueError(f"Quality checks failed for source {source_name}.")

        history_df, current_df, gold_df = _run_transform(spark, config, spark.table(config.bronze_table), None)
        watermark_value = _max_observed_watermark(bronze_df, config.ordering_column)
        update_source_watermark(spark, source_name, batch_id, watermark_value, catalog)
        finalize_batch_run(
            spark,
            batch_id,
            source_name,
            catalog,
            status="SUCCESS",
            rows_read=rows_read,
            bronze_rows_written=bronze_rows_written,
            silver_rows_written=current_df.count() + history_df.count(),
            gold_rows_written=gold_df.count() if gold_df is not None else 0,
            quality_status=quality_status,
            watermark_value=watermark_value,
        )
        return batch_id
    except Exception as exc:
        finalize_batch_run(
            spark,
            batch_id,
            source_name,
            catalog,
            status="FAILED",
            error_message=str(exc),
        )
        raise


def _filter_scope_for_request(
    bronze_df: DataFrame,
    config: SourceRegistration,
    request: ReplayRequest,
) -> DataFrame:
    if request.replay_mode == "DATE_RANGE":
        if not config.ordering_column:
            raise ValueError(f"Source {config.source_name} cannot run DATE_RANGE replay without an ordering column.")
        predicates: list[str] = []
        if request.start_watermark:
            predicates.append(f"{config.ordering_column} >= {_sql_literal(request.start_watermark, config.watermark_type)}")
        if request.end_watermark:
            predicates.append(f"{config.ordering_column} < {_sql_literal(request.end_watermark, config.watermark_type)}")
        return bronze_df.where(" AND ".join(predicates)) if predicates else bronze_df
    if request.replay_mode == "ENTITY":
        if not request.entity_predicate:
            raise ValueError("ENTITY replay requires entity_predicate metadata.")
        return bronze_df.where(request.entity_predicate)
    return bronze_df


def restore_iceberg_snapshot(
    spark: SparkSession,
    table_name: str,
    snapshot_id: int,
    catalog: str = CATALOG_DEFAULT,
) -> None:
    spark.sql(
        f"CALL {catalog}.system.rollback_to_snapshot(table => '{_escape(table_name)}', snapshot_id => {snapshot_id})"
    )


def run_replay_request(
    spark: SparkSession,
    replay_request_id: int,
    catalog: str = CATALOG_DEFAULT,
) -> int:
    request = get_replay_request(spark, replay_request_id, catalog)
    config = get_source_registration(spark, request.source_name, catalog)

    if request.replay_mode == "ICEBERG_RESTORE":
        if request.snapshot_id is None:
            raise ValueError("ICEBERG_RESTORE requires snapshot_id.")
        target_table = request.target_table or config.silver_current_table or config.bronze_table
        mark_replay_request(spark, replay_request_id, "RUNNING", catalog)
        try:
            restore_iceberg_snapshot(spark, target_table, request.snapshot_id, catalog)
            mark_replay_request(spark, replay_request_id, "COMPLETED", catalog)
            return 0
        except Exception:
            mark_replay_request(spark, replay_request_id, "FAILED", catalog)
            raise

    mark_replay_request(spark, replay_request_id, "RUNNING", catalog)
    batch_id = create_batch_run(
        spark,
        request.source_name,
        f"REPLAY_{request.replay_mode}",
        request.requested_by or "replay-runner",
        catalog,
        replay_request_id=replay_request_id,
    )

    try:
        bronze_df = spark.table(config.bronze_table)
        impacted_keys_df = None
        bronze_scope_df = bronze_df
        if request.replay_mode in {"DATE_RANGE", "ENTITY"}:
            if not config.primary_keys:
                raise ValueError(f"Source {config.source_name} requires primary_keys metadata for partial replay.")
            impacted_keys_df = _filter_scope_for_request(bronze_df, config, request).select(*config.primary_keys).distinct()
            if impacted_keys_df.count() == 0:
                finalize_batch_run(
                    spark,
                    batch_id,
                    request.source_name,
                    catalog,
                    status="SUCCESS",
                    quality_status="NO_DATA",
                )
                mark_replay_request(spark, replay_request_id, "COMPLETED", catalog)
                return batch_id
            bronze_scope_df = bronze_df.join(impacted_keys_df, config.primary_keys, "inner")

        history_df, current_df, gold_df = _run_transform(spark, config, bronze_scope_df, impacted_keys_df)
        finalize_batch_run(
            spark,
            batch_id,
            request.source_name,
            catalog,
            status="SUCCESS",
            rows_read=bronze_scope_df.count(),
            bronze_rows_written=0,
            silver_rows_written=current_df.count() + history_df.count(),
            gold_rows_written=gold_df.count() if gold_df is not None else 0,
            quality_status="REPLAYED",
        )
        mark_replay_request(spark, replay_request_id, "COMPLETED", catalog)
        return batch_id
    except Exception as exc:
        finalize_batch_run(
            spark,
            batch_id,
            request.source_name,
            catalog,
            status="FAILED",
            error_message=str(exc),
        )
        mark_replay_request(spark, replay_request_id, "FAILED", catalog)
        raise