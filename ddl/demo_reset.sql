DROP TABLE IF EXISTS demo.gold.sales_order_fulfillment_iceberg;

DROP TABLE IF EXISTS demo.gold.customer_serving_iceberg;
DROP TABLE IF EXISTS demo.gold.orders_serving_iceberg;
DROP TABLE IF EXISTS demo.gold.product_serving_iceberg;
DROP TABLE IF EXISTS demo.gold.invoice_serving_iceberg;
DROP TABLE IF EXISTS demo.gold.shipment_serving_iceberg;

DROP TABLE IF EXISTS demo.silver.customer_current_iceberg;
DROP TABLE IF EXISTS demo.silver.customer_history_iceberg;
DROP TABLE IF EXISTS demo.silver.orders_current_iceberg;
DROP TABLE IF EXISTS demo.silver.orders_history_iceberg;
DROP TABLE IF EXISTS demo.silver.product_current_iceberg;
DROP TABLE IF EXISTS demo.silver.product_history_iceberg;
DROP TABLE IF EXISTS demo.silver.invoice_current_iceberg;
DROP TABLE IF EXISTS demo.silver.invoice_history_iceberg;
DROP TABLE IF EXISTS demo.silver.shipment_current_iceberg;
DROP TABLE IF EXISTS demo.silver.shipment_history_iceberg;

DROP TABLE IF EXISTS demo.bronze.customer_incremental_iceberg;
DROP TABLE IF EXISTS demo.bronze.orders_incremental_iceberg;
DROP TABLE IF EXISTS demo.bronze.product_snapshot_iceberg;
DROP TABLE IF EXISTS demo.bronze.invoice_incremental_iceberg;
DROP TABLE IF EXISTS demo.bronze.shipment_incremental_iceberg;

DROP TABLE IF EXISTS demo.staging.customer_source;
DROP TABLE IF EXISTS demo.staging.orders_source;
DROP TABLE IF EXISTS demo.staging.product_source;
DROP TABLE IF EXISTS demo.staging.invoice_source;
DROP TABLE IF EXISTS demo.staging.shipment_source;

DROP TABLE IF EXISTS demo.ctl.replay_request;
DROP TABLE IF EXISTS demo.ctl.quality_result;
DROP TABLE IF EXISTS demo.ctl.pipeline_run;
DROP TABLE IF EXISTS demo.ctl.source_registration;