CREATE NAMESPACE IF NOT EXISTS demo.staging;

CREATE TABLE IF NOT EXISTS demo.staging.customer_source (
    customer_id BIGINT,
    customer_name STRING,
    location STRING,
    source_change_ts TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS demo.staging.orders_source (
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    order_status STRING,
    order_amount DECIMAL(12,2),
    order_date DATE,
    source_change_ts TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS demo.staging.product_source (
    product_id BIGINT,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(12,2),
    source_change_ts TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS demo.staging.invoice_source (
    invoice_id BIGINT,
    order_id BIGINT,
    invoice_status STRING,
    invoice_amount DECIMAL(12,2),
    source_change_ts TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS demo.staging.shipment_source (
    shipment_id BIGINT,
    order_id BIGINT,
    shipment_status STRING,
    warehouse_code STRING,
    source_change_ts TIMESTAMP
) USING iceberg;