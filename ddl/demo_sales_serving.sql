CREATE OR REPLACE TABLE demo.gold.sales_order_fulfillment_iceberg
USING iceberg
AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.location AS customer_location,
    o.product_id,
    p.product_name,
    p.category,
    o.order_status,
    i.invoice_id,
    i.invoice_status,
    s.shipment_id,
    s.shipment_status,
    s.warehouse_code,
    o.order_amount,
    i.invoice_amount,
    p.unit_price,
    o.order_date,
    greatest(
        o.effective_from,
        c.effective_from,
        p.effective_from,
        i.effective_from,
        s.effective_from
    ) AS serving_effective_from
FROM demo.gold.orders_serving_iceberg o
LEFT JOIN demo.gold.customer_serving_iceberg c
    ON o.customer_id = c.customer_id
LEFT JOIN demo.gold.product_serving_iceberg p
    ON o.product_id = p.product_id
LEFT JOIN demo.gold.invoice_serving_iceberg i
    ON o.order_id = i.order_id
LEFT JOIN demo.gold.shipment_serving_iceberg s
    ON o.order_id = s.order_id;