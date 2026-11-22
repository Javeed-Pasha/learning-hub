{{ config(partition_by=['months(order_date)']) }}   -- Iceberg hidden partitioning by month

select
    order_item_id,
    order_id,
    customer_id,
    product_id,
    to_date(ordered_at)                             as order_date,
    item_status,
    sale_price,
    product_cost,
    sale_price - product_cost                       as gross_margin,
    {{ margin_pct('sale_price', 'product_cost') }}  as margin_pct,
    category,
    brand,
    department
from {{ ref('int_order_items__enriched') }}
