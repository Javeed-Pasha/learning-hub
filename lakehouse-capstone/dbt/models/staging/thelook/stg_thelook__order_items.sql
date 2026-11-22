select
    id                                  as order_item_id,
    order_id,
    user_id                             as customer_id,
    product_id,
    lower(status)                       as item_status,
    cast(sale_price as decimal(12, 2))  as sale_price,
    created_at                          as ordered_at,
    shipped_at,
    delivered_at,
    returned_at
from {{ source('thelook', 'order_items') }}
