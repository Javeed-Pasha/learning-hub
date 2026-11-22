select
    order_id,
    user_id             as customer_id,
    lower(status)       as order_status,
    num_of_item         as item_count,
    created_at          as ordered_at,
    shipped_at,
    delivered_at,
    returned_at,
    _loaded_at
from {{ source('thelook', 'orders') }}
