select
    oi.order_item_id,
    oi.order_id,
    oi.customer_id,
    oi.product_id,
    oi.item_status,
    oi.sale_price,
    oi.ordered_at,
    p.product_cost,
    p.category,
    p.brand,
    p.department
from {{ ref('stg_thelook__order_items') }} as oi
left join {{ ref('stg_thelook__products') }} as p
    on oi.product_id = p.product_id
