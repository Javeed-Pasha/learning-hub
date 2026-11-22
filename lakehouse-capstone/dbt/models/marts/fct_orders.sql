with items as (

    select
        order_id,
        count(*)            as item_rows,
        sum(sale_price)     as order_revenue,
        sum(gross_margin)   as order_margin
    from {{ ref('fct_order_items') }}
    group by order_id

)

select
    o.order_id,
    o.customer_id,
    to_date(o.ordered_at)           as order_date,
    o.order_status,
    m.days_to_deliver,
    m.is_returned,
    coalesce(i.item_rows, 0)        as item_rows,
    coalesce(i.order_revenue, 0)    as order_revenue,
    coalesce(i.order_margin, 0)     as order_margin
from {{ ref('stg_thelook__orders') }} as o
left join items as i
    on o.order_id = i.order_id
left join {{ ref('int_orders__delivery_metrics') }} as m
    on o.order_id = m.order_id
