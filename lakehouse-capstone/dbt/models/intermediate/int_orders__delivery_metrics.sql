-- A real table (not ephemeral) so the unit test in _intermediate__models.yml can target it.
{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    order_status,
    ordered_at,
    delivered_at,
    datediff(to_date(delivered_at), to_date(ordered_at)) as days_to_deliver,
    order_status = 'returned'                              as is_returned
from {{ ref('stg_thelook__orders') }}
