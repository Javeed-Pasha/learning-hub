-- Fails if any order was delivered before it shipped.
select order_id, shipped_at, delivered_at
from {{ ref('stg_thelook__orders') }}
where delivered_at < shipped_at
