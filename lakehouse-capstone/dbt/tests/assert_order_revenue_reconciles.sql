-- Revenue must match between the order fact and the order-line fact (to the cent).
select o.total, i.total
from (select sum(order_revenue) as total from {{ ref('fct_orders') }}) as o
cross join (select sum(sale_price) as total from {{ ref('fct_order_items') }}) as i
where abs(o.total - i.total) > 0.01
