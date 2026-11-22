select
    product_id,
    product_name,
    brand,
    category,
    department,
    product_cost,
    retail_price
from {{ ref('stg_thelook__products') }}
