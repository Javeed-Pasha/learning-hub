select
    id                                  as product_id,
    name                                as product_name,
    brand,
    category,
    department,
    sku,
    cast(cost as decimal(12, 2))         as product_cost,
    cast(retail_price as decimal(12, 2)) as retail_price
from {{ source('thelook', 'products') }}
