select
    id                          as customer_id,
    first_name,
    last_name,
    lower(trim(email))          as email,
    age,
    gender,
    city,
    state,
    country,
    traffic_source,
    created_at                  as signed_up_at,
    _loaded_at
from {{ source('thelook', 'users') }}
