select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
    customer_id,
    email,
    city,
    state,
    country,
    traffic_source,
    dbt_valid_from                                          as valid_from,
    coalesce(dbt_valid_to, timestamp'9999-12-31 00:00:00')  as valid_to,
    dbt_valid_to is null                                    as is_current
from {{ ref('customers_snapshot') }}
