select
    u.customer_id,
    u.first_name,
    u.last_name,
    u.email,
    u.gender,
    u.age,
    u.city,
    u.state,
    u.country,
    coalesce(r.region, 'Unmapped')  as region,
    u.traffic_source,
    u.signed_up_at
from {{ ref('stg_thelook__users') }} as u
left join {{ ref('country_regions') }} as r
    on u.country = r.country
