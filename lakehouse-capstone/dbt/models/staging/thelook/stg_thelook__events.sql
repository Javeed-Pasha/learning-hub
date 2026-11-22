select
    id                  as event_id,
    user_id             as customer_id,
    session_id,
    sequence_number,
    event_type,
    uri,
    browser,
    traffic_source,
    created_at
from {{ source('thelook', 'events') }}
