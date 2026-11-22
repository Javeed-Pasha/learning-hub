{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by=['days(created_at)'],   -- Iceberg transform: no extra partition column needed
    on_schema_change='append_new_columns'
) }}

with source_events as (

    select *
    from {{ ref('stg_thelook__events') }}
    {% if is_incremental() %}
      -- 3-day lookback catches late events; the merge on event_id prevents duplicates
      where created_at > (select max(created_at) - interval 3 days from {{ this }})
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (partition by event_id order by created_at desc) as row_num
    from source_events

)

select
    event_id,
    customer_id,
    session_id,
    sequence_number,
    event_type,
    uri,
    browser,
    traffic_source,
    created_at,
    to_date(created_at) as event_date
from ranked
where row_num = 1
