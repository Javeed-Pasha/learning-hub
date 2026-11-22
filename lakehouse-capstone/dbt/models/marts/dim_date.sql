-- Spark-native date spine: one row per day
with days as (

    select explode(sequence(date'2024-01-01', date'2027-12-31', interval 1 day)) as date_day

)

select
    date_day,
    year(date_day)                      as year,
    quarter(date_day)                   as quarter,
    month(date_day)                     as month,
    date_format(date_day, 'MMM')        as month_name,
    date_format(date_day, 'EEEE')       as day_name,
    dayofweek(date_day) in (1, 7)       as is_weekend   -- Spark: 1 = Sunday, 7 = Saturday
from days
