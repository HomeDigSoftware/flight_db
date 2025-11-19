
with t1 as (
select
    *,
    actual_arrival - actual_departure AS duration_flight,
    scheduled_arrival - scheduled_departure AS flight_duration_expected
from {{ source('stg', 'flights') }}
)
select 
    *,
    case
        when t1.status like '%Cancelled%' then 'Cancelled'
        when duration_flight is null then t1.status || ' - In Flight'
        when t1.status = 'delayed' then 'flight is delayed'
        when flight_duration_expected > duration_flight then 'long'
        when flight_duration_expected < duration_flight then 'short'
        else 'as expected'
    end as flight_duration_category , 
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from t1