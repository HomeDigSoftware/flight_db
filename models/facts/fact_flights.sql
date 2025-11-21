{{ config(unique_key='flight_id') }}


with t1 as (
select
    *,
    actual_arrival - actual_departure AS duration_flight,
    scheduled_arrival - scheduled_departure AS flight_duration_expected
from {{ source('stg', 'flights') }}

{% if is_incremental() %}

    where last_update > (select last_update from {{ this }} order by last_update desc limit 1)

{% endif%}
)
select 
    flight_id,                                 -- PK
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    actual_departure,
    actual_arrival,
    status,
    aircraft_code,                             -- FK to dim_aircraft
    departure_airport,                         -- FK to dim_airport
    arrival_airport,                           -- FK to dim_airport
    scheduled_departure::date as flight_date,  -- FK to dim_date
    duration_flight,
    flight_duration_expected,
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

