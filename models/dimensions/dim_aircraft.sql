

select  
    a.aircraft_code,
    a.model ->> 'en' as model_English,
    a.model ->> 'ru' as model_Russian,
    case
        when a.range > 5600 then 'high'
        else 'low'
    end as range_category,
    a.range,
    s.seat_no,
    s.fare_conditions,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from {{ source('stg', 'aircrafts_data') }} a
left join {{ source('stg', 'seats') }} s on s.aircraft_code = a.aircraft_code




