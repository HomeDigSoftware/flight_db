

select 
    airport_code,
    airport_name ->> 'en' as airport_en_name,
    airport_name ->> 'ru' as airport_ru_name,
    city ->> 'en' as city_en_name,
    city ->> 'ru' as city_ru_name,
    coordinates,
    timezone,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from {{ source('stg', 'airports_data') }}