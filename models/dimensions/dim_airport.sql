

select 
    airport_code,
    airport_name ->> 'en' as airport_en_name,
    airport_name ->> 'ru' as airport_ru_name,
    city ->> 'en' as city_en_name,
    city ->> 'ru' as city_ru_name,
    coordinates,
    timezone
from stg.airports_data
