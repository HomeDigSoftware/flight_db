
select
    b.*,
    {{dbt_utils.star(source('stg', 'tickets'), except=['book_ref'])}},
    t.contact_data ->> 'phone' as phone,
    t.contact_data ->> 'email' as email,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from {{ source('stg', 'bookings') }} b
left join {{ source('stg', 'tickets') }} t on t.book_ref = b.book_ref