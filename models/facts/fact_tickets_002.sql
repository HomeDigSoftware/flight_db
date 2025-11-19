
with t1 as (
select
    {{dbt_utils.star(source('stg', 'tickets'), except=["contact_data"])  }},
    t.contact_data ->> 'phone' as contact_phone
from {{ source('stg', 'tickets') }} t
), t2 as (
select
    {{dbt_utils.star(source('stg', 'tickets'), except=["contact_data"])  }},
    t.contact_data ->> 'email' as contact_email
from {{ source('stg', 'tickets') }} t
)
select
    *,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from {{ source('stg', 'bookings') }} b
left join t1 on t1.book_ref = b.book_ref
left join t2 on t2.book_ref = b.book_ref