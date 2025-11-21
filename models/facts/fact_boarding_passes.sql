

select 
    tf.*,
    {{ dbt_utils.star(source('stg', 'boarding_passes'), except=["ticket_no", "flight_id", "last_update"])  }},
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as dbt_time
from {{ source('stg', 'ticket_flights') }} tf
left join {{ source('stg', 'boarding_passes') }} bp on bp.ticket_no = tf.ticket_no

{% if is_incremental() %}
    where tf.last_update::timestamp > (select tf.last_update from {{ this }} order by tf.last_update desc limit 1)
{% endif%}