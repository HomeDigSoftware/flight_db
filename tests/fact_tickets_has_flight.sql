select
    t.ticket_no
from {{ ref('fact_tickets') }} t
left join {{ ref('ticket_flights') }} tf
    on t.ticket_no = tf.ticket_no
where tf.ticket_no is null