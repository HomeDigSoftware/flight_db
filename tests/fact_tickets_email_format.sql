

select *
from {{ ref('fact_tickets') }}
where email is not null 
  and email not like '%@%.%'