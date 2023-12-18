-- upload to BQ
select 
    date_trunc('{period}',l.created_at at time zone 'America/Los_Angeles')::date as date,
    sum(cast(event_object ->> 'fee' as numeric)) as MAR801,
    sum(case when l.product_id = 105 then cast(event_object ->> 'fee' as numeric) else 0 end) as MAR801F,
    sum(case when l.product_id = 132 then cast(event_object ->> 'fee' as numeric) else 0 end) as MAR801T,   
    sum(case when l.product_id = 34 then cast(event_object ->> 'fee' as numeric) else 0 end) as MAR801D
from 
    core_lead l left join 
    customers_visitoraction cv on l.visitor_action_id = cv.id
where 
        cv.event_object ->> 'utm_source' ilike '%bark%'
group by 1
order by 1 desc
