-- upload to BQ
with
onsites as
(
select 
     date_trunc('{period}', ssa.date )::date as day,
     count(distinct formatted_address) as vis
from core_lead cl
left join store_order so on cl.order_id = so.id
left join core_house ch on ch.id = so.house_id
left join customers_customer cc on cc.id = ch.customer_id
left join schedule_appointment ssa on ssa.order_id = so.id
left join schedule_appointmenttype ssat on ssat.id = ssa.appointment_type_id
--left join quote_quote qq on qq.order_id = so.id
left join geo_address ga on ga.id = cl.address_id
--left join leads l on l.id = cl.id
where (cl.phone_number is not null or cl.email is not null)
                 and cl.full_name not ilike '%test%' and coalesce(cl.email,'') not ilike '%test%'
                 and cl.full_name not ilike '%fake%' and coalesce(cl.email,'') not ilike '%fake%'
                  and cl.created_at >= '2018-04-16'
                  and ssa.date >= '2018-04-16'  
                 and ssa.cancelled_at is null
                  and ssat.code <> 'quote_review'
                  and is_commercial::integer = 1 
group by 1
)
select 
        day as date, 
        sum(vis) as SAL101C --commercial onsites 
from onsites
where 
        day <= now()
group by 1
order by 1 desc
limit 40
