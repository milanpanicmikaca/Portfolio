-- upload to BQ
with
last_approved_quotes as 
(
  select 
    o.id as order_id,
    completed_at as cancelled_at,
    is_cancellation,
    rank() over(partition by o.id order by approved_at desc,q.id desc) as approved_rank
  from 
    store_order o join 
    quote_quote q on q.order_id = o.id 
  where 
    q.created_at >= '2018-04-16'
    and approved_at is not null
),
cancelled_projects as 
(
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
orders as
(
    select
                o.id,
                o.product_id,
                o.completed_at at time zone 'America/Los_Angeles' as completed_at,
                fo.submitted_at
        from store_order o
        left join quote_quote qa on qa.id = o.approved_quote_id
        left join feedback_orderfeedback fo on fo.order_id = o.id
        left join cancelled_projects cp on cp.order_id = o.id
        where
                 o.completed_at is not null and o.parent_order_id is null 
                   and cp.order_id is null
                   and qa.approved_at >= '2018-04-16' 
                   and fo.submitted_at is not null --added to avoid errors
                  and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
)
select
        date_trunc('{period}', completed_at at time zone 'America/Los_Angeles')::date as date,
        cast(count(*) filter(where submitted_at is not null) as float)/ cast(count(*) as float) as DEL108
from orders
group by 1
order by 1 desc
