with last_approved_quotes as 
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
all_orders_customers as
(
  select
    so.id as order_id,
    so.completed_at,
    qa.customer_id,
    count(qa.id) as cnt_quotes
  from store_order so
  join quote_quote qq on qq.order_id = so.id
  join quote_quoteapproval qa on qa.quote_id = qq.id
  left join cancelled_projects cp on cp.order_id = so.id
  where 
    so.completed_at is not null
  and 
  	so.parent_order_id is null  --wwo excluded
  and
    qq.deleted_at is null
   and cp.order_id is null
  group by 1,2,3
),
first_billed_at as 
(
  select
    customer_id,
    min(completed_at) as first_billed_at
  from all_orders_customers
  group by 1
),
total_calc as 
(
  select 
    c.*,
    f.first_billed_at,
    case when c.completed_at > f.first_billed_at then 1 else 0 end as repeat_order
  from all_orders_customers c
  left join first_billed_at f on f.customer_id = c.customer_id
)
select 
  date_trunc('{period}', completed_at at time zone 'America/Los_Angeles')::date as date,
  count(customer_id) as DEL387,
  sum(case when repeat_order = 0 then 1 else 0 end) as DEL388,
  cast(sum(repeat_order) as decimal(7,2))/nullif(count(customer_id),0) as DEL389
from total_calc
group by 1
order by 1 desc
