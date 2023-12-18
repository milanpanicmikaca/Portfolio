-- upload to BQ
with
calc_series as
(
 select
        generate_series('2018-04-15', current_date, '1 day')::date as day 
),
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
           (o.completed_at at time zone 'America/Los_Angeles')::date as completed_at
        from store_order o
        left join quote_quote qa on qa.id = o.approved_quote_id
        left join cancelled_projects cp on cp.order_id = o.id
        where
                   qa.approved_at >= '2018-04-16'
                   and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
                   and o.completed_at < now() - interval '14 days'
                   and o.completed_at is not null
                   and cp.order_id is null
),
customer_payments as
(
select
        order_id,
        sum(case when at2.name in ('Customer Billed','Customer Discounts','Warranty (customer discounts)') then amount else 0 end ) as customer_billed,
        sum(case when at2.name = 'Customer Paid' then amount else 0 end) as customer_payment
from accounting_transaction at
left join accounting_transactiontype at2 on at.type_id = at2.id
and at.deleted_at is null
group by order_id
),
balance as
(
select
        o.completed_at,
        o.id,
        customer_billed,
        cp.customer_payment,
        cp.customer_billed - cp.customer_payment as balance
from orders o
left join customer_payments cp on o.id = cp.order_id
where cp.customer_billed - cp.customer_payment > 0
and cp.customer_billed > 0 
),
final_data
as
(
select
        date_trunc('{period}',day)::date as date,
        sum(coalesce(balance,0)) over (order by day) as balance
from calc_series cs
left join balance b on cs.day = b.completed_at
)
select
        date,
        max(balance) as DEL144 
from final_data
group by 1
order by 1
