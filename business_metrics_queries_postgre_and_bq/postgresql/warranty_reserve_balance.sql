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
           o.product_id,
           (o.completed_at at time zone 'America/Los_Angeles')::date as completed_at,
           o.total_project_price
        from store_order o
        left join quote_quote qa on qa.id = o.approved_quote_id
        left join cancelled_projects cp on cp.order_id = o.id
        where
                 o.completed_at is not null
                  and cp.order_id is null 
                  and qa.approved_at >= '2018-04-16'
                  and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
)
,
warranty_reserve
as 
(
        select
                k.day, 
                max(k.warranty_reserve) as warranty_reserve 
        from 
        (select
                cc.day,
                coalesce(round(sum(total_project_price) over (order by cc.day) * 0.01, 2),0) as warranty_reserve
                --total_price
        from calc_series cc
        left join orders o on cc.day = o.completed_at) as k
        group by 1
),
transactions as
(
select
        at."date",
        amount
from accounting_transaction at
left join accounting_account aa on aa.id = at.account_id 
left join accounting_transactiontype at2 on at2.id = at.type_id
where at2."name" like '%Warranty%'),
warranty_expenditure
as 
(
select
        k.day,
        max(k.warranty_expenditure) as warranty_expenditure
from
(        select
        day,
        coalesce(sum(amount) over (order by day),0) as warranty_expenditure
        from calc_series cs
        left join transactions t on t."date" = cs.day) as k
        group by 1
),
calc_data
as
(
select
        date_trunc('{period}', ws.day)::date as date,
        warranty_reserve - warranty_expenditure as PRO122,
        ws.day,
        rank() over (partition by date_trunc('{period}', ws.day)::date order by ws.day desc) as ranked_days
from warranty_reserve ws
inner join warranty_expenditure we on ws.day = we.day
order by 1 desc
)
select
        date,
        PRO122
from calc_data
where ranked_days = 1
order by date desc
