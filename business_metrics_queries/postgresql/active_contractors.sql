-- upload to BQ
with
time_series as
(
select
  date_trunc('{period}', dd)::date as date,
        date_trunc('day', dd)::date as date_array,
  rank() over (partition by date_trunc('{period}', dd)::date order by dd desc) as period_rank
from generate_series ('2018-04-18'::date, current_date, '1 day'::interval) dd
),contractors_started as (
select
  coalesce(sum(case when start_date notnull then 1 else 0 end),0) as is_started ,
  date_trunc('{period}',start_date::date) as date
from contractor_contractor
  where deleted_at isnull
group by 2
),contractors_ended as (
select
  sum(case when end_date notnull then 1 else 0 end) as is_ended,
  date_trunc('{period}',end_date::date) as date
from contractor_contractor
where deleted_at isnull and end_date notnull
group by 2
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
active_installers as (
  select
      date_trunc('day',day) as date,
      count(distinct(full_name)) as active_installers
  from
    (select
        co.order_id,
        date_trunc('{period}',so.completed_at) as day,
        cu.full_name
    from store_order so
        left join contractor_contractororder co on co.order_id = so.id
        left join contractor_contractor hc on hc.id = co.contractor_id
        left join contractor_contractorcontact cc on cc.id = hc.contact_id 
        left join core_user cu on cu.id = cc.user_id
        left join cancelled_projects cp on cp.order_id = so.id
    where so.completed_at is not null and cp.order_id is null
  )sub
group by 1
  )
select
  ts.date as date,
  coalesce(is_started,0) as DEL239,    --contractors_started,
  coalesce(is_ended,0) as DEL240,   --contractors_ended,
  coalesce(active_installers,0) as DEL241  --active_contractors
from time_series ts
left join contractors_started cs on
  cs.date = ts.date
left join contractors_ended ce on
  ce.date = ts.date
left join active_installers on
  active_installers.date = ts.date
where period_rank = 1;
