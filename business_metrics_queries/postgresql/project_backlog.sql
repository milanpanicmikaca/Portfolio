-- upload to BQ
with
time_series as
(
select
  date_trunc('day', dd)::date as date,
        date_trunc('{period}', dd)::date as date_array,
  rank() over (partition by date_trunc('{period}', dd)::date order by dd desc) as period_rank
from generate_series ('2018-04-16'::timestamp, current_date, '1 day'::interval) dd
),
first_approved_quote
as
(
select
        so.id,
        min(qq.approved_at) as closed_at
from store_order so
left join quote_quote qq on qq.order_id = so.id
where qq.approved_at is not null
group by 1
),
order_backlog
as
(
select
        so.id,
  case when so.product_id = 105 then 1 else 0 end as is_fence,
  case when so.product_id = 34 then 1 else 0 end as is_driveway,
  completed_at,
        closed_at,
        so.cancelled_at,
        ts.date,
        case when so.parent_order_id is not null then 1 else 0 end as is_warranty_work,
        1 as backlog
from store_order so
left join first_approved_quote faq on faq.id = so.id
left join time_series ts on ts.date >= closed_at and ts.date <= coalesce(completed_at,cancelled_at, current_date)
where so.approved_quote_id is not null
)
select
 date_trunc('{period}',ts.date)::date as date,
 sum(case when is_fence = 1 then backlog else 0 end) as DEL230F,--backlog_fence projects,
 sum(case when is_driveway = 1 then backlog else 0 end) as DEL230D--backlog_driveway projects
from time_series ts
left join order_backlog ob on ob.date = ts.date
where period_rank = 1
group by 1
order by 1 desc;
