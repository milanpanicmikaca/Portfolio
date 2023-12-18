-- upload to BQ
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, {period}) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array desc) as period_rank
  from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
),
completed_orders as
(
  select
    date_trunc(coalesce(completed_at,cancelled_at),{period}) as period,
    sum(case when completed_at is not null then 1 else 0 end) as is_completed, --completed projects
    sum(case when change_order_count>0 then 1 else 0 end) as change_order --at least one change order
  from int_data.order_ue_materialized
  group by 1
  order by 1 desc
)
select
  date_trunc(t.period,{period}) as date,
  change_order/nullif(is_completed,0) as CTN030
from timeseries t
left join completed_orders co using(period)
where t.period_rank = 1
order by 1 desc
