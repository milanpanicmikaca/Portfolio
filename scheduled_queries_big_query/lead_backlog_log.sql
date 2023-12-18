select
 *
from
(
select
  cast(current_datetime('America/Los_Angeles') as datetime) as backlog_datetime,
  0 as product_id,
  name as queue_name,
  count(id) as leads_backlog,
  max(time_in_queue) as longest_lead_wait_time,
  avg(time_in_queue) as avg_lead_wait_time
from
(
  select
    so.id,
    --datetime_diff(current_datetime('America/Los_Angeles'),cast(timestamp_trunc(so.created_at, second,"America/Los_Angeles") as datetime), hour) as time_in_queue,
    datetime_diff(current_datetime('America/Los_Angeles'),datetime(so.created_at,'America/Los_Angeles'), hour) as time_in_queue,
    so.product_id,
    sa.name
  from ergeon.store_order so
  left join `bigquerydatabase-270315.ergeon.sales_schedule_assignmenttype` sa on sa.id = so.assignment_type_id
  where so.sales_rep_id is null 
  and so.parent_order_id is null
  and so.assignment_type_id is not null
  and so.deal_status_id != 9
  --and created_at > '2021-05-01'
order by 
  so.created_at asc
) as k
group by 2,3    
)
union all
(
select
    cast(current_datetime('UTC') as datetime) as backlog_datetime,
    product_id,
    'general' as queue_name,
    count(id) as leads_backlog,
    max(time_in_queue) as longest_lead_wait_time,
    avg(time_in_queue) as avg_lead_wait_time
from
(
  select
    so.id,cast(so.created_at as datetime),current_datetime('America/Los_Angeles'),
    datetime(so.created_at, 'America/Los_Angeles'),
    datetime_diff(current_datetime('America/Los_Angeles'),datetime(so.created_at, 'America/Los_Angeles'), hour) as time_in_queue,
    so.product_id
  from ergeon.store_order so
  where so.sales_rep_id is null 
  and so.parent_order_id is null
  and so.assignment_type_id is not null
  and so.deal_status_id != 9
  --and created_at > '2021-05-01'
order by 
  so.created_at asc
) as k
group by 2
)