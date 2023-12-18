-- upload to BQ
with
time_series as(
select 
    date_trunc(date_array,{period}) as date,
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as date_array
),
-- new quote requests being requested on each period, counts issues
new_quote_requests as (
select
  date_trunc(extract( date from datetime(cast(i.created as timestamp), "America/Los_Angeles")),{period}) as date,
  count(*) as new_quote_requests,
  countif(lower(product) like '%fence%') as fence_sum_new_quote_requests,
  countif(lower(product) like '%driveway%') as driveway_sum_new_quote_requests,
  countif(lower(product) like '%landscaping%') as turf_sum_new_quote_requests
from jira.issue  i
left join jira.user u on u.id = i.assignee_id
left join jira.project p on p.id = i.project_id
where
  i.issue_type = 'Task'
  and lower(i.summary) not like '%test%'
  and (status not in ('Open') or issue_type not in ('scoping.task'))
  and p.key = 'EST'
group by 1
order by 1 desc
),
-- counts quotes completed by checking changelog for to_string = 'completed'
calc_completed_quotes as (
  select
    issue_id,
    date_trunc(extract( date from datetime(cast(c.created as timestamp), "America/Los_Angeles")),{period}) as completion_date,
    -- if its the first completion for this issue_id then it is a new, otherwise requote
    case when rank() over (partition by issue_id order by c.created) = 1 then 'new' else 'requote' end as completion_type,
    i.product,
    safe_cast(substr(i.admin_link,38,5) as INT64) as order_id
  from jira.changelog c
  left join jira.issue i on c.issue_id = i.id
  left join jira.project p on p.id = i.project_id
  where
    c.field = 'status'
    and to_string = 'Completed'
    and p.key = 'EST'
    and issue_type = 'Task' --only Tasks need to be counted
),
completed_quotes as(
select
completion_date,
count(*) as sum_total_completed,
sum(case when completion_type = 'new' then 1 else 0 end)     as orders_completed_quotes,
sum(case when completion_type = 'requote' then 1 else 0 end) as requotes,
countif(lower(product) like '%fence%') as fence_sum_total_completed,
countif(lower(product) like '%driveway%') as driveway_sum_total_completed,
countif(lower(product) like '%landscaping%') as turf_sum_total_completed,
sum(case when lower(product) like '%fence%' and completion_type = 'new' then 1 else 0 end)  as fence_orders_completed_quotes,
sum(case when lower(product) like '%landscaping%' and completion_type = 'new' then 1 else 0 end)  as turf_orders_completed_quotes,
sum(case when lower(product) like '%driveway%' and completion_type = 'new' then 1 else 0 end)  as driveway_orders_completed_quotes,
sum(case when lower(product) like '%fence%' and completion_type = 'requote' then 1 else 0 end) as fence_requotes,
sum(case when lower(product) like '%driveway%' and completion_type = 'requote' then 1 else 0 end) as driveway_requotes,
sum(case when lower(product) like '%landscaping%' and completion_type = 'requote' then 1 else 0 end) as turf_requotes
from calc_completed_quotes
group by 1
order by 1 desc
)
select
t.date,
coalesce(nr.new_quote_requests,0) as CTN012,
coalesce(fence_sum_new_quote_requests,0) as CTN012F,
coalesce(turf_sum_new_quote_requests,0) as CTN012T,
coalesce(driveway_sum_new_quote_requests,0) AS CTN012D,
coalesce(sum_total_completed,0) as CTN013 ,
coalesce(fence_sum_total_completed,0) as CTN013F,
coalesce(turf_sum_total_completed,0) as CTN013T,
coalesce(driveway_sum_total_completed,0) as CTN013D,
coalesce(orders_completed_quotes,0) as CTN014,
coalesce(fence_orders_completed_quotes,0) as CTN014F,
coalesce(turf_orders_completed_quotes,0) as CTN014T,
coalesce(driveway_orders_completed_quotes,0) as CTN014D,
coalesce(requotes,0) as CTN015,
coalesce(fence_requotes,0) as CTN015F,
coalesce(turf_requotes,0) as CTN015T,
coalesce(driveway_requotes,0) as CTN015D
from time_series t
left join completed_quotes c on c.completion_date = t.date
left join new_quote_requests nr on nr.date = t.date
where period_rank = 1
order by 1 desc
