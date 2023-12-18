with
-- generate timeseries
calc_timeseries as
(
  with
  calc_dates as
  ( 
    select 
    date_array as my_date,
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank --change period to group by date
    from unnest(generate_date_array('2018-04-16',current_date('America/Los_Angeles'), interval 1 day)) as date_array
  )
    select
      cast(concat(my_date,' 00:00:00') as datetime) as start_time,
      case
        when my_date = current_date('America/Los_Angeles') then current_datetime('America/Los_Angeles')
        else cast(concat(my_date,' 23:59:59') as datetime)
      end as end_time,
    period_rank
    from calc_dates
),
calc_canceled_issues as
(
  -- Open to canceled before end of day
  select
    i.id,
    i.product,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as moved_into_canceled_at
  from jira.changelog c
  left join jira.issue i on i.id = c.issue_id
  left join jira.project p on p.id = i.project_id
  where
    c.to_string = 'Canceled'
    and  i.issue_type = 'Task'
    and lower(i.summary) not like '%test%'
),
time_in_canceled_issues
as
(
  select
    c.issue_id,
    i.product,
    c.from_string,
    c.to_string,
    cci.moved_into_canceled_at,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as modified_at,
    ct.start_time,
    ct.period_rank,
    datetime_diff(case when ct.end_time > cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) then cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) else ct.end_time end,
                    cast(cci.moved_into_canceled_at /*moved to canceled*/ as datetime), hour) as time_in_canceled
  from jira.changelog c
  left join jira.issue i on i.id = c.issue_id
  left join calc_canceled_issues cci on cci.id = c.issue_id
  join calc_timeseries ct on ct.end_time > cast(cci.moved_into_canceled_at as datetime) and cast(ct.end_time as date) <= cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as date)
  left join jira.project p on p.id = i.project_id
  where
    c.from_string = 'Canceled'
    and c.to_string = 'Open'
    and i.issue_type = 'Task'
    and cci.id is not null
    and p.key = 'EST'
    and lower(i.summary) not like '%test%'
   order by start_time
),
calc_incomplete_issues as
(
  -- Open to Incomplete before end of day
  select
    i.id,
    i.product,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as moved_into_incomplete_at
  from jira.changelog c
  left join jira.issue i on i.id = c.issue_id
  left join jira.project p on p.id = i.project_id
  where
    c.from_string = 'Open'
    and c.to_string = 'Incomplete'
    and i.issue_type = 'Task'
    and lower(i.summary) not like '%test%'
),
time_in_incomplete_issues
as
(
  select
    c.issue_id,
    i.product,
    c.from_string,
    c.to_string,
    cci.moved_into_incomplete_at,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as modified_at,
    ct.start_time,
    ct.period_rank,
    datetime_diff(case when ct.end_time > cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) then cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) else ct.end_time end,
                    cast(cci.moved_into_incomplete_at /*moved to canceled*/ as datetime), hour) as time_in_incomplete
  from jira.changelog c
  left join jira.issue i on i.id = c.issue_id
  left join calc_incomplete_issues cci on cci.id = c.issue_id  and cci.moved_into_incomplete_at < datetime(cast(c.created as timestamp), "America/Los_Angeles")
  join calc_timeseries ct on ct.end_time > cast(cci.moved_into_incomplete_at as datetime) and cast(ct.end_time as date) <= cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as date)
  left join jira.project p on p.id = i.project_id
  where
    c.from_string = 'Incomplete'
    and c.to_string = 'Open'
    and i.issue_type = 'Task'
    and cci.id is not null
    and p.key = 'EST'
    and lower(i.summary) not like '%test%'
   order by start_time
),
last_open_per_issue as
(
select
    issue_id,
    max(id) as changelog_id
from jira.changelog
where from_string = 'Open'
group by 1
),first_completed_per_issue as
(
  select 
  issue_id,
    max(created) as first_completed_at
from jira.changelog
where to_string = 'Completed'
group by 1),
  calc_open_issues as
  (
  --Calculating for each day in timeseries how much time has issue been in 'Open'
  select
    c.issue_id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created,
    i.product,
    safe_cast(REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as INT64) as order_id,
    ct.start_time,
    ct.end_time,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as modified_at,
    -- if change has not happened before end of timeseries day then use ct.end_time otherwise use changelog time
    datetime_diff(case when ct.end_time < datetime(cast(c.created as timestamp), "America/Los_Angeles") then ct.end_time else datetime(cast(c.created as timestamp), "America/Los_Angeles") end,
                cast(datetime(cast(i.created as timestamp), "America/Los_Angeles") as datetime), hour) as time_in_queue,
    -- if last day being open it isn't counted as in backlog            
    case when date_trunc(datetime(cast(c.created as timestamp), "America/Los_Angeles"), day) = ct.start_time then 0 else 1 end as backlog,
    tc.time_in_canceled,
    ti.time_in_incomplete,
    ct.period_rank,
    c.from_string,
    c.to_string
  from last_open_per_issue lo
  left join jira.changelog c on c.id = lo.changelog_id 
  left join jira.issue i on i.id = c.issue_id
  join calc_timeseries ct on cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) > ct.start_time 
  and cast(datetime(cast(i.created as timestamp), "America/Los_Angeles") as datetime) < ct.end_time
  --and cast(datetime(cast(c.created as timestamp), "America/Los_Angeles") as datetime) > ct.end_time 
  left join time_in_canceled_issues tc on tc.issue_id = c.issue_id and tc.start_time = ct.start_time
  left join time_in_incomplete_issues ti on ti.issue_id = c.issue_id and ti.start_time = ct.start_time
  left join jira.project p on p.id = i.project_id
  left join first_completed_per_issue fc on fc.issue_id = tc.issue_id 
  where
    c.from_string = 'Open'
    and i.issue_type = 'Task'
    and lower(i.summary) not like '%test%' 
    --and c.to_string not in ('Incomplete', 'Canceled')
    and p.key = 'EST'
    and 
     (case when 
     fc.first_completed_at is not null then fc.first_completed_at < c.created
     when fc.first_completed_at is null then true end)  
    union all
 --add open issues that are not yet available in changelog (created -->> end of day -->> in progress)
  select
    i.id as issue_id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created,
    i.product,
  case when safe_cast(REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as INT64) is null then 
  safe_cast(REGEXP_REPLACE(left(admin_link,53),'[^0-9 ]','') as INT64)  
  else safe_cast(REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as INT64) end as order_id,
    ct.start_time,
    ct.end_time,
    cast(current_datetime('America/Los_Angeles') as datetime) as modified_at,
    datetime_diff(case when ct.end_time > current_datetime("America/Los_Angeles") then current_datetime('America/Los_Angeles') else ct.end_time end,
                cast(datetime(cast(i.created as timestamp), "America/Los_Angeles") as datetime), hour) as time_in_queue,
    1 as backlog,
    tc.time_in_canceled,
    ti.time_in_incomplete,
    ct.period_rank,
    i.status as from_string,
    null as to_string,
  from jira.issue i
  join calc_timeseries ct on cast(datetime(cast(i.created as timestamp), "America/Los_Angeles") as datetime) < ct.end_time and current_datetime('America/Los_Angeles') > ct.start_time 
  left join time_in_canceled_issues tc on tc.issue_id = i.id and tc.start_time = ct.start_time
  left join time_in_incomplete_issues ti on ti.issue_id = i.id and ti.start_time = ct.start_time
  left join jira.project p on p.id = i.project_id
  where
    i.status = 'Open'
    and i.issue_type = 'Task'
        and lower(i.summary) not like '%test%'
    and p.key = 'EST'
    and datetime(cast(i.created as timestamp), "America/Los_Angeles") > '2021-05-21' 
 ),
final_calc
as
(
select
    co.*,
    abs(time_in_queue - coalesce(time_in_canceled,0)- coalesce(time_in_incomplete, 0)) as realtime_in_queue
from calc_open_issues co
left join ergeon.store_order so on so.id = co.order_id
left join ergeon.core_house ch on ch.id = so.house_id
left join ergeon.customers_customer cc on cc.id = ch.customer_id
where --cc.is_commercial is false  and
 --co.order_id is not null and
-- filtering this order - 47567 because it skews the data for the longest waiting time to values greater than 12K Seconds
 issue_id not in (47567,217953,226622) -- filtering order - 217953,226622 since they skew the data and also shows it was in queue for over 707 minutes but was also cancelled for about 600 minutes
order by start_time, modified_at
)
,calc_eod_metrics as
(
select
  date_trunc(cast(start_time as date),day) as date,
  sum(backlog) as CTN011, --backlog_eod
  sum(case when lower(product) like '%fence%' then backlog else 0 end) as CTN011F, --fence_backlog_eod,
  sum(case when lower(product) like '%landscaping%' then backlog else 0 end) as CTN011T, --turf_backlog_eod,
  sum(case when lower(product) like '%driveway%' then backlog else 0 end) as CTN011D,   -- driveway_backlog_eod,
  --max(case when backlog = 1 then realtime_in_queue else 0 end) as CTN016, --longest_quote_wait_time_eod
  max(case when backlog = 1 AND lower(product) like '%fence%' then realtime_in_queue else 0 end) as CTN016F,  --fence_longest_quote_wait_time_eod,
  max(case when backlog = 1 AND lower(product) like '%landscaping%' then realtime_in_queue else 0 end) as CTN016T,   --turf_longest_quote_wait_time_eod,
  max(case when backlog = 1 AND lower(product) like '%driveway%' then realtime_in_queue else 0 end) as CTN016D,   --driveway_longest_quote_wait_time_eod,
  max( 
    case when
      case when backlog = 1 AND lower(product) like '%driveway%' then realtime_in_queue else 0 end >
      case when backlog = 1 AND lower(product) like '%fence%' then realtime_in_queue else 0 end 
    then
      case when backlog = 1 AND lower(product) like '%driveway%' then realtime_in_queue else 0 end 
    when
      case when backlog = 1 AND lower(product) like '%landscaping%' then realtime_in_queue else 0 end >
      case when backlog = 1 AND lower(product) like '%fence%' then realtime_in_queue else 0 end 
    then
      case when backlog = 1 AND lower(product) like '%landscaping%' then realtime_in_queue else 0 end
    else 
      case when backlog = 1 AND lower(product) like '%fence%' then realtime_in_queue else 0 end
    end) as CTN016,
  max(realtime_in_queue) as CTN017,
  avg(case when backlog = 1 then realtime_in_queue else null end) as CTN018
from final_calc
group by 1
order by 1 desc
)
select  
 date_trunc(cast(start_time as date), {period}) as date, --change period to group by date
    cem.* except (date),
from calc_timeseries ct  
left join calc_eod_metrics cem  on cem.date = date_trunc(cast(ct.start_time as date), day)
where period_rank = 1 
order by 1 desc
