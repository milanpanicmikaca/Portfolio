with business_hours as (
  select 2 as weekday, '08:00:00' as start_time, '20:00:00' as end_time union all 
  select 3 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 4 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 5 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 6 as weekday, '05:00:00' as start_time, '17:00:00' as end_time union all  
  select 7 as weekday, '05:00:00' as start_time, '18:00:00' as end_time union all  
  select 1 as weekday, '07:00:00' as start_time, '15:00:00' as end_time 
), pd_calls as (
  select
    *,
    datetime(cast(add_time as timestamp), "America/Los_Angeles") as add_time_la,
    extract(dayofweek from datetime(cast(add_time as timestamp), "America/Los_Angeles")) as weekday,
    extract(time from datetime(cast(add_time as timestamp), "America/Los_Angeles")) as time
  from pipedrive.activity
  where subject in ('TD Inbound Call','TD Abandoned Call','TD Missed Call','TD Voicemail Left')
    and (note like '%inside sales%' or note like "%[Tags: csr]%")
), old_info as ( --information taken from PD, this is limited to 2022/07/27 and before
select
  pd.*,
  case when subject in ('TD Abandoned Call','TD Missed Call','TD Voicemail Left') then 1 else 0 end as missed_calls,
  case when bh.start_time is null then 0 else 1 end as is_business_hours
from pd_calls pd
left join business_hours bh on bh.weekday = pd.weekday and cast(bh.start_time as time) <= pd.time and cast(bh.end_time as time) >= pd.time
), td_calls as (
  select 
    date_trunc(date(cast(event_time as timestamp), 'America/Los_Angeles'),{period}) as date,
    count(*) as total_calls,
    sum(case when event_name = 'call_answered' then 1 else 0 end) as is_answered,
    sum(case when event_name = 'call_abandoned' then 1 else 0 end) as is_abandoned,
    sum(case when event_name = 'abandoned' then 1 else 0 end) as is_short_abandoned,
    sum(case when event_name like '%missed%' then 1 else 0 end) as is_missed,
    --case when bh.start_time is null then false else true end as bh,
  from talkdesk_us.call jc
    left join business_hours bh on time(jc.event_time) >= cast(bh.start_time as time) 
      and time(jc.event_time) < cast(bh.end_time as time) 
      and bh.weekday = extract(dayofweek from event_time)
  where bh.start_time is not null
    and ring_group = 'csr'
  group by 1
)
select
date,
is_answered/nullif((total_calls-is_short_abandoned),0) as MDR111
from td_calls
where date >= '2022-07-27'

UNION ALL

select 
date_trunc(extract(date from add_time_la),{period}) as date,
1-(sum(missed_calls)/nullif(count(*),0)) as MDR111
from old_info 
where add_time_la < '2022-07-27'
group by 1
order by 1 desc

