with business_hours as (
  select 2 as weekday, '08:00:00' as start_time, '20:00:00' as end_time union all 
  select 3 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 4 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 5 as weekday, '05:00:00' as start_time, '20:00:00' as end_time union all  
  select 6 as weekday, '05:00:00' as start_time, '17:00:00' as end_time union all  
  select 7 as weekday, '05:00:00' as start_time, '18:00:00' as end_time union all  
  select 1 as weekday, '07:00:00' as start_time, '15:00:00' as end_time 
), json_clean as (
select 
  id, 
  agent_details,
  case when (left(agent_details,1) = "+" or agent_details = 'If-No-Answer Agent') then null else lower(regexp_replace(agent_details, r'\s','.'))||'@ergeon.com' end as email,
  case when event_name = 'abandoned' then 'short_abandoned' else event_name end as event_name,
  ring_group,
  event_time,
  extract(dayofweek from event_time) as dayofweek,
from talkdesk_us.call
) 
  select 
    jc.* except(dayofweek),
    h.full_name,
    h.team_lead,
    h.house,
    h.title,
	case when event_name = 'call_answered' then 1 else 0 end as is_answered,
	case when event_name = 'call_abandoned' then 1 else 0 end as is_abandoned,
	case when event_name = 'short_abandoned' then 1 else 0 end as is_short_abandoned,
	case when event_name like '%missed%' then 1 else 0 end as is_missed,
  case when bh.start_time is null then false else true end as bh,
  case when event_name like '%out%' then true else false end as is_outbound
  from json_clean jc
    left join business_hours bh on time(jc.event_time) >= cast(bh.start_time as time) 
      and time(jc.event_time) < cast(bh.end_time as time) 
      and bh.weekday = jc.dayofweek
    left join int_data.hr_dashboard h on h.email = jc.email
