with deal_info as (
  select 
    d.id as deal_id,
    date(cast(d.add_time as timestamp), "America/Los_Angeles") as created_at,
    d.owner_name,
    d.title,
    p.name as person_name,  
    d.status,
    s.name as stage,
    d.lost_reason,
    d.activities_count,
    d.done_activities_count,
    d.undone_activities_count,
    date(cast(d.lost_time as timestamp), "America/Los_Angeles") as lost_at,
  from pipedrive.deal d
    left join pipedrive.person p on p.id = d.person_id
    left join pipedrive.stage s on s.id = d.stage_id
    left join pipedrive.pipeline pl on pl.id = s.pipeline_id
  where pl.name = 'FAM'
), activity_info as (
  select 
    a.deal_id,
    sum(case when subject like '%Call%' and subject not like '%Zap%' then 1 end) as total_call,
    sum(case when subject like 'TD Outbound Call' then 1 end) as outbound_call,
    sum(case when subject like 'TD Inbound Call' then 1 end) as inbound_call,
    sum(case when subject like '%SMS%' then 1 end) as sms,
  from pipedrive.activity a
    left join pipedrive.user u on u.id = a.user_id
    left join int_data.hr_dashboard h on h.email = u.email
  where h.title like '%Field Account Manager%'
  group by 1

)
select 
  *
from deal_info d
left join activity_info using(deal_id)