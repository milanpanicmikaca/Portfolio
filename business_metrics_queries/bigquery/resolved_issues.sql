-- upload to BQ
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
), 
    issues as
(
  select
    i.id,
    datetime(cast(i.created as timestamp), 'America/Los_Angeles') as created_at,
    datetime(cast(i.resolution_date as timestamp), 'America/Los_Angeles') as resolved_at,
    i.priority,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    case 
      when p.key = 'IT' then 'IT'
      when i.summary like 'DATA%' and p.key = 'ENG' then 'Data'
      when i.summary like 'ENHANCEMENT%' and p.key = 'ENG' then 'Enhancement'
      when (lower(i.description) like '%sentry%' or lower(i.source_of_the_report) like '%sentry%') and p.key = 'ENG'  then 'Sentry'
      when (i.summary like 'BUG%' or lower(i.source_of_the_report) like '%user%') and p.key = 'ENG' then 'Bug'
      else 'Other'
    end as type,
    case when (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%') then 1 else 0 end as is_user_reported,
    i.parent_id,
    i.issue_type
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  where
    p.key in ('ENG','IT', 'CM', 'HELP')
    --and (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%')
    and created <= current_datetime() 

),cal_issues as (
select
  date_trunc(extract(date from resolved_at),{period}) as date,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG003,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'Data' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG038,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'IT' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG039,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'Bug' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG040,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'Enhancement' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG041,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'Sentry' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG042,
  coalesce(avg(case when project in ('ENG', 'IT') 
            and issue_type <> 'Story' 
            and type = 'Other' 
            and is_user_reported = 1 
            and resolved_at is not null 
            and parent_id is null then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG043,
  coalesce(avg(case when project = 'ENG' 
            and issue_type = 'Story' 
            and resolved_at is not null 
            then datetime_diff(resolved_at,created_at,day) else null end),0) as ENG007,
  sum(case when project = 'ENG' 
            and issue_type = 'Story' 
            and resolved_at is not null then 1 else 0 end) as ENG006
from issues 
where resolved_at is not null
group by 1 
)
select 
t.day as date,
i.*except(date)
from timeseries t 
left join cal_issues i on i.date = t.day
where period_rank = 1 and t.day > '2018-04-16' 
order by 1 desc;
