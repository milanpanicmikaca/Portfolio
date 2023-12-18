with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array, day) order by date_array desc) as period_rank
    from unnest(generate_date_array('2019-01-01',current_date(), interval 1 day)) as date_array
),
issues as (
select 
date_trunc(day_created,day) as date,
date_trunc(resolved_at,day) as resolved_at,
metric,
type,
project,
is_user_reported,
tat,
status
from (
with
calc_data
as
(
select
    c.Issue_id,
    c.created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where from_string = 'In staging' and p.key in ('ENG', 'IT')
union all
select
    c.Issue_id,
    c.created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where to_string = 'In staging' and p.key in ('ENG', 'IT')
),
calc_time_in_staging as
(
select
    issue_id,
    created,
    to_string as from_string,
    lead(to_string) over (partition by issue_id order by created) as to_string,    
    datetime_diff(lead(created) over (partition by issue_id order by created), created, hour) as tat_staging,
from calc_data
order by issue_id, created
),
tat_per_issue as
(
select
    issue_id,
    coalesce(sum(tat_staging),0) as tat_staging
from calc_time_in_staging
where from_string = 'In staging' 
group by 1
),
assigned_timestamps as
(
with x_date as (
Select 
created as assigned_date, 
to_string as from_string,
issue_id
from (
select 
* from jira.changelog
where field = 'assignee' 
and from_string is null 
and regexp_contains(to_string, 'Ema Pijevcevic|Dan Craig|Nephele Troullinos|Jorgos Tsatalos|Odysseas Tsatalos|Sabina Alistar'))
),
y_date as(
select 
   created as resolved_date,
   to_string,
   issue_id
   from (
select * from jira.changelog
where regexp_contains(from_string, 'Ema Pijevcevic|Dan Craig|Nephele Troullinos|Jorgos Tsatalos|Odysseas Tsatalos|Sabina Alistar'))
)
Select 
     issue_id,
     assigned_date,
     resolved_date,
     from_string,
     to_string
from x_date x left join y_date using(issue_id)
where to_string is not null
and not regexp_contains(to_string, 'Ema Pijevcevic|Dan Craig|Nephele Troullinos|Jorgos Tsatalos|Odysseas Tsatalos|Sabina Alistar')),
calc_time_in_prod as(
select
     issue_id,
     assigned_date,
     resolved_date,    
     datetime_diff(resolved_date,assigned_date,day) as tat,
from assigned_timestamps 
order by issue_id, assigned_date),
tat_per_prod_issue as(
select 
    issue_id,
    coalesce(sum(tat),0) as tat_prod
from calc_time_in_prod
group by 1)
(
select
  'Issues' as metric,
  extract(date from created_at) as day_created,
  resolved_at,
  type,
  priority,
  status,
    datetime_diff(resolved_at,created_at,day) as tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            and (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  complexity
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") as resolved_at,
    i.priority,
    case 
       when p.key = 'IT' then 'IT'
      when i.summary like 'DATA%' and p.key = 'ENG' then 'Data'
      when i.summary like 'ENHANCEMENT%' and p.key = 'ENG' then 'Enhancement'
      when (lower(i.description) like '%sentry%' or lower(i.source_of_the_report) like '%sentry%') and p.key = 'ENG'  then 'Sentry'
      when (i.summary like 'BUG%' or lower(i.source_of_the_report) like '%user%') and p.key = 'ENG' then 'Bug'
      else 'Other'
    end as type,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
 coalesce(complexity,'Medium') as complexity
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join tat_per_issue ti on ti.issue_id = i.id
  where
    p.key in ('ENG','IT', 'CM', 'HELP')
          and i.parent_id is null
      and i.issue_type <> 'Story'
    --and (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%')
) as k
where 
  created_at <= current_datetime()
order by 3 desc)
union all 
(
select
  'Features' as metric,
  extract(date from created_at) as day_created,
  resolved_at,
  type,
  priority,
  status,
    datetime_diff(resolved_at,created_at,day) as tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            and (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  complexity
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") as resolved_at,
    i.priority,
    case 
      when p.key = 'IT' then 'IT'
      when i.summary like 'DATA%' and p.key = 'ENG' then 'Data'
      when i.summary like 'ENHANCEMENT%' and p.key = 'ENG' then 'Enhancement'
      when (lower(i.description) like '%sentry%' or lower(i.source_of_the_report) like '%sentry%') and p.key = 'ENG'  then 'Sentry'
      when (i.summary like 'BUG%' or lower(i.source_of_the_report) like '%user%') and p.key = 'ENG' then 'Bug'
      else 'Other'
    end as type,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    coalesce(complexity,'Medium') as complexity
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join tat_per_issue ti on ti.issue_id = i.id
  where
    p.key in ('ENG','IT')
          and i.parent_id is null
    and (i.issue_type = 'Story')
) as k
where 
  created_at <= current_datetime()
order by 3 desc
)
union all
(
select
  'Features subtasks' as metric,
  extract(date from created_at) as day_created,
  resolved_at,
  type,
  priority,
  status,
    datetime_diff(resolved_at,created_at,day) as tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            and (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  complexity 
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") as resolved_at,
    i.priority,
    case
      when p.key = 'IT' then 'IT'
      when i.summary like 'DATA%' and p.key = 'ENG' then 'Data'
      when i.summary like 'ENHANCEMENT%' and p.key = 'ENG' then 'Enhancement'
      when (lower(i.description) like '%sentry%' or lower(i.source_of_the_report) like '%sentry%') and p.key = 'ENG'  then 'Sentry'
      when (i.summary like 'BUG%' or lower(i.source_of_the_report) like '%user%') and p.key = 'ENG' then 'Bug'
      else 'Other'
    end as type,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    coalesce(i.complexity,'Medium') as complexity
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join jira.issue io on io.id = i.parent_id
  left join tat_per_issue ti on ti.issue_id = i.id
  where
    p.key in ('ENG','IT','CM')
          and (io.issue_type = 'Story')
    and i.story_points is not null
) as k
where 
  created_at <= current_datetime()
order by 3 desc
)
union all 
(
  select
  'Product Issues' as metric,
  extract(date from assigned_at) as day_created,
  resolved_at,
  case 
    when lower(summary) like ('%enhancement%') then 'Enhancement' 
    when lower(summary) like ('%bug%') then 'Bug'
    else 'other'
   end as type,
  priority,
  status,
  tat_prod as tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            and (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  complexity 
from 
(
select
    i.id,
    datetime(cast(a.assigned_date as timestamp), "America/Los_Angeles") as assigned_at,
    datetime(cast(a.resolved_date as timestamp), "America/Los_Angeles") as resolved_at,
    i.priority,
    i.summary,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    coalesce(i.complexity,'Medium') as complexity,
    tp.tat_prod
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join assigned_timestamps a on a.issue_id = i.id
  left join tat_per_issue ti on ti.issue_id = i.id
  left join tat_per_prod_issue tp on tp.issue_id = i.id
  where
    p.key in ('ENG', 'PROD')
    and (lower(summary) like '%enhancement%' or lower(summary) like '%bug%')
    and issue_type in ('Bug','Task')   
    and i.parent_id is null
    ) as k
where 
  assigned_at <= current_datetime()
order by 3 desc))sub
where is_user_reported = true and metric = 'Issues' 
/*),issues_reported as  (
select
 date_trunc(t.day, {period}) as date,
    countif(issues.type = 'Data'and issues.project = 'ENG') as ENG124,
    countif(issues.type = 'IT') as ENG125,
    countif(issues.type = 'Bug'and issues.project = 'ENG') as ENG126,
    countif(issues.type = 'Enhancement'and issues.project = 'ENG') as ENG127,
    countif(issues.type = 'Sentry'and issues.project = 'ENG') as ENG128,
    countif(issues.type = 'Other'and issues. project = 'ENG') as ENG129,

    coalesce(avg(case when issues.type = 'Data' and issues.project = 'ENG' then issues.tat else null end),0) as ENG130,
    coalesce(avg(case when issues.type = 'IT'then issues.tat else null end),0) as ENG131,
    coalesce(avg(case when issues.type = 'Bug' and issues.project = 'ENG' then issues.tat else null end),0) as ENG132,
    coalesce(avg(case when issues.type = 'Enhancement'and issues.project = 'ENG' then issues.tat else null end),0) as ENG133,
    coalesce(avg(case when issues.type = 'Sentry' and issues. project = 'ENG' then issues.tat else null end),0) as ENG134,
    coalesce(avg(case when issues.type = 'Other' and issues.project = 'ENG' then issues.tat else null end),0) as ENG135
 from timeseries t 
left join issues on issues.date = t.day
where period_rank = 1
group by 1
order by 1 desc*/),
issues_resolved as (
    select
     date_trunc(t.day, {period}) as date,
     countif(isres.project = 'ENG') as ENG002,
    countif(isres.type = 'Data'and isres.project = 'ENG' ) as ENG124,
    countif(isres.type = 'IT') as ENG125,
    countif(isres.type = 'Bug'and isres.project = 'ENG') as ENG126,
    countif(isres.type = 'Enhancement'and isres.project = 'ENG' ) as ENG127,
    countif(isres.type = 'Sentry'and isres.project = 'ENG')as ENG128,
    countif(isres.type = 'Other'and isres.project = 'ENG') as ENG129
 from timeseries t 
left join issues isres on isres.resolved_at= t.day
where period_rank = 1
group by 1
order by 1 desc
)
select 
issues_resolved.*  
 from issues_resolved 
order by 1 desc
