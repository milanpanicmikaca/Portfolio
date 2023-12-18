with
calc_data
as
(
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where from_string = 'In staging' and p.key in ('ENG', 'IT')
union all
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
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
    datetime_diff(lead(created) over (partition by issue_id order by created), created, hour) as tat_staging
from calc_data
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
calc_data_review
as
(
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where from_string = 'In review' and p.key in ('ENG', 'IT')
union all
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where to_string = 'In review' and p.key in ('ENG', 'IT')
),
calc_time_in_review as
(
select
    issue_id,
    created,
    to_string as from_string,
    lead(to_string) over (partition by issue_id order by created) as to_string,    
    datetime_diff(lead(created) over (partition by issue_id order by created), created, day) as tat_review
from calc_data_review
),tat_per_issue_review as (
select
    issue_id,
    coalesce(sum(tat_review),0) as tat_review
from calc_time_in_review
where from_string = 'In review' 
group by 1
),
calc_data_in_progress
as
(
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where from_string = 'In Progress' and p.key in ('ENG', 'IT')
union all
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where to_string = 'In Progress' and p.key in ('ENG', 'IT')
),
calc_time_in_progress as
(
select
    issue_id,
    created,
    to_string as from_string,
    lead(to_string) over (partition by issue_id order by created) as to_string,    
    datetime_diff(lead(created) over (partition by issue_id order by created), created, day) as tat_progress
from calc_data_in_progress
),
 tat_progress_per_issue as
 (
select
    issue_id,
    coalesce(sum(tat_progress),0) as tat_progress
from calc_time_in_progress
where from_string = 'In Progress' 
group by 1
),
story_points as 
(
with story_point_changelog as 
(
select 
issue_id,
datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
rank() over(partition by issue_id order by c.created) as rank_first,
rank() over(partition by issue_id order by c.created desc) as rank_last,
from_string,
to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where field = 'Story Points'
and i.issue_type = 'Story'
)
select
    i.id,
    datetime(cast(resolution_date as timestamp), "America/Los_Angeles") as resolution_date,
    spf.to_string as initial_estimate,
    case when i.resolution_date is not null then spl.to_string else null end as final_estimate
from jira.issue i
left join story_point_changelog spf on spf.issue_id = i.id and spf.rank_first = 1
left join story_point_changelog spl on spl.issue_id = i.id and spl.rank_last = 1
where i.issue_type = 'Story'
),triage_log as 
(
      select
        issue_id,
        created
      from
      (
        select  
          c.*,
          rank() over (partition by issue_id order by created) as rank
      from jira.changelog c
      where 
        from_string = 'Unconfirmed'
      ) as k
    where 
      rank = 1
    ),time_triage as (
    select
      i.id,
      i.key as issue_key,
      coalesce(datetime(cast(t.created as timestamp), "America/Los_Angeles"),current_datetime("America/Los_Angeles")) as time_changed_from_triage,
      date_diff(datetime(cast(t.created as timestamp), "America/Los_Angeles"),datetime(cast(i.created as timestamp), "America/Los_Angeles"),hour) as time_in_triage
    from jira.issue i
    left join jira.project p on p.id = i.project_id
    left join triage_log t on t.issue_id = i.id
),
calc_data_authored_prds
as
(
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where from_string = 'Authored' and p.key in ('PROD')
union all
select
    c.Issue_id,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as created,
    c.from_string,
    c.to_string
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join jira.project p on p.id = i.project_id
where to_string = 'Authored' and p.key in ('PROD')
),
calc_authored as
(
select
    issue_id,
    created,
    to_string as from_string,
    lead(to_string) over (partition by issue_id order by created) as to_string
from calc_data_authored_prds
),
cte as (
select
    issue_id
from calc_authored
where from_string = 'Authored'
),final_cte as (
select 
    issue_id,
    count(lag_count) duedate_changed
from(
select 
    c.*,
    case when lead(to_id) over (partition by c.issue_id order by c.created asc) is not null then lead(to_id) over (partition by c.issue_id order by c.created asc) else null end as lag_count
from jira.changelog c 
where  lower(c.field) like '%duedate%'
)
group by 1
),
final_authored_prd as (
select 
    distinct io.id as eng_issue_id,
    --cte.issue_id,
    duedate_changed
from cte  
left join final_cte ft on ft.issue_id = cte.issue_id
left join jira.issue i on i.id = ft.issue_id  
left join jira.issue io on io.summary = i.summary
where duedate_changed is not null
),
issue_cte as (
select 
metric,
day_created,
week_created,
month_created,
quarter_created,
year_created,
case when day_resolved <'2018-01-01' then null else day_resolved end as day_resolved,
week_resolved,
month_resolved,
quarter_resolved,
year_resolved,
type,
priority,
status,
tat,
eng_work_tat,
non_eng_work_tat,
story_points,
components,
project,
is_user_reported,
tat_staging,
tat_review,
tat_progress,
complexity,
issue_id,
initial_story_points,
completion_story_points,
case when type = 'Data' and priority = 'Highest' then 1 
when type = 'Data' and priority = 'High' then 5
when type = 'Data' and priority = 'Medium' then 14
when type = 'Data' and priority = 'Low' then 28
when type = 'Data' and priority = 'Lowest' then 56
when type in ('Bug','Sentry','Other')  and priority = 'Highest' then 1
when type in ('Bug','Sentry','Other')  and priority = 'High' then 5
when type in ('Bug','Sentry','Other')  and priority = 'Medium' then 21
when type in ('Bug','Sentry','Other')  and priority = 'Low' then 45
when type in ('Bug','Sentry','Other')  and priority = 'Lowest' then 45
when type = 'IT' and priority = 'Highest' then 1 
when type = 'IT' and priority = 'High' then 1
when type = 'IT' and priority = 'Medium' then 1
when type = 'IT' and priority = 'Low' then 10
when type = 'IT' and priority = 'Lowest' then 10
when type = 'Enhancement' and priority = 'Highest' then 14 
when type = 'Enhancement' and priority = 'High' then 28
when type = 'Enhancement' and priority = 'Medium' then 56
when type = 'Enhancement' and priority = 'Low' then 84
when type = 'Enhancement' and priority = 'Lowest' then 84
end as _target_tat,
resolution,
time_in_triage,
name,
email
from
((
select
  'Issues' as metric,
  extract(date from created_at) as day_created,
  date_trunc(extract(date from created_at),week(tuesday)) as week_created,
 -- date_add(date_trunc(extract(date from created_at),week(tuesday)),interval 1 day) as week_created,
  date_trunc(extract(date from created_at),month) as month_created,
  date_trunc(extract(date from created_at),quarter) as quarter_created,
  date_trunc(extract(date from created_at),year) as year_created,
  extract(date from resolved_at) as day_resolved,
  date_trunc(extract(date from resolved_at),week(tuesday)) as week_resolved,
  date_trunc(extract(date from resolved_at),month) as month_resolved,
  date_trunc(extract(date from resolved_at),quarter) as quarter_resolved,
  date_trunc(extract(date from resolved_at),year) as year_resolved,
  type,
  priority,
  status,
  datetime_diff(resolved_at,created_at,day) as tat,
  datetime_diff(eng_done_date,created_at,day) as eng_work_tat,
  datetime_diff(resolved_at,eng_done_date,day) as non_eng_work_tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            or (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  tat_review,
  tat_progress,
  complexity,
  id as issue_id,
  case when initial_estimate is null and story_points is null then 0 
       when initial_estimate is null and story_points is not null then story_points
       when initial_estimate is not null then initial_estimate end as initial_story_points,
  case when final_estimate is null and status = 'Done' and story_points is not null then story_points 
       when final_estimate is null and status = 'Resolved' and story_points is not null then story_points
       when final_estimate is not null then final_estimate else 0 end as completion_story_points,
  resolution,time_in_triage,
  name,email
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    case when status in ('Done','Resolved') then datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") else null end as resolved_at,
    datetime(cast(i.eng_done_date as timestamp), "America/Los_Angeles") as eng_done_date,
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
    cast(initial_estimate as int64) as initial_estimate,
    cast(final_estimate as int64) as final_estimate,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    tat_review,
    tp.tat_progress,
    coalesce(complexity,'Medium') as complexity,
    i.resolution,time_in_triage,
    u.name,email
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join tat_per_issue ti on ti.issue_id = i.id
  left join tat_per_issue_review tr on tr.issue_id = i.id
  left join tat_progress_per_issue tp on tp.issue_id = i.id
  left join story_points sp on sp.id = i.id
   left join time_triage tt on tt.id = i.id
  left join jira.user u on i.assignee_id = u.id
  where
    p.key in ('ENG','IT','HELP') 
  	--and i.parent_id is null 
    and i.issue_type <> 'Sub-task'
    and i.issue_type <> 'Story'
    --and (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%')
) as k
where 
  created_at <= current_datetime()
  )
union all 
(
select
  'Features' as metric,
  extract(date from created_at) as day_created,
  date_trunc(extract(date from created_at),week(tuesday)) as week_created,
  date_trunc(extract(date from created_at),month) as month_created,
  date_trunc(extract(date from created_at),quarter) as quarter_created,
  date_trunc(extract(date from created_at),year) as year_created,
  extract(date from resolved_at) as day_resolved,
  date_trunc(extract(date from resolved_at),week(tuesday)) as week_resolved,
  date_trunc(extract(date from resolved_at),month) as month_resolved,
  date_trunc(extract(date from resolved_at),quarter) as quarter_resolved,
  date_trunc(extract(date from resolved_at),year) as year_resolved,
  type,
  priority,
  status,
  datetime_diff(resolved_at,created_at,day) as tat,
  datetime_diff(eng_done_date,created_at,day) as eng_work_tat,
  datetime_diff(resolved_at,eng_done_date,day) as non_eng_work_tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            or (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  tat_review,
  tat_progress,
  complexity,
  id as issue_id,
  case when initial_estimate is null and story_points is null then 0 
       when initial_estimate is null and story_points is not null then story_points
       when initial_estimate is not null then initial_estimate end as initial_story_points,
  case when final_estimate is null and status = 'Done' and story_points is not null then story_points 
       when final_estimate is null and status = 'Resolved' and story_points is not null then story_points
       when final_estimate is not null then final_estimate else 0 end as completion_story_points,
  resolution,time_in_triage,
  name,email   
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    case when status in ('Done','Resolved') then datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") else null end as resolved_at,
    datetime(cast(i.eng_done_date as timestamp), "America/Los_Angeles") as eng_done_date,
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
    cast(initial_estimate as int64) as initial_estimate,
    cast(final_estimate as int64) as final_estimate,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    tat_review,
    tat_progress,
    coalesce(complexity,'Medium') as complexity,
    i.resolution,time_in_triage,
    u.name,email
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join tat_per_issue ti on ti.issue_id = i.id
  left join tat_per_issue_review tr on tr.issue_id = i.id
  left join tat_progress_per_issue tp on tp.issue_id = i.id
  left join story_points sp on sp.id = i.id
   left join time_triage tt on tt.id = i.id
  left join jira.user u on i.assignee_id = u.id
  where
    p.key in ('ENG','IT')
  	--and i.parent_id is null
    and (i.issue_type = 'Story')
    and status != 'Unconfirmed'
) as k
where 
  created_at <= current_datetime()
)
union all
(
select
  'Features subtasks' as metric,
  extract(date from created_at) as day_created,
  date_trunc(extract(date from created_at),week(tuesday)) as week_created,
  date_trunc(extract(date from created_at),month) as month_created,
  date_trunc(extract(date from created_at),quarter) as quarter_created,
  date_trunc(extract(date from created_at),year) as year_created,
  extract(date from resolved_at) as day_resolved,
  date_trunc(extract(date from resolved_at),week(tuesday)) as week_resolved,
  date_trunc(extract(date from resolved_at),month) as month_resolved,
  date_trunc(extract(date from resolved_at),quarter) as quarter_resolved,
  date_trunc(extract(date from resolved_at),year) as year_resolved,
  type,
  priority,
  status,
  datetime_diff(resolved_at,created_at,day) as tat,
  datetime_diff(eng_done_date,created_at,day) as eng_work_tat,
  datetime_diff(resolved_at,eng_done_date,day) as non_eng_work_tat,
  case when story_points is null then 0 else story_points end as story_points,
  components,
  project,
  case when (upper(description) like '%CAFL1N0A2%' or description like '%Issue created in Slack from a  message%' 
            or (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%')) then true else false end as is_user_reported,
  tat_staging,
  tat_review,
  tat_progress,
  complexity,
  id as issue_id,
  case when initial_estimate is null and story_points is null then 0 
       when initial_estimate is null and story_points is not null then story_points
       when initial_estimate is not null then initial_estimate end as initial_story_points,
  case when final_estimate is null and status = 'Done' and story_points is not null then story_points 
       when final_estimate is null and status = 'Resolved' and story_points is not null then story_points
       when final_estimate is not null then final_estimate else 0 end as completion_story_points,
  resolution,
  time_in_triage,
  name,
  email
from 
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    case when i.status in ('Done','Resolved') then datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") else null end as resolved_at,
    datetime(cast(i.eng_done_date as timestamp), "America/Los_Angeles") as eng_done_date,
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
    cast(initial_estimate as int64) as initial_estimate,
    cast(final_estimate as int64) as final_estimate,
    p.key as project,
    i.description,
    i.source_of_the_report,
    ti.tat_staging,
    tat_review,
    tp.tat_progress,
    coalesce(i.complexity,'Medium') as complexity,
    i.resolution,time_in_triage,
    u.name,email
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  left join jira.issue io on i.id = io.parent_id
  left join tat_per_issue ti on ti.issue_id = i.id
  left join tat_per_issue_review tr on tr.issue_id = i.id
  left join tat_progress_per_issue tp on tp.issue_id = i.id
  left join story_points sp on sp.id = i.id
   left join time_triage tt on tt.id = i.id
  left join jira.user u on i.assignee_id = u.id
  where
    p.key in ('ENG','IT','CM')
  	and (i.issue_type = 'Sub-task')
    and i.story_points is not null
) as k
where 
  created_at <= current_datetime()
))),
staff_name as (
select
  name,
  email,
  eng_team_name as team_name
from int_data.eng_team_grouping
),issues_final_cte as (
select 
  i.*,
  team_name
from issue_cte i
left join staff_name sn on sn.email = i.email
)
select 
ifc.*,fap.duedate_changed as times_due_datechanged,
from issues_final_cte as ifc 
left join final_authored_prd fap on fap.eng_issue_id = ifc.issue_id
order by times_due_datechanged desc;
