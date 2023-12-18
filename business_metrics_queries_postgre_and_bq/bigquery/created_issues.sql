-- upload to BQ
with 
    issues as
(
  select
    i.id,
    datetime(cast(i.created as timestamp), "America/Los_Angeles") as created_at,
    datetime(cast(i.resolution_date as timestamp), "America/Los_Angeles") as resolved_at,
    i.priority,
    i.status,
    i.components,
    i.story_points,
    p.key as project,
    case when (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%') then 1 else 0 end as is_user_reported,
    i.parent_id,
    i.issue_type
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  where
    p.key in ('ENG','IT', 'CM', 'HELP')
    --and (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%')
)
select
  date_trunc(extract(date from created_at), {period}) as date,
  sum(case when project = 'ENG' 
            and issue_type = 'Story' 
            and parent_id is null then 1 else 0 end) as ENG005
from issues
group by 1 
order by 1 desc
