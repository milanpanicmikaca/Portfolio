-- upload to BQ
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array desc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
),
backlog as (
select 
  *,
  sum(backlog) over (partition by key order by day) as days_in_backlog
from
(
select
    key,
    day,
    case 
      when day <= extract(date from resolved_at) then 1 
      when day <= current_date() and status = 'Backlog' then 1
      else 0 end as backlog,
    priority,
    type,issue_type
  from
    ( 
    with
    changelog as
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
        from_string = 'Backlog'
      ) as k
    where 
      rank = 1
    )
    select
      i.key,
      datetime(cast(c.created as timestamp), "America/Los_Angeles") as resolved_at,
      GENERATE_DATE_ARRAY(cast(i.created as date), current_date(), INTERVAL 1 day) AS date_array,
      i.status,
      i.priority,
      case 
      when p.key = 'IT' then 'IT'
      when i.summary like 'DATA%' and p.key = 'ENG' then 'Data'
      when i.summary like 'ENHANCEMENT%' and p.key = 'ENG' then 'Enhancement'
      when (lower(i.description) like '%sentry%' or lower(i.source_of_the_report) like '%sentry%') and p.key = 'ENG'  then 'Sentry'
      when (i.summary like 'BUG%' or lower(i.source_of_the_report) like '%user%') and p.key = 'ENG' then 'Bug'
      else 'Other'
    end as type,
          case when p.key in ('ENG','IT','CM') /*and i.parent_id is null*/ and (i.issue_type = 'Story' /*or i.summary like 'PRD%'*/) then "feature"
      when  p.key in ('ENG','IT') and i.parent_id is null and (upper(i.description) like '%CAFL1N0A2%' or i.description like '%Issue created in Slack from a  message%' 
            and (lower(source_of_the_report) like '%sentry%' or lower(source_of_the_report) like '%user%'))  
            and (i.issue_type = 'Story' or i.summary like 'PRD%') is false then "user_reported"     
      end as issue_type
    from jira.issue i
    left join jira.project p on p.id = i.project_id
    left join changelog c on c.issue_id = i.id
 )
  CROSS JOIN UNNEST(date_array) AS day
  )
where backlog = 1
)
,features as (
select
b.day,
coalesce(count(*),0) as ENG012
from backlog b
where issue_type = 'feature'
group by 1
order by 1 desc
),user_issues as (
select 
    b.day,
    coalesce(count(*),0) as ENG011,
    coalesce(countif(type = 'Data'),0) as ENG112,
    coalesce(countif(type = 'IT'),0) as ENG113,
    coalesce(countif(type = 'Enhancement'),0) as ENG114,
    coalesce(countif(type = 'Bug'),0) as ENG115,
    coalesce(countif(type = 'Sentry'),0) as ENG116,
    coalesce(countif(type = 'Other'),0) as ENG117,
  --  avg(days_in_backlog),
    coalesce(avg(days_in_backlog),0) as ENG004,
    coalesce(avg(case when type = 'Data'then days_in_backlog else null end),0) as ENG118,
    coalesce(avg(case when type = 'IT' then days_in_backlog else null end),0) as ENG119,
    coalesce(avg(case when type = 'Enhancement' then days_in_backlog else null end),0) as ENG120,
    coalesce(avg(case when type = 'Bug' then days_in_backlog else null end),0) as ENG121,
    coalesce(avg(case when type = 'Sentry' then days_in_backlog else null end),0) as ENG122,
    coalesce(avg(case when type = 'Other' then days_in_backlog else null end),0) as ENG123
from backlog b  
where issue_type = 'user_reported' 
group by 1)
select 
    date_trunc(t.day,{period}) as date,
    ui.*except(day),
    f.ENG012
from timeseries t 
left join user_issues ui on ui.day = t.day 
left join features f on f.day = t.day 
where t.period_rank = 1
order by 1 desc;
