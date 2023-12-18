-- upload to BQ
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
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
    priority
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
      --datetime(cast(i.resolution_date as timestamp), 'America/Los_Angeles') as resolution_date,
      GENERATE_DATE_ARRAY(cast(i.created as date), current_date(), INTERVAL 1 day) AS date_array,
      i.status,
      i.priority
    from jira.issue i
    left join jira.project p on p.id = i.project_id
    left join changelog c on c.issue_id = i.id
    where
      p.key in ('CM')  and issue_type<> 'Story'
 )
  CROSS JOIN UNNEST(date_array) AS day
  )
where backlog = 1
), 
 catalog_issue as  (
    select
    i.id,
    datetime(cast(i.created as timestamp), 'America/Los_Angeles') as created_at,
    datetime(cast(i.resolution_date as timestamp), 'America/Los_Angeles') as resolved_at,
    i.parent_id,
    i.issue_type
  from jira.issue i
  left join jira.project p on p.id = i.project_id
  where
    p.key in  ('CM') 
    and created <= current_datetime()
    and issue_type<> 'Story' and status = 'Done'
),catalog_issues_resolved as
(
  select
    date_trunc(resolved_at  ,{period}) as date,
      avg(datetime_diff(resolved_at,created_at,day)) as tat,
      count(resolved_at) as resolved_issues
      from catalog_issue
      group by 1
      ),catalog_issues_reported as
(
  select
    date_trunc(created_at  ,{period}) as date,
      count(created_at) as reported_issues,
      from catalog_issue
      group by 1
      )   
select 
    t.day as date,
    coalesce(count(*),0) as CTN024,
    coalesce(reported_issues,0) as CTN029,
    coalesce(resolved_issues,0) as CTN025,
    coalesce(tat,0)as CTN026,
    coalesce(max(days_in_backlog),0) as CTN027,
    coalesce(avg(days_in_backlog),0) as CTN028
from timeseries t
left join backlog b on b.day = t.day 
left join catalog_issues_resolved ci on ci.date = t.day
left join catalog_issues_reported re on re.date = t.day
where t.period_rank = 1 and t.day > '2018-04-16'
group by 1,3,4,5
order by 1 desc
