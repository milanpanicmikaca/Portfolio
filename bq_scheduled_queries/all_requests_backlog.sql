with backlog_cte as (
  select
    key,
    day,
    issue_type,
    eng_team_name,
    case 
      when day <= extract(date from resolved_at) then 1 
      when day <= current_date() and status = 'Backlog' then 1
      else 0 end as in_backlog
  from
    ( 
    with
    changelog as
    ( -- calculates every single issue in the changelog table for every instance is from backlog
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
        (from_string in  ('Backlog') or to_string in ('Backlog'))
      ) as k
    where 
      rank = 1
    )
    -- generates a span of days upon which the jira issues were in backlog
    select
      i.key,
      datetime(cast(c.created as timestamp), "America/Los_Angeles") as resolved_at,
      GENERATE_DATE_ARRAY(cast(i.created as date), current_date(), INTERVAL 1 day) AS date_array,
      i.status,
      case when  p.key in ('ENG','IT') and issue_type in ('Bug','Task') and issue_type <> 'Sub-task' and (i.issue_type = 'Story' or i.summary like 'PRD%') is false
      then "all_requests_reported"     
      when p.key in ('HELP') and i.parent_id is null then "help"
      end as issue_type,
      eng_team_name
    from jira.issue i
    left join jira.project p on p.id = i.project_id
    left join changelog c on c.issue_id = i.id
    left join jira.user u on i.assignee_id = u.id
    left join int_data.eng_team_grouping etm on etm.email = u.email
  where p.key in ('ENG','HELP')
 )
  CROSS JOIN UNNEST(date_array) AS day
),
ranked_backlog as ( -- we are getting the number of issues in backlog grouped by issuestype and eng team
select
  day as day,
  issue_type,
  eng_team_name,
  sum(in_backlog) as in_backlog
from backlog_cte
group by 1,2,3
), ranked_aui as (  -- calculates the number of days issues are in the backlog grouped by issuestype and eng team
  select  
    day,
    issue_type,
    eng_team_name,
    avg(days_in_backlog) as avg_age_of_backlog,
    max(days_in_backlog) as max_age_in_backlog,
  from   
    (
      select 
        *,
        sum(in_backlog) over (partition by key order by day) as days_in_backlog
      from backlog_cte
      where in_backlog = 1
    )sub
  group by 1,2,3
)
select 
  date_trunc(rb.day,week(tuesday)) as week,
  rank() over (partition by rb.issue_type,rb.eng_team_name,date_trunc(rb.day,week(Tuesday)) order by rb.day desc) rank_week, 
  date_trunc(rb.day,month) as month,
  rank() over (partition by rb.issue_type,rb.eng_team_name,date_trunc(rb.day, month) order by rb.day asc) rank_month,
   rb.issue_type,
  rb.eng_team_name,
  aui.max_age_in_backlog,
  aui.avg_age_of_backlog,
  rb.in_backlog
from ranked_backlog rb
left join ranked_aui aui on 
  rb.day = aui.day 
  and aui.issue_type = rb.issue_type 
  and aui.eng_team_name = rb.eng_team_name
