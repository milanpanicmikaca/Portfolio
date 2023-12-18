-- upload to BQ
select 
  date_trunc(cast(start_datetime as date), {period}) as date,
  avg(time_in_status_hour) as CTN019,
  ifnull(avg(case when status = 'Awaiting Input from CS' then time_in_status_hour else null end),0) as CTN020,
  ifnull(avg(case when status = 'Awaiting Input from Construction' then time_in_status_hour else null end),0) as CTN021
from
( 
  select 
    c.issue_id,
    c.to_string as status,
    created as start_datetime,
    coalesce(lead(created) over (partition by c.issue_id order by created),current_date('America/Los_Angeles')) as end_datetime,
        datetime_diff(coalesce(lead(created) over (partition by c.issue_id order by created),current_date('America/Los_Angeles')),datetime(cast(created as timestamp), 'America/Los_Angeles'), hour) as time_in_status_hour
  from jira.changelog c
)
where
  status like 'Awaiting%'
group by 1
order by 1 desc
