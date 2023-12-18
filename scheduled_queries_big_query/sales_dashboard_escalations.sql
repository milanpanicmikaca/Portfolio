with rank_escalations as (
  select 
  c.*,
  i.key,
  rank() over(partition by i.key order by c.created) as rank_esc
  from jira.changelog c 
  left join jira.issue i on i.id = c.issue_id
where to_string = 'Escalation Resolved'
), jira_esc as (
select * from rank_escalations re 
where rank_esc = 1
)
select 
    date(cast(j.created as timestamp), "America/Los_Angeles") as Date,
    o.id as order_id,
    cu.email,
    cu.full_name as sales_rep,
    'Escalation' as type,
    a.initial_revenue as value,
    a.bonus as bonus,
    case when a.project_status = 'Cancelled' then (a.bonus * (-1)) else (a.bonus * (-0.5)) end as final_bonus,
    a.project_status,
    s.id as staff_id,
    e.id as escalation_id
from ergeon.store_escalation e
    left join ergeon.store_order o on o.id = e.order_id
    left join ergeon.hrm_staff s on s.id = o.sales_rep_id
    left join ergeon.core_user cu on cu.id = s.user_id
    left join ergeon.core_statustype st on st.id = e.status_id
    left join ergeon.store_escalation_primary_teams_attributed pta on pta.escalation_id = e.id
    left join ergeon.store_escalationteamattributed ta on ta.id = pta.escalationteamattributed_id
    left join jira_esc j on j.key = e.jira_issue_key
    left join int_data.sales_dashboard_arts a on a.order_id = e.order_id
where lower(ta.name) like '%sales%'
and j.created is not null

