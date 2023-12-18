select
sa.date as date,
jo.key as jira_issue_number,
jo.admin_order_number as order_id,
ju.name as full_name --estimator name. Matching field name for estimation dashboard filters to work
from jira.issue jo 
left join jira.user ju on jo.assignee_id = ju.id
left join ergeon.schedule_appointment sa on sa.order_id = jo.admin_order_number
where issue_type = 'onsite.review'
and key not like ('%DEV%')
and jo.status = 'ONSITE COMPLETED'
and sa.cancelled_at is null
and appointment_type_id	= 1
and date > '2022-02-25'