select
  date(cast(date as timestamp),"America/Los_Angeles") as date_escalated,
  customer,
  ie.order_id,
  slack,
  installer,
  ie.id,
  ie.market,
  e.region,
  e.county,
  e.city,
  e.state,
  e.type,
  e.onsite_type,
  core_issue,
  primary_team_attributed,
  secondary_team_attributed,
  description,
  ie.modified_at,
  hd.full_name as senior_pm,
  hd1.full_name as project_manager,
  next_steps,
  revised,
  status,
  e.last_approved_quote_id_before_report as quote_id,
  e.jira_link_last_approved as jira_link,
  e.escalated_quote as admin_link,
  e.estimator,
  e.sales_rep as cs,
  e.multi_party_approval,
  e.product_quoted,
  [coalesce(primary_team_attributed,''),coalesce(secondary_team_attributed,'')] as team
from 
  int_data.inst_escalations_spreadsheet ie left join
  ergeon.hrm_stafflog hd on ie.senior_pm = hd.email left join
  ergeon.hrm_stafflog hd1 on ie.project_manager = hd1.email left join
  int_data.order_ue_materialized ue on ue.order_id = ie.order_id left join
  int_data.escalation_query e on ue.first_approved_quote_id = e.last_approved_quote_id_before_report
  --int_data.estimation_dashboard_v2 e on ue.first_approved_quote_id = e.quote_id
qualify rank() over (partition by hd.email,hd1.email order by hd.created_at desc,hd1.created_at desc) = 1