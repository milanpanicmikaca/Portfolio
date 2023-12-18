(with
  order_ids
as
(
select
  so.id,
  cu.full_name,
  date_trunc(extract(date from so.completed_at at time zone "America/Los_Angeles"),day) as date,
  qq.old_total_price,
  qq.total_cost
from ergeon.store_order so
left join ergeon.hrm_staff hs on so.project_manager_id = hs.id
left join ergeon.core_user cu on cu.id = hs.user_id
left join ergeon.quote_quote qq on qq.id = so.approved_quote_id
where so.completed_at is not null
  and qq.approved_at >= '2018-04-15'
        and so.status not in ('TST')
        and so.parent_order_id is null
        and so.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
order by 3 desc
),
first_approved_quote
as
(
select
  id,
   order_id,
   date_trunc(extract(date from approved_at at time zone "America/Los_Angeles"),day) as approved_date,
   rank() over (partition by order_id order by approved_at) as rank
from ergeon.quote_quote
where approved_at is not null
),
total_contractor_cost
as
(
select
  o.id,
  sum(co.total_cost) as total_contractor_cost
from order_ids o
left join ergeon.contractorapp_contractororder co on o.id = co.order_id
where co.status_id = 13
group by 1
), -- escalations
escalations
as 
(
select
  distinct order_id as Order_id
from ergeon.store_escalation
),
main_escalation_bucket
as 
(
  with calc_data
  as
    (
    select
        cu.full_name,
        sec.name,
        count(sec.name) as count
    from ergeon.store_escalation_escalation_core_issues see
    left join ergeon.store_escalation se on se.id = see.escalation_id
    left join ergeon.store_escalationcoreissue sec on sec.id  = see.escalationcoreissue_id
    left join ergeon.contractorapp_contractororder coo on coo.order_id = se.order_id
    left join ergeon.hrm_contractor hc on hc.id = coo.contractor_id
    left join ergeon.core_user cu on cu.id = hc.user_id
    where se.id in (
            select
                distinct se.id
            from ergeon.store_escalation se
            left join ergeon.store_escalation_primary_teams_attributed sep on sep.escalation_id = se.id
            left join ergeon.store_escalation_secondary_teams_attributed ses on ses.escalation_id = se.id
            left join ergeon.store_escalationteamattributed ep on ep.id = sep.escalationteamattributed_id
            left join ergeon.store_escalationteamattributed es on es.id = ses.escalationteamattributed_id
            where ep.name = 'Delivery Team' or es.name = 'Delivery Team'
            )
    group by 1, 2 
    ),
  rank
  as
    (
    select
    *,
    rank() over (partition by full_name order by count desc) as rank_issues
    from calc_data
    )
  select
        full_name as installer_name_admin,
        string_agg(name, ",") as core_issue
  from rank
  where rank_issues = 1
  group by 1
),
prepayments as
(
select 
    cu.full_name,
    sum(co.total_cost) as total_prepayments,
from ergeon.contractorapp_contractororder co 
left join ergeon.store_order so on so.id = co.order_id
left join ergeon.hrm_contractor hc on hc.id = co.contractor_id
left join ergeon.core_user cu on cu.id = hc.user_id
where co.status_id = 3
and so.completed_at is null
and so.status not in ('CMP', 'TST')
and so.parent_order_id is null
group by 1
),
material_balances
as
(
with
calc_series as
(
 select
        date
 from warehouse.generate_date_series
where date > '2018-04-16' and date <= current_date()
 ),
materials_balance
as
(
  select
          ac.date as day,
          order_id,
          case 
                  when at2.name = 'Materials Payment' then amount
                  when at2.name in ('Materials Purchased', 'Materials Returned') and order_id is null then amount else 0 end as amount,
          at2.name,
          cu.full_name,
          case 
                  when at2.name = 'Materials Payment' and ac.date >= date_sub(current_date(), interval 14 day) then amount
                  when at2.name in ('Materials Purchased', 'Materials Returned') and order_id is null  and ac.date >= date_sub(current_date, interval 14 day) then amount else 0 end as amount_14_days
  from ergeon.accounting_transaction ac
  left join ergeon.accounting_transactiontype at2 on at2.id = ac.type_id
  left join ergeon.hrm_contractor hc on hc.id = ac.contractor_id
  left join ergeon.core_user cu on cu.id = hc.user_id
  where ac.contractor_id is not null
  and ac.deleted_at is null
  ),
  calc_data
  as
  (
  select
          distinct cs.date,
          full_name,
          coalesce(sum(amount) over (partition by full_name order by cs.date),0) as material_balance,
          coalesce(sum(amount_14_days) over (partition by full_name order by cs.date),0) as material_balance_14,
          rank() over (partition by full_name order by cs.date desc) as rank
          from calc_series cs
          left join materials_balance mb on mb.day = cs.date
          order by 1 desc
  )
  select
  full_name,
  round(material_balance,2) as material_balance,
  round(material_balance_14, 2) as material_balance_14
  from calc_data 
  where rank = 1
),
reviews as 
(
select
    cu.full_name,
    count(*) as review_count
from ext_delivery.public_reviews_form prf
left join ergeon.core_user cu on cu.email = prf.email_address
group by 1 
),
count_active as 
(
select 
    cu.full_name,
    count(so.id) as active_count
from ergeon.store_order so
left join ergeon.hrm_staff hs on so.project_manager_id = hs.id
left join ergeon.core_user cu on cu.id = hs.user_id
where so.status in ('QIP', 'QUD', 'ONH', 'RTS', 'PGS')  
and so.approved_quote_id is not null
and so.project_manager_id is not null
and so.parent_order_id is null
group by 1
),
escalation_pm_metrics as
(
with
calc_escalation as
(
select
	eh.escalation_id,
	e.reported_at as start_date,
	eh.created_at as end_date,
	u.full_name as pm,
	st.code,
	case when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then 
	rank() over (partition by eh.escalation_id order by eh.created_at desc) else null end as rank_end_states,
	case when st2.code in ('escalation_received', 'escalation_fix_agreed') then 'active' else 'resolved' end as grouped_status
from ergeon.store_escalationstatushistory eh 
left join ergeon.store_escalation e on e.id = eh.escalation_id
left join ergeon.core_statustype st on st.id = eh.status_id
left join ergeon.core_statustype st2 on st2.id = e.status_id
left join ergeon.store_order o on o.id = e.order_id 
left join ergeon.hrm_staff s on s.id = o.project_manager_id 
left join ergeon.core_user u on u.id = s.user_id 
where reported_at >= '2021-04-28' 
order by 4,2
),
team_avg as
(
select avg(case when rank_end_states = 1 then timestamp_diff(end_date,start_date,day) else null end) as tat_team
from calc_escalation
)
select
  max(tat_team) as tat_team,
	pm,
	sum(case when rank_end_states = 1 then 1 else 0 end) as count_solved_pm,
	avg(case when rank_end_states = 1 then timestamp_diff(end_date,start_date,day) else null end) as tat,
	sum(case when timestamp_diff(current_timestamp(),end_date, day) <= 30 and code <> 'escallation_cancelled' and rank_end_states = 1 then 1 else 0 end) as resolved_30days,
	sum(case when grouped_status = 'active' and timestamp_diff(current_timestamp(),start_date,day) <= 30 then 1 else 0 end) as active_30days,
	sum(case when grouped_status = 'active' and timestamp_diff(current_timestamp(),start_date,day) between 30 and 60 then 1 else 0 end) as active_30_60days,
	sum(case when grouped_status = 'active' and timestamp_diff(current_timestamp(),start_date,day) > 60 then 1 else 0 end) as active_60days
from calc_escalation
cross join team_avg
where pm is not null
group by 2
)
select
  date as day,
  oi.full_name,
  DATE_DIFF(date, approved_date, day) as day_for_completion,
  cu.full_name as contractor_name,
  oi.id,
  oi.old_total_price,
  oi.total_cost,
  co.id as contractorapp_id,
  co.total_cost as contractor_cost,
  cc.total_contractor_cost,
  oi.old_total_price - oi.total_cost as expected_profit,
  oi.old_total_price - total_contractor_cost as real_profit,
  coalesce((oi.old_total_price - coalesce(total_contractor_cost,0))/nullif(oi.old_total_price,0),0) as Gross_Margin,
  rank() over (partition by oi.id order by co.id) as rank_order,
  nps as CSAT,
  case when fo.created_at is null then 0 else 1 end as has_feedback,
  'CMP' as status,
  case when ef.Order_id is null then 0 else 1 end as has_escalation,
  me.core_issue,
  mb.material_balance,
  mb.material_balance_14,
  tp.total_prepayments,
  hc.project_manager_id,
  cu2.full_name as pm_assigned,
  case when oi.full_name in ('Eliana Oleachea Dongo', 'Sheila Duran','Miguel Nieto', 'Edgar Elizarraras', 'Jhaset Jiron') then 'Eliana Oleachea Dongo'
       when oi.full_name in ('Samuel Duran', 'Sergio Hernandez', 'Alfredo Silva', 'Joan Moya', 'Diego Bonatti') then 'Samuel Duran'
       when oi.full_name in ('Nestor Baca', 'Rodrigo Garcia', 'Roselyn Alegria','Elsa Ruiz', 'Karen Velasquez') then 'Nestor Baca'
       when oi.full_name in ('Ricardo Terrazas Saavedra', 'Maricarmen Castellanos', 'Emily Quiroga','Joel Duran') then 'Ricardo Terrazas Saavedra' else 'Carmen Mendez' end as team_lead,
  re.review_count,
  ca.active_count,
  case when cu2.full_name in ('Eliana Oleachea Dongo', 'Sheila Duran','Miguel Nieto', 'Edgar Elizarraras', 'Jhaset Jiron') then 'Eliana Oleachea Dongo'
       when cu2.full_name in ('Samuel Duran', 'Sergio Hernandez', 'Alfredo Silva', 'Joan Moya', 'Diego Bonatti') then 'Samuel Duran'
       when cu2.full_name in ('Nestor Baca', 'Rodrigo Garcia', 'Roselyn Alegria','Elsa Ruiz', 'Karen Velasquez') then 'Nestor Baca'
       when cu2.full_name in ('Ricardo Terrazas Saavedra', 'Maricarmen Castellanos', 'Emily Quiroga','Joel Duran') then 'Ricardo Terrazas Saavedra' else 'Carmen Mendez' end as team_lead_contractor,
  epm.tat,
  epm.count_solved_pm,
  epm.resolved_30days,
  epm.active_30days,
  epm.active_30_60days,
  epm.active_60days,
  epm.tat_team,
  rank() over (partition by oi.full_name order by oi.id) as rank_pm
from order_ids oi
left join first_approved_quote fa on oi.id = fa.order_id and fa.rank = 1
left join ergeon.contractorapp_contractororder co on oi.id = co.order_id
left join total_contractor_cost cc on cc.id = oi.id
left join ergeon.hrm_contractor hc on hc.id = co.contractor_id
left join ergeon.core_user cu on hc.user_id = cu.id
left join ergeon.feedback_orderfeedback fo on oi.id = fo.order_id
left join escalations ef on ef.Order_id = oi.id
left join main_escalation_bucket me on cu.full_name = me.installer_name_admin
left join material_balances mb on cu.full_name = mb.full_name
left join prepayments tp on cu.full_name = tp.full_name
left join ergeon.hrm_staff hs on hs.id = hc.project_manager_id
left join ergeon.core_user cu2 on cu2.id = hs.user_id
left join reviews re on re.full_name = oi.full_name
left join count_active ca on ca.full_name = oi.full_name
left join escalation_pm_metrics epm on epm.pm = oi.full_name
--where date > DATE_SUB(current_date(), INTERVAL 30 day)
order by day desc, oi.id desc
)