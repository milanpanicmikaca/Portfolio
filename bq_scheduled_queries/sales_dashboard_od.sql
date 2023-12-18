with onsites as (
  select
    order_id,
    min(case when cancelled_at is null then date end) as first_onsite,
    count(distinct case when onsite_type = 'physical_onsite' and cancelled_at is null then onsite_id end) as is_physical,
    count(distinct case when onsite_type = 'remote_onsite' and cancelled_at is null then onsite_id end) as is_remote,
  from int_data.estimations_dashboard_photographersdetail
  group by 1
), escalations as (
  with esc as (
  select
    date(e.created_at, "America/Los_Angeles") as date,
    e.order_id,
    count(distinct e.id) as escalations,
    array_to_string(array_agg(distinct eta.name),", ") || " / " || array_to_string(array_agg(distinct eta2.name),", ") as teams,
  from ergeon.store_escalation e
    left join ergeon.store_escalation_core_issues seci on seci.escalation_id = e.id
    left join ergeon.store_escalation_primary_teams_attributed pta on pta.escalation_id = e.id
    left join ergeon.store_escalation_secondary_teams_attributed sta on sta.escalation_id = e.id
    left join ergeon.store_escalationteamattributed eta on eta.id = pta.escalationteamattributed_id
    left join ergeon.store_escalationteamattributed eta2 on eta2.id = sta.escalationteamattributed_id
  group by 1, 2
  ) select date, order_id, escalations from esc where teams like "%Sales%"
), reviews as (
  select
    fr.order_id,
    fr.sales_staff_attributed_id as staff_id,
    min(date(fr.posted_at, "America/Los_Angeles")) as review_at,
    avg(fr.score) as score,
    count(fr.score) as reviews,
  from ergeon.feedback_review fr
  group by 1, 2
), assigned as (
  select
    sc.assigned_to_id as order_id,
    sc.assigned_at as assigned_at,
    sc.queued_at as queued_at,
    datetime_diff(assigned_at, queued_at, hour) as queue_to_assigned_hrs,
    rank() over(partition by sc.assigned_to_id order by sc.assigned_at) as rank_a
  from ergeon.tasks_csqueue sc
  
)
select
  ue.order_id,
  ue.created_at,
  case 
    when h.full_name is not null and a.assigned_at is null then ue.created_at
    when h.full_name is null and a.assigned_at is null then null 
    else date(a.assigned_at) end as assigned_at,
  ue.quoted_at,
  ue.won_at,
  ue.cancelled_at,
  ue.completed_at,
  ue.closed_at,
  e.date as escalation_at,
  os.first_onsite,
  h.full_name,
  h.title,
  h.house,
  h.team_lead,
  h.email,
  z.code as zipcode,
  ue.segment,
  ue.region,
  sr.region as region_gm,
  ue.market,
  cast(ue.is_lead as int64) as is_lead,
  case when ue.cancelled_at is not null and ue.won_at is not null then 1 else 0 end as is_cancelled,
  case when ue.won_at is not null then 1 else 0 end as is_won,
  ue.first_approved_price as initial_revenue,
  ue.last_approved_price as final_revenue,
  ue.is_lead7,
  ue.is_order7,
  ue.is_quote7,
  ue.is_quoted7,
  ue.is_lead14, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_order14, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_quote14, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_quoted14, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_won14, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_quote30, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_won30, --might be removed later, as it was requested to be replaced by the 7 day variation
  ue.is_won30q, --won deals that happened within 30 days from quoted to won
  ue.is_won7,
  ue.is_won7q, --won deals that happened within 7 days from quoted to won
  ue.tat_ar, --arrival to quote requested
  ue.quotes_sent_count as revisions,
  r.review_at,
  r.reviews,
  r.score as int_score,
  fb_internal_quoting_score as csat,
  e.escalations,
  ue.last_approved_sales_discount,
  coalesce(os.is_physical,0) as os_physical,
  coalesce(os.is_remote,0) as os_remote,
  coalesce(os.is_physical + os.is_remote,0) as os_total,
  date_diff(os.first_onsite, ue.created_at,day) as tat_l2onsite,
  date_diff(ue.won_at,ue.created_at,day) as tat_l2w,
  quotes_sent_count,
  ue.revenue,
  ue.gp,
  ue.first_approved_price,
  st.label as deal_status,
  ue.tat_qw,
from int_data.order_ue_materialized ue
  left join assigned a on a.order_id = ue.order_id and a.rank_a = 1
  left join int_data.hr_dashboard h on h.staff_id = ue.sales_staff_id
  left join onsites os on os.order_id = ue.order_id
  left join ergeon.store_order o on o.id = ue.order_id
  left join ergeon.core_house ch on ch.id = o.house_id
  left join ergeon.geo_address ga on ga.id = ch.address_id
  left join ergeon.geo_zipcode z on z.id = ga.zip_code_id
  left join escalations e on e.order_id = ue.order_id
  left join int_data.segment_region sr on sr.segment = ue.segment
  left join ergeon.core_statustype st on st.id = o.deal_status_id
  left join reviews r on r.order_id = ue.order_id and r.staff_id = ue.sales_staff_id
where h.ladder_name = 'Sales'