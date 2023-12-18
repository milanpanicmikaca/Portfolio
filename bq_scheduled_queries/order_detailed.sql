with
quote_level as 
(
	(select 0 as min_value, 9 as max_value, 1 as points) union all
	(select 10 as min_value, 13 as max_value, 2 as points) union all
	(select 14 as min_value, 19 as max_value, 3 as points) union all
	(select 20 as min_value, 100 as max_value, 4 as points)
),
all_data as
(
with
jira_changelog as
  (
 -- generate changelog with all statuses for each issue
    select 
        i.key,
        c.created as created_at,
        from_string,
        to_string
    from jira.changelog_old c
    left join jira.issue_old i on i.id = c.issue_id
    left join jira.project p on p.id = i.project_id
      where
        p.key = 'EST'
        and c.field = 'status'  
union all
  select
      i.key,
      i.created as created_at,
      'New' as from_string,
      'Open' as to_string
    from jira.issue_old i
    left join jira.project p on p.id = i.project_id
      where
        p.key = 'EST'
  ),
  queue_time
  as
  (
    -- generate time between all statuses
    -- same source as in detailed_status
  select
      c.*,
      safe_cast(REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as INT64) as jira_order_id,
      datetime_diff(datetime(cast(c.created_at as timestamp), "America/Los_Angeles"),
      datetime(cast(lag(c.created_at) over (partition by c.key order by c.created_at) as timestamp), "America/Los_Angeles"), minute) as time_in_queue_minute
  from jira_changelog c
  left join jira.issue_old i on i.key = c.key
  ),
  queue_time_grouped as
  (
    select
      jira_order_id,
      from_string as status,
      sum(case when to_string = 'Completed' then 1 else 0 end) as tot_quotes,
      sum(time_in_queue_minute) as time_in_status_minutes
    from queue_time
    where
    from_string not in ('New','Completed')
    group by 1,2
  ),
  order_aggregate as
  (
   -- per order and jira issue, time spent on each status
   -- this is causing no duplicates on final query
  select
      jira_order_id,
      sum(tot_quotes) as tot_quotes_sent,
      sum(case when status = 'Open' then time_in_status_minutes else 0 end) as open,
      sum(case when status = 'In Progress' then time_in_status_minutes else 0 end) as in_progress,
      sum(case when status = 'Awaiting Input from CS' then time_in_status_minutes else 0 end) as awaiting_input_from_cs,
      sum(case when status = 'Awaiting Input from Construction' then time_in_status_minutes else 0 end) as awaiting_input_from_construction,
      sum(case when status = 'In QA' then time_in_status_minutes else 0 end) as in_qa,
      sum(case when status = 'Requote' then time_in_status_minutes else 0 end) as requote,
      sum(case when status = 'Canceled' then time_in_status_minutes else 0 end) as canceled
  from queue_time_grouped
  group by 1
),
jira_issues as
(
    -- seems to be getting the jira_order_id, key, created_at, claimed_at and completed_at for each jira_issue
    -- these are then used on final select
  with
  jira_issues as
  (
    -- warranty orders are causing duplicates in this order
    select
      rank() over (partition by i.key order by c.created) as rank,
      i.admin_link,
      i.created,
      c.created as claimed_at,
      i.resolution_date as completed_at,
      i.status,
      u.name as assignee,
      u.email as assignee_email,
      i.customer_specialist as cs,
      i.key
    from jira.changelog_old c
    left join jira.issue_old i on i.id = c.issue_id
    left join jira.user u on u.id = i.assignee_id
    left join jira.project p on p.id = i.project_id
    where
      p.key = 'EST' and
      to_string = 'In Progress'
      and lower(i.resolution) not like '%duplicate%'
      --and i.summary not like '%warranty%'
  )
  select
      REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as jira_order_id,
      key,
      created as created_at,
      claimed_at,
      completed_at,
      status,
      assignee as jira_estimator,
      assignee_email as jira_estimator_email,
      cs as jira_cs
    from jira_issues
    where
      rank = 1
),
lead_per_order as
(
  -- gets first id per order, to then get a unique service_category
select
  so.id as order_id,
  min(cl.id) as first_lead_id
from ergeon.store_order so
left join ergeon.core_lead cl on cl.order_id = so.id
group by 1
),
quotelines as 
(
  -- gets per quote: linear_feet, sides, styles, total price, items, and complications
	select
		-- ql.*,
		q.id,
		concat('https://admin.ergeon.in/quoting-tool/',o.id,'/quote/',q.id) as admin_link,
		ue.full_name as ergeon_estimator,
    ue.email as ergeon_estimator_email,
		uc.full_name as ergeon_cs,
		nullif(sum(case when ql.label in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then ql.quantity else 0 end),0) as linear_feet,
		nullif(sum(case when ql.price > 0 then 1 else 0 end),0) as no_of_sides,
		nullif(count(distinct ql.quote_style_id),0) as no_of_styles, 
		nullif(sum(ql.price),0) as total_price,
    STRING_AGG(distinct pct.item) AS items,
    max(replace(replace(replace(JSON_EXTRACT(q.calc_input,'$.complications'),'"',''),'[',''),']','')) as complications
	from ergeon.quote_quoteline ql
	left join ergeon.quote_quote q on q.id = ql.quote_id
	left join ergeon.store_order o on o.id = q.order_id
  left join lead_per_order lpo on lpo.order_id = o.id
	left join ergeon.core_lead l on l.id = lpo.first_lead_id
	left join ergeon.hrm_staff hs on hs.id = o.sales_rep_id 
	left join ergeon.core_user uc on uc.id = hs.user_id  
	left join ergeon.core_user ue on ue.id = q.sent_to_customer_by_id
  left join ergeon.product_catalog pc on pc.id = ql.catalog_id 
  left join ergeon.product_catalogtype pct on pct.id = pc.type_id
	group by 1,2,3,4,5
),
quotes as
(
  -- quotes from ergeon dataset, ranked
	select 
		rank() over (partition by so.id order by q.created_at desc) as rank,
		q.*
  from ergeon.store_order so
	left join ergeon.quote_quote q on q.order_id = so.id 
),
revisions_sent as
(
  -- number of revisions sent for each order 
  with
  quotes_order as
  (   
      select
          q.order_id,
          q.id as quote_id,
          rank() over (partition by q.order_id order by q.sent_to_customer_at) as rank,
          datetime_diff(datetime(q.sent_to_customer_at), 
                      datetime(lag(sent_to_customer_at) over (partition by q.order_id order by q.sent_to_customer_at)), minute) as time_from_previous
      from ergeon.quote_quote q
      where
          q.sent_to_customer_at is not null
  ),
  calc_revisions
  as
  (
      select
          rank,
          order_id,
          quote_id,
          case 
          when sum(time_from_previous) over (partition by order_id order by rank asc rows between unbounded preceding and current row) between 0 and 120 then 1 
          when sum(time_from_previous) over (partition by order_id order by rank asc rows between unbounded preceding and current row) is null then 1
          else 0 
          end as no_of_revisions_sent
      from quotes_order
  )
  select
      order_id,
      sum(no_of_revisions_sent) as no_of_revisions_sent
  from calc_revisions
  group by 1
),
orders as
(
  -- general order data from the ergeon and pipedrive datasets. 
	select
		o.id as order_id,
		max(lq.id) as quote_id,
    case when o.product_id = 34 then 'hardscape' else 'fence' end as product_id_main,
    max(d.lost_reason) as lost_reason,
    max(approved_quote_id) as approved_quote_id,
    max(pipedrive_deal_key) as pipedrive_deal_key,
    nullif(max(rs.no_of_revisions_sent),0) as no_of_revisions_sent,
    sum(case when q.rank = 1 and q.sent_to_customer_at is not null then 1 else 0 end) as new_quotes,
    sum(case when q.rank > 1 and q.sent_to_customer_at is not null then 1 else 0 end) as requotes,
    min(q.approved_at) as approved_at,
    max(is_commercial) as is_commercial
	from ergeon.store_order o
  left join ergeon.core_house h on h.id = o.house_id
  left join ergeon.customers_customer cc on cc.id = h.customer_id
  left join revisions_sent rs on rs.order_id = o.id
	left join quotes lq on lq.order_id = o.id and lq.rank = 1
	left join quotes q on q.order_id = o.id
	left join quotelines ql on ql.id = lq.id
  left join pipedrive.deal d on d.id = cast(o.pipedrive_deal_key as int64)
	-- where
		-- lq.id = 164827
  group by 1,3
),
estimation_qa as
(
  -- QA per order, in case of more than one jira issue evaluated it takes the average
select
  coalesce(qa.order_id,cast(REGEXP_REPLACE(left(admin_link,45),'[^0-9 ]','') as INT)) as order_id,
  avg(qa.final_score) as final_score,
  avg(qa.hand_off_score) as hand_off_score,
  avg(qa.offering_score) as offering_score,
  avg(qa.configuration_score) as configuration_score,
  avg(qa.complications_score) as complications_score,
  avg(qa.missed_elements_score) as missed_elements_score,
  avg(qa.regulations_score) as regulations_score,
  avg(qa.internal_comm_score) as internal_comm_score,
  avg(qa.drawings_score) as drawings_score,
  avg(qa.presentation_score) as presentation_score,
  max(qa.reviewer) as reviewer,
  max(qa.risk) as risk  
from googlesheets.estimation_qa qa
group by 1
)
select
	o.order_id,
	quote_id,
	ql.admin_link,
  concat('https://api.ergeon.in/public-admin/quote/quote/',quote_id,'/change/') as new_admin_link,
  concat('https://ergeon.pipedrive.com/deal/',o.pipedrive_deal_key) as pipedrive_deal_link,
  concat('https://ergeon.atlassian.net/browse/',ji.key) as jira_link,
	case when jira_estimator is null then ergeon_estimator else jira_estimator end as estimator,
  case when jira_estimator_email is null then ergeon_estimator_email else jira_estimator_email end as estimator_email,
	case when jira_cs is null then ergeon_cs else jira_cs end as cs,
	case when ql.linear_feet is null then 0 else ql.linear_feet end as linear_feet,
	case when ql.no_of_sides is null then 0 else ql.no_of_sides end as no_of_sides,
	case when ql.no_of_styles is null then 0 else ql.no_of_styles end as no_of_styles,
  case when o.is_commercial is false then 'No' else 'Yes' end as commercial,
  ql.items as products,
  case when ql.items like 'fence-side%' or ql.items like '%,fence-side%' then 'Yes' else 'No' end as wooden_fence,
  case when ql.items like '%cl-fence-side%' then 'Yes' else 'No' end as chainlink_fence,
  case when ql.items like '%bw-fence-side%' then 'Yes' else 'No' end as boxwire_fence,
  case when ql.items like '%fence-staining%' then 'Yes' else 'No' end as stain,
  case when ql.items like '%fence-repairs%' then 'Yes' else 'No' end as repair,
  case when ql.items like 'fence-gate%' or ql.items like '%,fence-gate%' then 'Yes' else 'No' end as fence_gate,
  case when ql.items like '%cl-fence-gate%' then 'Yes' else 'No' end as chainlink_gate,
  case when ql.items like '%bw-fence-gate%' then 'Yes' else 'No' end as boxwire_gate,
  case when ql.items like '%retaining-wall%' then 'Yes' else 'No' end as retaining_wall,
  case when ql.items like '%standalone-rw%' then 'Yes' else 'No' end as standalone_retaining_wall,
  case when ql.items like '%cl-fence-side%' or ql.items like '%cl-fence-gate%' then 'Yes' else 'No' end as chainlink_v2,
  case when ql.items like '%bw-fence-side%' and ql.items like '%bw-fence-gate' then 'Yes' else 'No' end as boxwire_v2,
  case when ql.items like '%vinyl-fence-side%' and ql.items like '%vinyl-fence-gate' then 'Yes' else 'No' end as vinyl,
  case when ql.items like '%fence-side-custom%' then 'Yes' else 'No' end as fence_custom,
  case when ql.items like '%sales-discount-fence%' then 'Yes' else 'No' end as fence_discount,
	case when o.no_of_revisions_sent is null then 0 else o.no_of_revisions_sent end as no_of_revisions_sent,
	case when ql.total_price is null then 0 else ql.total_price end as total_price,
	case when lfp.points + sp.points + stp.points + rp.points + pp.points is null then 0 else lfp.points + sp.points + stp.points + rp.points + pp.points end as points,
  ji.created_at,
  ji.claimed_at,
  extract(date from o.approved_at at time zone 'America/Los_Angeles') as approved_at,
  ji.completed_at,
  datetime_diff(datetime(timestamp(ji.completed_at)), datetime(timestamp(ji.created_at)), minute) as turnoaround_time,
  ji.status as quote_status,
  jc.tot_quotes_sent,
  case when jc.tot_quotes_sent = 1 then 1 when jc.tot_quotes_sent > 1 then 1 else 0 end as new_quotes_sent,
  case when jc.tot_quotes_sent = 1 then 0 when jc.tot_quotes_sent > 1 then jc.tot_quotes_sent - 1 else 0 end as requotes_sent,
  jc.open,
  jc.in_progress,
  jc.awaiting_input_from_cs,
  jc.awaiting_input_from_construction,
  jc.in_qa,
  jc.requote,
  jc.canceled,
  ql.complications,
  case when ql.complications like '%tree_removal%' then 'Yes' else 'No' end as complication_tree_removal,
  case when ql.complications like '%permit-needed%' then 'Yes' else 'No' end as complication_permit_needed,
  case when ql.complications like '%steps%' then 'Yes' else 'No' end as complication_steps,
  case when ql.complications like '%painted_fence%' then 'Yes' else 'No' end as complication_painted_fence,
  case when ql.complications like '%other%' then 'Yes' else 'No' end as complication_other,
  case when o.approved_quote_id > 0 then 'Yes' else 'No' end as approved_order,
  product_id_main,
  lost_reason,
  qa.final_score,
  qa.hand_off_score,
  qa.offering_score,
  qa.configuration_score,
  qa.complications_score,
  qa.missed_elements_score,
  qa.regulations_score,
  qa.internal_comm_score,
  qa.drawings_score,
  qa.presentation_score,
  qa.reviewer,
  qa.risk
from orders o
left join quotelines ql on ql.id = o.quote_id
join ext_quote.quote_point_system lfp on lfp.category = 'linear_feet' and lfp.min_value <= case when ql.linear_feet is null then 0 else ql.linear_feet end and lfp.max_value >= case when ql.linear_feet is null then 0 else ql.linear_feet end
join ext_quote.quote_point_system sp on sp.category = 'sides' and sp.min_value <= case when ql.no_of_sides is null then 0 else ql.no_of_sides end and sp.max_value >= case when ql.no_of_sides is null then 0 else ql.no_of_sides end
join ext_quote.quote_point_system stp on stp.category = 'styles' and stp.min_value <= case when ql.no_of_styles is null then 0 else ql.no_of_styles end and stp.max_value >= case when ql.no_of_styles is null then 0 else ql.no_of_styles end
join ext_quote.quote_point_system rp on rp.category = 'revisions' and rp.min_value <= case when o.no_of_revisions_sent is null then 0 else o.no_of_revisions_sent end and rp.max_value >= case when o.no_of_revisions_sent is null then 0 else o.no_of_revisions_sent end
join ext_quote.quote_point_system pp on pp.category = 'price' and pp.min_value <= case when ql.total_price is null then 0 else ql.total_price end and pp.max_value >= case when ql.total_price is null then 0 else ql.total_price end
left join jira_issues ji on ji.jira_order_id = cast(o.order_id as string)
left join order_aggregate jc on jc.jira_order_id = o.order_id
left join estimation_qa qa on qa.order_id = o.order_id
-- after exclusion of warranty work orders there are still about 50 duplicate jira issues
),
all_data2 as
(
  select 
  ad.*,
  nullif(case
    -- Wood product type
    when (
      case when (
        case when wooden_fence = 'Yes' then 1 else 0 end 
        + case when fence_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when stain = 'Yes' then 1 else 0 end
    ) = 1
    and 
    (
      case when chainlink_fence = 'Yes' then 1 else 0 end
      + case when boxwire_fence = 'Yes' then 1 else 0 end
      + case when chainlink_gate = 'Yes' then 1 else 0 end
      + case when boxwire_gate = 'Yes' then 1 else 0 end
      + case when retaining_wall = 'Yes' then 1 else 0 end
      + case when standalone_retaining_wall = 'Yes' then 1 else 0 end
      + case when repair = 'Yes' then 1 else 0 end
    ) = 0 then 1 -- Wood
    
    -- Repairs, Chainlink, Boxwire product type
    when (
      case when wooden_fence = 'Yes' then 1 else 0 end 
      + case when fence_gate = 'Yes' then 1 else 0 end
      + case when stain = 'Yes' then 1 else 0 end
    ) = 0
    and
    (
      case when (
        case when chainlink_fence = 'Yes' then 1 else 0 end 
        + case when chainlink_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when (
        case when boxwire_fence = 'Yes' then 1 else 0 end 
        + case when boxwire_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when retaining_wall = 'Yes' then 1 else 0 end
      + case when standalone_retaining_wall = 'Yes' then 1 else 0 end
      + case when repair = 'Yes' then 1 else 0 end
    ) = 1 then 2
    
    -- Custom product type
    when (
      case when wooden_fence = 'Yes' then 1 else 0 end 
      + case when fence_gate = 'Yes' then 1 else 0 end
      + case when stain = 'Yes' then 1 else 0 end
    ) = 0
    and
    (
      case when chainlink_fence = 'Yes' then 1 else 0 end
      + case when boxwire_fence = 'Yes' then 1 else 0 end
      + case when chainlink_gate = 'Yes' then 1 else 0 end
      + case when boxwire_gate = 'Yes' then 1 else 0 end
      + case when retaining_wall = 'Yes' then 1 else 0 end
      + case when standalone_retaining_wall = 'Yes' then 1 else 0 end
      + case when repair = 'Yes' then 1 else 0 end
    ) > 1 then 3
    
    -- Multiple product type
    when (
      case when (
        case when wooden_fence = 'Yes' then 1 else 0 end
        + case when fence_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when (
        case when chainlink_fence = 'Yes' then 1 else 0 end 
        + case when chainlink_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when (
        case when boxwire_fence = 'Yes' then 1 else 0 end 
        + case when boxwire_gate = 'Yes' then 1 else 0 end
      ) > 0 then 1 else 0 end
      + case when retaining_wall = 'Yes' then 1 else 0 end
      + case when standalone_retaining_wall = 'Yes' then 1 else 0 end
      + case when repair = 'Yes' then 1 else 0 end
    ) > 1 then 4
    else 0 end,0) as product_type,

from all_data ad
)
select 
  ad.*,
  ad.points + coalesce(ad.product_type,0) as points_tot,
  l.points as quote_level,
  sh.*,
  case when product_type = 1 then 'Wood' 
       when product_type = 2 then 'Repairs, Chainlink, Boxwire' 
       when product_type = 3 then 'Custom' 
       when product_type = 4 then 'Multiple' else '' 
       end as product_type_str
from all_data2 ad
join quote_level l on l.min_value <= case when ad.points + ad.product_type is null then 0 else ad.points + ad.product_type end and l.max_value >= case when ad.points + ad.product_type is null then 0 else ad.points + ad.product_type end
left join ext_quote.staff_house sh on sh.email = ad.estimator_email