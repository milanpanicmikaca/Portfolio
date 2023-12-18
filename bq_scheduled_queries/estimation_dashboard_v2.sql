CREATE TEMPORARY FUNCTION intersection(x ARRAY<INT64>, y ARRAY<INT64>)
RETURNS ARRAY<INT64>
LANGUAGE js AS """
  var res =  x.filter(value => -1 !== y.indexOf(value));
  return res;
;
""";
with
quote_level as 
(
	(select 3 as min_value, 5 as max_value, 'Low' as category) union all
	(select 6 as min_value, 8 as max_value, 'Medium' as category) union all
	(select 9 as min_value, 18 as max_value, 'High' as category) 
),
quotelines_products 
as
(
select
    q.id,
    array_agg(distinct case when catalog_id in (352, 353, 354, 416, 417, 418) then 'hardscape'
        when catalog_id in (370, 371, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438) then 'vinyl'
        when catalog_id in (393, 394) then 'chainlink'
        when catalog_id in (409, 410) then 'boxwire'
        when catalog_id in (281, 395, 454) then 'stain' 
        when catalog_id = 396 then 'repairs' 
        when catalog_id in (357,358,359,360,361,362,363,364,365,366,367,368,376,392,406,414,415) then 'wood'
        end) as catalog_products,
  	from ergeon.quote_quoteline ql
	left join ergeon.quote_quote q on q.id = ql.quote_id
  left join ergeon.product_catalog pc on pc.id = ql.catalog_id 
  left join ergeon.product_catalogtype pct on pct.id = pc.type_id
  where 
  case when catalog_id in (352, 353, 354, 416, 417, 418) then 'hardscape'
        when catalog_id in (370, 371, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438) then 'vinyl'
        when catalog_id in (393, 394) then 'chainlink'
        when catalog_id in (409, 410) then 'boxwire'
        when catalog_id in (281, 395, 454) then 'stain' 
        when catalog_id = 396 then 'repairs' 
        when catalog_id in (357,358,359,360,361,362,363,364,365,366,367,368,376,392,406,414,415) then 'wood' end is not null
  and is_cancellation = False
  group by 1
),
quotelines as 
(
  -- gets per quote: linear_feet, sides, styles, total price, items, and complications
	select
		-- ql.*,
		q.id as quote_id,
    max(approved_quote_id) as approved_quote_id,
            -- change with regex
		nullif(sum(case when ql.label in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then ql.quantity else 0 end),0) as linear_feet,
		nullif(sum(case when ql.price > 0 then 1 else 0 end),0) as no_of_sides,
		nullif(count(distinct ql.quote_style_id),0) as no_of_styles, 
		nullif(sum(ql.price),0) as total_price,
    STRING_AGG(distinct pct.item) AS items,
    ARRAY_AGG(distinct ql.catalog_id) as catalog_items,
    max(replace(replace(replace(JSON_EXTRACT(q.calc_input,'$.complications'),'"',''),'[',''),']','')) as complications
	from ergeon.quote_quoteline ql
	left join ergeon.quote_quote q on q.id = ql.quote_id
	left join ergeon.store_order o on o.id = q.order_id
	left join ergeon.hrm_staff hs on hs.id = o.sales_rep_id 
  left join ergeon.product_catalog pc on pc.id = ql.catalog_id 
  left join ergeon.product_catalogtype pct on pct.id = pc.type_id
  where is_cancellation = False
	group by 1
),
quotes as
(
  -- quotes from ergeon dataset, ranked
	select 
		rank() over (partition by o.id order by q.created_at) as rank_quote,
      rank() over (partition by o.id order by q.sent_to_customer_at desc) as rank_last_quote,
		q.*,
		concat('https://admin.ergeon.in/quoting-tool/',o.id,'/quote/',q.id) as admin_link,
		coalesce(up.full_name,ue.full_name) as estimator,
        coalesce(up.email,ue.email) as estimator_email,
        coalesce(hrm.id,he.id) as staff_id,
		    uc.full_name as cs,
        cs.label as quote_status,
        o.pipedrive_deal_key,
        ca.full_name as cancelled_by,
        cc.full_name as customer_name,
        qp.catalog_products,
        -- hsg.team_lead_id,
        -- tml.team_name as house
    from ergeon.store_order o
    left join ergeon.quote_quote q on q.order_id = o.id
    left join ergeon.hrm_staff hs on hs.id = o.sales_rep_id 
    left join ergeon.core_user uc on uc.id = hs.user_id  
    left join ergeon.core_user ue on ue.id = q.sent_to_customer_by_id
    left join ergeon.hrm_staff  hrm on hrm.id = q.preparation_completed_by_id
    left join ergeon.core_user up on hrm.user_id = up.id
    left join ergeon.core_statustype cs on cs.id = q.quote_status_id
    left join ergeon.core_user ca on ca.id = q.cancelled_by_id
    left join ergeon.hrm_staff he on he.user_id = ue.id
    left join ergeon.core_house h on h.id = o.house_id
    left join ergeon.customers_customer c on c.id = h.customer_id
    left join ergeon.customers_contact cc on cc.id = c.contact_id
    left join quotelines_products qp on qp.id = q.id
  where is_cancellation = False
),
jira_issue_per_order as
(
select
    admin_order_number as order_id,
    --safe_cast(REGEXP_REPLACE(left(ji.admin_link,45),'[^0-9 ]','') as INT64) as order_id, --old methodology of getting an order_id. This extract will give only scoping task jira card link
    min(ji.id) as first_jira_id
from jira.issue ji
group by 1
),
rank_completed_quote as
(
  select
    id,
    rank() over (partition by order_id order by sent_to_customer_at) as rank_completed_quote
  from quotes
  where sent_to_customer_at is not null and not (cancelled_at is not null and cancelled_by = estimator)

),
detailed as
(
select
  q.order_id,
	q.id as quote_id,
  od.market,
	q.admin_link,
  concat('https://api.ergeon.in/public-admin/quote/quote/',q.id,'/change/') as new_admin_link,
  concat('https://ergeon.pipedrive.com/deal/',q.pipedrive_deal_key) as pipedrive_deal_link,
  concat('https://ergeon.atlassian.net/browse/',ji.key) as jira_link,
	q.estimator,
    q.estimator_email,
    q.staff_id,
    staff_image,
    q.customer_name,
	q.cs,
	case when ql.linear_feet is null then 0 else ql.linear_feet end as linear_feet,
	case when ql.no_of_sides is null then 0 else ql.no_of_sides end as no_of_sides,
	case when ql.no_of_styles is null then 0 else ql.no_of_styles end as no_of_styles,
  case when od.type = '/Commercial' then true else false end as is_commercial,
  ql.items as products,
  q.catalog_products[safe_offset(0)] as products_array,
--  q.catalog_products as products_array,
  case when ql.items like 'fence-side%' or ql.items like '%,fence-side%' 
        or ql.items like 'fence-gate%' or ql.items like '%,fence-gate%' then true else false end as wooden_fence,
  case when ql.items like '%fence-staining%' then true else false end as stain,
  case when ql.items like '%fence-repairs%' then true else false end as repair,
  case when ql.items like '%retaining-wall%' then true else false end as retaining_wall,
  case when ql.items like '%standalone-rw%' then true else false end as standalone_retaining_wall,
  case when ql.items like '%cl-fence-side%' or ql.items like '%cl-fence-gate%' then true else false end as chainlink,
  case when ql.items like '%bw-fence-side%' and ql.items like '%bw-fence-gate' then true else false end as boxwire,
  case when ql.items like '%vinyl-fence-side%' and ql.items like '%vinyl-fence-gate' then true else false end as vinyl,
  case when ql.items like '%fence-side-custom%' then true else false end as fence_custom,
  case when ql.items like '%sales-discount-fence%' then true else false end as fence_discount,
	case when ql.total_price is null then 0 else ql.total_price end as total_price,
  coalesce(q.total_cost,0) as total_cost, 
  coalesce(case when array_length(intersection(coalesce(catalog_items,[0]), [370, 371, 409, 410, 393, 394, 396, 352, 353, 354])) > 0 then 4 else stp.points end + sp.points + ipp.points,0) as points,
  extract(date from q.created_at at time zone "America/Los_Angeles") as created_at,
  coalesce(extract(date from q.preparation_requested_at at time zone "America/Los_Angeles"),extract(date from q.created_at at time zone "America/Los_Angeles")) as preparation_requested_at,
  od.won_at as approved_at,
  extract(date from q.sent_to_customer_at at time zone "America/Los_Angeles") as completed_at,
  timestamp_diff(q.sent_to_customer_at, coalesce(q.preparation_requested_at,q.created_at), minute) as turnaround_time,
   q.quote_status as quote_status,
  ql.complications,
  case when ql.complications like '%tree_removal%' then true else false end as complication_tree_removal,
  case when ql.complications like '%permit-needed%' then true else false end as complication_permit_needed,
  case when ql.complications like '%steps%' then true else false end as complication_steps,
  case when ql.complications like '%painted_fence%' then true else false end as complication_painted_fence,
  case when ql.complications like '%other%' then true else false end as complication_other,
  case when q.approved_at is not null then true else false end is_approved,
  case when od.product_quoted like '%Fence Installation%' then 'Fence'
       when od.product_quoted like '%Driveway Installation%' then 'Driveway' end as main_product,
  lost_reason,
  q.cancelled_by,
  extract(date from q.cancelled_at at time zone "America/Los_Angeles") as cancelled_at,
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
  qa.qa_not_passed_notification_needed,
  qa.reviewer,
  qa.risk,
  case when risk = 'Low' then 1 else 0 end as risk_low,  
  case when risk = 'Medium' then 1 else 0 end as risk_medium,
  case when risk = 'High' then 1 else 0 end as risk_high,
  case when risk = 'Ultra' then 1 else 0 end as risk_ultra,
  case 
       when rank_completed_quote = 1 then 'new_quote'
       when rank_completed_quote > 1 then 'requote'
       end as quote_class,
  case when approved_quote_id > 0 then 'Yes' else 'No' end as approved_order,
  rank_last_quote,
  rank_completed_quote,
  q.is_scope_change
from quotes q
left join `bigquerydatabase-270315.int_data.order_ue_materialized` od on od.order_id = q.order_id
left join quotelines ql on ql.quote_id = q.id
left join googlesheets.estimation_qa qa on safe_cast(regexp_extract(qa.admin_link, r'quote/(\d+)') as INT64) = q.id
left join jira_issue_per_order j on j.order_id = q.order_id
left join jira.issue ji on ji.id = j.first_jira_id
left join rank_completed_quote rc on rc.id = q.id
left join ext_quote.staff_house sh on sh.email = q.estimator_email
left join ext_quote.quote_point_system_v2 sp on sp.category = 'sides' and sp.min_value <= coalesce(ql.no_of_sides,0) and sp.max_value >= coalesce(ql.no_of_sides, 0)
left join ext_quote.quote_point_system_v2 stp on stp.category = 'styles' and stp.min_value <= coalesce(ql.no_of_styles,0) and stp.max_value >= coalesce(ql.no_of_styles, 0)
left join ext_quote.quote_point_system_v2 ipp on ipp.category = 'installer_pay' and ipp.min_value <= coalesce(q.total_cost,0) and ipp.max_value >= coalesce(q.total_cost, 0)

),
staff_details as (
select 
  staff_id,
  hs.id,
  hsg.team_id,
  ht.name as team_name,
  email,
  full_name,
  hsg.modified_at,
  hsg.created_at,
  effective_date,
  Rank() OVER (PARTITION BY hsg.staff_id order by hsg.created_at desc)rn
from ergeon.hrm_staff hs
left join ergeon.hrm_stafflog hsg on hs.id = hsg.staff_id
left join ergeon.hrm_team ht on ht.id = hsg.team_id
where hsg.team_id is not null and hsg.change_type in ('hired','changed')
qualify Rank() OVER (PARTITION BY staff_id order by created_at desc) = 1
), final as (
select
  f.* except(points, rank_completed_quote),team_name as house,
  case when rank_completed_quote = 1 then points
       when rank_completed_quote > 1 and is_scope_change is true then points
       else points * 0.50 end as points,
  case when quote_class = 'new_quote' then 1 else 0 end as new_quote,
  case when quote_class = 'requote' then 1 else 0 end as requote,
  case when completed_at is not null then 1 else 0 end as completed_quotes,
  case when is_approved = true then 1 else 0 end as approved_quote,
  case when rank_last_quote = 1 then 1 else 0 end is_last_quote,
  case when boxwire = true  then 'boxwire'
       when chainlink = true then 'chainlink'
       when vinyl = true then 'vinyl'
       when wooden_fence = true then 'wood' else 'other' end as product,
  turnaround_time/60 as turnaround_time_hrs,
  case when qa_not_passed_notification_needed  is not null then 1 else 0 end as qa_analysed,
case when  qa_not_passed_notification_needed = 'yes' then 1 else 0 end as pre_escalation,
case when final_score <= 80 then 'score below_80'
    when final_score >80 and final_score < 90 then 'score between 80 and 90'
    else 'score greater than 90' end as score_grouping,
--  case when is_last_quote = true and is_approved = 1 then 1 else 0 end)/sum(case when is_last_quote = true then 1 else 0 end)
  l.category,
  rank() over(partition by order_id order by quote_id desc) as quote_rank
from detailed f
left join quote_level l on l.min_value <= points and l.max_value >= points
left join staff_details as sd on sd.staff_id = f.staff_id --sd.email = f.estimator_email
where
  not (f.cancelled_at is not null and f.cancelled_by = f.estimator)
and (estimator_email <> 'alexeyevseev@ergeon.com' and estimator_email <> 'yolaina.guillen@ergeon.com')
order by order_id asc 
), pgr_info as (
  select 
    order_id,
    count(*) as onsites_booked,
    sum(case when canceled is false then 1 else 0 end) as onsites_active,
    sum(case when canceled is true then 1 else 0 end) as onsites_canceled,
    from int_data.estimations_dashboard_photographersdetail d
    group by 1
)
select 
f.* ,
d.onsites_booked,
onsites_active,
onsites_canceled,
from final f
left join pgr_info d on d.order_id = f.order_id and f.quote_rank = 1
where house in ('Grand Central','Tjibaou House','Playhouse')