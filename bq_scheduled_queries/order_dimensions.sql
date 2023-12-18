#standardSQL Creatte custom function to use parameter inside javascript
CREATE TEMPORARY FUNCTION CUSTOM_JSON_EXTRACT(json STRING, json_path STRING)
RETURNS STRING
LANGUAGE js AS """
    try { var parsed = JSON.parse(json);
        return JSON.stringify(jsonPath(parsed, json_path));
    } catch (e) { returnnull }
"""
OPTIONS (
    library="gs://custom-function/jsonpath-0.8.0.js"
);
with oleads as 
( -- making sure we only grab one lead per order - the first one
select 
  order_id, min(id) as lead_id 
from 
  ergeon.core_lead l
where 
  created_at >= '2018-04-16' 
group by 1
),
leads as 
( --we only grab one lead per order - the first one (excluding duplicate/fakes/tests)
select 
  order_id, min(id) as lead_id 
from 
  ergeon.core_lead l
where 
  created_at >= '2018-04-16' 
  and  (l.phone_number is not null or l.email is not null)
  and lower(l.full_name) not like '%test%' and lower(coalesce(l.email,'')) not like '%test%' --8/9/2022 change
  and lower(l.full_name) not like '%fake%' and lower(coalesce(l.email,'')) not like '%fake%' --8/9/2022 change
  and lower(l.full_name) not like '%duplicate%'
group by 1
),
min_lead_service as --in cases of a lead with multiple services we grab the one with the smallest ID 
(
select 
	lead_id,
	min(cls.id) as first_lead_service
from 
	ergeon.core_lead_services cls
group by 1
),
approved_boolean as 
(
select 
  order_id,min(approved_at) as approved 
from 
  ergeon.quote_quote 
where 
  sent_to_customer_at is not null 
  and created_at > '2018-04-15' 
  and is_cancellation = False
group by 1
),
approved_quotes as 
(
select 
  qq.order_id,
  id, 
  approved,
  if((calc_input) LIKE '%cad_objects%',1,0) as is_draft_editor,
  if(approved_at is null,null,rank() over(partition by qq.order_id order by coalesce(approved_at,'2100-01-01'))) as approved_rank,
  rank() over(partition by qq.order_id order by sent_to_customer_at,id) as non_approved_rank
from 
  ergeon.quote_quote qq left join 
  approved_boolean a on a.order_id = qq.order_id
where 
  sent_to_customer_at is not null 
  and created_at > '2018-04-15'
  and is_cancellation = False
),
approved_quotes_last as 
(
select 
  qq.order_id,
  id, 
  approved,
  if(approved_at is null,null,rank() over(partition by qq.order_id order by coalesce(approved_at,'2000-01-01') desc,id desc)) as approved_rank,
  rank() over(partition by qq.order_id order by sent_to_customer_at desc,id desc) as non_approved_rank 
from 
  ergeon.quote_quote qq left join 
  approved_boolean a on a.order_id = qq.order_id
where 
  sent_to_customer_at is not null 
  and created_at > '2018-04-15'
  and qq.cancelled_at is null 
  and is_cancellation = False
),
ofq_last as 
( --find quote id of the last approved (if approved) or sent to customer quote
select 
  order_id,id as quote_id
from 
  approved_quotes_last 
where 
  (approved is null and non_approved_rank = 1) or (approved is not null and approved_rank = 1)
),
ofq as 
( --find quote id of the first approved (if approved) or sent to customer quote
select 
  order_id,id as quote_id,is_draft_editor
from 
  approved_quotes 
where 
  (approved is null and non_approved_rank = 1) or (approved is not null and approved_rank = 1)
),
qlt as 
(
  select 
    ql.id, 
    coalesce(tys.service_id,ty.service_id,qlt.service_id) as service_id,
    coalesce(tys.item,ty.item,qlt.item) as lst_catalog_type
  from 
    ergeon.quote_quoteline ql left join 
    ergeon.quote_quotestyle qs on qs.id = ql.quote_style_id left join
    ergeon.product_catalog sct on sct.id = qs.catalog_id left join
    ergeon.product_catalog ct on ct.id = ql.catalog_id left join
    ergeon.product_catalogtype ty on ty.id = ct.type_id left join
    ergeon.product_catalogtype tys on tys.id = sct.type_id left join 
    ergeon.product_catalogtype qlt on qlt.id = ql.catalog_type_id left join
    ergeon.quote_quote qq on qq.id = ql.quote_id 
  where
    is_cancellation = False
),
ofq_services as
( --rank service of an order according to the total price of each quoted service
select 
  order_id,service_id, 
  rank() over (partition by order_id order by sum(price) desc,coalesce(service_id,10000)) as rank
from 
  ofq join 
  ergeon.quote_quoteline ql on ql.quote_id = ofq.quote_id left join 
  qlt on qlt.id = ql.id
group by 1,2
),
ofq_service as 
( --Grab the service id with the highest total amount of quoted price for each order 
select 
  order_id,service_id 
from 
  ofq_services 
where 
  rank = 1
),
ot as --find the last tier attributed to the order 
( 
select 
  ofq.order_id, tier_id 
from 
  ofq join 
  ergeon.quote_quote q on ofq.quote_id = q.id
), 
ha_unique as -- need to use ha_spent_flat records if visitor action missing-- join by email/earliest lead whose date is before the order
( 
select 
  email, 
  srOid,  
  min(date) as date, 
  min(lead_description) as lead_description
from 
  ext_marketing.ha_spend_flat ha
group by 1,2
qualify rank() over(partition by email order by srOid) = 1
),
lost_reason as 
( --reason of lost leads
select 
  o.id, 
  lr.label as lost_reason,
  lost_reason_text 
from 
  ergeon.store_order o left join 
  ergeon.store_orderlostreason lr on lr.id = o.lost_reason_id
where 
  lost_reason_id is not null
),
core_issues as (
  select 
  	 escalation_id as id,
  	 count(*) as scoping_count
  from ergeon.store_escalation_core_issues
  where escalationcoreissue_id in (66,67)
  group by 1
),
escalation_status as ( 
  select 
  	id,
  	count(*) scoping_count
  from ergeon.store_escalation se 
  where current_status_bucket = 'scoping_process'
  group by 1
),
scoping_task_escalations as 
(
select 
  id as escalation_id
from ergeon.store_escalation se
left join core_issues ci using(id)
left join escalation_status es using(id)
where (ci.scoping_count > 0
  or es.scoping_count > 0)
  and se.deleted_at is null
),
escalations as (
select
   order_id,
   count(*) as escalation_count
from 
  ergeon.store_escalation se left join
  scoping_task_escalations ste on ste.escalation_id = se.id
where 
  se.deleted_at is null
  and ste.escalation_id is null
group by 1 
),
multi_party as ( --customers per order
select 
  order_id,
  count(distinct(cc.id)) as customers
from 
  ergeon.quote_quoteapproval qq2  left join 
  ergeon.quote_quote qq on qq2.quote_id = qq.id left join 
  ergeon.store_order o on qq.order_id = o.id left join 
  ergeon.customers_customer cc on qq2.customer_id = cc.id
where 
  qq.approved_at is not null 
  and qq.cancelled_at is null 
  and qq2.deleted_at is null
  and o.created_at >'2018-04-16'
  and is_cancellation = False
group by 1
),
va as 
( --Channel attribution to orders
select 
  cv.id,
  c.customer_id,
case
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%yelp%' then '/Paid/Yelp/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%home%advisor%' then
    case 
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%ads%' then '/Paid/Home Advisor/Ads'
      else '/Paid/Home Advisor/'
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%thumbtack%' then '/Paid/Thumbtack'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%nextdoor%' then '/Paid/Nextdoor'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%lawson%'  then '/Paid/Lawson'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%borg%'  then '/Paid/Borg'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%bark%'  then '/Paid/Bark'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%facebook%' then 
    case 
      when json_extract_scalar(cv.event_object,'$.utm_campaign') is not null and 
             cv.landing_page like '%ergeon.com/blog/%' then '/Non Paid/Facebook/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))
      else '/Paid/Facebook/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%instagram%' or
       lower(json_extract_scalar(cv.event_object,'$.browser_name')) like '%instagram%' then 
    case
      when json_extract_scalar(cv.event_object,'$.utm_campaign') is not null and 
          cv.landing_page like '%ergeon.com/blog/%' then '/Non Paid/Facebook/Instagram/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))
      when cv.landing_page like '%ergeon.com/' or cv.landing_page like '%ergeon.com/?fbclid%' then '/Non Paid/Facebook/Instagram/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))     
      else '/Paid/Facebook/Instagram/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),'')) 
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%directmail%'  then '/Non Paid/Direct Mail/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%email%' or
       lower(json_extract_scalar(cv.event_object,'$.initial_referrer')) like '%android.gm%' then '/Non Paid/Email Marketing/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_source'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),'')) --added initial_referrer android.gm
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%bing%' then '/Paid/Bing'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%google%' or lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%gmb%' then
    case 
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%gls%' then '/Paid/Google/GLS/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))
      when lower(cv.landing_page) like '%gclid%' or
           lower(cv.landing_page) like '%gbraid%' or
           lower(cv.landing_page) like '%wbraid%' or
           (lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%ads%' and 
           lower(json_extract_scalar(cv.event_object,'$.utm_medium')) like '%call%') then '/Paid/Google/Ads/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),'')) 
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%gmb%' or 
           lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%gmb%' then '/Non Paid/Google/GMB/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),''))  
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%ls%brand%' then '/Non Paid/Google/Direct/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),"")) --changed brand to ls-brand  
      when cv.landing_page is null and lower(json_extract_scalar(cv.event_object,'$.utm_medium')) like '%website%' then '/Non Paid/Google/SEO/cities/website_wp_form'
      when cv.landing_page is null then '/Non Paid/Google/Direct/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),"")) --added
      when lower(cv.landing_page) like '%ergeon.com/' then '/Non Paid/Google/Direct/organic/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),"")) --added
      when lower(cv.landing_page) like '%ergeon.com/blog%' then '/Non Paid/Google/SEO/blog/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/cities%' then '/Non Paid/Google/SEO/cities/website_wp' --took place only until July
      when lower(cv.landing_page) like '%ergeon.com/%cities%' then '/Non Paid/Google/SEO/cities/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/%gallery%' then '/Non Paid/Google/SEO/gallery/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),'')) --gallery and projects-gallery
      when lower(cv.landing_page) like '%ergeon.com/contacts%' then '/Non Paid/Google/SEO/contacts/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/fences%' then '/Non Paid/Google/SEO/fences/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/locations%' then '/Non Paid/Google/SEO/locations/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/careers%' then '/Non Paid/Google/SEO/careers/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/%grass%' then '/Non Paid/Google/SEO/grass/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),''))
      when lower(cv.landing_page) like '%ergeon.com/%' and cv.referrer is not null then '/Non Paid/Google/SEO/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),"")) --added
      else '/Non Paid/Google/Unknown/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) = 'direct' then
    case 
      when lower(json_extract_scalar(cv.event_object,'$.utm_medium')) = 'webchat' then '/Non Paid/Direct/Webchat'
      else '/Non Paid/Direct'
    end 
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%sdr%' then '/Paid/SDR/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))
  else '/Non Paid/Unknown/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_source'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_medium'),""))||'/'||lower(coalesce(json_extract_scalar(cv.event_object,'$.utm_campaign'),""))
end as channel,
  lower(json_extract_scalar(cv.event_object,'$.utm_medium')) as utm_medium,
  case when lower(json_extract_scalar(cv.event_object,'$.lead_type')) like '%business%' then '/C' else '/R' end as ha_type,
  case when (l.service_location_residence = false or l.service_customer_person = false or s.code = 'asphalt_paving') then '/C' else '/R' end as customer_lead_channel,
  regexp_extract(json_extract(event_object, '$.job_id'),r"[0-9]+") as ha_job_id
from 
  ergeon.core_lead l join 
  ergeon.customers_visitoraction cv on cv.id = l.visitor_action_id join 
  ergeon.customers_contact c on c.id = l.contact_id left join
  min_lead_service ml on l.id = ml.lead_id left join 
  ergeon.core_lead_services cls on cls.id = ml.first_lead_service left join 
  ergeon.product_service s on s.id = cls.service_id
),
cva1 as 
( --Grab 1st channel historically of the customer 
select 
  customer_id, 
  min(id) as visitor_action_id 
from 
  va 
group by 1
), 
droplines as 
(
select
	order_id,
	qdl.quote_line_id 
FROM ergeon.quote_quote q
left join ergeon.quote_quotedropline qdl on qdl.quote_id = q.id
where 
	approved_at is not null
	and qdl.quote_line_id is not null
),
project_lines as 
(
select 
	q.order_id,
  q.id as quote_id,
	ql.id as quoteline_id
FROM ergeon.quote_quote q
left join ergeon.quote_quoteline ql on ql.quote_id = q.id
left join droplines dl on dl.quote_line_id = ql.id
where 
	approved_at is not null
	and dl.quote_line_id is null
),--linear feet of each quote
length_quote as 
(
select 
  qq.id,
  sum(distinct(coalesce(case 
    when lst_catalog_type in ('fence-side', 'cl-fence-side', 'vinyl-fence-side', 'bw-fence-side' ) and map.array ='sides' then coalesce(round(st_distance(st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng1]')),r'["\[\]]', '') AS FLOAT64),
    SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat1]')),r'["\[\]]', '') AS FLOAT64)),
    st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng2]')),r'["\[\]]', '') AS FLOAT64),
    SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat2]')),r'["\[\]]', '') AS FLOAT64)))*3.280839895,2),0) 
  end,0))) as distance,
  sum(case 
      when map.array ='polygons' and lower(calc_input) like ('%cad_objects%') then SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.polygons[',coalesce(index,0),'].area')),r'["\[\]]', '') AS FLOAT64) end) as sqft
from 
  ergeon.quote_quoteline qql left join
  int_data.calc_index_mapping map on qql.label = map.label left join
  ergeon.quote_quote qq on qql.quote_id = qq.id left join
  qlt on qlt.id = qql.id
where 
  map.label is not null
  and is_cancellation = False
group by 1
), 
length_quote_last_approved as 
(
select 
  pl.order_id,
  sum(distinct(coalesce(case 
    when lst_catalog_type in ('fence-side', 'cl-fence-side', 'vinyl-fence-side', 'bw-fence-side' ) and map.array ='sides' then coalesce(round(st_distance(st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng1]')),r'["\[\]]', '') AS FLOAT64),
    SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat1]')),r'["\[\]]', '') AS FLOAT64)),
    st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng2]')),r'["\[\]]', '') AS FLOAT64),
    SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat2]')),r'["\[\]]', '') AS FLOAT64)))*3.280839895,2),0) 
  end,0))) as distance,
  sum(case 
      when map.array ='polygons' and lower(calc_input) like ('%cad_objects%') then SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.polygons[',coalesce(index,0),'].area')),r'["\[\]]', '') AS FLOAT64) end) as sqft
from 
	project_lines pl left join
  ergeon.quote_quoteline qql on qql.id = pl.quoteline_id left join
  int_data.calc_index_mapping map on qql.label = map.label left join
  ergeon.quote_quote qq on pl.quote_id = qq.id left join
  qlt on qlt.id = qql.id
where 
  map.label is not null
  and is_cancellation = False
group by 1
),
length_quote_gates as 
(
select 
  quote_id,
  sum(gate_length) as gate_length
from 
  int_data.order_ql_materialized
group by 1
),
length_order as --linear feet of fence for orders with fence approved quote
(
select 
  ofq_last.order_id,coalesce(la.distance,lq.distance) as distance,coalesce(la.sqft,lq.sqft) as sqft,gate_length 
from 
  ofq_last left join 
  length_quote lq on lq.id = ofq_last.quote_id left join
  length_quote_gates lqg on lqg.quote_id = ofq_last.quote_id left join
	length_quote_last_approved la on la.order_id = ofq_last.order_id
where 
  coalesce(la.distance,lq.distance) is not null or coalesce(la.sqft,lq.sqft) is not null
),
won_date as 
( 
select 
  order_id, 
  min(approved_at) as won_at 
from 
  ergeon.quote_quote 
where 
  approved_at is not null 
  and created_at > '2018-04-15' 
  and is_cancellation = False
group by 1
),
quotes_per_order as 
(
select 
  order_id,
  sum(case when (sent_to_customer_at <= won_at or won_at is null) and (is_estimate = False) then 1 else 0 end) as quotes_sent_count,
  sum(case when approved_at > won_at and (lower(title) not like '%scop%') then 1 else 0 end) as change_order_count,
  sum(case when approved_at > won_at and lower(title) like '%scop%' and method = 'measured' then 1 else 0 end) as scoping_task_count,
  sum(case when approved_at > won_at and lower(title) like '%scop%' and method = 'measured' and total_price > 0 then 1 else 0 end) as qa_process_count,
  min(case when approved_at > won_at and lower(title) like '%scop%' and method = 'measured' then DATETIME(preparation_completed_at, "America/Los_Angeles") end) as scoping_task_at
from 
  ergeon.quote_quote q left join 
  won_date using(order_id)
where 
  q.sent_to_customer_at is not null 
  and q.created_at > '2018-04-15' 
  and is_cancellation = False
group by 1
),
last_approved_quotes as 
(
select 
  o.id as order_id,
  completed_at as cancelled_at,
  is_cancellation 
from 
  ergeon.store_order o join 
  ergeon.quote_quote q on q.order_id = o.id 
where 
  q.created_at >= '2018-04-16'
  and approved_at is not null
qualify rank() over(partition by o.id order by approved_at desc,q.id desc) = 1
),
cancelled_projects as 
(
select 
  * 
from last_approved_quotes 
where is_cancellation = TRUE
),
dir_events as
(
select 
  regexp_extract(json_extract(payload,"$.object.job_id"), r"[0-9]+") as ha_job_id,
  json_extract(payload,"$.object.other_pros_matched") as other_pros_matched,
  row_number() OVER (partition by id order by id) AS rn
from 
  directorbus.director_event 
where 
  created_at >= '2018-04-16' 
  and (event_name = 'lead:arrived' or event_name = 'email:parsed-ha-lead') 
  and json_extract(payload,"$.object.other_pros_matched") is not null
qualify rank() over (partition by regexp_extract(json_extract(payload,"$.object.job_id"), r"[0-9]+") order by created_at) = 1
),
waiver_orders as 
(
select  
  order_id,
  min(signoff_at) as waiver_signoff_at
from 
  ergeon.quote_quoteapproval qa left join
  ergeon.quote_quote qq on qq.id = qa.quote_id
where 
  qa.signoff_at is not null
  and qa.deleted_at is null
group by 1
)
select
  o.id as order_id,
  leads.lead_id,
  leads.lead_id is not null as is_lead,
  extract( date from o.created_at AT TIME ZONE 'America/Los_Angeles') as created_at,
  DATETIME(o.created_at, "America/Los_Angeles") as created_ts_at, --timestamp of created_at
  coalesce(extract(date from cp.cancelled_at AT TIME ZONE 'America/Los_Angeles'),extract(date from o.cancelled_at AT TIME ZONE 'America/Los_Angeles'), extract(date from cast(d.lost_time as timestamp) AT TIME ZONE 'America/Los_Angeles')) as cancelled_at,
  coalesce(DATETIME(cp.cancelled_at, "America/Los_Angeles"),DATETIME(o.cancelled_at, "America/Los_Angeles"),Datetime(cast(d.lost_time as timestamp),'America/Los_Angeles')) as cancelled_ts_at, --timestamp of created_at
  --coalesce(extract(date from o.cancelled_at AT TIME ZONE 'America/Los_Angeles'), extract(date from cast(d.lost_time as timestamp) AT TIME ZONE 'America/Los_Angeles')) as cancelled_at,
  case when cp.cancelled_at is null then date(o.marked_completed_at,"America/Los_Angeles") else null end as marked_completed_at,
  case --Assign Home Advisor as a channel whenever home advisor email is not null
    when va.channel = '/Paid/Home Advisor/' then va.channel ||coalesce(ha.lead_description, '')
    when va.channel is not null then va.channel
    when ha.email is not null then '/Paid/Home Advisor/'|| coalesce(ha.lead_description, '')        
    else '/Non Paid/Unknown'
  end as channel,
  case --Channel 2 (Attributing channel according to the first customer channel - whenever customer have multiple orders)
    when va1.channel = '/Paid/Home Advisor/' then coalesce(va1.ha_type,va1.customer_lead_channel)||va1.channel ||coalesce(ha.lead_description, '')       
    when va1.channel is not null then va1.customer_lead_channel||va1.channel
    when ha.email is not null then coalesce(va1.ha_type,va1.customer_lead_channel)||'/Paid/Home Advisor/'|| coalesce(ha.lead_description, '') 
    else va1.customer_lead_channel||'/Non Paid/Unknown'
  end as channel1,
  case when m.id is null or r.id is null  then '/Unknown' else '/'||r.name||'/'||m.code||'/'||cn.name end as geo, --/Region/Market/County
  case when (l.service_location_residence = false or l.service_customer_person = false or s.code = 'asphalt_paving') then '/C' else '/R' end as lead_channel,
  va.ha_type,
  case when c.is_commercial then '/Commercial' else '/Residential' end as type,
  case --/Product/Service according to lead atttributes 
    when s.label is not null then '/'||sp.name||'/'||s.label
    when p.id = 105 then '/'||p.name||'/'||'Install a Wood Fence'
    when p.id = 132 then '/'||p.name||'/'||'Install Artificial Grass'
    when p.id = 34 then '/'||p.name||'/Install Concrete Driveways & Floors'
  end as product,
  case --/Product/Service according to quoted product and service
    when os.label is not null then '/'||osp.name||'/'||os.label
    when s.label is not null then '/'||sp.name||'/'||s.label
    when p.id = 105 then '/'||p.name||'/'||'Install a Wood Fence'
    when p.id = 132 then '/'||p.name||'/'||'Install Artificial Grass'
    when p.id = 34 then '/'||p.name||'/Install Concrete Driveways & Floors'
  end as product_quoted,
  t.name as tier,
  lr.lost_reason,
  lr.lost_reason_text,
  sales_rep,sales_staff_id,sales_team,sales_title,
  project_manager,pm_team,
  pm_id,
  quoter,quoted_dep,delta,first_quoted_dept,last_quoted_dept,
  photographer,
  contractor,
  contractor_count,
  if(ef.order_id is null,0 ,1) as has_escalation,
  coalesce(msa.name,'Other-CA-MSA') as msa,
  coalesce(cmsa.name,'Other-CA-CMSA') as cmsa,
  ga.latitude,
  ga.longitude,
  case when parent_order_id is not null then true else false end as is_warranty_order,
  coalesce(m.code,'Unknown') as market,
  m.id as market_id,
  coalesce(r.name,'Unknown') as region,
  coalesce(concat(ci.name,', ',st.code),'Unknown') as city,
  coalesce(cn.name,'Unknown') as county,
  distance as total_length, 
  sqft,
  gate_length, 
  va.utm_medium,
  quotes_sent_count,
  change_order_count,
  scoping_task_count,
  qa_process_count,scoping_task_at,
  o.house_id,
  h.customer_id,
  cc.full_name as contact_name,
  st.name as state,
  case 
    when customers > 1 then 'yes'
    when customers = 1 then 'no'
  end as multi_party_approval,
  is_draft_editor,
  gz.id as zip_admin_id,
  gz.code as zipcode_id,
  a.median_income as zip_median_income,
  a.median_age as zip_median_age,
  a.owner_occupied_housing_units_median_value as zip_median_house_value,
  extract(year from o.created_at) - a.median_year_structure_built as zip_median_house_age,
  a.occupied_housing_units as zip_housing_units,
  a.total_pop as zip_total_population,
  coalesce(round(a.family_households / nullif(a.households,0),2),0) as zip_family_households,
  cast(dir.other_pros_matched as int64) as other_pros_matched,
  if(waiver_signoff_at is not null,1,0) as is_waiver
from 
  ergeon.store_order o left join 
  ot on ot.order_id = o.id left join 
  ergeon.product_tier t on t.id = ot.tier_id left join 
  oleads ol on ol.order_id = o.id left join 
  leads on leads.order_id = o.id left join 
  ergeon.core_lead l on l.id = ol.lead_id left join  
  ofq_service ofs on ofs.order_id = o.id left join 
  ergeon.product_service os on os.id = ofs.service_id left join 
  ergeon.store_product osp on osp.id = os.product_id left join 
  va on va.id = l.visitor_action_id left join 
  ergeon.core_house h on h.id = o.house_id left join 
  ergeon.customers_customer c on c.id = h.customer_id left join 
  ergeon.customers_contact cc on cc.id = c.contact_id left join
  cva1 on cva1.customer_id = h.customer_id left join 
  va va1 on va1.id = cva1.visitor_action_id left join 
  ergeon.geo_address ga on ga.id = h.address_id left join 
  ergeon.geo_county cn on cn.id = ga.county_id left join 
  ergeon.geo_city ci on ci.id =  ga.city_id left join 
  ergeon.geo_state st on st.id = ci.state_id left join 
  ergeon.geo_msa msa on msa.id = cn.msa_id left join 
  ergeon.geo_msa cmsa on cmsa.id = msa.msa_id left join 
  ergeon.product_countymarket pcnm on pcnm.county_id = cn.id left join 
  ergeon.product_market m on m.id = pcnm.market_id left join 
  ergeon.product_region r on r.id = m.region_id left join 
  min_lead_service ml on l.id = ml.lead_id left join 
  ergeon.core_lead_services cls on cls.id = ml.first_lead_service left join 
  ergeon.product_service s on s.id = cls.service_id left join 
  ergeon.store_product sp on s.product_id = sp.id left join 
  ergeon.store_product p on p.id = o.product_id left join 
  ha_unique ha on ha.email = l.email and ha.date <= extract( date from l.created_at AT TIME ZONE 'America/Los_Angeles') left join 
  lost_reason lr on lr.id = o.id  left join 
  int_data.order_staff_dimension sd on sd.order_id = o.id left join 
  escalations ef on ef.order_id = o.id left join 
  pipedrive.deal d on d.id = cast(o.pipedrive_deal_key as INT64) left join 
  length_order lo on lo.order_id = o.id left join 
  quotes_per_order qpo on qpo.order_id = o.id left join 
  multi_party mp on mp.order_id = o.id left join
  cancelled_projects cp on cp.order_id = o.id left join
  ofq on ofq.order_id = o.id left join 
  ergeon.geo_zipcode gz on gz.id = ga.zip_code_id left join
  census_bureau_acs.zip_codes_2018_5yr a on cast(a.geo_id as int64) = cast(gz.code as int64) left join--added to find correlation between those properties and CPA30
  dir_events dir on dir.ha_job_id = va.ha_job_id and dir.rn = 1 left join
  waiver_orders wo on wo.order_id = o.id
--left join cancellations can on can.order_id = o.id
