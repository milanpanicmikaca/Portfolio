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
  from
  (
  select
    jira_order_id,
    from_string as status,
    sum(case when to_string = 'Completed' then 1 else 0 end) as tot_quotes,
    sum(time_in_queue_minute) as time_in_status_minutes
  from
  (
  select
    c.*,
    safe_cast(REGEXP_REPLACE(left(adminLink,45),'[^0-9 ]','') as INT64) as jira_order_id,
    rank() over (partition by c.key order by c.created_at) as rank,
    datetime_diff(datetime(cast(c.created_at as timestamp), "America/Los_Angeles"),datetime(cast(lag(c.created_at) over (partition by c.key order by c.created_at) as timestamp), "America/Los_Angeles"), minute) as time_in_queue_minute
  from
  (
    select *
    from jira.depr_changelog c
    union all
    select
      i.key,
      null as history_id,
      i.created_at,
      'New' as from_string,
      'Open' as to_string
    from jira.depr_issues i
  ) as c
  left join jira.depr_issues i on i.key = c.key
  )
  where
    from_string not in ('New','Completed')
  group by 1,2
  )
  group by 1
),
jira_issues as
(
  select
    REGEXP_REPLACE(left(adminLink,45),'[^0-9 ]','') as jira_order_id,
    extract(datetime from created_at) as created_at,
    extract(datetime from claimed_at) as claimed_at,
    parse_datetime('%Y-%m-%d %H:%M:%S', concat(left(completed_at,10),' ',substr(completed_at,12,8))) as completed_at,
    status,
    assignee as jira_estimator,
    cs as jira_cs
  from
  (
  select
    rank() over (partition by i.key order by c.created_at) as rank,
    i.adminLink,
    i.created_at,
    c.created_at as claimed_at,
    i.resolutionDate as completed_at,
    i.status,
    i.assignee,
    i.cs as cs
  from jira.depr_changelog c
  left join jira.depr_issues i on i.key = c.key
  where
    to_string = 'In Progress'
  ) as k
  where
    rank = 1
),
linear_feet_points as 
(
	(select 0 as min_value, 60 as max_value, 1 as points) union all
	(select 61 as min_value, 120 as max_value, 2 as points) union all
	(select 121 as min_value, 200 as max_value, 3 as points) union all
	(select 200 as min_value, 100000 as max_value, 4 as points)
),
sides_points as 
(
	(select 0 as min_value, 3 as max_value, 1 as points) union all
	(select 4 as min_value, 6 as max_value, 2 as points) union all
	(select 7 as min_value, 9 as max_value, 3 as points) union all
	(select 10 as min_value, 100 as max_value, 4 as points)
),
styles_points as 
(
	(select 0 as min_value, 2 as max_value, 1 as points) union all
	(select 3 as min_value, 4 as max_value, 2 as points) union all
	(select 5 as min_value, 6 as max_value, 3 as points) union all
	(select 7 as min_value, 100 as max_value, 4 as points)
),
revisions_points as 
(
	(select 0 as min_value, 1 as max_value, 1 as points) union all
	(select 2 as min_value, 2 as max_value, 2 as points) union all
	(select 3 as min_value, 3 as max_value, 3 as points) union all
	(select 4 as min_value, 100 as max_value, 4 as points)
),
price_points as 
(
	(select 0 as min_value, 2500 as max_value, 1 as points) union all
	(select 2501 as min_value, 4000 as max_value, 2 as points) union all
	(select 4001 as min_value, 8000 as max_value, 3 as points) union all
	(select 8001 as min_value, 1000000 as max_value, 4 as points)
),
quotelines as 
(
	select
		-- ql.*,
		q.id,
		concat('https://admin.ergeon.in/quoting-tool/',o.id,'/quote/',q.id) as admin_link,
		ue.full_name as ergeon_estimator,
		uc.full_name as ergeon_cs,
		l.service_category as product,
		sum(case when ql.label in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then ql.quantity else 0 end) as linear_feet,
		sum(case when ql.price > 0 then 1 else 0 end) as no_of_sides,
		count(distinct ql.quote_style_id) as no_of_styles, 
		sum(ql.price) as total_price,
    STRING_AGG(distinct pct.item) AS items,
    max(JSON_EXTRACT_SCALAR(q.calc_input,'$.complications')) as complications
	from ergeon.quote_quoteline ql
	left join ergeon.quote_quote q on q.id = ql.quote_id
	left join ergeon.store_order o on o.id = q.order_id
	left join ergeon.core_lead l on l.order_id = o.id
	left join ergeon.hrm_staff hs on hs.id = o.sales_rep_id 
	left join ergeon.core_user uc on uc.id = hs.user_id  
	left join ergeon.core_user ue on ue.id = q.sent_to_customer_by_id
  left join ergeon.product_catalog pc on pc.id = ql.catalog_id 
  left join ergeon.product_catalogtype pct on pct.id = pc.type_id
	group by 1,2,3,4,5
),
quotes as
(
	select 
		rank() over (partition by q.order_id order by q.created_at desc) as rank,
		q.*
	from ergeon.quote_quote q
),
orders as
(
	select
		o.id as order_id,
		lq.id as quote_id,
		sum(case when q.sent_to_customer_at is not null and q.cancelled_at is null then 1 else 0 end) as no_of_revisions_sent,
    sum(case when q.rank = 1 and q.sent_to_customer_at is not null then 1 else 0 end) as new_quotes,
    sum(case when q.rank > 1 and q.sent_to_customer_at is not null then 1 else 0 end) as requotes
	from ergeon.store_order o
	left join quotes lq on lq.order_id = o.id and lq.rank = 1
	left join quotes q on q.order_id = o.id
	left join quotelines ql on ql.id = lq.id
	--where
		--lq.id = 162045
	group by 1,2
)
select
	order_id,
	quote_id,
	admin_link,
	case when jira_estimator is null then ergeon_estimator else jira_estimator end as estimator,
	case when jira_cs is null then ergeon_cs else jira_cs end as cs,
	ql.linear_feet,
	ql.no_of_sides,
	ql.no_of_styles,
  ql.items,
	o.no_of_revisions_sent,
	ql.total_price,
	lfp.points + sp.points + stp.points + rp.points + pp.points as points,
  ji.created_at,
  ji.claimed_at,
  ji.completed_at,
  datetime_diff(datetime(timestamp(ji.completed_at)), datetime(timestamp(ji.created_at)), minute) as turnoaround_time,
  ji.status as quote_status,
  new_quotes as no_of_new_quotes_sent,
  requotes as no_of_requotes_sent,
  jc.tot_quotes_sent,
  jc.open,
  jc.in_progress,
  jc.awaiting_input_from_cs,
  jc.awaiting_input_from_construction,
  jc.in_qa,
  jc.requote,
  jc.canceled,
  ql.complications
from orders o
left join quotelines ql on ql.id = o.quote_id
join linear_feet_points lfp on lfp.min_value <= ql.linear_feet and lfp.max_value >= ql.linear_feet
join sides_points sp on sp.min_value <= ql.no_of_sides and sp.max_value >= ql.no_of_sides
join styles_points stp on stp.min_value <= ql.no_of_styles and stp.max_value >= ql.no_of_styles
join revisions_points rp on rp.min_value <= o.no_of_revisions_sent and rp.max_value >= o.no_of_revisions_sent
join price_points pp on pp.min_value <= ql.total_price and pp.max_value >= ql.total_price
left join jira_issues ji on ji.jira_order_id = cast(o.order_id as string)
join jira_changelog jc on jc.jira_order_id = o.order_id
)
select 
  ad.*,
  l.points as quote_level
from all_data ad
join quote_level l on l.min_value <= ad.points and l.max_value >= ad.points