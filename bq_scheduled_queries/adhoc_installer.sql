with
cs_data as
(
  select
	 o.id as order_id,
	 u.full_name as cs
  from ergeon.store_order o
  left join ergeon.hrm_staff s on s.id = o.sales_rep_id
  left join ergeon.core_user u on u.id = s.user_id
),
quoter_data as
(
  select
	 q.id as quote_id,
	 STRING_AGG(distinct u.full_name) as quoter
  from ergeon.quote_quote q
  left join ergeon.core_user u on u.id = q.sent_to_customer_by_id
  group by 1
),
pm_data as
(
  select
	 o.id as order_id,
	 u.full_name as pm
  from ergeon.store_order o
  left join ergeon.hrm_staff s on s.id = o.project_manager_id
  left join ergeon.core_user u on u.id = s.user_id
),
market_data as
(
  select
	 q.id as quote_id,
	 pt.name as tier,
	 ci.name as city,
	 gz.code as zipcode,
	 co.name as county,
	 pm.code as market,
	 sp.name as product,
   case
    when o.status = 'PGS' then 'In Progress'
    when o.status = 'ONH' then 'On Hold'
    when o.status = 'CMP' then 'Completed'
    when o.status = 'QUD' then 'Quoted'
    when o.status = 'RTS' then 'Ready To Schedule'
    when o.status = 'QIP' then 'Quote In Progress'
    when o.status = 'QND' then 'Quote Needed'
    when o.status = 'LST' then 'Lost'
    when o.status = 'CAN' then 'Cancelled'
    when o.status = 'TST' then 'Test'
    else 'Unknown'
  end as status
  from ergeon.quote_quote q
  left join ergeon.product_tier pt on pt.id = q.tier_id
  left join ergeon.store_order o on o.id = q.order_id
  left join ergeon.core_house h on h.id = o.house_id
  left join ergeon.geo_address ga on ga.id = h.address_id 
  left join ergeon.geo_zipcode gz on gz.id = ga.zip_code_id 
  left join ergeon.geo_county co on co.id = ga.county_id 
  left join ergeon.geo_city ci on ci.id = ga.city_id 
  left join ergeon.product_countymarket pcm on pcm.county_id = ga.county_id 
  left join ergeon.product_market pm on pm.id = pcm.market_id
  left join ergeon.store_product sp on sp.id = o.product_id
),
contractor_data as
(
  select
    order_id,
    STRING_AGG(distinct u.full_name) as installer,
    sum(total_cost) as actual_cost
  from ergeon.contractorapp_contractororder cco
  left join ergeon.hrm_contractor c on c.id = cco.contractor_id
  left join ergeon.core_user u on u.id = c.user_id
  where
    status_id = 3
  group by 1
)
select 
  k.*,
  md.status,
  md.tier,
  md.city,
  md.zipcode,
  md.county,
  md.market,
  md.product,
  csd.cs,
  qd.quoter,
  pd.pm,
  cd.installer,
  date_diff(completed_at, first_approved_at, day) as days_close2completion,
  cd.actual_cost,
  case
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 0 then '0 - 1k'
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 1 then '1k - 2k'
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 2 then '2k - 3k'
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 3 then '3k - 4k'
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 4 then '4k - 5k'
    when range_bucket(cast(k.price as float64),[1000.0, 2000.0, 3000.0, 4000.0, 5000.0]) = 5 then '5k+'
    else ''
  end as price_bucket,
  case
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 0 then '0 - 30ft'
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 1 then '30 - 60ft'
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 2 then '60 - 90ft'
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 3 then '90 - 120ft'
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 4 then '120 - 150ft'
    when range_bucket(cast(k.fence_length as float64),[30.0, 60.0, 90.0, 120.0, 150.0]) = 5 then '150ft+'
    else ''
  end as length_bucket
from 
(
with
config_flat as
(
	select
  		cv.config_id,
  		min(case when attr_id = 3 then v.name else null end) as finish_height,
  		min(case when attr_id = 4 then v.name else null end) as frame_style,
  		min(case when attr_id = 5 then v.name else null end) as section_length,
  		min(case when attr_id = 6 then v.name else null end) as picket_build,
  		min(case when attr_id = 7 then v.name else null end) as picket_style,
  		min(case when attr_id = 8 then v.name else null end) as picket_size,
  		min(case when attr_id = 9 then v.name else null end) as picket_material,
  		min(case when attr_id = 10 then v.name else null end) as picket_overlap,
  		min(case when attr_id = 11 then v.name else null end) as post_size,
  		min(case when attr_id = 12 then v.name else null end) as post_material,
  		min(case when attr_id = 13 then v.name else null end) as post_hole_depth,
  		min(case when attr_id = 14 then v.name else null end) as post_length,
  		min(case when attr_id = 15 then v.name else null end) as post_reveal,
  		min(case when attr_id = 17 then v.name else null end) as kick_board_size,
  		min(case when attr_id = 16 then v.name else null end) as kick_board_material,
 		min(case when attr_id = 18 then v.name else null end) as rails,
  		min(case when attr_id = 19 then v.name else null end) as rails_material,
  		min(case when attr_id = 21 then v.name else null end) as lattice_height,
  		min(case when attr_id = 20 then v.name else null end) as lattice_style,
  		min(case when attr_id = 22 then v.name else null end) as retaining_wall,
  		min(case when attr_id = 24 then v.name else null end) as gate_type,
  		min(case when attr_id = 25 then v.name else null end) as gate_design,
  		min(case when attr_id = 26 then v.name else null end) as gate_finish_height,
  		min(case when attr_id = 27 then v.name else null end) as gate_width,
  		min(case when attr_id = 28 then v.name else null end) as gate_picket_material,
  		min(case when attr_id = 29 then v.name else null end) as gate_post_size,
  		min(case when attr_id = 30 then v.name else null end) as steel_frame,
  		min(case when attr_id = 31 then v.name else null end) as latch_kit,
  		min(case when attr_id = 32 then v.name else null end) as slope,
  		min(case when attr_id = 33 then v.name else null end) as additional_man_hours,
  		min(case when attr_id = 34 then v.name else null end) as additional_crew_hours,
  		min(case when attr_id = 35 then v.name else null end) as additional_crew_days,
  		min(case when attr_id = 36 then v.name else null end) as sales_discount,
  		min(case when attr_id = 37 then v.name else null end) as stain_color,
  		min(case when attr_id = 38 then v.name else null end) as power_wash,
  		min(case when attr_id = 39 then v.name else null end) as small_project_overhead,
  		min(case when attr_id = 41 then v.name else null end) as standalone_retaining_wall,
  		min(case when attr_id = 44 then v.name else null end) as chain_link_fence,
  		min(case when attr_id = 45 then v.name else null end) as chain_link_gate,
  		min(case when attr_id = 46 then v.name else null end) as additional_city_surcharge,
  		min(case when attr_id = 47 then v.name else null end) as rails_material1,
  		min(case when attr_id = 48 then v.name else null end) as rails_size,
  		min(case when attr_id = 49 then v.name else null end) as rails_cap,
  		min(case when attr_id = 50 then v.name else null end) as no_margin_amount
	from ergeon.calc_value v 
	join ergeon.calc_configvalue cv on cv.value_id = v.id
	group by 1
),
length_flat as
(
  select
	 q.id as quote_id,
	 sum(case when pct.item = 'fence-side' then ql.quantity else 0 end) as fence_length,
   sum(case when pct.item = 'fence-gate' then cast (REGEXP_EXTRACT(cf.gate_width, '^([0-9]*).*') as int64) else 0 end) as gate_length
  from ergeon.quote_quoteline ql
  left join ergeon.quote_quote q on q.id = ql.quote_id
  left join ergeon.product_catalog pc on pc.id = ql.catalog_id
  left join ergeon.product_catalogtype pct on pct.id = pc.type_id
  left join config_flat cf on cf.config_id = ql.config_id
  group by 1  
),
quoteline_flat as 
(
	select 
		case when rank() over (partition by q.order_id order by q.approved_at desc) = 1 then true else false end as is_last_approved,
		case when rank() over (partition by q.order_id order by q.approved_at) = 1 then true else false end as is_first_approved,
		case when rank() over (partition by q.order_id order by q.approved_at desc, unit desc, cast(quantity as int64) desc) = 1 then true else false end as is_primary_quantity,
		*
	from ergeon.quote_quoteline ql
	left join config_flat cf on cf.config_id = ql.config_id
	left join ergeon.quote_quote q on q.id = ql.quote_id
	where
		q.approved_at is not null
		-- and q.order_id = 111353
)
select
	qf.quote_id,
	qf.order_id,
	max(date_trunc(extract(date from o.created_at at time zone 'America/Los_Angeles'),day)) as created_at,
  max(date_trunc(extract(date from qff.approved_at at time zone 'America/Los_Angeles'),day)) as first_approved_at,
	max(date_trunc(extract(date from qf.approved_at at time zone 'America/Los_Angeles'),day)) as last_approved_at,
	max(date_trunc(extract(date from o.completed_at at time zone 'America/Los_Angeles'),day)) as completed_at,	
	max(qf.old_total_price) as price,
	max(qf.total_cost) as estimated_cost,
	max(qf.old_total_price) - max(qf.total_cost) as estimated_margin,
	max(lf.fence_length) as fence_length,
	max(lf.gate_length) as gate_length,
	max(case when qf.is_primary_quantity is true then concat(ifnull(qf.frame_style,''),ifnull(qf.chain_link_fence,'')) else '' end) as primary_frame_style,
	max(qf.finish_height) as primary_finish_height,
	max(qf.slope) as primary_slope,
	case when count(qf.gate_type) > 0 then true else false end as has_gate
from quoteline_flat qf
left join ergeon.store_order o on o.id = qf.order_id
left join quoteline_flat qff on qff.order_id = qf.order_id and qff.is_first_approved = true
left join ergeon.product_catalog pc on pc.id = qf.catalog_id
left join ergeon.product_catalogtype pct on pct.id = pc.type_id
left join length_flat lf on lf.quote_id = qf.quote_id
where
	qf.is_last_approved = true
group by 1,2
order by 3 desc
) as k
left join contractor_data cd on cd.order_id = k.order_id
left join market_data md on md.quote_id = k.quote_id
left join cs_data csd on csd.order_id = k.order_id
left join quoter_data qd on qd.quote_id = k.quote_id
left join pm_data pd on pd.order_id = k.order_id
where
  md.status not in ('Test')
order by 3 desc