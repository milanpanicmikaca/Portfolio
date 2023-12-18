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
with 
prop_cfg as (
select
  cv.config_id,
  min(case when attr_id = 1 then v.name else null end) as demo,
  min(case when attr_id = 2 then v.name else null end) as debris_removal,
  min(case when attr_id = 38 then v.name else null end) as power_wash,
  min(case when attr_id = 132 then v.name else null end) as special_soil_conditions_surcharge,
  min(case when attr_id = 231 then v.name else null end) as digging_difficulty,
  min(case when attr_id = 247 then v.name else null end) as left_post_type,
  min(case when attr_id = 248 then v.name else null end) as right_post_type,
  min(case when attr_id = 253 then v.name else null end) as existing_left_post,
  min(case when attr_id = 254 then v.name else null end) as existing_right_post
from ergeon.calc_value v join
  ergeon.calc_configvalue cv on cv.value_id = v.id
group by 1
),
fence_cfg as (
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
  min(case when attr_id = 236 then v.name else null end) as post_cap,
  min(case when attr_id = 12 then v.name else null end) as post_material,
  min(case when attr_id = 13 then v.name else null end) as post_hole_depth,
  min(case when attr_id = 14 then v.name else null end) as post_length,
  min(case when attr_id = 15 then v.name else null end) as post_reveal,
  min(case when attr_id = 17 then v.name else null end) as kick_board_size,
  min(case when attr_id = 16 then v.name else null end) as kick_board_material,
  min(case when attr_id = 18 then v.name else null end) as rails,
  min(case when attr_id = 19 then v.name else null end) as rails_material, --deleted
  min(case when attr_id = 21 then v.name else null end) as lattice_height,
  min(case when attr_id = 20 then v.name else null end) as lattice_style,
  min(case when attr_id = 252 then v.name else null end) as lattice_material,
  min(case when attr_id = 22 then v.name else null end) as retaining_wall,
  min(case when attr_id = 24 then v.name else null end) as gate_type,
  --min(case when attr_id = 25 then v.name else null end) as gate_design,
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
  min(case when attr_id = 36 then v.name else null end) as sales_discount, --deleted
  min(case when attr_id = 37 then v.name else null end) as stain_color,
  min(case when attr_id = 39 then v.name else null end) as small_project_overhead,
  min(case when attr_id = 41 then v.name else null end) as standalone_retaining_wall,
  min(case when attr_id = 44 then v.name else null end) as chain_link_fence,
  min(case when attr_id = 45 then v.name else null end) as chain_link_gate,
  min(case when attr_id = 46 then v.name else null end) as additional_city_surcharge,
  min(case when attr_id = 47 then v.name else null end) as rails_material1,
  min(case when attr_id = 48 then v.name else null end) as rails_size,
  min(case when attr_id = 49 then v.name else null end) as rails_cap,
  min(case when attr_id = 50 then v.name else null end) as no_margin_amount,
  min(case when attr_id = 160 then v.name else null end) as repair_only,
  min(case when attr_id = 111 then v.name else null end) as repair_post, --new
  min(case when attr_id = 112 then v.name else null end) as repair_rail, --new
  min(case when attr_id = 113 then v.name else null end) as repair_kickboard, --new
  min(case when attr_id = 114 then v.name else null end) as repair_lattice, --new
  min(case when attr_id = 115 then v.name else null end) as repair_picket, --new
  min(case when attr_id = 116 then v.name else null end) as repair_fascia, --new
  min(case when attr_id = 117 then v.name else null end) as repair_cap_rail, --new
  min(case when attr_id = 119 then v.name else null end) as repais_post_cap, --new
  min(case when attr_id = 120 then v.name else null end) as repair_hinge_latch, --new
  min(case when attr_id = 121 then v.name else null end) as repair_trim, --new
  min(case when attr_id = 122 then v.name else null end) as repair_overhead, --new
  min(case when attr_id = 123 then v.name else null end) as repair_material_discount, --new
  min(case when attr_id = 125 then v.name else null end) as boxwire_fence, --deleted
  min(case when attr_id = 126 then v.name else null end) as boxwire_gate, --deleted
  min(case when attr_id = 130 then v.name else null end) as slope_style,
  min(case when attr_id = 133 then v.name else null end) as standalone_retaining_wall_height, --new
  min(case when attr_id = 134 then v.name else null end) as standalone_retaining_wall_middle_post, --new
  min(case when attr_id = 135 then v.name else null end) as standalone_retaining_wall_section_length, --new
  min(case when attr_id = 136 then v.name else null end) as standalone_retaining_wall_post_size, --new
  min(case when attr_id = 137 then v.name else null end) as standalone_retaining_wall_post_hole_depth, --new
  min(case when attr_id = 138 then v.name else null end) as standalone_retaining_wall_slope_style, --new
  min(case when attr_id = 139 then v.name else null end) as standalone_retaining_wall_slope, --new
  min(case when attr_id = 140 then v.name else null end) as standalone_retaining_wall_boards_material, --new
  min(case when attr_id = 141 then v.name else null end) as standalone_retaining_wall_cap, --new
  min(case when attr_id = 144 then v.name else null end) as rails_placement, --new
  min(case when attr_id = 161 then v.name else null end) as boxwire_custom_fence, --new
  min(case when attr_id = 162 then v.name else null end) as boxwire_custom_gate, --new
  min(case when attr_id = 214 then v.name else null end) as display_price_adjustment, --new
  min(case when attr_id = 215 then v.name else null end) as staining_height, --new
  min(case when attr_id = 216 then v.name else null end) as staining_gaps, --new
  min(case when attr_id = 217 then v.name else null end) as post_staining_package, --new
  min(case when attr_id = 218 then v.name else null end) as post_staining_color,
  min(case when attr_id = 219 then v.name else null end) as power_wash_day,
  min(case when attr_id = 220 then v.name else null end) as pre_stain, --new
  min(case when attr_id = 221 then v.name else null end) as pre_staining_color, --new
  min(case when attr_id = 222 then v.name else null end) as picket_gap, --new
  min(case when attr_id = 223 then v.name else null end) as rails_cap_material,
  min(case when attr_id = 224 then v.name else null end) as hardscape_line, --new
  min(case when attr_id = 225 then v.name else null end) as hardscape_point, --new
  min(case when attr_id = 226 then v.name else null end) as fence_package, --new
  min(case when attr_id = 184 then v.name else null end) as vinyl_side_length,
  min(case when attr_id = 185 then v.name else null end) as vinyl_length_fraction,
  min(case when attr_id = 186 then v.name else null end) as vinyl_finish_height,
  min(case when attr_id = 187 then v.name else null end) as vinyl_color,
  min(case when attr_id = 188 then v.name else null end) as vinyl_base_style,
  min(case when attr_id = 190 then v.name else null end) as vinyl_cap_style,
  min(case when attr_id = 191 then v.name else null end) as vinyl_lattice,
  min(case when attr_id = 192 then v.name else null end) as vinyl_slope,
  min(case when attr_id = 194 then v.name else null end) as vinyl_wind_rated,
  min(case when attr_id = 195 then v.name else null end) as vinyl_demo_haul_away,
  min(case when attr_id = 197 then v.name else null end) as vinyl_gate_type,
  min(case when attr_id = 198 then v.name else null end) as vinyl_gate_width,
  min(case when attr_id = 199 then v.name else null end) as vinyl_gate_height,
  min(case when attr_id = 200 then v.name else null end) as vinyl_gate_color,
  min(case when attr_id = 201 then v.name else null end) as vinyl_gate_base_style,
  min(case when attr_id = 202 then v.name else null end) as vinyl_gate_cap_style,
  min(case when attr_id = 203 then v.name else null end) as vinyl_gate_metal_frame,
  min(case when attr_id = 204 then v.name else null end) as vinyl_gate_lattice,
  min(case when attr_id = 207 then v.name else null end) as vinyl_gate_drop_pin,	
  min(case when attr_id = 208 then v.name else null end) as vinyl_gate_masonry_concrete,	
  min(case when attr_id = 209 then v.name else null end) as vinyl_gate_demo_haul_away,
  min(case when attr_id = 210 then v.name else null end) as vinyl_custom_fence,
  min(case when attr_id = 211 then v.name else null end) as vinyl_custom_gate,
  min(case when attr_id = 227 then v.name else null end) as post_covering,
  min(case when attr_id = 228 then v.name else null end) as mesh_height,
  min(case when attr_id = 229 then v.name else null end) as mesh_style,
  min(case when attr_id = 235 then v.name else null end) as gate_drop_rod,
  min(case when attr_id = 232 then v.name else null end) as standalone_demo,
  min(case when attr_id = 233 then v.name else null end) as standalone_debris_removal,
  min(case when attr_id = 234 then v.name else null end) as additional_labor_time,
  min(case when attr_id = 241 then v.name else null end) as gate_frame_style,
  min(case when attr_id = 295 then v.name else null end) as gate_frame_material,
  min(case when attr_id = 242 then v.name else null end) as gate_top,
  min(case when attr_id = 243 then v.name else null end) as gate_picket_orientation,
  min(case when attr_id = 244 then v.name else null end) as gate_picket_build,
  min(case when attr_id = 245 then v.name else null end) as gate_lattice_height,
  min(case when attr_id = 246 then v.name else null end) as gate_lattice_style,
  min(case when attr_id = 257 then v.name else null end) as gate_cap_rail_material,
  min(case when attr_id = 258 then v.name else null end) as gate_cap_rail_size,
  min(case when attr_id = 259 then v.name else null end) as gate_trims,	
  min(case when attr_id = 260 then v.name else null end) as gate_trims_size,
  min(case when attr_id = 269 then v.name else null end) as intended_use,
  min(case when attr_id = 270 then v.name else null end) as traffic_level,
  min(case when attr_id = 271 then v.name else null end) as turf_pile_height,
  min(case when attr_id = 272 then v.name else null end) as infill_type,
  min(case when attr_id = 273 then v.name else null end) as infill_amount,
  min(case when attr_id = 274 then v.name else null end) as edging_material,
  min(case when attr_id = 275 then v.name else null end) as jumping_pad,
  min(case when attr_id = 276 then v.name else null end) as project_location,
  min(case when attr_id = 277 then v.name else null end) as golfing_components,
  min(case when attr_id = 278 then v.name else null end) as access_size,
  min(case when attr_id = 279 then v.name else null end) as installation_type,
  min(case when attr_id = 282 then v.name else null end) as extra_grading,
  min(case when attr_id = 283 then v.name else null end) as concrete_cutting,
  min(case when attr_id = 284 then v.name else null end) as excesive_roots,
  min(case when attr_id = 285 then v.name else null end) as stepper_cutting,
  min(case when attr_id = 264 then v.name else null end) as landscape_additional_labor_time,
  min(case when attr_id = 267 then v.name else null end) as landscape_labor_adjustment,
  min(case when attr_id = 298 then v.name else null end) as edging,
  min(case when attr_id = 189 then v.name else null end) as conected_to_house,
  min(case when attr_id = 196 then v.name else null end) as side_configuration,
  min(case when attr_id = 193 then v.name else null end) as masonry_concrete,
  min(case when attr_id = 309 then v.name else null end) as section_design,
  min(case when attr_id = 335 then v.name else null end) as panel_orientation,
  min(case when attr_id = 453 then v.name else null end) as alternating_panels_material
from ergeon.calc_value v join
  ergeon.calc_configvalue cv on cv.value_id = v.id
group by 1
),
oleads as (
  select order_id, min(id) as lead_id from ergeon.core_lead where created_at > '2018-04-15' group by 1
),
oqs as (
select 
  o.id as order_id,
  max(
    if(approved_quote_id is not null, 
      approved_quote_id, 
    if(q.sent_to_customer_at is not null, 
      q.id, 
    null
  ))) as quote_id,
  min(
    if(o.approved_quote_id = q.id and datetime_sub( datetime(q.approved_at), interval 28 day) < datetime(o.created_at), 
      'APP028',
    if(o.approved_quote_id is not null,
      'APP1',
    if(q.sent_to_customer_at is not null,
        'SNT',
    null
    )))) as status
from 
  ergeon.quote_quote q join 
  ergeon.store_order o on q.order_id = o.id
where 
  o.created_at > '2018-04-15'
  --and is_cancellations = False
group by 1
),
qlt as (
  select ql.id, 
    coalesce(sct.type_id,ct.type_id,ql.catalog_type_id) as type_id,
    coalesce(sct.name,ct.name) as name,
    coalesce(tys.item,ty.item,qlt.item) as lst_catalog_type,
    if(preview_image is not null and case 
                                        when tys.item is not null then tys.cad_support
                                        when ty.item is not null then ty.cad_support
                                        when qlt.item is not null then qlt.cad_support
                                     end = TRUE ,1,0) as quoteline_rendered
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
length_ql as (
select 
  qql.id as quoteline_id,
  case 
    when map.array ='sides' then coalesce(round(st_distance(st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng1]')),r'["\[\]]', '') AS FLOAT64),
              SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat1]')),r'["\[\]]', '') AS FLOAT64)),
              st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng2]')),r'["\[\]]', '') AS FLOAT64),
              SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat2]')),r'["\[\]]', '') AS FLOAT64)))*3.280839895,2),0) 
  end as ql_length,
  if(map.array ='polygons' and lower(calc_input) like ('%cad_objects%'),SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.polygons[',coalesce(index,0),'].area')),r'["\[\]]', '') AS FLOAT64),0) as sqft
from 
  ergeon.quote_quoteline qql left join
  int_data.calc_index_mapping map on qql.label = map.label left join
  ergeon.quote_quote qq on qql.quote_id = qq.id
where 
  map.label is not null
  and is_cancellation = False
),
qls as 
(
select
  q.order_id,
  l.quote_id,
  l.id as quoteline_id,
  oqs.status,
  z.code as zipcode,
  ca.name as catalog,
  substr(l.label,0,5) as label,
  fence_cfg.*,prop_cfg.*,
  quantity as length,
  qlt.name as cat_name,
  case when lst_catalog_type in ('fence-side', 'cl-fence-side', 'vinyl-fence-side', 'bw-fence-side' ) then quantity else 0 end as fence_length,
  case when lst_catalog_type in ('fence-gate') then cast (REGEXP_EXTRACT(gate_width, '^([0-9]*).*')as int64) else 0 end  as gate_length,
  price, cost,
  l.description,
  lst_catalog_type as item_type,
  case when lst_catalog_type in ('fence-side', 'cl-fence-side', 'vinyl-fence-side', 'bw-fence-side' )  then coalesce(ql_length,0) else 0 end as ql_length,
  coalesce(sqft,0) as sqft,
  quoteline_rendered
from 
  ergeon.quote_quoteline l join
  oqs on l.quote_id = oqs.quote_id join
  ergeon.quote_quote q on l.quote_id = q.id join
  ergeon.product_tier t on t.id = q.tier_id join
  ergeon.store_order o on o.id = q.order_id join 
  fence_cfg on fence_cfg.config_id = l.config_id join 
  ergeon.core_house h on o.house_id = h.id join 
  ergeon.geo_address a on a.id = h.address_id join 
  ergeon.geo_zipcode z on z.id = a.zip_code_id join 
  ergeon.geo_county co on co.id = a.county_id left join 
  ergeon.geo_city ci on ci.id = a.city_id left join 
  ergeon.product_countymarket cm on cm.county_id = a.county_id left join 
  ergeon.product_market m on m.id = cm.market_id join 
  ergeon.store_product p on p.id = o.product_id left join 
  oleads on oleads.order_id = o.id left join
  ergeon.core_lead lead on lead.id = oleads.lead_id left join
  ergeon.product_catalog ca on ca.id = l.catalog_id left join
  ergeon.product_catalogtype ty on ty.id = ca.type_id left join
  ergeon.customers_visitoraction va on va.id = lead.visitor_action_id left join 
  prop_cfg on prop_cfg.config_id = l.property_config_id left join
  qlt on qlt.id = l.id left join 
  ergeon.product_catalogtype pct on pct.id = qlt.type_id left join
  length_ql lql on lql.quoteline_id = l.id
where 
  sent_to_customer_at is not null
)
select
  order_id,
  quote_id,
  quoteline_id,
  status,
  description,
  zipcode,
  item_type,
  quoteline_rendered,
  finish_height,
  frame_style,
  section_length,
  picket_build,
  picket_style,
  picket_size,
  picket_material,
  picket_overlap,
  post_size,
  post_cap,
  post_material,
  post_hole_depth,
  post_length,
  post_reveal,
  kick_board_size,
  kick_board_material,
  rails,
  rails_material,
  lattice_height,
  lattice_style,
  lattice_material,
  retaining_wall,
  gate_type,
  --gate_design,
  gate_finish_height,
  gate_width,
  gate_picket_material,
  gate_post_size,
  steel_frame,
  latch_kit,
  slope,
  slope_style,
  additional_man_hours,
  additional_crew_hours,
  additional_crew_days,
  sales_discount, --deleted
  stain_color,
  power_wash,
  small_project_overhead,
  standalone_retaining_wall,
  standalone_retaining_wall_height,--new
  standalone_retaining_wall_middle_post,--new
  standalone_retaining_wall_section_length,--new
  standalone_retaining_wall_post_size,--new
  standalone_retaining_wall_post_hole_depth,--new
  standalone_retaining_wall_slope_style,--new
  standalone_retaining_wall_slope,--new
  standalone_retaining_wall_boards_material,--new
  standalone_retaining_wall_cap,--new
  repair_only,
  repair_post,--new
  repair_rail,--new
  repair_kickboard,--new
  repair_lattice,--new
  repair_picket,--new
  repair_fascia,--new
  repair_cap_rail,--new
  repais_post_cap,--new
  repair_hinge_latch,--new
  repair_trim,--new
  repair_overhead,--new
  repair_material_discount,--new 
  chain_link_fence,
  chain_link_gate,
  additional_city_surcharge,
  rails_material1,
  rails_size,
  rails_cap,
  no_margin_amount,
  boxwire_fence, --deleted
  boxwire_gate, --deleted
  boxwire_custom_fence,--new
  boxwire_custom_gate,--new
  rails_placement,--new
  display_price_adjustment,
  staining_height,
  staining_gaps,
  post_staining_package,
  post_staining_color,
  power_wash_day,
  pre_stain,
  pre_staining_color,
  picket_gap,
  rails_cap_material,
  hardscape_line,
  hardscape_point,
  fence_package,
  vinyl_side_length,
  vinyl_length_fraction,
  vinyl_finish_height,
  vinyl_color,
  vinyl_base_style,
  vinyl_cap_style,
  vinyl_lattice,
  vinyl_slope,
  vinyl_wind_rated,
  vinyl_demo_haul_away,
  vinyl_gate_type,
  vinyl_gate_width,
  vinyl_gate_height,
  vinyl_gate_color,
  vinyl_gate_base_style,
  vinyl_gate_cap_style,
  vinyl_gate_metal_frame,
  vinyl_gate_lattice,
  vinyl_gate_drop_pin,
  vinyl_gate_masonry_concrete,
  vinyl_gate_demo_haul_away,
  vinyl_custom_fence,
  vinyl_custom_gate,
  post_covering,
  mesh_height,
  mesh_style,
  cat_name,
  gate_drop_rod,
  digging_difficulty,
  standalone_demo,
  standalone_debris_removal,
  additional_labor_time,
  demo,
  debris_removal,	
  special_soil_conditions_surcharge,
  existing_left_post,
  existing_right_post,
  left_post_type,
  right_post_type,
  gate_frame_style,
  gate_frame_material,
  gate_top,
  gate_picket_orientation,
  gate_picket_build,
  gate_lattice_height,
  gate_lattice_style,
  gate_cap_rail_material,
  gate_cap_rail_size,
  gate_trims,	
  gate_trims_size,
  intended_use,
  traffic_level,
  turf_pile_height,
  infill_type,
  infill_amount,
  edging_material,
  jumping_pad,
  project_location,
  golfing_components,
  access_size,
  installation_type,
  extra_grading,
  concrete_cutting,
  excesive_roots,
  stepper_cutting,
  landscape_additional_labor_time,
  landscape_labor_adjustment,
  edging,
  conected_to_house,
  side_configuration,
  masonry_concrete,
  section_design,
  panel_orientation,
  alternating_panels_material,
  sum(length) as length,
  sum(fence_length) as fence_length,
  sum(gate_length) as gate_length,
  sum(ql_length) as ql_length,
  sum(sqft) as sqft,
  sum(price) as price,
  sum(cost) as cost,
  round(sum(price)*1.0/nullif(sum(length),0),2) as unit_price,
  round(sum(cost)*1.0/nullif(sum(length),0),2) as unit_cost
from qls
group by 
 1,2,3,4,5,6,7,8,9,10,
 11,12,13,14,15,16,17,18,19,20,
 21,22,23,24,25,26,27,28,29,30,
 31,32,33,34,35,36,37,38,39,40,
 41,42,43,44,45,46,47,48,49,50,
 51,52,53,54,55,56,57,58,59,60,
 61,62,63,64,65,66,67,68,69,70,
 71,72,73,74,75,76,77,78,79,80,
 81,82,83,84,85,86,87,88,89,90,
 91,92,93,94,95,96,97,98,99,100,
 101,102,103,104,105,106,107,108,109,110,
 111,112,113,114,115,116,117,118,119,120,
 121,122,123,124,125,126,127,128,129,130,
 131,132,133,134,135,136,137,138,139,140,
 141,142,143,144,145,146,147,148,149,150,
 151,152,153,154,155,156,157,158,159,160,
 161,162,163,164,165,166,167,168
