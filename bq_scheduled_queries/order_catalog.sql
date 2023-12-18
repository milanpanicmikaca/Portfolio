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
length_ql as (
select 
  qql.id as quoteline_id,
  case 
    when map.array ='sides' then coalesce(round(st_distance(st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng1]')),r'["\[\]]', '') AS FLOAT64),
              SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat1]')),r'["\[\]]', '') AS FLOAT64)),
              st_geogpoint(SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lng2]')),r'["\[\]]', '') AS FLOAT64),
              SAFE_CAST(REGEXP_REPLACE(CUSTOM_JSON_EXTRACT(calc_input,concat('$.sides[',coalesce(index,0),'].coordinates[lat2]')),r'["\[\]]', '') AS FLOAT64)))*3.280839895,2),0) 
  end as ql_length
from 
  ergeon.quote_quoteline qql left join
  int_data.calc_index_mapping map on qql.label = map.label left join
  ergeon.quote_quote qq on qql.quote_id = qq.id
where 
  map.label is not null
  --and is_cancellation = False
),
qlt as 
(
select 
  ql.id, 
  coalesce(sct.type_id,ct.type_id,ql.catalog_type_id) as type_id
from 
  ergeon.quote_quoteline ql left join 
  ergeon.quote_quotestyle qs on qs.id = ql.quote_style_id left join
  ergeon.product_catalog sct on sct.id = qs.catalog_id left join
  ergeon.product_catalog ct on ct.id = ql.catalog_id --left join 
  --ergeon.quote_quote qq on qq.id = ql.quote_id
--where
  --is_cancellation = False
),
fence_cfg as (
select
  cv.config_id,
  min(case when attr_id = 3 then v.name else null end) as finish_height,
  min(case when attr_id = 4 then v.name else null end) as frame_style,
  min(case when attr_id = 6 then v.name else null end) as picket_build,
  min(case when attr_id = 7 then v.name else null end) as picket_style,
  min(case when attr_id = 8 then v.name else null end) as picket_size,
  min(case when attr_id = 9 then v.name else null end) as picket_material,
  min(case when attr_id = 10 then v.name else null end) as picket_overlap,
  min(case when attr_id = 222 then v.name else null end) as picket_gap,
  min(case when attr_id = 11 then v.name else null end) as post_size,
  min(case when attr_id = 12 then v.name else null end) as post_material,
  min(case when attr_id = 13 then v.name else null end) as post_hole_depth,
  min(case when attr_id = 14 then v.name else null end) as post_length,
  min(case when attr_id = 15 then v.name else null end) as post_reveal,
  min(case when attr_id = 227 then v.name else null end) as post_covering,
  min(case when attr_id = 17 then v.name else null end) as kick_board_size,
  min(case when attr_id = 16 then v.name else null end) as kick_board_material,
  min(case when attr_id = 18 then v.name else null end) as rails,
  min(case when attr_id = 47 then v.name else null end) as rails_material1,
  min(case when attr_id = 223 then v.name else null end) as rails_cap_material,
  min(case when attr_id = 48 then v.name else null end) as rails_size,
  min(case when attr_id = 49 then v.name else null end) as rails_cap,
  min(case when attr_id = 144 then v.name else null end) as rails_placement,
  min(case when attr_id = 21 then v.name else null end) as lattice_height,
  min(case when attr_id = 20 then v.name else null end) as lattice_style,
  min(case when attr_id = 22 then v.name else null end) as retaining_wall,
  min(case when attr_id = 5 then v.name else null end) as section_length,
  min(case when attr_id = 220 then v.name else null end) as pre_stain,
  min(case when attr_id = 221 then v.name else null end) as pre_staining_color,
  min(case when attr_id = 226 then v.name else null end) as fence_package,
  min(case when attr_id = 228 then v.name else null end) as mesh_height,
  min(case when attr_id = 229 then v.name else null end) as mesh_style,  
  min(case when attr_id = 3 then v.code else null end) as finish_height_code,
  min(case when attr_id = 4 then v.code else null end) as frame_style_code,
  min(case when attr_id = 6 then v.code else null end) as picket_build_code,
  min(case when attr_id = 7 then v.code else null end) as picket_style_code,
  min(case when attr_id = 8 then v.code else null end) as picket_size_code,
  min(case when attr_id = 9 then v.code else null end) as picket_material_code,
  min(case when attr_id = 10 then v.code else null end) as picket_overlap_code,
  min(case when attr_id = 222 then v.code else null end) as picket_gap_code,
  min(case when attr_id = 11 then v.code else null end) as post_size_code,
  min(case when attr_id = 12 then v.code else null end) as post_material_code,
  min(case when attr_id = 13 then v.code else null end) as post_hole_depth_code,
  min(case when attr_id = 14 then v.code else null end) as post_length_code,
  min(case when attr_id = 15 then v.code else null end) as post_reveal_code,
  min(case when attr_id = 227 then v.code else null end) as post_covering_code,
  min(case when attr_id = 17 then v.code else null end) as kick_board_size_code,
  min(case when attr_id = 16 then v.code else null end) as kick_board_material_code,
  min(case when attr_id = 18 then v.code else null end) as rails_code,
  min(case when attr_id = 47 then v.code else null end) as rails_material1_code,
  min(case when attr_id = 223 then v.code else null end) as rails_cap_material_code,
  min(case when attr_id = 48 then v.code else null end) as rails_size_code,
  min(case when attr_id = 49 then v.code else null end) as rails_cap_code,
  min(case when attr_id = 144 then v.code else null end) as rails_placement_code,
  min(case when attr_id = 21 then v.code else null end) as lattice_height_code,
  min(case when attr_id = 20 then v.code else null end) as lattice_style_code,
  min(case when attr_id = 22 then v.code else null end) as retaining_wall_code,
  min(case when attr_id = 5 then v.code else null end) as section_length_code,
  min(case when attr_id = 220 then v.code else null end) as pre_stain_code,
  min(case when attr_id = 221 then v.code else null end) as pre_staining_color_code,
  min(case when attr_id = 226 then v.code else null end) as fence_package_code,
  min(case when attr_id = 228 then v.code else null end) as mesh_height_code,
  min(case when attr_id = 229 then v.code else null end) as mesh_style_code,
  min(case when attr_id = 24 then v.name else null end) as gate_type,
  min(case when attr_id = 26 then v.name else null end) as gate_finish_height,
  min(case when attr_id = 27 then v.name else null end) as gate_width,
  min(case when attr_id = 28 then v.name else null end) as gate_picket_material,
  min(case when attr_id = 29 then v.name else null end) as gate_post_size,
  min(case when attr_id = 30 then v.name else null end) as steel_frame,
  min(case when attr_id = 31 then v.name else null end) as latch_kit,
  min(case when attr_id = 235 then v.name else null end) as gate_drop_rod,
  min(case when attr_id = 32 then v.name else null end) as slope,
  min(case when attr_id = 130 then v.name else null end) as slope_style,
  min(case when attr_id = 111 then v.name else null end) as repair_post, 
  min(case when attr_id = 112 then v.name else null end) as repair_rail, 
  min(case when attr_id = 113 then v.name else null end) as repair_kickboard, 
  min(case when attr_id = 114 then v.name else null end) as repair_lattice,
  min(case when attr_id = 115 then v.name else null end) as repair_picket, 
  min(case when attr_id = 116 then v.name else null end) as repair_fascia, 
  min(case when attr_id = 117 then v.name else null end) as repair_cap_rail, 
  min(case when attr_id = 119 then v.name else null end) as repais_post_cap, 
  min(case when attr_id = 120 then v.name else null end) as repair_hinge_latch, 
  min(case when attr_id = 121 then v.name else null end) as repair_trim, 
  min(case when attr_id = 122 then v.name else null end) as repair_overhead, 
  min(case when attr_id = 123 then v.name else null end) as repair_material_discount,
  min(case when attr_id = 44 then v.name else null end) as chain_link_fence,
  min(case when attr_id = 45 then v.name else null end) as chain_link_gate,
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
  min(case when attr_id = 41 then v.name else null end) as standalone_retaining_wall,
  min(case when attr_id = 133 then v.name else null end) as standalone_retaining_wall_height, 
  min(case when attr_id = 134 then v.name else null end) as standalone_retaining_wall_middle_post, 
  min(case when attr_id = 135 then v.name else null end) as standalone_retaining_wall_section_length, 
  min(case when attr_id = 136 then v.name else null end) as standalone_retaining_wall_post_size, 
  min(case when attr_id = 137 then v.name else null end) as standalone_retaining_wall_post_hole_depth, 
  min(case when attr_id = 138 then v.name else null end) as standalone_retaining_wall_slope_style, 
  min(case when attr_id = 139 then v.name else null end) as standalone_retaining_wall_slope,
  min(case when attr_id = 140 then v.name else null end) as standalone_retaining_wall_boards_material,
  min(case when attr_id = 141 then v.name else null end) as standalone_retaining_wall_cap,
  min(case when attr_id = 37 then v.name else null end) as stain_color,
  min(case when attr_id = 217 then v.name else null end) as post_staining_package, 
  min(case when attr_id = 218 then v.name else null end) as post_staining_color,
  min(case when attr_id = 219 then v.name else null end) as power_wash_day,
  min(case when attr_id = 241 then v.name else null end) as gate_frame_style,
  min(case when attr_id = 242 then v.name else null end) as gate_top,
  min(case when attr_id = 243 then v.name else null end) as gate_picket_orientation,
  min(case when attr_id = 244 then v.name else null end) as gate_picket_build,
  min(case when attr_id = 245 then v.name else null end) as gate_lattice_height,
  min(case when attr_id = 246 then v.name else null end) as gate_lattice_style,
  min(case when attr_id = 257 then v.name else null end) as gate_cap_rail_material,
  min(case when attr_id = 258 then v.name else null end) as gate_cap_rail_size,
  min(case when attr_id = 259 then v.name else null end) as gate_trims,	
  min(case when attr_id = 260 then v.name else null end) as gate_trims_size
from ergeon.calc_value v join
  ergeon.calc_configvalue cv on cv.value_id = v.id
group by 1
),
prop_cfg as 
(
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
)
select 
  order_id,
  last_quote_id as quote_id,
  qq.id as quoteline_id, 
  t.item as catalog_type,
  a.name as attr_name,
  cfg.*,
  ql_length,
  qq.price as ql_price,qq.cost as ql_cost,
  ue.* except(order_id, last_quote_id),
  prop_cfg.* except(config_id),
  row_number() over (partition by order_id) as rank_order-- added 24/8/2022
from 
  int_data.order_ue_materialized ue left join 
  ergeon.quote_quoteline qq on qq.quote_id = ue.last_quote_id left join 
  qlt on qlt.id = qq.id left join
  ergeon.product_catalogtype t on t.id = qlt.type_id left join 
  ergeon.calc_configvalue cv on cv.config_id = qq.config_id left join
  ergeon.calc_value cvv on cvv.id = cv.value_id left join
  ergeon.calc_attr a on a.id = cvv.attr_id left join
  fence_cfg cfg on cfg.config_id = cv.config_id left join 
  prop_cfg on prop_cfg.config_id = qq.property_config_id left join
  length_ql lql on lql.quoteline_id = qq.id