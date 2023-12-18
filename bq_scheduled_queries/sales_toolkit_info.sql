with active_markets as (
  select distinct
    m.code as market,
  from ergeon.product_marketproduct mp
  left join ergeon.product_market m on m.id = mp.market_id
  where mp.is_active is true
), priority_pages as (
  select
    market,
    priority_1 as p1,
    priority_2 as p2,
  from googlesheets.sales_toolkit_pages p
), info_pages as (
select 
  market,
  case when p1 is null then "Google" else p1 end as p1,
  case when p2 is null then "Yelp" else p2 end as p2,
from active_markets am 
left join priority_pages pp using(market) 
), priority_pivot as (
select * from info_pages
unpivot
  (channel for level
  in (p1, p2))
), pages as (
select distinct
  p.*,
  mp.city,
  case when mp.county not like '%County' then mp.county else LEFT(mp.county, length(mp.county) - 7) end as county,
  case
    when channel like '%Yelp%' then yelp_review_url
    when channel like '%Google%' then google_page_url
    when channel like '%HomeAdvisor%' then homeadvisor_page_url
    when channel like '%Thumbtack%' then thumbtack_page_url
    when channel like '%BBB%' then bbb_page_url
    when channel like '%Angi%' then angi_page_url
    end as url,
  case
    when channel like '%Yelp%' then yelp_page
    when channel like '%Google%' then google_page
    when channel like '%HomeAdvisor%' then homeadvisor_page
    when channel like '%Thumbtack%' then thumbtack_page
    when channel like '%BBB%' then bbb_page
    when channel like '%Angi%' then angi_page
    end as page,
from priority_pivot p
left join googlesheets.zip_to_review mp using (market)
),services as (
  select
    gc.id as city_id,
    gc.name as city,
    case when go.name not like '%County' then go.name else LEFT(go.name, length(go.name) - 7) end as county,
    case when sum(case when ts.service_id = 10 then 1 else 0 end) > 0 then true end as wood_fence,
    case when sum(case when s.label like '%Grass%' then 1 else 0 end) >0 then true end as artificial_grass,
    case when sum(case when s.label like '%Vinyl%'
      or s.label like '%PVC%' then 1 else 0 end) > 0  then true end as vinyl,
    case when sum(case when s.label like '%Painting%'
      or s.label like '%Staining&' then 1 else 0 end) > 0 then true end as  staining,
    case when sum(case when s.label like '%Chain Link%' then 1 else 0 end) > 0 then true end as chain_link,
    case when sum(case when s.label like 'Repair or Partially Replace a Wood Fence' then 1 else 0 end) > 0 then true end as repair_wood_fence,
  from ergeon.geo_zipcode gz
    left join ergeon.product_tierzipcode tz on tz.zip_code_id = gz.id
    left join ergeon.product_tier t on t.id = tz.tier_id
    left join ergeon.product_tierservice ts on ts.tier_id = t.id
    left join ergeon.product_service s on s.id = ts.service_id
    left join ergeon.geo_city gc on gc.id = gz.city_id 
    left join ergeon.geo_county go on go.id = gz.county_id
    left join ergeon.core_statustype st on st.id = t.status_id
  where st.id = 63 -- 63 is tier_status -> active
    and t.name is not null
  group by 1,2,3
), general_info as (
  (select
    cast(zi.code as integer) as id,
    ci.id as city_id,
    'zipcode' as search_type,
    ci.name as city,
    case when co.name not like '%County' then co.name else LEFT(co.name, length(co.name) - 7) end as county,
    "" as formatted_address,
    "" as type,
    "" as pd_link,
    "" as admin_link,
    pl.quote_string as license,
    pl.id as license_id,
    pm.code as market,
    coalesce(rs.rocky_soil, 'partially') as rocky_soil,
    array_to_string(array_agg(p.name),", ") as product,
  from ergeon.geo_zipcode zi
    left join ergeon.product_tierzipcode pt on pt.zip_code_id = zi.id
    left join ergeon.product_tier t on t.id = pt.tier_id and t.type = "customer"
    left join ergeon.product_tierproduct tp on tp.tier_id = t.id
    left join ergeon.store_product p on p.id = tp.product_id
    left join ergeon.geo_city ci on ci.id = zi.city_id
    left join ergeon.geo_county co on co.id = zi.county_id
    left join ergeon.product_license pl on pl.city_id = ci.id and  pl.county_id = co.id
    left join ergeon.product_countymarket cm on cm.county_id = co.id
    left join ergeon.product_market pm on pm.id = cm.market_id
    left join ergeon.core_statustype cs on cs.id = t.status_id
    left join int_data.sales_toolkit_rockysoil rs on rs.zip_code = cast(zi.code as int64)
  where cs.type = 'tier_status'
    and cs.code = 'active'
    and p.name <> 'Driveway Installation'
    and pt.tier_id is not null
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
union all
  (select
    o.id as id,
    ci.id as city_id,
    'order' as search_type,
    ci.name as city,
    case when co.name not like '%County' then co.name else LEFT(co.name, length(co.name) - 7) end as county,
    ga.formatted_address,
    case when cc.is_commercial is true then 'commercial' else 'residential' end as type,
    'https://ergeon.pipedrive.com/deal/' || pipedrive_deal_key as pd_link,
    'https://admin.ergeon.in/quoting-tool/' || o.id || '/overview/' as admin_link,
    pl.quote_string as license,
    pl.id as license_id,
    pm.code as market,
    rs.rocky_soil,
    p.name as product,
  from ergeon.store_order o
    left join ergeon.core_house h on h.id = o.house_id
    left join ergeon.geo_address ga on ga.id = h.address_id
    left join ergeon.geo_city ci on ci.id = ga.city_id
    left join ergeon.geo_county co on co.id = ga.county_id
    left join ergeon.geo_zipcode zi on zi.id = ga.zip_code_id
    left join ergeon.customers_customer cc on cc.id = h.customer_id
    left join ergeon.product_license pl on pl.city_id = ga.city_id and  pl.county_id = ga.county_id
    left join ergeon.product_countymarket cm on cm.county_id = co.id
    left join ergeon.product_market pm on pm.id = cm.market_id
    left join int_data.sales_toolkit_rockysoil rs on rs.zip_code = cast(zi.code as int64)
    left join ergeon.store_product p on p.id = o.product_id
  where cancelled_at is null
  order by o.created_at desc)
)
select
  gi.* except(city_id),
  tp.* except(city_id,city,county),
  cr.* except(city_id,city, county), 
  rl.email as ls_email,
  rl.phone_number as ls_phone_number,
  rl.city_web_url as ls_city_web_url,
  rl.county_web_url as ls_county_web_url,
  rl.landscape_regulation_url as ls_landscape_regulation_url,
  rl.front_yard_coverage as ls_front_yard_coverage,
  rl.plant_list as ls_plant_list,
  rl.decks as ls_decks,
  rl.arbors as ls_arbors,
  rl.accesory_structures as ls_accesory_structures,
  rl.pools as ls_pools,
  rl.artificial_turf as ls_artificial_turf,
  rl.permit as ls_permit,
  p1.page as p1_page,
  p2.page as p2_page,
  p3.page as p3_page,
  p4.page as p4_page,
  p5.page as p5_page,
  p6.page as p6_page,
  p1.url as p1_url,
  p2.url as p2_url,
  p3.url as p3_url,
  p4.url as p4_url,
  p5.url as p5_url,
  p6.url as p6_url,
  ct.comment as tat,
  coalesce(dc.consideration,dc1.consideration) as regional_considerations,
  fs.* except(market),
  sd.discount,
  se.* except(city, county),
  tg.tag
from general_info gi
left join pages p1 on p1.city = gi.city and p1.county = gi.county and p1.level = 'p1'
left join pages p2 on p2.city = gi.city and p2.county = gi.county and p2.level = 'p2'
left join pages p3 on p3.city = gi.city and p3.county = gi.county and p3.level = 'p3'
left join pages p4 on p4.city = gi.city and p4.county = gi.county and p4.level = 'p4'
left join pages p5 on p5.city = gi.city and p5.county = gi.county and p5.level = 'p5'
left join pages p6 on p6.city = gi.city and p6.county = gi.county and p6.level = 'p6'
left join int_data.sales_toolkit_photographers tp on tp.city_id = gi.city_id
left join int_data.cityregulations cr on cr.city_id = gi.city_id
left join int_data.cityregulations_landscape rl on cast(rl.city_id as int64) = gi.city_id
left join int_data.delivery_considerations_tat ct on ct.market = gi.market
left join int_data.delivery_considerations dc on dc.market = gi.market
left join int_data.delivery_considerations dc1 on dc1.county = gi.county
left join int_data.sales_toolkit_fencestyle fs on fs.market = gi.market
left join int_data.sales_toolkit_discounts sd on sd.order_id = gi.id and search_type = 'order'
left join services se on se.city_id = gi.city_id
left join int_data.sales_toolkit_tags tg on tg.market = gi.market