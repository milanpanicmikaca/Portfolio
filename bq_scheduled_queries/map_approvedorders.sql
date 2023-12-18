select 
  od.won_at, 
  od.order_id,
  cc.full_name as customer_name,
  p.name as product,
  od.initial_revenue,
  od.deal_status,
  st.label as project_status,
  a.formatted_address as address,
  ci.name as city,
  ci.location as city_location,
  co.name as county,
  co.border as county_border,
  z.location as zip_location,
  z.code as zipcode,
  z.border as zip_border,
  'https://admin.ergeon.in/quoting-tool/'||od.order_id||'/overview' as url,
  pr.name as region,
  pm.code as market,
  case when od.completed_at is null then false else true end as is_completed,
  date_diff(current_date(),od.completed_at,day) as days_completion,
from int_data.sales_dashboard_od od
  left join ergeon.store_order o on o.id = od.order_id
  left join ergeon.core_house h on h.id = o.house_id
  left join ergeon.geo_address a on a.id = h.address_id
  left join ergeon.geo_county co on co.id = a.county_id
  left join ergeon.geo_city ci on ci.id = a.city_id
  left join ergeon.geo_zipcode z on z.id = a.zip_code_id
  left join ergeon.hrm_staff hs on hs.id = o.project_manager_id
  left join ergeon.product_countymarket cm on cm.county_id = co.id
  left join ergeon.product_market pm on pm.id = cm.market_id
  left join ergeon.product_region pr on pr.id = pm.region_id
  left join ergeon.core_user u on u.id = hs.user_id
  left join ergeon.customers_contact cc on cc.address_id = a.id
  left join ergeon.store_product p on p.id = o.product_id
  left join ergeon.core_statustype st on st.id = o.project_status_id
where won_at is not null