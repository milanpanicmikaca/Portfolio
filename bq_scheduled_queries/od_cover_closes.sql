with min_won_id as (
  select 
    order_id,
    id, 
    rank() over(partition by order_id order by approved_at,id) as rank,
    approved_at
  from ergeon.quote_quote 
  where approved_at is not null 
  and created_at > '2018-04-15'
), won_date as (
  select 
    order_id,
    id as first_approved_quote_id,
    approved_at as won_at 
  from min_won_id 
  where rank = 1
)
select
  o.id as order_id,
  extract( date from wd.won_at AT TIME ZONE 'America/Los_Angeles') as won_at,
  coalesce(round(fap.total_price, 0), 0) as first_approved_price,
  1 as is_won,
  u.full_name,
  r.name as region,
  u3.full_name as team_lead,
  t.name as house,
  ue.segment,
from ergeon.store_order o 
  left join won_date wd on wd.order_id = o.id
  left join ergeon.quote_quote fap on fap.id = wd.first_approved_quote_id
  left join ergeon.core_house h on h.id = o.house_id
  left join ergeon.geo_address ga on ga.id = h.address_id
  left join ergeon.geo_county cn on cn.id = ga.county_id 
  left join ergeon.product_countymarket pcnm on pcnm.county_id = cn.id 
  left join ergeon.product_market m on m.id = pcnm.market_id 
  left join ergeon.product_region r on r.id = m.region_id 
  left join ergeon.hrm_staff hs on hs.id = o.sales_rep_id
  left join ergeon.core_user u on u.id = hs.user_id
  left join ergeon.hrm_stafflog sl on sl.id = hs.current_stafflog_id
  left join ergeon.hrm_team t on t.id = sl.team_id
  left join ergeon.hrm_staff tl on tl.id = t.lead_id
  left join ergeon.core_user u3 on u3.id = tl.user_id
  left join int_data.order_ue_materialized ue on ue.order_id = o.id
where wd.won_at is not null
and u.full_name is not null
and o.parent_order_id is null
--cover page od cost test