with timeseries as (
  select 
    date_trunc(date_array,week(monday)) as date,
  from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
  group by 1
), active_zipcodes as (
  select distinct
    z.code as zipcode,
    ci.name as city,
    co.name as county,
    m.code as market,
    s.name as state,
  from ergeon.geo_zipcode z 
    left join ergeon.product_tierzipcode tz on tz.zip_code_id = z.id
    left join ergeon.product_tier t on t.id = tz.tier_id
    left join ergeon.geo_city ci on ci.id = z.city_id
    left join ergeon.geo_county co on co.id = z.county_id
    left join ergeon.geo_state s on s.id = co.state_id
    left join ergeon.product_countymarket cm on cm.county_id = co.id
    left join ergeon.product_market m on m.id = cm.market_id
  where t.status_id = 63 --Active
), active_zip_week as (
  select
    date,
    zipcode,
    city,
    county,
    market,
    state,
  from timeseries, active_zipcodes
), deals as (
  select 
    date_trunc(od.created_at, week(monday)) as date,
    od.zipcode,
    count(*) as deals,
  from int_data.sales_dashboard_od od
  where is_cancelled = 0
  group by 1, 2
  order by 1 desc
), regions_zip as (
  select 
    id,
    name,
    replace(zipcode,'"', '') as zipcode
  from ergeon.schedule_salesregion r, unnest(zip_codes) as zipcode
  where name not like "%(Unused)%"
    and name not like "%(Hold)%"
), photographers_regions as (
  select 
    sa.sales_region_id as id,
    u.full_name,
    date,
  from ergeon.schedule_availability sa
    left join ergeon.hrm_staff s on s.id = sa.employee_id
    left join ergeon.core_user u on u.id = s.user_id
), coverage as (
  select 
    date_trunc(date, week(monday)) as date,
    zipcode,
    c.name as city,
    co.name as county,
    string_agg(distinct pr.full_name,", ") as photographers,
  from regions_zip z 
    left join photographers_regions pr using (id)
    left join ergeon.geo_zipcode gz on gz.code = z.zipcode
    left join ergeon.geo_city c on c.id = gz.city_id
    left join ergeon.geo_county co on co.id = gz.county_id
  where full_name is not null
  and date <= current_date()
  group by 1, 2, 3, 4
)
select 
  az.date,
  az.zipcode,
  az.city,
  az.county, 
  az.market,
  az.state,
  d.deals,
  c.photographers,
  z.border,
  case when c.photographers is not null then 1 end as coverage,
  case when c.photographers is null then false else true end as has_coverage
from active_zip_week az
left join coverage c on c.date = az.date and c.zipcode = az.zipcode
left join deals d on d.date = az.date and d.zipcode = az.zipcode
left join ergeon.geo_zipcode z on z.code = az.zipcode
where coalesce(deals,case when c.photographers is not null then 1 end) is not null
