with tiers as (
      select 1 as tmin, 1000 as tmax, 20 as tier union all
      select 1000 as tmin, 1500 as tmax, 30 as tier union all
      select 1500 as tmin, 2500 as tmax, 50 as tier union all
      select 2500 as tmin, 4500 as tmax, 70 as tier union all
      select 4500 as tmin, 6500 as tmax, 90 as tier union all
      select 6500 as tmin, 10000 as tmax, 110 as tier union all
      select 10000 as tmin, 20000 as tmax, 150 as tier union all
      select 20000 as tmin, 50000 as tmax, 200 as tier union all
      select 50000 as tmin, 250000 as tmax, 300 as tier union all
      select 250000 as tmin, 500000 as tmax, 400 as tier union all
      select 500000 as tmin, 1000000 as tmax, 500 as tier union all
      select 1000000 as tmin, 100000000 as tmax, 600 as tier
), calc_data as (
select 
    won_at as date,
    timestamp,
    email,
    sales_rep,
    staff_id,
    order_id,
    initial_revenue,
    bonus,
    st_geogpoint(cast(regexp_extract(latlong, '([-]\\d+[.]\\d+)') as float64),cast(regexp_extract(latlong, '(\\d+[.]\\d+)[,]') as FLOAT64)) as geopoint,
from int_data.sales_dashboard_arts
where deal_status = 'Deal Won'
),
filtered_data as (
select 
    a.email,
    a.date parent_date,
    a2.date child_date,
    a.sales_rep,
    a.order_id as parent,
    a2.order_id as child,
    a.initial_revenue as parent_revenue,
    a2.initial_revenue as child_revenue,
    a.bonus as parent_bonus,
    a2.bonus as child_bonus,
    a.staff_id,
    rank() over (partition by a2.order_id order by a.timestamp) as rank,
from calc_data a 
    left join calc_data a2 on a.sales_rep = a2.sales_rep
    and date_diff(a2.date, a.date, day) between 0 and 90
    and a.timestamp < a2.timestamp
    and a.order_id <> a2.order_id
where a2.order_id is not null
    and st_dwithin(a2.geopoint, a.geopoint, 61)
),
final_h as (
select 
    * except(rank)
from filtered_data
where rank = 1
), lists as (
select distinct
  parent_date as close_date,
  email,
  sales_rep,
  parent as order_id, 
  parent as parent, 
  parent_revenue as revenue,
  parent_bonus as bonus,
  staff_id
from final_h fh
union all
select
  child_date as close_date,
  email,
  sales_rep,
  child as order_id, 
  parent, 
  child_revenue as revenue,
  child_bonus as bonus,
  staff_id
from final_h fh
), running_sum as (
select 
  *,
  sum(revenue) over (partition by parent order by close_date,order_id) as running_rev,
  sum(bonus) over (partition by parent order by close_date,order_id) as running_bonus
from lists
)
select 
  rs.*,
  t.tier,
  coalesce(lag(rs.running_bonus - t.tier) over(partition by parent order by close_date,order_id) - (rs.running_bonus - t.tier),0) adjustment
from running_sum rs 
left join tiers t on t.tmin <= rs.running_rev and t.tmax >= rs.running_rev 
