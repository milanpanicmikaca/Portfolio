with timeseries as (
  select 
    date_trunc(date_array,day) as date,
    date_trunc(date_array, {period}) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
  from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as date_array
) 
, installation_scheduled as (
  select 
    a.order_id,
    datetime(a.created_at, "America/Los_Angeles") as last_booked,
    datetime(timestamp(concat(a.date, " ", a.time_start)), "America/Los_Angeles") as last_scheduled
  from ergeon.schedule_appointment a
  left join ergeon.schedule_appointmenttype apt on apt.id = a.appointment_type_id
  where 
    a.cancelled_at is null
  and
    apt.code = 'installation'
  qualify row_number() over (partition by a.order_id order by a.created_at desc) = 1
)
, orders as (
select
  om.order_id,
  om.product_id,
  om.market_id,
  isch.last_booked,
  isch.last_scheduled,
  om.won_ts_at,
  datetime_diff(last_scheduled, won_ts_at, hour)/24 as won_to_scheduled
from int_data.order_ue_materialized om
join installation_scheduled isch on isch.order_id = om.order_id
where om.won_at is not null
)
, last_ten_booked as (
select
  o.*,
  t.date,
  row_number() over (partition by product_id, market_id, date order by last_booked desc) as rn
from orders o
left join timeseries t on cast(date_trunc(o.last_booked, day) as date) <= t.date
qualify rn <= 10
)
, last_ten_booked_market as (
select
  o.*,
  t.date,
  row_number() over (partition by market_id, date order by last_booked desc) as rn
from orders o
left join timeseries t on cast(date_trunc(o.last_booked, day) as date) <= t.date
qualify rn <= 10
)
, final as (
  select 
    date,
    product_id,
    market_id,
    round(percentile_cont(won_to_scheduled, 0.5) over (partition by product_id, market_id, date)) as median_w2sch,
    round((round(percentile_cont(won_to_scheduled, 0.5) over (partition by product_id, market_id, date)))/7) as median_w2sch_week
  from last_ten_booked
  qualify row_number() over (partition by product_id, market_id, date) = 1
)
, final_market as (
  select 
    date,
    market_id,
    round(percentile_cont(won_to_scheduled, 0.5) over (partition by market_id, date)) as median_w2sch_m,
    round((round(percentile_cont(won_to_scheduled, 0.5) over (partition by market_id, date)))/7) as median_w2sch_week_m
  from last_ten_booked_market
  qualify row_number() over (partition by market_id, date) = 1
)
, current_wait_time as (
select 
  t.date,
  --Fence metrics
  sum(case when product_id = 105 and market_id = 2 then median_w2sch_week else null end) as DEL423F, --CN-EB fence
  sum(case when product_id = 105 and market_id = 8 then median_w2sch_week else null end) as DEL563F, --CN-SF fence
  sum(case when product_id = 105 and market_id = 3 then median_w2sch_week else null end) as DEL424F, --CN-SA fene
  sum(case when product_id = 105 and market_id = 4 then median_w2sch_week else null end) as DEL425F, --CN-WA fence
  sum(case when product_id = 105 and market_id = 9 then median_w2sch_week else null end) as DEL430F, --CN-NB fence
  sum(case when product_id = 105 and market_id = 10 then median_w2sch_week else null end) as DEL431F, --CN-FR fence
  sum(case when product_id = 105 and market_id = 29 then median_w2sch_week else null end) as DEL443F, --CN-ST fence
  sum(case when product_id = 105 and market_id = 30 then median_w2sch_week else null end) as DEL444F, --CN-SJ fence
  sum(case when product_id = 105 and market_id = 31 then median_w2sch_week else null end) as DEL445F, --CN-PA fence
  sum(case when product_id = 105 and market_id = 1 then median_w2sch_week else null end) as DEL422F, --CS-SD fence
  sum(case when product_id = 105 and market_id = 5 then median_w2sch_week else null end) as DEL426F, --CS-OC fence
  sum(case when product_id = 105 and market_id = 6 then median_w2sch_week else null end) as DEL427F, --CS-LA fence
  sum(case when product_id = 105 and market_id = 7 then median_w2sch_week else null end) as DEL428F, --CS-VC fence
  sum(case when product_id = 105 and market_id = 14 then median_w2sch_week else null end) as DEL435F, --CS-SV fence
  sum(case when product_id = 105 and market_id = 16 then median_w2sch_week else null end) as DEL436F, --TX-DL fence
  sum(case when product_id = 105 and market_id = 17 then median_w2sch_week else null end) as DEL437F, --TX-FW fence
  sum(case when product_id = 105 and market_id = 18 then median_w2sch_week else null end) as DEL438F, --TX-HT fence
  sum(case when product_id = 105 and market_id = 19 then median_w2sch_week else null end) as DEL439F, --TX-SA fence
  sum(case when product_id = 105 and market_id = 32 then median_w2sch_week else null end) as DEL446F, --TX-AU fence
  sum(case when product_id = 105 and market_id = 20 then median_w2sch_week else null end) as DEL440F, --GA-AT fence
  sum(case when product_id = 105 and market_id = 22 then median_w2sch_week else null end) as DEL442F, --MD-BL fence
  sum(case when product_id = 105 and market_id = 21 then median_w2sch_week else null end) as DEL441F, --MD-DC fence
  sum(case when product_id = 105 and market_id = 33 then median_w2sch_week else null end) as DEL447F, --PA-PH fence
  sum(case when product_id = 105 and market_id = 35 then median_w2sch_week else null end) as DEL448F, --VA-AR fence
  sum(case when product_id = 105 and market_id = 24 then median_w2sch_week else null end) as DEL429F, --FL-MI fence
  sum(case when product_id = 105 and market_id = 26 then median_w2sch_week else null end) as DEL432F, --FL-OR fence
  sum(case when product_id = 105 and market_id = 43 then median_w2sch_week else null end) as DEL434F, --WA-SE fence
  sum(case when product_id = 105 and market_id = 42 then median_w2sch_week else null end) as DEL578F, --WN-CH fence
  sum(case when product_id = 105 and market_id = 57 then median_w2sch_week else null end) as DEL589F, --WN-NA fence
  sum(case when product_id = 105 and market_id = 58 then median_w2sch_week else null end) as DEL600F, --WN-LA fence
  --Turf metrics
  sum(case when product_id = 132 and market_id = 2 then median_w2sch_week else null end) as DEL423T, --CN-EB turf
  sum(case when product_id = 132 and market_id = 3 then median_w2sch_week else null end) as DEL424T, --CN-SA turf
  sum(case when product_id = 132 and market_id = 4 then median_w2sch_week else null end) as DEL425T, --CN-WA turf
  sum(case when product_id = 132 and market_id = 9 then median_w2sch_week else null end) as DEL430T, --CN-NB turf
  sum(case when product_id = 132 and market_id = 10 then median_w2sch_week else null end) as DEL431T, --CN-FR turf
  sum(case when product_id = 132 and market_id = 29 then median_w2sch_week else null end) as DEL443T, --CN-ST turf
  sum(case when product_id = 132 and market_id = 30 then median_w2sch_week else null end) as DEL444T, --CN-SJ turf
  sum(case when product_id = 132 and market_id = 31 then median_w2sch_week else null end) as DEL445T, --CN-PA turf
  sum(case when product_id = 132 and market_id = 1 then median_w2sch_week else null end) as DEL422T, --CS-SD turf
  sum(case when product_id = 132 and market_id = 5 then median_w2sch_week else null end) as DEL426T, --CS-OC turf
  sum(case when product_id = 132 and market_id = 6 then median_w2sch_week else null end) as DEL427T, --CS-LA turf
  sum(case when product_id = 132 and market_id = 7 then median_w2sch_week else null end) as DEL428T, --CS-VC turf
  sum(case when product_id = 132 and market_id = 14 then median_w2sch_week else null end) as DEL435T, --CS-SV turf
  sum(case when product_id = 132 and market_id = 16 then median_w2sch_week else null end) as DEL436T, --TX-DL turf
  sum(case when product_id = 132 and market_id = 17 then median_w2sch_week else null end) as DEL437T, --TX-FW turf
  sum(case when product_id = 132 and market_id = 18 then median_w2sch_week else null end) as DEL438T, --TX-HT turf
  sum(case when product_id = 132 and market_id = 19 then median_w2sch_week else null end) as DEL439T, --TX-SA turf
  sum(case when product_id = 132 and market_id = 32 then median_w2sch_week else null end) as DEL446T, --TX-AU turf
  sum(case when product_id = 132 and market_id = 20 then median_w2sch_week else null end) as DEL440T, --GA-AT turf
  sum(case when product_id = 132 and market_id = 22 then median_w2sch_week else null end) as DEL442T, --MD-BL turf
  sum(case when product_id = 132 and market_id = 21 then median_w2sch_week else null end) as DEL441T, --MD-DC turf
  sum(case when product_id = 132 and market_id = 33 then median_w2sch_week else null end) as DEL447T, --PA-PH turf
  sum(case when product_id = 132 and market_id = 35 then median_w2sch_week else null end) as DEL448T, --VA-AR turf
  sum(case when product_id = 132 and market_id = 24 then median_w2sch_week else null end) as DEL429T, --FL-MI turf
  sum(case when product_id = 132 and market_id = 26 then median_w2sch_week else null end) as DEL432T, --FL-OR turf
  sum(case when product_id = 132 and market_id = 43 then median_w2sch_week else null end) as DEL434T --WA-SE turf
from timeseries t
left join final f on f.date = t.date
group by 1
qualify rank() over (partition by date_trunc(t.date, {period}) order by t.date desc) = 1
)
, current_wait_time_market as (
select 
  t.date,
  sum(case when market_id = 2 then median_w2sch_week_m else null end) as DEL423, --CN-EB
  sum(case when market_id = 3 then median_w2sch_week_m else null end) as DEL424, --CN-SA
  sum(case when market_id = 4 then median_w2sch_week_m else null end) as DEL425, --CN-WA
  sum(case when market_id = 9 then median_w2sch_week_m else null end) as DEL430, --CN-NB
  sum(case when market_id = 10 then median_w2sch_week_m else null end) as DEL431, --CN-FR
  sum(case when market_id = 29 then median_w2sch_week_m else null end) as DEL443, --CN-ST
  sum(case when market_id = 30 then median_w2sch_week_m else null end) as DEL444, --CN-SJ
  sum(case when market_id = 31 then median_w2sch_week_m else null end) as DEL445, --CN-PA
  sum(case when market_id = 1 then median_w2sch_week_m else null end) as DEL422, --CS-SD
  sum(case when market_id = 5 then median_w2sch_week_m else null end) as DEL426, --CS-OC
  sum(case when market_id = 6 then median_w2sch_week_m else null end) as DEL427, --CS-LA
  sum(case when market_id = 7 then median_w2sch_week_m else null end) as DEL428, --CS-VC
  sum(case when market_id = 14 then median_w2sch_week_m else null end) as DEL435, --CS-SV
  sum(case when market_id = 16 then median_w2sch_week_m else null end) as DEL436, --TX-DL
  sum(case when market_id = 17 then median_w2sch_week_m else null end) as DEL437, --TX-FW
  sum(case when market_id = 18 then median_w2sch_week_m else null end) as DEL438, --TX-HT
  sum(case when market_id = 19 then median_w2sch_week_m else null end) as DEL439, --TX-SA
  sum(case when market_id = 32 then median_w2sch_week_m else null end) as DEL446, --TX-AU
  sum(case when market_id = 20 then median_w2sch_week_m else null end) as DEL440, --GA-AT
  sum(case when market_id = 22 then median_w2sch_week_m else null end) as DEL442, --MD-BL
  sum(case when market_id = 21 then median_w2sch_week_m else null end) as DEL441, --MD-DC
  sum(case when market_id = 33 then median_w2sch_week_m else null end) as DEL447, --PA-PH
  sum(case when market_id = 35 then median_w2sch_week_m else null end) as DEL448, --VA-AR
  sum(case when market_id = 24 then median_w2sch_week_m else null end) as DEL429, --FL-MI
  sum(case when market_id = 26 then median_w2sch_week_m else null end) as DEL432, --FL-OR
  sum(case when market_id = 43 then median_w2sch_week_m else null end) as DEL434, --WA-SE
  sum(case when market_id = 42 then median_w2sch_week_m else null end) as DEL578, --WN-CH
  sum(case when market_id = 57 then median_w2sch_week_m else null end) as DEL589, --WN-NA
  sum(case when market_id = 58 then median_w2sch_week_m else null end) as DEL600 --WN-LA
from timeseries t
left join final_market fr on fr.date = t.date
group by 1
qualify rank() over (partition by date_trunc(t.date, {period}) order by t.date desc) = 1
)
select 
  t.period as date,
  c.* except (date),
  cm.* except(date)
from timeseries t
left join current_wait_time c on date_trunc(c.date, {period}) = t.period
left join current_wait_time_market cm on date_trunc(cm.date, {period}) = t.period
where period_rank = 1
