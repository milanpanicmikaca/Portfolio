-- yelp rank changes made
-- yelp rank changes made
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
    from unnest(generate_date_array('2022-01-01',current_date(), interval 1 day)) as date_array
),
yelp_rank as (
select
  pm.code,
  pm.id,
  pm.region_id,
  pr.name,yr.*
from int_data.yelp_rank yr
left join ergeon.product_market pm on pm.code =  yr.market
left join ergeon.product_region pr on pr.id = pm.region_id
left join ergeon.product_countymarket pcnm on pm.id = pcnm.market_id
),calc_rank as(
select 
date_trunc(date,{period}) as date,
coalesce(avg(case when id in (2,10,9,3,29,4,31,30,8,13) then rank else null end),0) as MAR2096,
coalesce(avg(case when id = 3 then rank else null end),0) as MAR2128,
coalesce(avg(case when id = 10 then rank else null end),0) as MAR2103,
coalesce(avg(case when id = 9 then rank else null end),0) as MAR2104,
coalesce(avg(case when id = 2 then rank else null end),0) as MAR2105,
coalesce(avg(case when id = 31 then rank else null end),0) as MAR2106,
coalesce(avg(case when id = 8 then rank else null end),0) as MAR2107,
coalesce(avg(case when id = 30 then rank else null end),0) as MAR2108,
coalesce(avg(case when id = 29 then rank else null end),0) as MAR2109,
coalesce(avg(case when id = 4 then rank else null end),0) as MAR2110,
coalesce(avg(case when id in (6,5,14,7,1,12,11) then rank else null end),0) as MAR2095,
coalesce(avg(case when id = 6 then rank else null end),0) as MAR2111,
coalesce(avg(case when id = 5 then rank else null end),0) as MAR2112,
coalesce(avg(case when id = 1 then rank else null end),0) as MAR2113,
coalesce(avg(case when id = 14 then rank else null end),0) as MAR2114,
coalesce(avg(case when id = 7 then rank else null end),0) as MAR2115,
coalesce(avg(case when code like '%-FL-%' then rank else null end),0) as MAR2100,
coalesce(avg(case when id = 24 then rank else null end),0) as MAR2116,
coalesce(avg(case when id in (21,22,33,35) then rank else null end),0) as MAR2127,
coalesce(avg(case when id = 22 then rank else null end),0) as MAR2118,
coalesce(avg(case when id = 21 then rank else null end),0) as MAR2119,
coalesce(avg(case when id = 33 then rank else null end),0) as MAR2120,
coalesce(avg(case when id = 35 then rank else null end),0) as MAR2126,
coalesce(avg(case when name = 'West South Central' then rank else null end),0) as MAR2097,
coalesce(avg(case when id = 32 then rank else null end),0) as MAR2121,
coalesce(avg(case when id = 16 then rank else null end),0) as MAR2122,
coalesce(avg(case when id = 17 then rank else null end),0) as MAR2123,
coalesce(avg(case when id = 18 then rank else null end),0) as MAR2124,
coalesce(avg(case when id = 19 then rank else null end),0) as MAR2125,
coalesce(avg(case when code like '%-GA-%' then rank else null end),0) as MAR2098,
coalesce(avg(case when id = 20 then rank else null end),0) as MAR2117,
coalesce(avg(case when code like '%-MD-%' then rank else null end),0) as MAR2099,
coalesce(avg(case when code like '%-PA-%' then rank else null end),0) as MAR2101,
coalesce(avg(case when code like '%-VA-%' then rank else null end),0) as MAR2102,
coalesce(avg(case when id = 43 then rank else null end),0) as MAR2272,
coalesce(avg(case when id in (42,57,58) then rank else null end),0) as MAR2511,
coalesce(avg(case when id = 42 then rank else null end),0) as MAR2906,
coalesce(avg(case when id = 57 then rank else null end),0) as MAR2965,
coalesce(avg(case when id = 58 then rank else null end),0) as MAR3024
from yelp_rank yr 
group by 1
)
select 
t.day as date,
c.*except(date)
from calc_rank c
left join timeseries t on t.day = c.date
where period_rank = 1
