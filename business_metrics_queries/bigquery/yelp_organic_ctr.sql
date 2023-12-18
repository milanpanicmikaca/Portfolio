with yelp_ranks as 
( --collect all yelp ranks from old table and new table
  select date_trunc(date, week(Monday)) as date, city, market, round(avg(rank)) as rank  from ( --it will be always be by week, because generator created to run once a week and rank is valid for the whole given week
    select 
      date,
      city,
      new_market as market,
      rank
    from int_data.yelp_rank
    where date >= '2022-08-31'
    and rank is not null
    union all
    select
      date,
      city,
      market,
      rank
    from int_data.yelp_rank_history
    where date < '2022-08-31'
    and rank is not null
    )
  group by 1,2,3
),
yelp_ranks_borders as 
(
  --borders of first rank and last rank to use for substitution
  select 
    market, 
    city, 
    min(date_trunc(date, week(monday))) as first_rank, 
    max(date_add(date_trunc(date, week(monday)), interval 6 day)) last_rank
  from yelp_ranks
  group by 1,2
),
timeseries_data as 
( --get all dates for all cities and markets
  select
    dd as date, 
    market, 
    city
  from unnest(generate_date_array('2019-10-15',current_date("America/Los_Angeles"), interval 1 day)) as dd
  cross join (select distinct market, city from yelp_ranks) 
),
yelp_ranks_ranges as 
( --generate date ranges for each city, market
  select 
    t.*,
    b.first_rank, 
    b.last_rank 
  from timeseries_data t
  join yelp_ranks_borders b on b.market = t.market
                            and b.city = t.city
                            and t.date between b.first_rank and b.last_rank
), 
timeseries_data_rank as 
( --get rank for all periods
  select 
    t.date, 
    t.market, 
    t.city, 
    ifnull(last_value(y.rank ignore nulls) over (partition by t.market, t.city order by t.date ROWS BETWEEN unbounded preceding AND current row),0) as rank
  from yelp_ranks_ranges t 
  left join yelp_ranks y on date_trunc(t.date, week(monday)) = date_trunc(y.date, week(monday)) 
                         and t.market = y.market 
                         and t.city = y.city
), 
cities_population as 
(
  select 
    pm.id as market_id,
    pm.code as market,
    gcty.name as county_name,
    gc.name as city_name,
    gc.population,
    case 
        when pm.id in (2,10,9,3,29,4,31,30,8,13) then 'North California'
        when pm.id in (6,5,14,7,1,12,11) then 'South California'
        when pm.code like '%-TX-%' then 'Texas'
        when pm.code like '%-GA-%' then 'Georgia'
        when pm.code like '%-MD-%' then 'Maryland'
        when pm.code like '%-PA-%' then 'Pennsylvania'
        when pm.code like '%-VA-%' then 'Virginia'
        when pm.code like '%-FL-%' then 'Florida'
        when pm.code like '%-WA-%' then 'Washington'
        when pm.code like '%-IL-%' then 'Illinois'
        else 'Other'
    end as region,
  from ergeon.product_marketproduct pmp
  join ergeon.product_market pm on pm.id = pmp.market_id
  join ergeon.product_countymarket cm on cm.market_id = pm.id
  join ergeon.geo_county gcty on gcty.id = cm.county_id
  join ergeon.geo_city gc on gc.county_id = gcty.id
  where 
    gc.population > 10000
  and
    pmp.product_id = 105
  and 
    pmp.is_active
  qualify row_number() over (partition by pm.id, gc.name order by gc.population) = 1
), 
market_total_ppl as 
( --sum total population by market
  select 
    market, 
    sum(population) as total_population
  from cities_population
  group by 1
), 
region_total_ppl as 
( --sum total population by region
  select 
    region, 
    sum(population) as total_population
  from cities_population
  group by 1
), 
population_data as
(
  select 
    f.*, 
    c.region,
    ifnull(c.population,0) as population 
  from timeseries_data_rank f
  left join cities_population c on c.city_name = f.city 
                                and c.market = f.market
), 
weighted_population_data as 
(
  select
    p.*,
    cast(c.clicks as int64) / 100 * population as weighted_population_by_rank
  from population_data p
  left join int_data.yelp_click_by_rank c on cast(c.rank as int64) = p.rank
), 
market_ctr as 
(
  select
    w.date, w.market,
    ifnull(sum(weighted_population_by_rank) / nullif(min(m.total_population),0),0) as market_ctr
  from weighted_population_data w
  left join market_total_ppl m on m.market = w.market
  group by 1,2
), 
region_ctr as 
(
  select
    date, w.region,
    ifnull(sum(weighted_population_by_rank) / nullif(min(r.total_population),0),0) as region_ctr
  from weighted_population_data w
  left join region_total_ppl r on r.region = w.region
  group by 1,2
), 
timeseries as 
(
  select
    dd as date,
    date_trunc(dd, {period}) as period
  from unnest(generate_date_array('2019-10-15',current_date("America/Los_Angeles"), interval 1 day)) as dd  
),
final_market_ctr as 
(
  select 
    t.period,
    f.* except(date)
  from timeseries t
  left join market_ctr f on f.date = t.date
  qualify rank() over (partition by market,date_trunc(f.date, {period}) order by f.date desc) = 1
),
final_region_ctr as 
(
  select 
    t.period,
    f.*except(date)
  from timeseries t
  left join region_ctr f on f.date = t.date
  qualify rank() over (partition by region,date_trunc(f.date, {period}) order by f.date desc) = 1
)
select 
  t.period as date,
  avg(case when yr.region = 'North California' then yr.region_ctr else null end) as MAR2322,
  avg(case when ym.market like '%CA-EB' then ym.market_ctr else null end) as MAR2323,
  avg(case when ym.market like '%CA-NB' then ym.market_ctr else null end) as MAR2324,
  avg(case when ym.market like '%CA-SA' then ym.market_ctr else null end) as MAR2325,
  avg(case when ym.market like '%CA-ST' then ym.market_ctr else null end) as MAR2326,
  avg(case when ym.market like '%CA-FR' then ym.market_ctr else null end) as MAR2327,
  avg(case when ym.market like '%CA-WA' then ym.market_ctr else null end) as MAR2328,
  avg(case when ym.market like '%CA-SJ' then ym.market_ctr else null end) as MAR2329,
  avg(case when ym.market like '%CA-PA' then ym.market_ctr else null end) as MAR2330,
  avg(case when ym.market like '%CA-SF' then ym.market_ctr else null end) as MAR2331,
  avg(case when yr.region = 'South California' then yr.region_ctr else null end) as MAR2332,
  avg(case when ym.market like '%CA-LA' then ym.market_ctr else null end) as MAR2333,
  avg(case when ym.market like '%CA-OC' then ym.market_ctr else null end) as MAR2334,
  avg(case when ym.market like '%CA-SD' then ym.market_ctr else null end) as MAR2335,
  avg(case when ym.market like '%CA-SV' then ym.market_ctr else null end) as MAR2336,
  avg(case when ym.market like '%CA-VC' then ym.market_ctr else null end) as MAR2337,
  avg(case when yr.region = 'Texas' then yr.region_ctr else null end) as MAR2338,
  avg(case when ym.market like '%TX-AU' then ym.market_ctr else null end) as MAR2339,
  avg(case when ym.market like '%TX-DL' then ym.market_ctr else null end) as MAR2340,
  avg(case when ym.market like '%TX-FW' then ym.market_ctr else null end) as MAR2341,
  avg(case when ym.market like '%TX-HT' then ym.market_ctr else null end) as MAR2342,
  avg(case when ym.market like '%TX-SA' then ym.market_ctr else null end) as MAR2343,
  avg(case when yr.region = 'Georgia' then yr.region_ctr else null end) as MAR2344,
  avg(case when ym.market like '%GA-AT' then ym.market_ctr else null end) as MAR2345,
  avg(case when yr.region in ('Maryland', 'Pennsylvania', 'Virginia') then yr.region_ctr else null end) as MAR2346,
  avg(case when ym.market like '%MD-BL' then ym.market_ctr else null end) as MAR2347,
  avg(case when ym.market like '%MD-DC' then ym.market_ctr else null end) as MAR2348,
  avg(case when ym.market like '%PA-PH' then ym.market_ctr else null end) as MAR2349,
  avg(case when ym.market like '%VA-AR' then ym.market_ctr else null end) as MAR2350,
  avg(case when yr.region = 'Florida' then yr.region_ctr else null end) as MAR2351,
  avg(case when ym.market like '%FL-MI' then ym.market_ctr else null end) as MAR2352,
  avg(case when ym.market like '%FL-OR' then ym.market_ctr else null end) as MAR2353,
  avg(case when yr.region = 'Washington' then yr.region_ctr else null end) as MAR2354,
  avg(case when ym.market like '%WA-SE' then ym.market_ctr else null end) as MAR2355,
  avg(case when yr.region = 'Illinois' then yr.region_ctr else null end) as MAR2522,
  avg(case when ym.market like '%IL-CH' then ym.market_ctr else null end) as MAR2907,
  avg(case when ym.market like '%IL-NA' then ym.market_ctr else null end) as MAR2966,
  avg(case when ym.market like '%IL-LA' then ym.market_ctr else null end) as MAR3025
from timeseries t
left join final_region_ctr yr using(period)
left join final_market_ctr ym using(period)
group by 1
