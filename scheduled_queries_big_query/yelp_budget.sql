with 
dates as (select date from ext_marketing.calendar where date <= current_date and date >= '2018-04-15' ),
locations as (select distinct location from ext_marketing.yelp_budget_sheet),
--c as (
--select date from 
--  (SELECT MIN(date) minDate, current_date maxDate FROM ext_marketing.yelp_spend_sheet), 
--  UNNEST(GENERATE_DATE_ARRAY(minDate, maxDate)) date 
--),
budgets as (
select date, location, 
  sum(case when product = 'fence' then amount else null end) as fence_budget, 
  sum(case when product = 'driveway' then amount else null end) as driveway_budget,
  sum(case when product = 'landscaping_design' then amount else null end) as turf_budget 
from ext_marketing.yelp_budget_sheet
group by 1,2),
daily_budgets0 as (
select 
  d.date, 
  l.location,
  LAST_VALUE(fence_budget IGNORE NULLS) OVER(PARTITION BY location ORDER BY date) fence_budget,
  LAST_VALUE(driveway_budget IGNORE NULLS) OVER(PARTITION BY location ORDER BY date) driveway_budget,
  LAST_VALUE(turf_budget IGNORE NULLS) OVER(PARTITION BY location ORDER BY date) turf_budget  
from 
dates d
CROSS JOIN locations l
LEFT JOIN budgets b
USING(location, date)
),
daily_budgets as (
select
  date,location,fence_budget, driveway_budget, turf_budget,
  coalesce(fence_budget,0)+coalesce(driveway_budget,0)+coalesce(turf_budget,0) as budget,
  fence_budget*1.0/nullif((coalesce(fence_budget,0)+coalesce(driveway_budget,0)+coalesce(turf_budget,0)),0) as fence_ratio,
  driveway_budget*1.0/nullif((coalesce(fence_budget,0)+coalesce(driveway_budget,0)+coalesce(turf_budget,0)),0) as driveway_ratio,
  turf_budget*1.0/nullif((coalesce(fence_budget,0)+coalesce(driveway_budget,0)+coalesce(turf_budget,0)),0) as turf_ratio
from daily_budgets0
) 
select * from daily_budgets where budget >0 order by date asc, location