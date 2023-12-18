with ha_ads_timeseries as (
  select
    date_trunc(dates, week(Monday)) as date, 
    market
  from unnest(generate_date_array('2023-01-02',current_date("America/Los_Angeles"), interval 1 day)) as dates
  cross join (select distinct market from googlesheets.ha_ads_spend)
  group by 1,2
  ),
ha_ads_final as (
  select 
    t.date,
    t.market,
    s.weekly_fee as ha_ads_fee
  from ha_ads_timeseries t
  left join googlesheets.ha_ads_spend s on s.market = t.market
                                        and cast(s.date as date) = date_trunc(t.date,month) --fixed weekly spend calculated as fixed monthly spend*12/52
  )
select
    date_trunc(date, {period}) as date,
    sum(ha_ads_fee) as MAR2357, 
    sum(ha_ads_fee) as MAR2357F --currently available only for Fence
from ha_ads_final
group by 1
