with timeseries as (
  select
    date_trunc(dd, {period}) as date
  from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as dd
  group by 1
),
sdr_spend_source as (
  select 
    date_trunc(date_sub(date,interval 1 week),week(Monday)) as date,
    abs(0.16*sum(0.6*amount)) as sdr_fee
  from int_data.sdr_data 
  where 
    date_sub(date,interval 1 week) = '2022-06-27'
  group by 1
  union all
  select 
    date_trunc(date_sub(date,interval 1 week),week(Monday)) as date,
    abs(sum(0.6*amount)) as sdr_fee
  from int_data.sdr_data 
  where 
    date_sub(date,interval 1 week) >= '2022-07-01'
  group by 1
),
sdr_spend as (
  select
    date_trunc(date, {period}) as date,
    sum(sdr_fee) as sdr_spend 
  from sdr_spend_source
  group by 1
)
select 
  t.date,
  s.sdr_spend as MAR2356
from timeseries t
left join sdr_spend s on s.date = t.date
