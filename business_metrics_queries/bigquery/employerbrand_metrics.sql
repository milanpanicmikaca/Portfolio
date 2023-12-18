-- upload to BQ
with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
    from unnest(generate_date_array('2020-01-01',current_date(), interval 1 day)) as date_array
),linkedin_data as(
select
  date,
  new_followers,
  total_followers
from int_data.linkedin
),glassdoor as (
  select
  date,
  reviews,
  rating
from int_data.glassdoor_data
)
select 
  date_trunc(ts.day,{period}) as date,
  coalesce(total_followers,0) as HRS138,
  coalesce(reviews,0) as HRS137,
  coalesce(rating,0) as HRS139
from timeseries ts 
left join linkedin_data ld on ts.day = ld.date 
left join glassdoor gd on cast(gd.date as date) = ld.date
where period_rank = 1;
