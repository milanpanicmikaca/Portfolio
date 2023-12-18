select
  date_trunc(date,{period}) as date,
  cast(case when avg(response_time) is null then 0 else avg(response_time) end as int64) as MAR180 -- avg_response_time
from
(
  with
  location as
  (
    select
      distinct location as location
    from googlesheets.yelp_response_time
  ),
  dateseries as
  (
    select *
    from unnest(generate_date_array('2018-04-16', current_date(), interval 1 day)) as date
    cross join location
  )
  select
    date_trunc(d.date,{period}) as date,
    last_value(yrt.response_time ignore nulls) over (partition by d.location order by d.date) as response_time
  from dateseries d
  left join googlesheets.yelp_response_time yrt on yrt.date = d.date and yrt.location = d.location
)
group by 1
order by 1 desc
