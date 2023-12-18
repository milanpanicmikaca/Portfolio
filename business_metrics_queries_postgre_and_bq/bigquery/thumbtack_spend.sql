-- upload to BQ
Select 
  date_trunc(date,{period}) as date,
  sum(ifnull(initial_charge,0) - ifnull(refund,0)) as MAR137, -- thumbtack_spend
  sum(case when product = 'fence' then ifnull(initial_charge,0) - ifnull(refund,0) end) as MAR137F, -- thumbtack_fence_spend
  sum(case when product = 'driveway' then ifnull(initial_charge,0) - ifnull(refund,0) end) as MAR137D, -- thumbtack_driveway_spend
  sum(case when product = 'turf' then ifnull(initial_charge,0) - ifnull(refund,0) end) as MAR137T -- thumbtack_turf_spend
from googlesheets.thumbtack
group by 1
order by 1 desc
