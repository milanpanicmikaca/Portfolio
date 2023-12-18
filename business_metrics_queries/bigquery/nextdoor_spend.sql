-- upload to BQ
Select 
  date_trunc(date, {period}) as date,
  sum(coalesce(amount,0)) as MAR237, -- nextdoor_spend
  sum(case when product = 'Fence' and amount is not null then amount else 0 end) as MAR237F,
  sum(case when product = 'Driveway' and amount is not null then amount else 0 end) as MAR237D,
  sum(case when lower(product) like '%artificial%grass%' and amount is not null then amount else 0 end) as MAR237T
from googlesheets.nextdoor_spend
group by 1
