-- upload to BQ
select
    date_trunc(date,{period}) as date,
    sum(coalesce(total,0)) as MAR123,
    sum(coalesce(fence,0)) as MAR123F,
    sum(coalesce(driveway,0)) as MAR123D,
    sum(coalesce(turf,0)) as MAR123T
from googlesheets.facebook_spend
where date > '2018-04-15'
group by 1
order by 1 desc
