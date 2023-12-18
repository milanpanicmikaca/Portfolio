-- upload to BQ
with yelp_messages as (
select 
  date_trunc(date, {period}) as date,
  count(*) as messages_tot
from ext_marketing.yelp_message3_int
where date < '2022-06-06' -- using this date June 6th 2022 because everything after this date is fence only
group by 1
union all 
select 
  date_trunc(cast(created_at as date), {period}) as date,
  count(*) as messages_tot
from 
ergeon.marketing_yelpmessage
where date_trunc(cast(created_at as date), day) >= '2022-06-06' -- using this date June 6th 2022 because everything after this date is fence only
group by 1
)
select
date,
sum(messages_tot) as MAR178 --messages_tot
from yelp_messages
group by 1
order by 1 desc
