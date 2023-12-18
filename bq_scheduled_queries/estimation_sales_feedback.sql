with sales_feedback as 
(
select
date(cast(date as timestamp),"America/Los_Angeles") as date,
estimator,
cast (REGEXP_SUBSTR(estimator,"[0-9]+") as int) as staff_id,
quote_rating
from int_data.sales_to_quote_feedback
where extract(date from date) >= '2022-04-27'
  and lower(issue_description) not like '%test%'
)
select
sf.date,
hs.full_name as estimator,
quote_rating,
from 
  sales_feedback sf left join
  ergeon.hrm_stafflog hs on sf.staff_id = hs.staff_id
where change_type = 'hired'
qualify rank() over (partition by hs.full_name order by hs.created_at desc) = 1