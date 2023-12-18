-- upload to BQ
with 
    ranked_requests
as
(
select
    regexp_extract(substr(trim(admin_link,'<>'),38),r'^[0-9]{{5,6}}') as order_id,
    added_at,
    request_stage,
    description,
    extra_details,
    rank() over (partition by regexp_extract(substr(trim(admin_link,'<>'),38),r'^[0-9]{{5,6}}') order by drr.added_at) as rank
from googlesheets.delivery_requote_requests drr
)
select
    cast(date_trunc(added_at,{period}) as date) as date,
    count(*) as DEL169,
    sum(case when request_stage = 'pre_installation' then 1 else 0 end)/nullif(count(*),0) as DEL170,
    sum(case when request_stage = 'during_installation' then 1 else 0 end)/nullif(count(*),0) as DEL171,
    sum(case when request_stage = 'post_installation' then 1 else 0 end)/nullif(count(*),0) as DEL172,
    sum(case when request_stage like '%warranty%' then 1 else 0 end)/nullif(count(*),0) as DEL173
from ranked_requests where rank = 1
and ((lower(description) like '%tier%' or lower(extra_details) like '%tier%') or
      (lower(description) like '%margin%' or lower(extra_details) like '%margin%') or
      (lower(description) like '%adj%' or lower(extra_details) like '%adj%') or
      (lower(description) like '%honor%' or lower(extra_details) like '%honor%') or
      (lower(description) like '%match%' or lower(extra_details) like '%match%')) is false
group by 1
order by 1 desc
