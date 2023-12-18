-- upload to BQ
with
last_approved_quotes as 
(
  select 
    o.id as order_id,
    completed_at as cancelled_at,
    is_cancellation,
    rank() over(partition by o.id order by approved_at desc,q.id desc) as approved_rank
  from 
    store_order o join 
    quote_quote q on q.order_id = o.id 
  where 
    q.created_at >= '2018-04-16'
    and approved_at is not null
),
cancelled_projects as 
(
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
raw_data as 
(
        select
            f.id,
            f.submitted_at as feedback_date,
            f.order_id,
            f.nps,
            case when f.nps >= 4.5 then 1 else 0 end as promoters,
            case when f.nps <= 3 then 1 else 0 end as detractors,
            case when (f.nps >= 4.5 and o.product_id = 105) then 1 else 0 end as fence_promoters,
            case when (f.nps <= 3 and o.product_id = 105) then 1 else 0 end as fence_detractors,
            case when (f.nps >= 4.5 and o.product_id = 34) then 1 else 0 end as driveway_promoters,
            case when (f.nps <= 3 and o.product_id = 34) then 1 else 0 end as driveway_detractors,
            1 as total_feedback,
            case when o.product_id = 105 then 1 else 0 end as fence_total_feedback,
            case when o.product_id = 34 then 1 else 0 end as driveway_total_feedback,
            o.completed_at as order_date
        from feedback_orderfeedback f
        left join store_order o on o.id = f.order_id
        left join cancelled_projects cp on cp.order_id = f.id
        where
            f.nps is not null
            and o.completed_at is not null
            and cp.order_id is null
            and f.submitted_at is not null --added to avoid errors
)
select
    date_trunc('{period}', order_date at time zone 'America/Los_Angeles')::date as date,
    coalesce(((cast(sum(promoters) as decimal)/sum(total_feedback)) - (cast(sum(detractors) as decimal)/sum(total_feedback)))*100,0) as DEL125, -- nps
    coalesce(((cast(sum(fence_promoters) as decimal)/nullif(sum(fence_total_feedback),0)) - (cast(sum(fence_detractors) as decimal)/nullif(sum(fence_total_feedback),0)))*100,0) as DEL125F, -- fence_nps
    coalesce(((cast(sum(driveway_promoters) as decimal)/nullif(sum(driveway_total_feedback),0)) - (cast(sum(driveway_detractors) as decimal)/nullif(sum(driveway_total_feedback),0)))*100,0) as DEL125D -- driveway_nps
from raw_data
group by 1
order by 1 desc
limit 40
