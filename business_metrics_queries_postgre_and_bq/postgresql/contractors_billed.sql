-- upload to BQ
with
contractors as 
(
        select
                cc.id,
                cc.order_id,
                cc.contractor_id
        from contractor_contractororder cc
        where 
                cc.status_id = 13
),
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
orders as 
(
        select
                  o.id,
                  o.product_id,
                  o.completed_at at time zone 'America/Los_Angeles' as completed_at,
                  c.contractor_id
         from store_order o
         left join quote_quote qa on qa.id = o.approved_quote_id
         left join contractors c on c.order_id = o.id
         left join cancelled_projects cp on cp.order_id = o.id
         where  o.completed_at is not null
            and qa.approved_at >= '2018-04-16'
            and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
            and o.parent_order_id is null
            and cp.order_id is null
)
,
count_per_contractor
as
(
select
        date_trunc('{period}', completed_at)::date as date,
        contractor_id,
        count(o.id) as count_projects,
        count((case when product_id = 105 then o.id else null end)) as count_fence_projects,
        count((case when product_id = 34 then o.id else null end)) as count_driveway_projects
          --no_of_contractor
from orders o
where contractor_id is not null
group by 1,2
order by 1 desc
)
select 
        date,
        sum(case when count_projects > 0 then 1 else 0 end) as DEL127,
        sum(case when count_fence_projects > 0 then 1 else 0 end) as DEL127F,
        sum(case when count_driveway_projects > 0 then 1 else 0 end) as DEL127D,
        sum(case when count_projects <= 3 and count_projects > 0 then 1 else 0 end) as DEL185, --trial_contractors_active,
        sum(case when count_projects > 3 then 1 else 0 end) as DEL184, --full_time_contractors_active
        sum(case when count_fence_projects <= 3 and count_fence_projects > 0 then 1 else 0 end) as DEL185F, --trial_fence_contractors
        sum(case when count_fence_projects > 3 then 1 else 0 end) as DEL184F, --full_time_fence_contractors
        sum(case when count_driveway_projects <= 3 and count_driveway_projects > 0 then 1 else 0 end) as DEL185D, --trial_driveway_contractors
        sum(case when count_driveway_projects > 3 then 1 else 0 end) as DEL184D --full_time_driveway_contractors
from count_per_contractor
group by 1
order by 1 desc
