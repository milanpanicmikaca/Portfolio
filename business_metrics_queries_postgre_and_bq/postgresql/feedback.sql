-- upload to BQ
with
leads as
(
        select
                so.id as order_id,
                min(cl.id) as lead_id
        from store_order so
        left join core_lead cl on cl.order_id = so.id
        group by 1
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
data_detailed as
(
        select
                o.id as order_id,
                f.submitted_at,
                o.product_id,
                f.nps,
                f.quoting,
                f.scheduling,
                f.communication,
                f.installation
        from store_order o
        left join feedback_orderfeedback f on f.order_id = o.id
        left join leads l on l.order_id = o.id
        left join core_lead cl on cl.id = l.lead_id
        left join customers_contact co on co.id = cl.contact_id
        left join core_user cu on cu.id = co.user_id
        left join cancelled_projects cp on cp.order_id = o.id
        where
                o.created_at > '2018-04-15'
                and o.completed_at is not null
                and cp.order_id is null
                and o.parent_order_id is null
                and f.submitted_at is not null --added to avoid errors
                and coalesce(cl.full_name,'')||coalesce(co.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
                and coalesce(cl.email,'')||coalesce(cu.email,'') not ilike '%+test%'
)
select
        date_trunc('{period}',submitted_at at time zone 'America/Los_Angeles')::date as date,
        count(nps) as DEL102, -- has_feedback
        sum(case when nps <= 3 then 1 else 0 end) as DEL106, -- has_bad_feedback
        coalesce(avg(nps),0) as DEL101, -- avg_feedback
        coalesce(avg(quoting),0) as MDR112, -- avg_feedback_quoting
        coalesce(avg(scheduling),0) as DEL103, -- avg_feedback_scheduling
        coalesce(avg(communication),0) as DEL105, -- avg_feedback_communication
        coalesce(avg(installation),0) as DEL104, -- avg_feedback_installation
        coalesce(cast(sum(case when nps <= 3 then 1 else 0 end) as decimal)/nullif(count(nps),0),0) as DEL107, -- has_bad_feedback_perc
        -- Fence Feedback
        sum(case when product_id = 105 and nps is not null then 1 else 0 end) as DEL102F, -- has_feedback_fence
        sum(case when nps <= 3 and product_id = 105 then 1 else 0 end) as DEL106F, -- has_bad_feedback_fence
        coalesce(avg(case when product_id = 105 then nps else 0 end),0) as DEL101F, -- avg_feedback_fence
          coalesce(avg(case when product_id = 105 then quoting else null end),0) as MDR112F, -- avg_feedback_quoting_fence
        coalesce(avg(case when product_id = 105 then scheduling else 0 end),0) as DEL103F, -- avg_feedback_scheduling_fence
        coalesce(avg(case when product_id = 105 then communication else 0 end),0) as DEL105F, -- avg_feedback_communication_fence
        coalesce(avg(case when product_id = 105 then installation else 0 end),0) as DEL104F, -- avg_feedback_installation_fence
        coalesce(cast(sum(case when nps <= 3 and product_id = 105 then 1 else 0 end) as decimal)/nullif(sum(case when product_id = 105 and nps is not null then 1 else 0 end),0),0) as DEL107F, -- has_bad_feedback_perc_fence
        -- Hardscape Feedback
        sum(case when product_id = 34 and nps is not null then 1 else 0 end) as DEL102D, -- has_feedback_hardscape
        sum(case when nps <= 3 and product_id = 34 then 1 else 0 end) as DEL106D, -- has_bad_feedback_hardscape
        coalesce(avg(case when product_id = 34 then nps else 0 end),0) as DEL101D, -- avg_feedback_hardscape
          coalesce(avg(case when product_id = 34 then quoting else null end),0) as MDR112D, -- avg_feedback_quoting_hardscape
        coalesce(avg(case when product_id = 34 then scheduling else 0 end),0) as DEL103D, -- avg_feedback_scheduling_hardscape
        coalesce(avg(case when product_id = 34 then communication else 0 end),0) as DEL105D, -- avg_feedback_communication_hardscape
        coalesce(avg(case when product_id = 34 then installation else 0 end),0) as DEL104D, -- avg_feedback_installation_hardscape
        coalesce(cast(sum(case when nps <= 3 and product_id = 34 then 1 else 0 end) as decimal)/nullif(sum(case when product_id = 34 and nps is not null then 1 else 0 end),0),0) as DEL107D -- has_bad_feedback_perc_hardscape
from data_detailed
group by 1
order by 1 desc
