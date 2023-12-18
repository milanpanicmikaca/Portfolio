
with
date_array as
(
select
    cast(date as timestamp) as date,
    timestamp_add(cast(date as timestamp), interval 86399 second) as end_of_day_ts, 
from warehouse.generate_date_series gs
where date > '2018-04-15' and date <= current_date
),
orders as
(
select
    ue.order_id,
    segment,
    case 
        when channel like '%Advisor%' then 'Paid-HA'
        when channel like '%Facebook%' then 'Paid-FB'
        when channel like '%Thumb%' then 'Paid-TT'
        when channel like '%Next%' then 'Paid-MISC'
        when channel like '%/Paid%Goog%' then 'Paid-GG'
        when channel like '%Yelp%' then 'NP-YE'
        when channel like '%/Paid%' then 'Paid-MISC'
        else  'NP-MISC' -- when channel like '%/Non% then
    end as lead_channel,
    created_at,
    coalesce(won_at,cancelled_at) as final_date
from int_data.order_ue_materialized ue
where 
    segment <> 'Other'
--left join completed_and_cancelled_orders co on co.order_id = ue.order_id
),
order_statuses as
(
select
    extract( date from cs.created_at AT TIME ZONE 'America/Los_Angeles') as date, 
    cst.label as status,
    so.id as order_id,
    rank() over (partition by so.id, extract( date from cs.created_at AT TIME ZONE 'America/Los_Angeles') order by cs.created_at desc) as desc_rank,
    -- lag(cst.label) over (partition by so.id order by cs.created_at) as previous_label,
    lead(extract( date from cs.created_at AT TIME ZONE 'America/Los_Angeles')) over (partition by so.id order by cs.created_at) as next_timestamp
from ergeon.core_statushistory cs
left join ergeon.core_statustype cst on cst.id = cs.status_id
left join ergeon.django_content_type ct on ct.id = cs.content_type_id
left join ergeon.store_order so on so.id = cs.object_id
where ct.app_label = 'store' and ct.model = 'order'
qualify desc_rank = 1
order by 3,1,2
)
,
cross_join_final as
(
select
    cast(d.date as date) as date,
    o.order_id,
    o.segment,
    o.created_at,
    o.final_date,
    o.lead_channel,
    od.status,
    od.date as status_change,
    -- case 
    --     when oh.on_hold is null and oh.left_on_hold is null then 1
    --     when oh.on_hold > d.date and oh.left_on_hold < d.end_of_day_ts then 1 else 0 end as is_in_backlog,
    -- rank() over (partition by o.order_id,date order by left_on_hold desc) as rank_orders,
    count(o.order_id) over (partition by o.order_id order by d.date) as days_in_backlog
from date_array d
left join orders o on cast(d.date as date) >= o.created_at and cast(d.date as date) <= coalesce(o.final_date,date_add(current_date, interval 1 day))
left join order_statuses od on o.order_id = od.order_id and cast(d.date as date) >= od.date and cast(d.date as date) < coalesce(od.next_timestamp,date_add(current_date, interval 1 day))
order by 1,2
)
select
    date,
    c.created_at,
    segment,
    lead_channel,
    -- status,
    -- status_change,
    -- so.cancelled_at
from cross_join_final c
where status not in ('Lost')
and days_in_backlog < 365
-- and segment = 'Wood Fence-CN-SJ' and date = '2022-04-01'
and segment is not null
order by 1
