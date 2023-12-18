with
date_array as
(
select
    cast(date as timestamp) as date,
    timestamp_add(cast(date as timestamp), interval 86399 second) as end_of_day_ts, 
from warehouse.generate_date_series gs
where date > '2018-04-15' and date <= current_date
),
completed_and_cancelled_orders as
(
select
    order_id,
    max(date) as final_date
from
    (
    select
        order_id,
        cancelled_at as date
    from int_data.order_ue_materialized
    where cancelled_at is not null and completed_at is not null
    union all
    select
        order_id,
        completed_at as date
    from int_data.order_ue_materialized
    where cancelled_at is not null and completed_at 
    is not null
    ) 
group by 1
),
orders as
(
select
    ue.order_id,
    segment,
    won_at,
    coalesce(co.final_date,ue.completed_at, ue.cancelled_at) as final_date
from int_data.order_ue_materialized ue
left join completed_and_cancelled_orders co on co.order_id = ue.order_id
),
order_statuses as
(
select
    cs.created_at, 
    cst.label,
    so.id as order_id,
    rank() over (partition by so.id order by cs.created_at desc) as desc_rank,
    lag(cst.label) over (partition by so.id order by cs.created_at) as previous_label,
    lag(cs.created_at) over (partition by so.id order by cs.created_at) as previous_timestamp
from ergeon.core_statushistory cs
left join ergeon.core_statustype cst on cst.id = cs.status_id
left join ergeon.django_content_type ct on ct.id = cs.content_type_id
left join ergeon.store_order so on so.id = cs.object_id
where ct.app_label = 'store' and ct.model = 'order'
order by 3,1,2
),
on_hold_ts as
(
select
    order_id,
    previous_timestamp as on_hold, 
    created_at as left_on_hold
from order_statuses
where previous_label = 'On Hold'
union all
select
    order_id,
    created_at as on_hold,
    timestamp_add(current_timestamp, interval 1 day) as left_on_hold
from order_statuses
where desc_rank = 1 and label = 'On Hold'
),
cross_join_final as
(
select
    d.date,
    o.order_id,
    o.segment,
    o.won_at,
    oh.on_hold,
    oh.left_on_hold,
    case 
        when oh.on_hold is null and oh.left_on_hold is null then 1
        when oh.on_hold > d.date and oh.left_on_hold < d.end_of_day_ts then 1 else 0 end as is_in_backlog,
    rank() over (partition by o.order_id,date order by left_on_hold desc) as rank_orders
from date_array d
left join orders o on cast(d.date as date) >= o.won_at and cast(d.date as date) <= coalesce(o.final_date,date_add(current_date, interval 1 day))
left join on_hold_ts oh on oh.order_id = o.order_id 
and oh.on_hold >= d.date 
and oh.left_on_hold <= d.end_of_day_ts
order by 2,1
)
select
    cast(date as date) as date,
    segment,
    count(*) as count
from cross_join_final  where rank_orders = 1
and segment is not null
group by 1,2
order by 2,1