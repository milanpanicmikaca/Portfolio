-- upload to BQ
with
leads as
(
    select
    id,
    product_id, order_id
    from core_lead
    where
    (core_lead.phone_number is not null or core_lead.email is not null)
    and core_lead.full_name not ilike '%test%' and coalesce(core_lead.email,'') not ilike '%test%'
    and core_lead.full_name not ilike '%fake%' and coalesce(core_lead.email,'') not ilike '%fake%'
    and core_lead.full_name not ilike '%duplicate%'
    and core_lead.created_at >= '2018-04-16'
),
rescheduled_appointments as
(
    with
    appointments as
    (
        select
        distinct order_id,
        min(created_at) as first_date,
        max(created_at) as last_date,
        count(created_at) as no_of_entries,
        max(cancelled_at) as last_cancelled
        from schedule_appointment
        where created_at > '2018-04-16'
        group by order_id
        order by order_id asc
    )
    select
    order_id,
    first_date,
    last_date,
    no_of_entries,
    last_cancelled
    from appointments
    where no_of_entries > 1
),
mix as
(
    select
    l.id as my_leads,
    first_date as my_first_date,
    last_date as my_last_date,
    last_cancelled as my_last_cancelled,
    case when o.product_id = 105 or o.product_id is null then 1 else 0 end as fence_product,
    case when o.product_id = 34 then 1 else 0 end as driveway_product,
    case when o.product_id = 132 then 1 else 0 end as turf_product,
    is_commercial
    from leads l
    left join store_order o on o.id = l.order_id
    left join rescheduled_appointments on rescheduled_appointments.order_id = o.id
    left join core_house ch on ch.id = o.house_id
    left join customers_customer cc on cc.id = ch.customer_id
    where
    last_date is not null
    and last_cancelled < last_date
)
select
date_trunc('{period}', my_first_date at time zone 'America/Los_Angeles')::date as date,
count(my_leads) as SAL127, --appts_rescheduled
sum(CASE WHEN is_commercial IS FALSE THEN fence_product ELSE 0 END) as SAL127F, --fence_appts_rescheduled
sum(CASE WHEN is_commercial IS FALSE THEN driveway_product ELSE 0 END) as SAL127D, --driveway_appts_rescheduled
sum(CASE WHEN is_commercial IS FALSE THEN turf_product ELSE 0 END) as SAL127T --turf_appts_rescheduled
from mix
group by 1
order by 1 desc
