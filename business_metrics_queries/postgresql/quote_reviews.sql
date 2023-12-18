with
calc_approved as
(
    select
        id,
        order_id,
        approved_at
    from
    (
        select 
            q.*,
            rank() over (partition by q.order_id order by q.approved_at) as rank
        from quote_quote q
        where 
            q.approved_at is not null
    ) as k
    where 
        rank = 1
),
calc_lead as 
(
    select *
    from
    (
        select 
            l.id,
            l.created_at,
            l.address_id,
            l.order_id, 
            rank() over (partition by l.order_id order by l.created_at) as rank
        from core_lead l
    ) as k
    where 
        rank = 1
),
calc_booking as
(
    select 
        ssa.*,
        ga.formatted_address,
        case when ssat.code = 'quote_review' then 1 else 0 end as is_quote_review,
        case when ssat.code = 'quote_review' and o.product_id = 105 then 1 else 0 end as is_fence_quote_review,
        case when ssat.code = 'quote_review' and o.product_id = 34 then 1 else 0 end as is_driveway_quote_review,
        case when ssat.code = 'quote_review' and o.product_id = 132 then 1 else 0 end as is_turf_quote_review,
        rank() over (partition by ga.formatted_address, o.product_id order by ssa.created_at desc) as rank,
        is_commercial 
    from schedule_appointment ssa
    left join store_order o on o.id = ssa.order_id
    left join core_house h on h.id = o.house_id 
    left join customers_customer cc on cc.id = h.customer_id 
    left join geo_address ga on ga.id = h.address_id
    left join schedule_appointmenttype ssat on ssat.id =ssa.appointment_type_id
    where
        ssa.cancelled_at is null
        and ssat.code = 'quote_review'
        and ssa.date <= now()
        and ssa.date > '2018-04-16'
),
calc_last_booking as 
(
    select 
        cb.*
    from calc_booking cb
    left join calc_lead cl on cl.order_id = cb.order_id
    left join core_lead l on l.id = cl.id
    left join calc_approved ca on ca.order_id = cb.order_id
    left join customers_contact cco on cco.id = l.contact_id
    left join core_user cu on cu.id = cco.user_id
    where coalesce(l.full_name,'')||coalesce(cco.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
    and coalesce(l.email,'')||coalesce(cu.email,'') not ilike '%+test%' and cb.rank = 1
)
select
    date_trunc('{period}',date)::date as date,
    sum(is_quote_review) as SAL228, --quote_review
    sum(case when is_commercial IS false then is_fence_quote_review else 0 end) as SAL228F, --fence_quote_review
    sum(case when is_commercial IS false then is_driveway_quote_review else 0 end) as SAL228D, --driveway_quote_review
    sum(case when is_commercial IS false then is_turf_quote_review else 0 end) as SAL228T --turf_quote_review
from calc_last_booking
where 
    created_at is not null
group by 1
order by 1 desc
