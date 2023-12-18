with
bonus_ladder as ( --map bonuses for won projects
    (select 0 as min, 1000 as max, 20 as bonus) union all
    (select 1001 as min, 1500 as max, 30 as bonus) union all
    (select 1501 as min, 2500 as max, 50 as bonus) union all
    (select 2501 as min, 4500 as max, 70 as bonus) union all
    (select 4501 as min, 6500 as max, 90 as bonus) union all
    (select 6501 as min, 10000 as max, 110 as bonus) union all
    (select 10001 as min, 20000 as max, 150 as bonus) union all
    (select 20001 as min, 50000 as max, 200 as bonus) union all
    (select 50001 as min, 250000 as max, 300 as bonus) union all
    (select 250001 as min, 500000 as max, 400 as bonus) union all
    (select 500001 as min, 1000000 as max, 500 as bonus) union all
    (select 1000001 as min, 999999999 as max, 600 as bonus)
),

ofq as (
    select
        order_id,
        id as quote_id,
        extract(date from approved_at) as won_at
    from
        ergeon.quote_quote
    where
        approved_at is not null
        and created_at > '2018-04-15'
        and is_cancellation = False
    qualify rank() over(partition by order_id order by approved_at, id) = 1
),

logs as (
    select
        a.id,
        ladder
    from
        ergeon.schedule_appointment a
    join ergeon.schedule_appointmenttype t on t.id = a.appointment_type_id
    join ergeon.store_order o on o.id = a.order_id
    left join ergeon.hrm_staff s on s.user_id = a.agent_user_id
    left join useful_sql.hrm l on l.staff_id = s.id and a.date between l.started_at and l.end_date and ladder = 'Field'
    qualify rank() over(partition by order_id order by started_at desc, end_date desc) = 1
),

order_phys as (
    select
        a.order_id,
        max(date) as photographer_visit_at,
        sum(case when t.code = 'physical_onsite' and a.cancelled_at is null then 1 else 0 end) as photographer_visits,
        sum(case when t.code = 'physical_onsite' and a.cancelled_at is null and date >= won_at then 1 else 0 end) as photographer_visits_post_won,
        sum(
            case
                when
                    t.code = 'physical_onsite' and ladder = 'Field' and (
                        a.cancelled_at is null or timestamp_diff(extract(date from a.cancelled_at), a.date, day) < 2
                    )
                    then 35 end) as sales_photographer_fee
    from
        ergeon.schedule_appointment a
    join ergeon.schedule_appointmenttype t on t.id = a.appointment_type_id
    join ergeon.store_order o on o.id = a.order_id
    left join ergeon.hrm_staff s on s.user_id = a.agent_user_id
    left join logs l on l.id = a.id
    left join ofq on ofq.order_id = o.id
    where
        o.created_at > '2018-04-15'
        and a.date < current_date()
    group by 1
),

onsite_install_date as (
    select
        order_id,
        min(
            case
                when t.code in ('physical_onsite', 'remote_onsite') then datetime(timestamp(concat(Date, " ", time_start)), 'America/Los_Angeles')
            end
        ) as onsite_ts_at,
        min(
            case when t.code in ('installation') then datetime(timestamp(concat(Date, " ", time_start)), 'America/Los_Angeles') end
        ) as install_planned_ts_at,
        max(
            case when t.code in ('installation') then datetime(timestamp(concat(Date, " ", time_start)), 'America/Los_Angeles') end
        ) as last_install_planned_ts_at,
        min(case when t.code in ('installation') then datetime(a.created_at, 'America/Los_Angeles') end) as install_booked_ts_at
    from
        ergeon.schedule_appointment a
    join ergeon.schedule_appointmenttype t on t.id = a.appointment_type_id
    where
        t.code in ('physical_onsite', 'remote_onsite', 'installation')
        and a.cancelled_at is null
        and date <= current_date()
    group by 1
),

booked_date as (
    select
        order_id,
        min(datetime(a.created_at, 'America/Los_Angeles')) as booked_ts_at,
        count(*) as bookings,
        sum(if(cancelled_at is null, 1, 0)) as completed_onsites,
        sum(if(cancelled_at is not null, 1, 0)) as cancelled_onsites,
        sum(
            if(datetime(a.cancelled_at, 'America/Los_Angeles') > datetime(timestamp(concat(Date, " ", time_start)), 'America/Los_Angeles'), 1, 0)
        ) as no_show_onsites
    from
        ergeon.schedule_appointment a
    join ergeon.schedule_appointmenttype t on t.id = a.appointment_type_id
    where
        t.code in ('physical_onsite', 'remote_onsite')
        and date <= current_date()
    group by 1
),

cancelled_bookings as (
    select
        order_id,
        if(cancelled_at is not null, 1, 0) as is_booking_cancelled,
        if(
            datetime(a.cancelled_at, 'America/Los_Angeles') > datetime(timestamp(concat(Date, " ", time_start)), 'America/Los_Angeles'), 1, 0
        ) as is_booking_no_show
    from
        ergeon.schedule_appointment a
    join ergeon.schedule_appointmenttype t on t.id = a.appointment_type_id
    where
        t.code in ('physical_onsite', 'remote_onsite')
        and date <= current_date()
    qualify rank() over(partition by order_id order by a.created_at, a.id) = 1
),

estimate_date as (
    select
        order_id,
        min(datetime(created_at, "America/Los_Angeles")) as estimated_ts_at,
        count(*) as estimates
    from
        ergeon.quote_quote
    where
        is_estimate
        and created_at >= '2018-04-16'
        and is_cancellation = False
    group by 1
),

order_review_data as ( --review bonus attribution 
    select
        order_id,
        sum(case when sales_staff_attributed_id is not null then coalesce(bonus, 0) else 0 end) as sales_review_fee,
        sum(case when delivery_staff_attributed_id is not null then coalesce(bonus, 0) else 0 end) as delivery_review_fee
    from
        int_data.review_detail
    where
        bonus > 0
    group by 1
    union all
    select
        order_id,
        sum(case when department = 'sales' then amount end) as sales_review_fee,
        sum(case when department = 'delivery' then amount end) as delivery_review_fee
    from
        compensation_system.review_adjustments
    where
        amount is not null
        and review_id not in (6010, 6006, 6007, 6008, 6009, 6012, 6016, 6017, 6011, 6013, 6014, 6015) --Duplicate payments adjustments
    group by 1
),

order_review as (
    select
        order_id,
        sum(sales_review_fee) as sales_review_fee,
        sum(delivery_review_fee) as delivery_review_fee
    from order_review_data
    group by 1
),

commissions as (
    select
        ofq.order_id,
        case
            when marked_completed_at is not null then l.bonus
            else 0.75 * l.bonus
        end as sales_commission_fee
    from
        ofq join
        ergeon.store_order o on ofq.order_id = o.id join
        ergeon.quote_quote q on q.id = ofq.quote_id join
        bonus_ladder l on q.total_price between l.min and l.max
    where
        won_at < '2022-07-01' or extract(date from marked_completed_at at time zone 'America/Los_Angeles') < '2022-07-01'
    union all
    select
        order_id,
        sum(amount) as sales_commission_fee
    from
        int_data.sales_won_commissions_rb
    where type not in ('balance', 'commission payment')
    group by 1
),

order_bonus as (
    select order_id, sum(sales_commission_fee) as sales_commission_fee from commissions group by 1
),

orders1 as ( --Overhead costs for reviews,wins,photographers
    select
        o.id as order_id,
        coalesce(sales_commission_fee, 0) as sales_commission_fee,
        coalesce(sales_review_fee, 0) as sales_review_fee,
        photographer_visit_at, onsite_ts_at, estimated_ts_at, install_planned_ts_at, install_booked_ts_at, booked_ts_at, last_install_planned_ts_at,
        coalesce(estimates, 0) as estimates,
        coalesce(photographer_visits, 0) as photographer_visits,
        coalesce(cancelled_onsites, 0) as cancelled_onsites,
        coalesce(completed_onsites, 0) as completed_onsites,
        coalesce(no_show_onsites, 0) as no_show_onsites,
        coalesce(bookings, 0) as bookings,
        coalesce(photographer_visits_post_won, 0) as photographer_visits_post_won,
        coalesce(sales_photographer_fee, 0) as sales_photographer_fee,
        coalesce(delivery_review_fee, 0) as delivery_review_fee,
        is_booking_cancelled, is_booking_no_show
    from
        ergeon.store_order o
    left join order_review ore on ore.order_id = o.id
    left join order_phys op on op.order_id = o.id
    left join order_bonus ob on ob.order_id = o.id
    left join onsite_install_date od on od.order_id = o.id
    left join estimate_date ed on ed.order_id = o.id
    left join booked_date bd on bd.order_id = o.id
    left join cancelled_bookings cb on cb.order_id = o.id
    where o.created_at > '2018-04-15'
)

select
    *,
    (sales_commission_fee + sales_review_fee + sales_photographer_fee) as sales_attributed_fee,
    (delivery_review_fee) as delivery_attributed_fee
from orders1
