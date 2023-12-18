with onsites as (
    select
        sa.id as onsite_id,
        so.id as order_id,
        sa.date,
        sat.code as onsite_type,
        us.full_name as sales_rep,
        case when ga.formatted_address like '% TX %' then time_add(sa.time_start, interval 2 hour) else sa.time_start end as start_time,
        coalesce(sa.cancelled_by_id is not null, false) as canceled,
        case when dp.name = 'Operations' and lap.name != 'Project Manager' then up.full_name end as photographer,
        date(sa.cancelled_at, 'America/Los_Angeles') as cancelled_at
    from ergeon.schedule_appointment as sa
    left join ergeon.store_order as so on so.id = sa.order_id
    left join ergeon.core_house as ch on ch.id = so.house_id
    left join ergeon.geo_address as ga on ga.id = ch.address_id
    left join ergeon.schedule_appointmenttype as sat on sat.id = sa.appointment_type_id
    left join ergeon.core_user as up on up.id = sa.agent_user_id --photographer
    left join ergeon.hrm_staff as sp on sp.user_id = up.id --photographer
    left join ergeon.hrm_stafflog as lp on lp.id = sp.current_stafflog_id --photographer
    left join ergeon.hrm_staffposition as pp on pp.id = lp.position_id --photographer
    left join ergeon.hrm_ladder as lap on lap.id = pp.ladder_id --photographer
    left join ergeon.hrm_department as dp on dp.id = lap.department_id --photographer
    left join ergeon.hrm_staff as ss on ss.id = so.sales_rep_id --sales_rep
    left join ergeon.core_user as us on us.id = ss.user_id --sales_rep
    where sat.code in ('physical_onsite', 'remote_onsite')
        and sa.date between '2018-04-16' and date_sub(current_date('America/Los_Angeles'), interval 0 day)
        and coalesce(date_diff(sa.date, date(sa.cancelled_at, 'America/Los_Angeles'), day), 0) <= 2 --shows only cancelations of 1 day
),

consultants as (
    select
        sa.id as onsite_id,
        sa.order_id,
        sa.date,
        uc.full_name as consultant,
        sh.house,
        rank() over(partition by sa.id order by sa.created_at) as rank
    from ergeon.schedule_appointment as sa
    left join ergeon.schedule_appointmenttype as sat on sat.id = sa.appointment_type_id
    left join ergeon.schedule_appointmentconsultant as sac on sac.appointment_id = sa.id --consultant
    left join ergeon.core_user as uc on uc.id = sac.consultant_user_id --consultant
    left join ext_quote.staff_house as sh on sh.email = uc.email
    where sat.code in ('physical_onsite', 'remote_onsite')
        and sa.date between '2018-04-16' and date_sub(current_date('America/Los_Angeles'), interval 0 day)
        and coalesce(date_diff(sa.date, date(sa.cancelled_at, 'America/Los_Angeles'), day), 0) <= 2 --shows only cancelations of of 1 day
        and sh.house is not null
),

f_consultants as (
    select * from consultants where rank = 1
),

estimators as (
    select
        order_id,
        full_name as estimator,
        house,
        row_number() over(partition by order_id order by date) as count
    from int_data.estimation_dashboard_v3
    where house is not null
        and quote_class = 'new_quote'
),

f_estimators as (
    select * from estimators where count = 1
)

select
    os.*,
    c.consultant,
    e.estimator,
    e.house,
    coalesce(date_diff(os.date, os.cancelled_at, day), 0) as days_cancelled
from onsites as os
left join f_consultants as c using (onsite_id)
left join f_estimators as e on e.order_id = os.order_id
