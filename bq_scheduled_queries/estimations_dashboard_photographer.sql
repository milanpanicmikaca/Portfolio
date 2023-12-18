with appt_consultant as (
    select 
        so.id as order_id, 
        sa.date,
        sa.time_start as start_time, 
        case when sa.cancelled_by_id is null then 'Active' else 'Canceled' end as active,
        sp.id as photographer_id,
        case when dp.name = 'Operations' and lap.name <> 'Project Manager' then up.full_name end as photographer,
        sat.code as onsite_type,
    from ergeon.schedule_appointment sa
        left join ergeon.store_order so on so.ID = sa.order_id  
        left join ergeon.schedule_appointmenttype sat on sat.id = sa.appointment_type_id
        left join ergeon.core_user up on up.id = sa.agent_user_id --photographer
        left join ergeon.hrm_staff sp on sp.user_id = up.id --photographer
        left join ergeon.hrm_stafflog lp on lp.id = sp.current_stafflog_id --photographer
        left join ergeon.hrm_staffposition pp on pp.id = lp.position_id --photographer
        left join ergeon.hrm_ladder lap on lap.id = pp.ladder_id --photographer
        left join ergeon.hrm_department dp on dp.id = lap.department_id --photographer        
    where sat.code in ('physical_onsite')
        and sa.date between "2022-01-01" and date_sub(current_date("America/Los_Angeles"),interval 1 day)
        and coalesce(date_diff(sa.date,date(sa.cancelled_at,"America/Los_Angeles"),day),0) = 0 --shows only cancelations of same day
), avail_cap as (
    (select 
    distinct 
        sas.date,
        sas.time_start,
        sas.time_end,   
        u.full_name,
        sas.capacity,
        from ergeon.schedule_availability sas
        left join ergeon.hrm_staff sp on sp.id = sas.employee_id
        left join ergeon.core_user u on u.id = sp.user_id
    )
    union all
    (select 
    distinct 
        sas.date,
        sas.time_start,
        sas.time_end,   
        u.full_name,
        sas.capacity,
        from ergeon.schedule_availability sas
        left join ergeon.hrm_staff sp on sp.id = sas.employee_id
        left join ergeon.core_user u on u.id = sp.user_id
    )
), pgr_cap as (
select 
    date,
    full_name as photographer,
    sum(capacity) as capacity
from avail_cap
group by 1, 2
), final as (
select 
    ca.*,
    pc.capacity,
    rank() over  (partition by ca.date, ca.photographer, active order by start_time) as rank,
    from appt_consultant ca
    left join pgr_cap pc on pc.photographer = ca.photographer and pc.date = ca.date
    order by onsite_type, date desc, photographer, start_time asc 
)
select 
    date,
    photographer,
    max(capacity) as capacity,
    sum(case when active = 'Active' then 1 else 0 end) as active,
    sum(case when active = 'Canceled' then 1 else 0 end) as canceled,
from final f
group by 1, 2
order by 1 desc
