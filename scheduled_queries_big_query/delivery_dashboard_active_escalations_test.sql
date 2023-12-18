with timeseries_day as (
    select
        date_trunc(date_array, day) as date, full_name as project_manager, staff_id, ue.market, ue.segment
    from unnest(generate_date_array('2018-04-16', current_date(), interval 1 day)) as date_array
    cross join (select full_name, staff_id
        from int_data.hr_dashboard
        where ladder_name = 'Project Management')
    cross join int_data.order_ue_materialized ue
    group by 1, 2, 3, 4, 5
),

orders_comp as (
    select
        ue.completed_at as date,
        -- ue.project_manager,
        cu.full_name as project_manager,
        ue.market,
        ue.segment,
        count(ue.order_id) as completed_orders
    from int_data.order_ue_materialized ue
    left join ergeon.store_order so on so.id = ue.order_id
    left join ergeon.hrm_staff hs on hs.id = so.project_manager_id
    left join ergeon.core_user cu on cu.id = hs.user_id
    where ue.completed_at is not null
    group by 1, 2, 3, 4
),

esc_end_date as (
    select
        eh.object_id as escalation_id,
        datetime(eh.created_at, 'America/Los_Angeles') as end_date,
        case when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then
                rank() over (partition by eh.object_id order by eh.created_at desc) else null end as rank_end_states
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e on e.id = eh.object_id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    where reported_at >= '2021-04-28'
        and d.model = 'escalation'
        and d.app_label = 'store'
        and e.deleted_at is null
    qualify rank_end_states = 1
),

escalation_information as (
    select
        e.date_the_order_escalated as date,
        e.escalation_id,
        e.order_id,
        e.escalation_status,
        e.project_manager,
        ue.market,
        ue.segment,
        array_to_string(e.primary_team, ", ") as primary_teams,
        array_to_string(e.secondary_team, ", ") as secondary_teams,
        case when e.escalation_status = 'Escalation Received' then 1 else 0 end as is_active,
        case
            when e.escalation_status = 'Escalation Received' then date_diff(current_date(), e.date_the_order_escalated, day) else null
        end as days_escalated,
        timestamp_diff(eed.end_date, datetime(se.reported_at, 'America/Los_Angeles'), day) as tat
    from int_data.escalation_query e
    left join ergeon.store_escalation se on se.id = e.escalation_id
    left join int_data.order_ue_materialized ue on ue.order_id = e.order_id
    left join esc_end_date eed on eed.escalation_id = e.escalation_id
    where /*escalation_status = 'Escalation Received'
  and*/ e.project_manager is not null
  and e.primary_team is not null
  and se.deleted_at is null
  and (lower(e.core_issues_string) not like '%scoping%' or lower(e.current_status_bucket) not like '%scoping_process%')
),

active_escalations as (
    select
        date,
        project_manager,
        market,
        segment,
        sum(is_active) as is_active,
        sum(case when days_escalated between 0 and 30 and is_active = 1 then 1 else 0 end) as active_esc_30days,
        sum(case when days_escalated between 31 and 90 and is_active = 1 then 1 else 0 end) as active_esc_30_90days,
        sum(case when days_escalated > 90 and is_active = 1 then 1 else 0 end) as active_esc_90days,
        avg(days_escalated) as avg_escalation_age,
        avg(tat) as tat
    from escalation_information
    --where (primary_teams like "%Delivery%" or secondary_teams like "%Delivery%")
    group by 1, 2, 3, 4
    order by date desc
)

select
    t.date,
    t.project_manager,
    tl.team_leader,
    case
        when tl.house is null then 'No House'
        else tl.house
    end as house,
    t.market,
    t.segment,
    o.completed_orders,
    a.is_active,
    a.active_esc_30days,
    a.active_esc_30_90days,
    a.active_esc_90days,
    a.avg_escalation_age,
    a.tat
from timeseries_day t
left join orders_comp o using (date, project_manager, market, segment)
left join active_escalations a using (date, project_manager, market, segment)
left join int_data.delivery_team_lead tl on tl.staff_id = t.staff_id
where coalesce(o.completed_orders, a.is_active, a.active_esc_30days, a.active_esc_30_90days, a.active_esc_90days) is not null
