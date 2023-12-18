with scoping_esc as (
    select
        se.id as escalation_id
    from ergeon.store_escalation se
    left join ergeon.store_escalation_core_issues seci on seci.escalation_id = se.id
    left join ergeon.store_escalationcoreissue se2 on se2.id = seci.escalationcoreissue_id
    where lower(se.current_status_bucket) like '%scoping_process%' or lower(se2.name) like '%scoping%'
),

escalations as (
    select
        eh.object_id as escalation_id,
        datetime(cast(e.reported_at as timestamp), 'America/Los_Angeles') as start_date,
        datetime(cast(eh.created_at as timestamp), 'America/Los_Angeles') as status_created_at,
        generate_date_array(cast(e.reported_at as date), current_date(), interval 1 day) as date_array,
        st.code,
        u.full_name as project_manager,
        case
            when s.id in (1636, 1698) then 'KAM'
            else ht.name
        end as house,
        case
            when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then
                rank() over (partition by eh.object_id order by eh.created_at desc) else null
        end as rank_end_states,
        case
            when
                st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled') then 'active'
            else 'resolved'
        end as grouped_status,
        case
            when st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled') then
                rank() over (partition by e.id order by eh.created_at desc) else null end as act_rank
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e on e.id = eh.object_id
    left join scoping_esc sces on sces.escalation_id = e.id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    left join ergeon.core_statustype st2 on st2.id = e.status_id
    left join ergeon.store_order o on o.id = e.order_id
    left join ergeon.hrm_staff s on s.id = o.project_manager_id
    left join ergeon.core_user u on s.user_id = u.id
    left join ergeon.hrm_stafflog hs on hs.id = s.current_stafflog_id
    left join ergeon.hrm_team ht on ht.id = hs.team_id
    where reported_at >= '2021-04-28'
          and d.model = 'escalation'
          and d.app_label = 'store'
          and e.deleted_at is null
          and sces.escalation_id is null
),

active_data as (
    select
        escalation_id,
        day,
        project_manager,
        house,
        grouped_status,
        start_date,
        status_created_at, --last day of active status or resolution date
        sum(case when grouped_status = 'resolved' and day <= extract(date from status_created_at) then 1
                                                                     when grouped_status = 'active' and day <= current_date() then 1
            else 0 end)
        over (partition by escalation_id order by day) as active_days,
        case
            when grouped_status = 'resolved' and day <= extract(date from status_created_at) then 1
            when grouped_status = 'active' and day <= current_date() then 1
            else 0 end as active
    -- case 
    --     when grouped_status = 'resolved' and day >= extract(date from status_created_at) then timestamp_diff(end_date,start_date,day) 
    -- end as tat,
    from escalations
    cross join unnest(date_array) as day
    where (grouped_status = 'active' and act_rank = 1) or rank_end_states = 1
),

projects_completed as (
    select
        date_trunc(completed_at, month) as date,
        project_manager,
        pm_team as house,
        count(order_id) as completed_orders
    from int_data.order_ue_materialized
    where is_completed = 1
    group by 1, 2, 3
)

select
    date_trunc(day, month) as date,
    project_manager,
    house,
    case
        when active = 1 then escalation_id
        else null
    end as is_active,
    case
        when active_days < 30 then escalation_id
        else null
    end as act_30,
    case
        when active_days between 30 and 90 then escalation_id
        else null
    end as act_30_90,
    case
        when active_days > 90 then escalation_id
        else null
    end as act_90,
    active_days,
    0 as completed_projects
from active_data
where active = 1
qualify rank() over (partition by date_trunc(day, month) order by day desc) = 1

union all

select
    date,
    project_manager,
    house,
    null as is_active,
    null as act_30,
    null as act_30_90,
    null as act_90,
    null as active_days,
    completed_orders
from projects_completed
