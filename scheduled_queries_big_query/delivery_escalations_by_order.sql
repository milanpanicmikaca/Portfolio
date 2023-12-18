with leads as (
    select
        so.id as order_id,
        min(cl.id) as lead_id
    from ergeon.store_order so
    left join ergeon.core_lead cl on cl.order_id = so.id
    group by 1
),

scoping_esc as (
    select
        se.id as escalation_id
    from ergeon.store_escalation se
    left join ergeon.store_escalation_core_issues seci on seci.escalation_id = se.id
    left join ergeon.store_escalationcoreissue se2 on se2.id = seci.escalationcoreissue_id
    where lower(se.current_status_bucket) like '%scoping_process%' or lower(se2.name) like '%scoping%'
),

esc_end_date as (
    select
        eh.object_id as escalation_id,
        eh.created_at as end_date,
        case when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then
                rank() over (partition by eh.object_id order by eh.created_at desc) else null end as rank_end_states
    from ergeon.core_statushistory eh
    left join int_data.escalation_query e on e.escalation_id = eh.object_id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    where e.date_the_order_escalated >= '2021-04-28'
        and d.model = 'escalation'
        and d.app_label = 'store'
        and e.date_the_escalation_deleted is null
    qualify rank_end_states = 1
),

revenue_calc as (
    select
        order_id,
        sum(amount) as revenue
    from ergeon.accounting_transaction
    where type_id in (8, 10)
    group by 1
),

active_escalations as (
    select
        e.id as escalation_id,
        1 as is_active,
        revenue as revenue_held,
        case when timestamp_diff(current_timestamp(), e.reported_at, day) < 30 then 1 else 0 end as active_esc_30days,
        case when timestamp_diff(current_timestamp(), e.reported_at, day) between 30 and 90 then 1 else 0 end as active_esc_30_90days,
        case when timestamp_diff(current_timestamp(), e.reported_at, day) > 90 then 1 else 0 end as active_esc_90days
    from ergeon.store_escalation e
    left join int_data.escalation_query se on se.escalation_id = e.id
    left join ergeon.core_statustype st on st.id = e.status_id
    left join revenue_calc rc on rc.order_id = e.order_id
    where st.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled')
        and deleted_at is null
        and (lower(se.core_issues_string) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')
),

resolved_escalations as (
    select
        e.id as escalation_id,
        1 as is_resolved,
        case when timestamp_diff(current_timestamp(), cs.created_at, day) <= 30 then 1 else 0 end as resolved_30days
    from ergeon.store_escalation e
    left join int_data.escalation_query se on se.escalation_id = e.id
    left join ergeon.core_statustype st on st.id = e.status_id
    left join ergeon.core_statushistory cs on cs.object_id = e.id
    where st.code = 'escalation_resolved' and content_type_id = 491 and cs.status_id = 20
        and deleted_at is null
        and (lower(se.core_issues_string) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')
    qualify row_number() over (partition by cs.object_id order by cs.created_at desc) = 1
),

initial_data as (
    select
        cast(date_trunc(e.reported_at, day, "America/Los_Angeles") as date) as date,
        case
            when (qq.is_cancellation = false or qq.is_cancellation is null) then cast(date_trunc(so.completed_at, day, "America/Los_Angeles") as date)
            else null
        end as date_completion,
        so.id as order_id,
        case when sces.escalation_id is null then e.id else null end as escalation_id,
        ue.market,
        ue.segment,
        so.approved_quote_id,
        tl.full_name as project_manager,
        case
            when tl.house is null then 'No House'
            else tl.house
        end as house,
        tl.team_leader,
        se.name as issue,
        st.code,
        case
            when e.current_status_bucket is null then 'no stage'
            else e.current_status_bucket
        end as status_bucket,
        case
            when so.parent_order_id is not null and so.project_status_id not in (24, 31, 32)
                and so.approved_quote_id is not null then 1
            else 0
        end as is_active_wwo,
        case
            when so.parent_order_id is null and so.project_status_id not in (24, 31, 32)
                and so.approved_quote_id is not null and e.id is not null then 1
            else 0
        end as is_active_proj_with_esc,
        so.parent_order_id,
        so.project_status_id,
        ae.is_active,
        ae.active_esc_30days,
        ae.active_esc_30_90days,
        ae.active_esc_90days,
        re.resolved_30days,
        ae.revenue_held,
        count(distinct e.id) as esc,
        avg(timestamp_diff(eed.end_date, e.reported_at, day)) as tat,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) < 30 then 1 else 0 end) as esc_30days,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) between 30 and 90 then 1 else 0 end) as esc_30_90days,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) > 90 then 1 else 0 end) as esc_90days,
        avg(timestamp_diff(current_timestamp(), e.reported_at, day)) as esc_age
    from ergeon.store_escalation e
    left join ergeon.core_statustype st on st.id = e.status_id
    left join ergeon.store_escalation_core_issues seci on seci.escalation_id = e.id
    left join ergeon.store_escalationcoreissue se on se.id = seci.escalationcoreissue_id
    right join ergeon.store_order so on so.id = e.order_id -- added 28/7/2022
    left join int_data.order_ue_materialized ue on ue.order_id = so.id
    left join ergeon.quote_quote qq on qq.id = so.approved_quote_id
    left join ergeon.hrm_staff hs on hs.id = so.project_manager_id -- added 28/7/2022
    left join int_data.delivery_team_lead tl on tl.staff_id = hs.id -- added 28/7/2022
    left join active_escalations ae on ae.escalation_id = e.id
    left join esc_end_date eed on eed.escalation_id = e.id
    left join resolved_escalations re on re.escalation_id = e.id
    left join leads l on l.order_id = so.id
    left join ergeon.core_lead cl on cl.id = l.lead_id
    left join ergeon.customers_contact c on cl.contact_id = c.id
    left join ergeon.core_user cu2 on cu2.id = c.user_id
    left join scoping_esc sces on sces.escalation_id = e.id
    --where st.code in ('escalation_received', 'escalation_fix_agreed','escalation_revisit_scheduled','escalation_QA_scheduled')
    -- and team_lead_id is not null -- added 28/7/2022
    --added 6042 and 64870 to remove deprecated TST conditional
    where
        so.id not in (
            50815,
            56487,
            59225,
            59348,
            59404,
            59666,
            59670,
            59743,
            59753,
            59789,
            59805,
            59813,
            59878,
            59908,
            59922,
            60273,
            60283,
            60401,
            60547,
            60589,
            60590,
            60595,
            60596,
            60597,
            60612,
            60642,
            64870
        )
        and upper(coalesce(cl.full_name, '') || coalesce(c.full_name, '') || coalesce(cu2.full_name, '')) not like '%[TEST]%'
        and lower(coalesce(cl.email, '') || coalesce(cu2.email, '')) not like '%+test%'
        and e.deleted_at is null
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
)

select
    *,
    row_number() over (partition by escalation_id) as escalation_id_rank,
    row_number() over (partition by order_id) as order_id_rank,
    case
        when parent_order_id is not null and project_status_id not in (24, 31, 32)
            then row_number() over (partition by parent_order_id)
        else null
    end as active_wwwo_rank
from initial_data
