with scoping_esc as (
    select
        se.id as escalation_id
    from ergeon.store_escalation se
    left join ergeon.store_escalation_core_issues seci on seci.escalation_id = se.id
    left join ergeon.store_escalationcoreissue se2 on se2.id = seci.escalationcoreissue_id
    where lower(se.current_status_bucket) like '%scoping_process%' or lower(se2.name) like '%scoping%'
),

calc_escalation as (
    select
        eh.object_id as escalation_id,
        e.reported_at as start_date,
        eh.created_at as end_date,
        u.full_name as pm,
        st.code,
        s.id as staff_id,
        case when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then
                rank() over (partition by eh.object_id order by eh.created_at desc) else null end as rank_end_states,
        case
            when
                st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled') then 'active'
            else 'resolved'
        end as grouped_status,
        case
            when st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled') then
                rank() over (partition by e.id order by eh.created_at desc) else null
        end as act_rank
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e on e.id = eh.object_id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    left join ergeon.core_statustype st2 on st2.id = e.status_id
    left join ergeon.store_order o on o.id = e.order_id
    left join ergeon.hrm_staff s on s.id = o.project_manager_id
    left join ergeon.core_user u on u.id = s.user_id
    left join scoping_esc sces on sces.escalation_id = eh.object_id
    where reported_at >= '2021-04-28'
        and d.model = 'escalation'
        and d.app_label = 'store'
        and e.deleted_at is null
        and sces.escalation_id is null
),

team_avg as (
    select avg(case when rank_end_states = 1 then timestamp_diff(end_date, start_date, day) else null end) as tat_team
    from calc_escalation
),

staff_detail as (
    select
        cu.full_name,
        hr.id
    from ergeon.hrm_staff hr
    left join ergeon.hrm_stafflog hl on hr.current_stafflog_id = hl.id
    left join ergeon.core_user cu on hr.user_id = cu.id
    left join ergeon.hrm_staffposition hsp on hsp.id = hl.position_id
    left join ergeon.hrm_ladder ha on ha.id = hsp.ladder_id
    where change_type <> 'left' and ha.department_id = 4 and internal_title like '%Project Manager%'
        and internal_title not like '%Team%'
    order by 1
),

generate_date_series as (
    select
        date
    from warehouse.generate_date_series
    where date >= '2018-04-15'
        and date <= current_date()
),

revenue_calc as (
    select
        order_id,
        sum(amount) as revenue
    from ergeon.accounting_transaction
    where type_id in (8, 10)
    group by 1
),

initial_data as (
    select
        csh.created_at as added_at,
        extract(date from csh.created_at at time zone "America/Los_Angeles") as day,
        ps.label as status,
        --soh.status, --deprecated field
        csh.object_id as order_id,
        cu.full_name,
        case when m.id is null then "Unknown"
            else m.code
        end as geo,
        m.region_id,
        hs.id as staff_id,
        so.parent_order_id,
        se.id as escalation_id
    from ergeon.core_statushistory csh
    left join ergeon.store_order so on so.id = csh.object_id and csh.content_type_id = 108
    left join ergeon.hrm_staff hs on so.project_manager_id = hs.id
    left join ergeon.core_user cu on cu.id = hs.user_id
    left join staff_detail s on s.full_name = cu.full_name
    left join int_data.delivery_team_lead tl on tl.staff_id = hs.id --added 28/7/2022
    left join ergeon.core_house h on h.id = so.house_id
    left join ergeon.customers_customer c on c.id = h.customer_id
    left join ergeon.geo_address ga on ga.id = h.address_id
    left join ergeon.geo_county cn on cn.id = ga.county_id
    left join ergeon.product_countymarket pcnm on pcnm.county_id = cn.id
    left join ergeon.product_market m on m.id = pcnm.market_id
    left join ergeon.core_statustype ds on ds.id = so.deal_status_id
    left join ergeon.core_statustype ps on ps.id = so.project_status_id
    left join ergeon.store_escalation se on se.order_id = so.id
    where ps.label in ('Ready to Schedule', 'Pre-visit Done', 'Pre-visit Info Sent', 'Scheduled',
        'Planned Scheduling', 'Intro Call Done', 'On Hold', 'Pre-visit Agreed', 'Cancelled', 'Completed')
        and hs.slack_username not in ('carmen', 'magdalena', 'gerardo.sosa') and hs.slack_username is not null
        and so.approved_quote_id is not null
        --and so.parent_order_id is null
        and cu.full_name in (s.full_name)
        and ps.label is not null
        and ps.type = 'order_project_status'
        and team_lead_id is not null -- added 28/7/2022
    qualify row_number() over (partition by csh.object_id order by csh.created_at desc) = 1
),

join_data as (
    select
        date,
        day,
        status,
        order_id,
        full_name,
        staff_id,
        geo,
        region_id,
        parent_order_id,
        escalation_id,
        rank() over (partition by date, order_id order by added_at desc) as rank
    from initial_data
    cross join generate_date_series
    where day <= date
--order by 1,2,3,4 desc
),

count_data as (
    select
        date,
        status,
        full_name,
        staff_id,
        geo,
        region_id,
        count(case when parent_order_id is null then order_id else null end) as count,
        count(case when parent_order_id is not null then order_id else null end) as count_wwo,
        count(case when parent_order_id is null and escalation_id is not null then order_id else null end) as count_act_with_esc
    from join_data
    where rank = 1
        and status not in ('Completed', 'Cancelled', 'On Hold') -- last one added at 20/6/2022
    group by 1, 2, 3, 4, 5, 6
--order by 1,2,3,4,5
),

active_projects as (
    select
        full_name as pm,
        staff_id,
        region_id,
        sum(count) as active_count,
        sum(count_wwo) as active_wwo,
        sum(count_act_with_esc) as active_with_esc,
        sum(case when geo = "CN-EB" then count else 0 end) as EB,
        sum(case when geo = "CS-VC" then count else 0 end) as VC,
        sum(case when geo = "CN-SF" then count else 0 end) as SF,
        sum(case when geo = "CS-SV" then count else 0 end) as SV,
        sum(case when geo = "CS-LA" then count else 0 end) as LA,
        sum(case when geo = "CN-SA" then count else 0 end) as SAC,
        sum(case when geo = "CN-NB" then count else 0 end) as NB,
        sum(case when geo = "CS-OC" then count else 0 end) as OC,
        sum(case when geo = "CN-FR" then count else 0 end) as FR,
        sum(case when geo = "CN-WA" then count else 0 end) as WA,
        sum(case when geo = "CN-SJ" then count else 0 end) as SJ,
        sum(case when geo = "CN-PA" then count else 0 end) as PA,
        sum(case when geo = "CN-ST" then count else 0 end) as ST,
        sum(case when geo = "CS-SD" then count else 0 end) as SD,
        sum(case when geo in ("CN-WA", "CN-SJ", "CN-PA") then count else 0 end) as SB,
        sum(case when geo = 'TX-DL' then count else 0 end) as DL,
        sum(case when geo = 'TX-FW' then count else 0 end) as FW,
        sum(case when geo = "Unknown" then count else 0 end) as Unknown
    from count_data
    where date >= current_date()
    group by 1, 2, 3
),

active_escalations as (
    select
        count(*) as active_esc,
        sum(revenue) as revenue_held,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) < 30 then 1 else 0 end) as active_esc_30days,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) between 30 and 90 then 1 else 0 end) as active_esc_30_90days,
        sum(case when timestamp_diff(current_timestamp(), e.reported_at, day) > 90 then 1 else 0 end) as active_esc_90days,
        avg(timestamp_diff(current_timestamp(), e.reported_at, day)) as avg_esc_age
    from ergeon.store_escalation e
    left join ergeon.core_statustype st on st.id = e.status_id
    left join revenue_calc x on x.order_id = e.order_id
    -- left join ergeon.store_order so on so.id = e.order_id -- added 28/7/2022
    -- left join ergeon.hrm_staff hs on hs.id = so.project_manager_id -- added 28/7/2022
    -- left join int_data.delivery_team_lead tl on tl.staff_id = hs.id -- added 28/7/2022
    where st.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled')
        -- and team_lead_id is not null -- added 28/7/2022
        and deleted_at is null
)

select
    a.pm as pm_assigned,
    tl.team_lead_id,
    a.staff_id,
    a.region_id,
    coalesce(house, 'Carmen Mendez') as team_lead,
    max(tat_team) as tat_team,
    sum(case when rank_end_states = 1 then 1 else 0 end) as count_solved_pm,
    avg(case when rank_end_states = 1 then timestamp_diff(end_date, c.start_date, day) else null end) as tat,
    sum(case when timestamp_diff(current_timestamp(), end_date, day) <= 30 and code = 'escalation_resolved' then 1 else 0 end) as resolved_30days,
    sum(
        case when (grouped_status = 'active' and act_rank = 1 and timestamp_diff(current_timestamp(), c.start_date, day) < 30) then 1 else 0 end
    ) as active_30days,
    sum(
        case
            when
                (grouped_status = 'active' and act_rank = 1 and timestamp_diff(current_timestamp(), c.start_date, day) between 30 and 90) then 1
            else 0
        end
    ) as active_30_90days,
    sum(
        case when (grouped_status = 'active' and act_rank = 1 and timestamp_diff(current_timestamp(), c.start_date, day) > 90) then 1 else 0 end
    ) as active_90days,
    avg(case when grouped_status = 'active' and act_rank = 1 then timestamp_diff(current_timestamp(), c.start_date, day) end) as avg_escalation_age,
    max(active_count) as active_projects,
    max(active_wwo) as active_wwo,
    max(active_with_esc) as active_with_esc,
    max(revenue_held) as revenue_held,
    max(EB) as EB,
    max(VC) as VC,
    max(SF) as SF,
    max(SV) as SV,
    max(LA) as LA,
    max(SAC) as SAC,
    max(NB) as NB,
    max(OC) as OC,
    max(FR) as FR,
    max(SB) as SB,
    max(DL) as DL,
    max(WA) as WA,
    max(SJ) as SJ,
    max(PA) as PA,
    max(ST) as ST,
    max(FW) as FW,
    max(SD) as SD,
    max(Unknown) as Unknown,
    max(active_esc) as active_esc,
    max(active_esc_30days) as active_esc_30days,
    max(active_esc_30_90days) as active_esc_30_90days,
    max(active_esc_90days) as active_esc_90days,
    max(avg_esc_age) as avg_esc_age
from active_projects a
left join staff_detail s on s.id = a.staff_id
left outer join calc_escalation c on c.staff_id = s.id
left join int_data.delivery_team_lead tl on tl.staff_id = a.staff_id
cross join team_avg
cross join active_escalations
where tl.staff_id in (s.id) and team_lead_id is not null
group by 1, 2, 3, 4, 5
order by 1
