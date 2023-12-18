with order_ids as (
    select
        so.id,
        cu.full_name,
        date_trunc(extract(date from so.completed_at at time zone "America/Los_Angeles"), day) as date,
        date_trunc(extract(date from qq.approved_at at time zone "America/Los_Angeles"), day) as approved_date,
        date_trunc(extract(date from so.cancelled_at at time zone "America/Los_Angeles"), day) as cancelled_date,
        so.total_project_cost as total_cost,
        so.total_project_price as old_total_price,
        parent_order_id,
        if(cc.is_commercial = True, 1, 0) as is_commercial,
        hs.id as staff_id, -- added 8/6/2022
        row_number() over (partition by hs.id order by hs.id) as staff_rank -- added 9/6/2022
    from ergeon.store_order so
    left join ergeon.hrm_staff hs on so.project_manager_id = hs.id
    left join ergeon.core_user cu on cu.id = hs.user_id
    left join ergeon.quote_quote qq on qq.order_id = so.id
    left join ergeon.core_house h on h.id = so.house_id
    left join ergeon.customers_customer cc on cc.id = h.customer_id
    --left join leads l on l.order_id = so.id
    left join (select   ---first lead
            so.id as order_id,
            min(cl.id) as lead_id
        from ergeon.store_order so
        left join ergeon.core_lead cl on cl.order_id = so.id
        group by 1) l on l.order_id = so.id
    left join ergeon.core_lead cl on cl.id = l.lead_id
    left join ergeon.customers_contact c on cl.contact_id = c.id
    left join ergeon.core_user cu2 on cu2.id = c.user_id
    where so.completed_at is not null
        and qq.approved_at >= '2018-04-15'
        and qq.created_at >= '2018-04-15'
        and so.approved_quote_id is not null
        --added 6042 and 64870 to remove deprecated TST conditional
        and so.id not in (
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
        and so.id not in (
            select qq.order_id
            from ergeon.quote_quote qq
            where is_cancellation = True)
    qualify rank() over (partition by qq.order_id order by qq.approved_at) = 1 --1st approved quote
),

waiver as (
    select
        so.id as order_id,
        qqa.quote_id,
        case when qqa.signoff_at is not null then 1 end as waiver,
        case when qqa.signoff_at is not null and qqa.signoff_by_id is not null then 1 end as waiver_automatic,
        case when qqa.signoff_at is not null and qqa.signoff_by_id is null then 1 end as waiver_manual
    from ergeon.quote_quoteapproval qqa
    left join ergeon.quote_quote qq on qq.id = qqa.quote_id
    left join ergeon.store_order so on so.id = qq.order_id
    where qqa.signoff_at is not null
),

total_contractor_cost as (
    select
        o.id,
        sum(co.total_cost) as total_contractor_cost
    from order_ids o
    left join ergeon.contractor_contractororder co on o.id = co.order_id
    where co.status_id = 13
    group by 1
),

feedback as (
    select
        order_id,
        sum(nps) / count(nps) as CSAT,
        sum(case when created_at is null then 0 else 1 end) as has_feedback
    from ergeon.feedback_orderfeedback
    group by 1
),

escalations as (
    select
        se.order_id as Order_id,
        count(escalation_id) as esc_per_order
    from int_data.escalation_query se
    where date_the_escalation_deleted is null
        and (lower(se.core_issues_string) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')
    group by order_id
    order by 1
),

main_escalation_bucket as (
    with calc_data as (
        select
            cu.full_name,
            sec.name,
            count(sec.name) as count
        from ergeon.store_escalation_core_issues see
        left join ergeon.store_escalation se on se.id = see.escalation_id
        left join ergeon.store_escalationcoreissue sec on sec.id = see.escalationcoreissue_id
        left join ergeon.contractor_contractororder coo on coo.order_id = se.order_id
        left join ergeon.contractor_contractor hc on hc.id = coo.contractor_id
        left join ergeon.contractor_contractorcontact cc on cc.id = hc.contact_id
        left join ergeon.core_user cu on cu.id = cc.user_id
        where se.id in (
            select
                se.id
            from ergeon.store_escalation se
            left join ergeon.store_escalation_primary_teams_attributed sep on sep.escalation_id = se.id
            left join ergeon.store_escalation_secondary_teams_attributed ses on ses.escalation_id = se.id
            left join ergeon.store_escalationteamattributed ep on ep.id = sep.escalationteamattributed_id
            left join ergeon.store_escalationteamattributed es on es.id = ses.escalationteamattributed_id
            where ep.name = 'Delivery Team' or es.name = 'Delivery Team'
            qualify row_number() over (partition by se.id) = 1
        )
        and (lower(sec.name) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')
        group by 1, 2
    ),

    rank as (
        select *,
               rank() over (partition by full_name order by count desc) as rank_issues
        from calc_data
    )

    select
        full_name as installer_name_admin,
        string_agg(name, ",") as core_issue
    from rank
    where rank_issues = 1
    group by 1
)

select
    date as day,
    oi.full_name,
    oi.staff_id,
    date_diff(date, approved_date, day) as day_for_completion,
    oi.cancelled_date,
    cu.full_name as contractor_name,
    oi.id,
    oi.old_total_price,
    oi.total_cost,
    case
        when co.status_id in (3, 13, 66) then co.id
        else Null
    end as contractorapp_id,
    co.total_cost as contractor_cost,
    co.status_id as co_status,
    cc.total_contractor_cost,
    oi.old_total_price - oi.total_cost as expected_profit,
    oi.old_total_price - total_contractor_cost as real_profit,
    CSAT,
    has_feedback,
    'CMP' as status,
    case when ef.Order_id is null then 0 else esc_per_order end as has_escalation,
    hc.project_manager_id,
    tl2.full_name as pm_assigned,
    coalesce(tl.house, 'Carmen Mendez') as team_lead,
    coalesce(tl2.team_leader, 'Carmen Mendez') as team_lead_contractor,
    coalesce(tl2.house, 'No House') as house_contractor,
    parent_order_id,
    is_commercial,
    me.core_issue,
    wa.waiver,
    wa.waiver_automatic,
    wa.waiver_manual
from order_ids oi
left join waiver wa on wa.order_id = oi.id
left join ergeon.contractor_contractororder co on oi.id = co.order_id
left join total_contractor_cost cc on cc.id = oi.id
left join ergeon.contractor_contractor hc on hc.id = co.contractor_id
left join ergeon.contractor_contractorcontact cc2 on cc2.id = hc.contact_id
left join ergeon.core_user cu on cc2.user_id = cu.id
left join ergeon.hrm_staff hs on hs.id = hc.project_manager_id
left join int_data.delivery_team_lead tl2 on tl2.staff_id = hs.id
left join feedback f on oi.id = f.order_id
left join escalations ef on ef.Order_id = oi.id
left join main_escalation_bucket me on cu.full_name = me.installer_name_admin
left join int_data.delivery_team_lead tl on tl.staff_id = oi.staff_id
order by day desc, oi.id desc
