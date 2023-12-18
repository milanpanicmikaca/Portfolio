with test_leads as (
    select
        cl.order_id,
        cl.id as lead_id
    from ergeon.core_lead as cl
    left join ergeon.customers_contact as co on co.id = cl.contact_id
    left join ergeon.core_user as cu on cu.id = co.user_id
    where
        cl.created_at >= '2018-04-16'
        and
        (
            cl.order_id in
            (
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
                60612
            )
            or
            lower(cl.full_name) like '%test%'
            or lower(cl.full_name) like '%fake%'
            or
            lower(co.full_name) like '%test%'
            or lower(co.full_name) like '%fake%'
            or
            lower(cu.full_name) like '%test%'
            or lower(cu.full_name) like '%fake%'
            or
            lower(cl.email) like '%+test%' or lower(cl.email) like '%@test.%'
            or
            lower(cu.email) like '%+test%' or lower(cu.email) like '%@test.%'
        )
    qualify
        row_number() over (partition by cl.order_id order by cl.created_at) = 1
),

order_feedback as (
    select
        order_id,
        sum(nps) / count(nps) as csat,
        sum(case when created_at is null then 0 else 1 end) as has_feedback
    from ergeon.feedback_orderfeedback
    group by 1
),

order_escalations as (
    select
        eq.order_id,
        min(eq.date_the_order_escalated) as date_the_order_escalated,
        min(eq.date_the_escalation_resolved) as date_the_escalation_resolved,
        count(eq.escalation_id) as escalations_count,
        sum(case
            when
                eq.escalation_status in (
                    'Escalation Received',
                    'Revisit Scheduled',
                    'QA Scheduled',
                    'Escalation Fix Agreed'
                )
                and eq.date_the_escalation_deleted is null then 1
            else 0
            end) as is_active,
        sum(case
            when
                eq.escalation_status in (
                    'Escalation Received',
                    'Revisit Scheduled',
                    'QA Scheduled',
                    'Escalation Fix Agreed'
                )
                and date_diff(
                    current_date('America/Los_Angeles'),
                    eq.date_the_order_escalated,
                    day
                )
                < 30
                and eq.date_the_escalation_deleted is null then 1
            else 0
            end) as is_active_within_30_days,
        sum(case
            when
                eq.escalation_status in (
                    'Escalation Received',
                    'Revisit Scheduled',
                    'QA Scheduled',
                    'Escalation Fix Agreed'
                )
                and date_diff(
                    current_date('America/Los_Angeles'),
                    eq.date_the_order_escalated,
                    day
                ) between 30 and 90
                and eq.date_the_escalation_deleted is null then 1
            else 0
            end) as is_active_between_30_90_days,
        sum(case
            when
                eq.escalation_status in (
                    'Escalation Received',
                    'Revisit Scheduled',
                    'QA Scheduled',
                    'Escalation Fix Agreed'
                )
                and date_diff(
                    current_date('America/Los_Angeles'),
                    eq.date_the_order_escalated,
                    day
                )
                > 90
                and eq.date_the_escalation_deleted is null then 1
            else 0
            end) as is_active_more_90_days,
        sum(case
            when
                eq.escalation_status = 'Escalation Resolved'
                and date_diff(
                    current_date('America/Los_Angeles'),
                    eq.date_the_order_escalated,
                    day
                )
                <= 30
                then 1
            else 0
            end) as resolved_within_30_days,
        avg(case
            when
                eq.escalation_status in (
                    'Escalation Received',
                    'Revisit Scheduled',
                    'QA Scheduled',
                    'Escalation Fix Agreed'
                )
                and eq.date_the_escalation_deleted is null
                then
                date_diff(
                    current_date('America/Los_Angeles'),
                    eq.date_the_order_escalated,
                    day
                )
            end) as escalation_age,
        avg(case
            when
                eq.date_the_escalation_deleted is null
                then
                date_diff(
                    eq.date_the_escalation_resolved,
                    eq.date_the_order_escalated,
                    day
                )
            end) as tat
    from int_data.escalation_query as eq
    where
        lower(eq.core_issues_string) not like '%scoping%'
        or lower(eq.current_status_bucket) not like '%scoping_process%'
    group by 1
    order by 1
),

installer_escalations as (
    select
        order_id,
        case when count(id) != 0 then 1 else 0 end as has_installer_escalation
    from int_data.inst_escalations
    where order_id is not null
    group by 1
),

ranked_requests as (
    select
        cast(trim(regexp_extract(trim(admin_link), r'/(\d+)/')) as bignumeric)
        as order_id,
        added_at,
        request_stage
    from googlesheets.delivery_requote_requests as drr
    qualify
        row_number()
        over (
            partition by
                cast(
                    trim(
                        regexp_extract(trim(admin_link), r'/(\d+)/')
                    ) as bignumeric
                )
            order by drr.added_at
        )
        = 1
),

customer_approved_data as (
    with initial_data as (
        select
            ue.order_id,
            cu.full_name as customer_name
        from ergeon.quote_quote as qq
        left join ergeon.quote_quoteapproval as qq2 on qq2.quote_id = qq.id
        left join
            int_data.order_ue_materialized as ue
            on ue.order_id = qq.order_id
        left join ergeon.customers_customer as cc on cc.id = qq2.customer_id
        left join ergeon.core_user as cu on cu.id = cc.user_id
        where
            qq.approved_at is not null
            and qq2.approved_at is not null
            and qq.cancelled_at is null
            and ue.completed_at is not null
            and qq.is_cancellation = false
        qualify
            row_number() over (partition by ue.order_id, qq2.customer_id) = 1
    )

    select
        order_id,
        string_agg(cast(customer_name as string), ', ') as customer_name
    from initial_data
    group by 1
)

select
    ue.order_id,
    ue.completed_at as completion_date,
    ue.won_at as won_date,
    ue.won_ts_at as won_timestamp,
    ue.cancelled_at as cancellation_date,
    ue.project_manager,
    ga.formatted_address as address,
    ue.contractor,
    ue.contractor_pay,
    ue.revenue,
    ue.first_approved_price,
    ue.last_approved_cost,
    ue.last_approved_delivery_discount as cx_discount,
    ue.market,
    ue.old_region,
    ue.is_completed,
    cs.label as project_status,
    oe.date_the_order_escalated,
    oe.date_the_escalation_resolved,
    ue.has_escalation,
    oe.escalations_count,
    oe.is_active,
    oe.is_active_within_30_days,
    oe.is_active_between_30_90_days,
    oe.is_active_more_90_days,
    oe.resolved_within_30_days,
    oe.escalation_age,
    oe.tat,
    ie.has_installer_escalation,
    coalesce(cad.customer_name, cu2.full_name) as customer_name,
    case
        when
            ue.pm_team not in (
                'Vikings Crew',
                'House of Ninjas',
                'Rodeo Rangers',
                'House of Liberty',
                'Hollywood Stars',
                'Falcons Tribe',
                'KAM',
                'Launch'
            ) or ue.pm_team is null then 'No House'
        else ue.pm_team
    end as house,
    ue.revenue * ue.has_escalation as stuck_revenue,
    case
        when oe.is_active = 1 then ue.revenue
        else 0
    end as revenue_held,
    ue.last_approved_delivery_discount
    + ue.contractor_pay
    - ue.last_approved_cost
    + ue.wwo_installer_leakage as delivery_discount,
    ue.contractor_pay
    - ue.last_approved_cost
    + ue.wwo_installer_leakage as installer_discount,
    ue.revenue - ue.last_approved_cost as expected_profit,
    ue.revenue - ue.contractor_pay as real_profit,
    case
        when ue.is_completed = 1 then orf.csat
    end as csat,
    case
        when ue.is_completed = 1 then orf.has_feedback
    end as has_feedback,
    case
        when
            ue.is_completed = 1
            and orf.order_id is null then 1
        else 0
    end as missing_feedback,
    case
        when ue.is_completed = 1 and ue.has_escalation = 1 then 1
        else 0
    end as is_completed_with_escalation,
    case when rr.request_stage = 'post_installation' then 1 else 0 end
    as change_order_post_installation
from int_data.order_ue_materialized as ue
left join test_leads as l on l.order_id = ue.order_id
left join order_feedback as orf on orf.order_id = ue.order_id
left join order_escalations as oe on oe.order_id = ue.order_id
left join ergeon.store_order as so on so.id = ue.order_id
left join ergeon.core_statustype as cs on cs.id = so.project_status_id
left join customer_approved_data as cad on cad.order_id = ue.order_id
left join ergeon.core_house as ch on ch.id = so.house_id
left join ergeon.geo_address as ga on ga.id = ch.address_id
left join ergeon.customers_customer as cc on cc.id = ch.customer_id
left join ergeon.customers_contact as cc2 on cc2.id = cc.contact_id
left join ergeon.core_user as cu2 on cu2.id = cc2.user_id
left join installer_escalations as ie on ie.order_id = ue.order_id
left join ranked_requests as rr on rr.order_id = ue.order_id
where l.order_id is null
