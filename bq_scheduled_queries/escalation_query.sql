with
escalation_detailed as (
    select
        se.id as escalation_id,
        se.order_id,
        ou.first_approved_quote_id,
        reported_at,
        se.deleted_at,
        slack_link,
        ou.project_manager,
        ou.pm_team,
        ou.multi_party_approval,
        ou.region,
        ou.market,
        ou.county,
        ou.city,
        ou.state,
        ou.type,
        cs.label as escalation_status,
        se.current_status_bucket,
        ou.is_completed,
        ou.onsite_type,
        concat('https://api.ergeon.in/public-admin/store/escalation/', se.id, '/change/') as Escalation_Admin_URL,
        concat('https://api.ergeon.in/public-admin/store/order/', se.order_id, '/change/') as Public_Admin_URL,
        ou.product_quoted,
        concat('https://ergeon.pipedrive.com/deal/', pipedrive_deal_key) as sales_pipedrive_url,
        hs2.full_name as sales_rep,
        hs.slack_user_key as sales_rep_slack,
        concat('https://ergeon.pipedrive.com/deal/', pipedrive_project_key) as project_pipedrive_url,
        concat('https://admin.ergeon.in/quoting-tool/', ou.order_id, '/quote/', ou.first_approved_quote_id) as escalated_quote,
        ou.won_at as date_the_order_got_approved,
        sp.name,
        cc2.full_name as customer_name,
        long_term_process_improvement,
        immediate_actions,
        cu.full_name as reported_by
    from ergeon.store_escalation se
    left join `int_data.order_ue_materialized`ou on se.order_id = ou.order_id
    left join ergeon.core_statustype cs on se.status_id = cs.id
    left join ergeon.store_order so on ou.order_id = so.id
    left join ergeon.core_house ch on ch.id = so.house_id
    left join ergeon.customers_customer cc on cc.id = ch.customer_id
    left join ergeon.customers_contact cc2 on cc2.id = cc.contact_id
    left join ergeon.hrm_staff hs on hs.id = so.sales_rep_id
    left join ergeon.hrm_stafflog hs2 on hs2.id = hs.current_stafflog_id
    left join ergeon.core_user cu on se.created_by_id = cu.id
    left join ergeon.store_product sp on sp.id = so.product_id
    where se.reported_at >= '2021-04-28'
        and not ou.is_warranty_order
        and se.deleted_at is null
),

escalation_end_date as (
    select
        eh.object_id as escalation_id,
        date(eh.created_at) as end_date,
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

primary_teams_attributed as (
    select
        se.escalation_id,
        array_agg(et.name) as primary_team
    from escalation_detailed se
    left join ergeon.store_escalation_primary_teams_attributed sep on sep.escalation_id = se.escalation_id
    left join ergeon.store_escalationteamattributed et on sep.escalationteamattributed_id = et.id
    where et.name is not null
    group by 1
),

secondary_teams_attributed as (
    select
        se.escalation_id,
        array_agg(et1.name) as secondary_team
    from escalation_detailed se
    left join ergeon.store_escalation_secondary_teams_attributed ses on ses.escalation_id = se.escalation_id
    left join ergeon.store_escalationteamattributed et1 on ses.escalationteamattributed_id = et1.id
    where et1.name is not null
    group by 1
),

teams_no_null as ( -- CTE for substituting null with '' so arrays can be joined, THIS CTE IS FOR TEAM FILTER ONLY
    select
        se.escalation_id,
        array_agg(coalesce(et.name, '')) as primary_team,
        array_agg(coalesce(et1.name, '')) as secondary_team
    from escalation_detailed se
    left join ergeon.store_escalation_primary_teams_attributed sep on sep.escalation_id = se.escalation_id
    left join ergeon.store_escalationteamattributed et on sep.escalationteamattributed_id = et.id
    left join ergeon.store_escalation_secondary_teams_attributed ses on ses.escalation_id = se.escalation_id
    left join ergeon.store_escalationteamattributed et1 on ses.escalationteamattributed_id = et1.id
    group by 1
),

teams_filter as (
    select
        *,
        array(select distinct x
            from unnest(array_concat(primary_team, secondary_team)) x
            order by x
        ) as concatenated_teams_array_distinct
    from teams_no_null
),

core_issues as (
    select
        ed.escalation_id,
        array_agg(ec.name) as core_issues,
        string_agg(ec.name, ', ') as core_issues_string
    from ergeon.store_escalation_core_issues ei
    left join ergeon.store_escalationcoreissue ec on ec.id = ei.escalationcoreissue_id
    left join escalation_detailed ed on ed.escalation_id = ei.escalation_id
    where ed.escalation_id is not null
        and ec.deleted_at is null
    group by 1
),

contractor_per_escalation as (
    select
        ed.escalation_id,
        ed.order_id,
        cu.full_name as contractor,
        cs.label as project_status
    from escalation_detailed ed
    left join ergeon.contractor_contractororder co on ed.order_id = co.order_id and ed.reported_at > co.created_at
    left join ergeon.contractor_contractor cc on cc.id = co.contractor_id
    left join ergeon.contractor_contractorcontact cc2 on cc2.id = cc.contact_id
    left join ergeon.core_user cu on cc2.user_id = cu.id
    left join ergeon.core_statustype cs on co.status_id = cs.id
    qualify row_number() over (partition by ed.escalation_id order by co.created_at desc) = 1
),

jira_issue_per_order as (
    select
        admin_order_number as order_id,
        min(ji.id) as first_jira_id
    from jira.issue ji
    group by 1
),

last_approved_quote_before_escalation as (
    -- this CTE includes an estimator name
    select
        ed.escalation_id,
        qq.id as last_approved_quote_id_before_report,
        cu.full_name as estimator,-- estimator of the last approved before report
        concat('https://ergeon.atlassian.net/browse/', ji.key) as jira_link_last_approved
    from escalation_detailed ed left join
        ergeon.quote_quote qq on qq.order_id = ed.order_id and qq.approved_at < ed.reported_at left join
        ergeon.core_user cu on qq.sent_to_customer_by_id = cu.id left join
        int_data.estimation_dashboard_v3 est on est.id = qq.id left join
        jira_issue_per_order j on j.order_id = ed.order_id left join
        jira.issue ji on ji.id = j.first_jira_id
    where qq.approved_at is not null
    qualify row_number() over (partition by ed.escalation_id order by qq.approved_at desc) = 1
)

select
    se.order_id,
    date(cast(reported_at as timestamp), "America/Los_Angeles") as date_the_order_escalated,
    date(cast(eed.end_date as timestamp), "America/Los_Angeles") as date_the_escalation_resolved,
    date(cast(se.deleted_at as timestamp), "America/Los_Angeles") as date_the_escalation_deleted,
    slack_link,
    se.escalation_id,
    p.primary_team,
    s.secondary_team,
    se.project_manager,
    se.pm_team,
    multi_party_approval,
    region,
    market,
    county,
    city,
    state,
    type,
    core_issues,
    core_issues_string,
    escalation_status,
    se.current_status_bucket,
    onsite_type,
    Escalation_Admin_URL,
    Public_Admin_URL,
    product_quoted,
    customer_name,
    long_term_process_improvement,
    immediate_actions,
    reported_by,
    escalated_quote,
    sales_pipedrive_url,
    sales_rep,
    date_the_order_got_approved,
    estimator,
    jira_link_last_approved,
    contractor,
    project_status,
    concat(se.project_manager, ' - ', contractor) as combo_pm_contractor,
    concat(contractor, ' - ', se.market) as combo_contractor_market,
    le.last_approved_quote_id_before_report,
    concatenated_teams_array_distinct
from escalation_detailed se
left join primary_teams_attributed p using (escalation_id)
left join secondary_teams_attributed s using (escalation_id)
left join core_issues using (escalation_id)
left join contractor_per_escalation using (escalation_id)
left join last_approved_quote_before_escalation le using (escalation_id)
left join teams_filter using (escalation_id)
left join escalation_end_date eed using (escalation_id)
