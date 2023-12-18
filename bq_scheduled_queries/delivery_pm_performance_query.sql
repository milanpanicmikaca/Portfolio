with date_array as (
    select
        cast(date as timestamp) as date,
        timestamp_add(cast(date as timestamp), interval 86399 second) as end_of_day_ts,
        rank() over (partition by date_trunc(date, day) order by date desc) as rank_day
    from warehouse.generate_date_series
    where date > '2018-04-15' and date <= current_date
),

orders as (
    select
        ue.order_id,
        ue.project_manager,
        won_at,
        coalesce(
            case
                when cancelled_at is not null and completed_at is not null and cancelled_at > completed_at then cancelled_at
                when cancelled_at is not null and completed_at is not null and completed_at > cancelled_at then completed_at
            end,
            ue.completed_at,
            ue.cancelled_at
        ) as final_date
    from int_data.order_ue_materialized ue
--left join completed_and_cancelled_orders co on co.order_id = ue.order_id
),

order_statuses as (
    select
        extract(date from cs.created_at at time zone 'America/Los_Angeles') as status_change_date,
        cs.status_id,
        cs.object_id as order_id,
        row_number() over (
            partition by cs.object_id, extract(date from cs.created_at at time zone 'America/Los_Angeles') order by cs.created_at desc
        ) as desc_rank,
        lead(
            extract(date from cs.created_at at time zone 'America/Los_Angeles')
        ) over (partition by cs.object_id order by cs.created_at) as next_timestamp
    from ergeon.core_statushistory cs
    where field_name = 'project_status'
    qualify desc_rank = 1
),

cross_join_final as (
    select
        d.date,
        d.rank_day,
        o.order_id,
        o.project_manager,
        -- o.won_at,
        od.status_id
    -- od.status_change_date as status_change,
    from date_array d
    left join orders o on
        cast(d.date as date) >= o.won_at
        and cast(d.date as date) <= coalesce(o.final_date, date_add(current_date, interval 1 day))
    left join order_statuses od
        on o.order_id = od.order_id
            and cast(d.date as date) >= od.status_change_date
            and cast(d.date as date) < coalesce(od.next_timestamp, date_add(current_date, interval 1 day))
    order by 2, 1
),

project_day as (
    select
        cast(date_trunc(date, day) as date) as date,
        project_manager as pm,
        count(*) as active_projects
    from cross_join_final where status_id <> 24 --excluding orders that were on hold at end of day
        and project_manager is not null and rank_day = 1
    group by 1, 2
),

escalations as (
    select
        eh.object_id as escalation_id,
        datetime(cast(e.reported_at as timestamp), 'America/Los_Angeles') as start_date,
        datetime(cast(eh.created_at as timestamp), 'America/Los_Angeles') as end_date,
        generate_date_array(cast(e.reported_at as date), current_date(), interval 1 day) as date_array,
        st.code,
        u.full_name as pm,
        case
            when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled')
                then rank() over (partition by eh.object_id order by eh.created_at desc) else null
        end as rank_end_states,
        case
            when st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled')
                then 'active' else 'resolved'
        end as grouped_status,
        case
            when st2.code in ('escalation_received', 'escalation_fix_agreed', 'escalation_revisit_scheduled', 'escalation_QA_scheduled')
                then rank() over (partition by e.id order by eh.created_at desc) else null end as act_rank
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e on e.id = eh.object_id
    left join int_data.escalation_query se on se.escalation_id = e.id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    left join ergeon.core_statustype st2 on st2.id = e.status_id
    left join ergeon.store_order o on o.id = e.order_id
    left join ergeon.hrm_staff s on s.id = o.project_manager_id
    left join ergeon.core_user u on s.user_id = u.id
    where reported_at >= '2021-04-28'
        and d.model = 'escalation'
        and d.app_label = 'store'
        and e.deleted_at is null
        and (lower(se.core_issues_string) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')

),

active_data as (
    select
        escalation_id,
        pm,
        day,
        grouped_status,
        -- start_date,
        -- end_date,
        case
            when grouped_status = 'resolved' and day <= extract(date from end_date) then 1
            when grouped_status = 'active' and day <= current_date() then 1
            else 0
        end as active,
        case
            when grouped_status = 'resolved' and day >= extract(date from end_date) then timestamp_diff(end_date, start_date, day)
        end as tat
    from escalations
    cross join unnest(date_array) as day
    where (grouped_status = 'active' and act_rank = 1) or rank_end_states = 1
),

aggregate_calc as (
    select
        day,
        pm,
        sum(active) as active_esc,
        avg(tat) as tat
    from active_data
    where active = 1
    group by 1, 2
),

esc_metrics_per_day as (
    select
        --day as date,
        date_trunc(day, day) as date,
        pm,
        active_esc,
        avg(tat) as tat
    from aggregate_calc a
    left join date_array d on cast(d.date as date) = a.day
    where d.rank_day = 1
    group by 1, 2, 3
),

--reviews table
reviews as (
    select
        --date(fr.posted_at,"America/Los_Angeles") as date,
        date_trunc(date(fr.posted_at, "America/Los_Angeles"), day) as date,
        cu.full_name as pm,
        count(*) as review_count
    from ergeon.feedback_review fr
    left join ergeon.marketing_localaccount mnl on mnl.id = fr.account_id
    left join ergeon.marketing_channel mnc on mnc.id = mnl.channel_id
    left join ergeon.hrm_staff hs on fr.delivery_staff_attributed_id = hs.id
    left join ergeon.core_user cu on hs.user_id = cu.id
    where cu.full_name is not null
    group by 1, 2
    order by 1 desc
),

cancelled_projects as (
    select
        o.id as order_id,
        min(completed_at) as cancelled_at
    from ergeon.store_order o
    join ergeon.quote_quote q on q.order_id = o.id
    where is_cancellation = true
        and q.created_at > '2018-04-15'
    group by 1
),

orders_completed as (
    select
        date_trunc(extract(date from so.completed_at at time zone "America/Los_Angeles"), day) as date,
        so.id as order_id,
        cu.full_name as pm,
        parent_order_id,
        case when fo.created_at is null then 0 else 1 end as has_feedback,
        case when so.completed_at is null then 0 else 1 end as completed_count
    from ergeon.store_order so
    left join ergeon.hrm_staff hs on so.project_manager_id = hs.id
    left join ergeon.core_user cu on cu.id = hs.user_id
    left join ergeon.quote_quote qq on qq.id = so.approved_quote_id
    left join ergeon.core_house h on h.id = so.house_id
    left join ergeon.customers_customer cc on cc.id = h.customer_id
    left join ergeon.customers_contact c on cc.contact_id = c.id --adding new table to filter old TST statuses through cx names
    left join ergeon.core_statustype p on p.id = so.project_status_id
    left join ergeon.feedback_orderfeedback fo on so.id = fo.order_id
    left join cancelled_projects cp on cp.order_id = so.id
    where so.completed_at is not null
        and cp.cancelled_at is null
        and qq.approved_at >= '2018-04-15'
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
        and lower(c.full_name) not like '%test%' --testing status quotes
        and lower(c.full_name) not like '%bootcamp%'--testing status quotes
        and parent_order_id is null
    --and so.project_status_id = 31 --new row from core_statustype that will be used after new PRD is implemented
    order by 1 desc
),

margin_leakage as (
    select
        ue.order_id as order_id,
        /*(last_approved_mktg_discount + last_approved_sales_discount + last_approved_delivery_discount +
        (last_approved_price - revenue) + (contractor_pay - last_approved_cost) +
        cost_of_sales + materials_pay + last_approved_pricing_discount)/nullif(revenue,0) as margin_leak,*/
        --last_approved_delivery_discount as delivery,
        last_approved_delivery_discount as delivery,
        contractor_pay - last_approved_cost + wwo_installer_leakage as installer,
        materials_pay as materials,
        last_approved_pricing_discount as pricing,
        revenue
    from int_data.order_ue_materialized ue
    where is_completed = 1
),

feedback_and_leakage_join as (
    select
        date,
        pm,
        o.order_id as order_id,
        delivery,
        installer,
        revenue,
        has_feedback,
        completed_count
    from orders_completed o
    left join margin_leakage ml on o.order_id = ml.order_id
),

--feedback collection and margin leak
sums as (
    select
        --date,
        date_trunc(date, day) as date,
        pm,
        sum(has_feedback) as has_feedback,
        sum(completed_count) as completed_count,
        sum(delivery) as delivery,
        sum(installer) as installer,
        sum(revenue) as revenue,
        (sum(delivery + installer) / nullif(sum(revenue), 0)) * 100 as margin_leakage
    from feedback_and_leakage_join
    where pm is not null
    group by 1, 2
),

scores_per_day as (
    select
        date_trunc(date(time_stamp), day) as date,
        project_manager as pm,
        avg(cast(score as int)) as score_per_PM
    from int_data.delivery_pm_qa_query
    where not regexp_contains(score, r'[a-zA-Z\s]+')
    group by 1, 2
)

select * except (full_name, team_lead_id, team_leader, house)
from project_day
left join esc_metrics_per_day using (date, pm)
--left join tat_count t using(date,pm)
left join reviews using (date, pm)
left join sums using (date, pm)
left join scores_per_day using (date, pm)
left join int_data.delivery_team_lead tl on tl.full_name = project_day.pm -- added 5/7/2022
where
    date >= '2021-04-01'
    and pm not in ('Alfredo Silva', 'Carmen Mendez', 'Daniela Vidal', 'Diego Bonatti', 'Jazmin Barrera',
        'Jhony Duran', 'Eliana Oleachea Dongo', 'Jazmin Barrera', 'Magdalena Achilleoudis',
        'Ricardo Saavedra', 'Sheila Duran', 'Teresita Alfaro', 'Samuel Duran', 'Nestor Baca',
        'Maricarmen Castellanos', 'Joan Moya', 'Sergio Hernandez')
    and team_lead_id is not null -- added 5/7/2022
order by 1 desc, 2
