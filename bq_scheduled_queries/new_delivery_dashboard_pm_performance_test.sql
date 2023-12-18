--reviews table
with reviews as (
    select
        project_manager,
        date_trunc(posted_at, month) as date,
        count(review_id) as review_count
    from int_data_tests.delivery_dashboard_reviews_query_test
    group by 1, 2
),

feedback_and_ml as (
    select
        project_manager,
        date_trunc(completion_date, month) as date,
        sum(coalesce(has_feedback, 0)) / count(order_id) as feedback_collection,
        sum(cx_discount) / nullif(sum(revenue), 0) as delivery_margin_leakage
    from int_data_tests.new_delivery_dashboard_order_test
    where is_completed = 1
    group by 1, 2
),

qa as (
    select
        project_manager,
        date_trunc(date(time_stamp), month) as date,
        avg(cast(score as int)) as qa_score
    from int_data.delivery_pm_qa_query
    -- remove scores that include texting
    where regexp_contains(score, r'\D+') = false
    group by 1, 2
),

active_esc as (
    select
        date,
        project_manager,
        count(is_active) as active_escalations
    from int_data_tests.new_delivery_dashboard_escalation_test
    group by 1, 2
),

active_projects as (

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
                lower(cl.email) like '%+test%'
                or lower(cl.email) like '%@test.%'
                or
                lower(cu.email) like '%+test%'
                or lower(cu.email) like '%@test.%'
            )
        qualify
            row_number() over (partition by cl.order_id order by cl.created_at)
            = 1
    ),

    won_and_close as (
        select
            ue.order_id,
            ue.won_at as won_date,
            ue.project_manager,
            case
                when
                    ue.closed_at is not null and ue.completed_at is null
                    and ue.cancelled_at is not null
                    and ue.cancelled_at < ue.won_at
                    and so.deal_status_id != 9 then null
                else ue.closed_at
            end as close_date
        from int_data.order_ue_materialized as ue
        left join ergeon.store_order as so on so.id = ue.order_id
        left join test_leads as t on t.order_id = ue.order_id
        where
            ue.won_at is not null
            and
            t.order_id is null
    ),

    data_ as (
        select
            sub.*,
            day
        from
            (
                select
                    c.*,
                    generate_date_array(
                        cast(won_date as date), current_date(), interval 1 day
                    ) as date_array
                from won_and_close as c
            ) as sub
        cross join unnest(date_array) as day
    )

    select
        project_manager,
        date_trunc(day, month) as date,
        sum(
            case
                when day <= coalesce(close_date, current_date()) then 1 else 0
            end
        ) as backlog
    from data_
    group by 1, 2, day
    qualify
        row_number()
        over (
            partition by date_trunc(day, month), project_manager
            order by day desc
        )
        = 1
),

active_esc_perc as (
    select
        date,
        project_manager,
        ae.active_escalations
        / nullif(ap.backlog, 0) as active_escalations_percentage
    from active_esc as ae
    left join active_projects as ap using (date, project_manager)
),

esc_tat as (
    select
        eq.project_manager,
        date_trunc(eq.date_the_escalation_resolved, month) as date,
        avg(
            date_diff(
                eq.date_the_escalation_resolved,
                eq.date_the_order_escalated,
                day
            )
        ) as tat
    from int_data.escalation_query as eq
    where
        eq.date_the_escalation_deleted is null
        and eq.date_the_escalation_resolved is not null
        and (
            lower(eq.core_issues_string) not like '%scoping%'
            or lower(eq.current_status_bucket) not like '%scoping_process%'
        )
    group by 1, 2
),

final_data as (
    select
        date,
        project_manager,
        r.review_count,
        f.feedback_collection,
        qa.qa_score,
        f.delivery_margin_leakage,
        aep.active_escalations_percentage,
        et.tat
    from reviews as r
    full join feedback_and_ml as f using (date, project_manager)
    full join qa using (date, project_manager)
    full join active_esc_perc as aep using (date, project_manager)
    full join esc_tat as et using (date, project_manager)
    where project_manager is not null
-- order by 1 desc, 2
)

select
    fd.date,
    fd.project_manager,
    cast(round(fd.qa_score, 0) as int) as qa_score_points,
    case
        when hr.staff_id in (1636, 1698) then 'KAM'
        else hr.house
    end as house,
    case
        when fd.active_escalations_percentage < 0.15 then 2
        when fd.active_escalations_percentage between 0.15 and 0.25 then 1
        when fd.active_escalations_percentage >= 0.25 then 0
    end as active_escalations_percentage_points,
    case
        when fd.review_count <= 3 then 0
        when fd.review_count = 4 then 1
        when fd.review_count >= 5 then 2
    end as review_count_points,
    case
        when fd.tat < 30 then 2
        when fd.tat between 30 and 60 then 1
        when fd.tat > 60 then 0
    end as tat_points,
    case
        when fd.feedback_collection < 0.75 then 0
        when fd.feedback_collection between 0.75 and 0.80 then 1
        when fd.feedback_collection > 0.8 then 2
    end as feedback_collection_poins,
    case
        when fd.delivery_margin_leakage < 0.015 then 2
        when fd.delivery_margin_leakage between 0.015 and 0.03 then 1
        when fd.delivery_margin_leakage > 0.03 then 0
    end as delivery_margin_leakage_points
from final_data as fd
left join int_data.hr_dashboard as hr on hr.full_name = fd.project_manager
where
    lower(hr.title) not like '%house head%'
    and (lower(hr.title) like 'project manager%' or lower(hr.title) like '%junior project manager%')
    and hr.staff_id != 1636 -- Nestor Baca
    and hr.change_type != 'left'
