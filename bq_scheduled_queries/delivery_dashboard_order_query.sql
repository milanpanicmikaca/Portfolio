with timeseries_day as (
    select
        date_trunc(date_array, day) as date
    from unnest(generate_date_array('2018-04-16', current_date(), interval 1 day)) as date_array
    group by 1
),

day_and_pm as (
    select
        date,
        full_name,
        team_lead,
        staff_id
    from timeseries_day
    cross join int_data.delivery_dashboard_order_query_initial
    where full_name is not null
    qualify row_number() over (partition by date, full_name) = 1
),

pm_qa as (
    select
        cast(cast(time_stamp as timestamp) as date) as date,
        project_manager,
        sum(cast(score as bignumeric)) as sum_score,
        count(score) as count_score
    from int_data.delivery_pm_qa_query
    where not regexp_contains(score, r'[a-zA-Z\s]+')
    group by 1, 2
),

margin_leakage as (
    select
        ue.order_id,
        (
            last_approved_mktg_discount + last_approved_sales_discount + last_approved_delivery_discount + (
                last_approved_price - revenue
            ) + (contractor_pay - last_approved_cost + wwo_installer_leakage)
            + cost_of_sales + materials_pay + last_approved_pricing_discount) / nullif(revenue, 0) as margin_leak,
        last_approved_delivery_discount as delivery,
        contractor_pay - last_approved_cost + wwo_installer_leakage as installer,
        materials_pay as materials,
        last_approved_pricing_discount as pricing,
        revenue as revenue
    from int_data.order_ue_materialized ue
    where ue.is_warranty_order is false
        and ue.completed_at is not null
),

--added 11/10/2022
delivery_leakage as (
    select
        cc.order_id,
        so.total_project_price as total_revenue,
        so.total_project_cost as total_estimated_cost,
        sum(total_cost) as final_cost
    from ergeon.store_order so
    left join ergeon.contractor_contractororder cc on cc.order_id = so.id
    where status_id = 13
    group by 1, 2, 3
),

esc_tat_time as (
    select
        e.id,
        e.order_id,
        timestamp_diff(eh.created_at, e.reported_at, day) as tat_esc
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e on e.id = eh.object_id and eh.content_type_id = 491
    left join int_data.escalation_query se on se.escalation_id = e.id
    left join ergeon.core_statustype st on st.id = eh.status_id
    left join ergeon.core_statustype st2 on st2.id = e.status_id
    where (st.code = 'escalation_resolved' or st2.code = 'escalation_resolved')
        and (lower(se.core_issues_string) not like '%scoping%' or lower(se.current_status_bucket) not like '%scoping_process%')
    qualify row_number() over (partition by e.id order by reported_at) = 1
),

ranked_requests as (
    select
        cast(trim(regexp_extract(trim(admin_link), r'/(\d+)/')) as bignumeric) as order_id,
        added_at,
        request_stage,
        description,
        extra_details
    from googlesheets.delivery_requote_requests drr
    qualify row_number() over (partition by cast(trim(regexp_extract(trim(admin_link), r'/(\d+)/')) as bignumeric) order by drr.added_at) = 1
),

wwo_materials_cost as (
    select
        order_id,
        sum(case when type_id = 7 then amount else 0 end) as warranty_material_cost,
        sum(case when type_id in (6, 7) then amount when type_id = 14 then -amount else 0 end) as warranty_pay
    from ergeon.accounting_transaction
    --where type_id = 7 --warranty materials
    group by 1
),

pm_reviews as (
    select
        day,
        staff_id,
        project_manager,
        sum(reviews_count) as review_count,
        sum(bad_reviews_count) as bad_reviews_count,
        sum(good_reviews_count) as good_reviews_count,
        avg(avg_stars) as avg_stars
    from int_data.delivery_reviews_query
    group by 1, 2, 3
),

initial_revenue as (
    select
        ue.project_manager,
        ue.won_at as date,
        sum(first_approved_price) as first_approved_price
    from int_data.order_ue_materialized ue
    where won_at is not null
    group by 1, 2
),

cancelled_revenue as (
    select
        ue.project_manager,
        ue.cancelled_at as date,
        sum(first_approved_price) as first_approved_price
    from int_data.order_ue_materialized ue
    where won_at is not null and cancelled_at is not null
    group by 1, 2
),

final_data as (
    select
        dp.date as day,
        dp.full_name,
        dp.team_lead,
        dp.staff_id,
        od.* except(day, full_name, team_lead, staff_id),
        margin_leak,
        delivery,
        installer,
        pricing,
        materials,
        revenue,
        review_count,
        bad_reviews_count,
        good_reviews_count,
        sum_score,
        count_score,
        tat_esc,
        total_revenue,
        total_estimated_cost,
        final_cost,
        warranty_material_cost,
        warranty_pay,
        coalesce(i.first_approved_price, 0) as initial_revenue,
        coalesce(c.first_approved_price, 0) as cancelled_revenue,
        od.total_cost + coalesce(wmc.warranty_material_cost, 0) as total_warranty_cost,
        case
            when core_issue like '%Craftsmanship%' or core_issue like '%Installer Miss%' or core_issue like '%Material Quality%' then 1 else 0
        end as installer_esc,
        case when request_stage = 'post_installation' then 1 else 0 end as change_order_post_inst
    from day_and_pm dp
    left join int_data.delivery_dashboard_order_query_initial od on od.day = dp.date and od.full_name = dp.full_name
    left join margin_leakage ml on ml.order_id = od.id
    left join pm_reviews rq on rq.day = dp.date and rq.staff_id = dp.staff_id
    left join pm_qa pq on pq.date = dp.date and pq.project_manager = dp.full_name
    left join esc_tat_time et on et.order_id = od.id
    left join ranked_requests rr on cast(rr.order_id as int) = od.id
    left join delivery_leakage dl on dl.order_id = od.id
    left join wwo_materials_cost wmc on wmc.order_id = od.id
    left join initial_revenue i on i.project_manager = dp.full_name and i.date = dp.date
    left join cancelled_revenue c on c.project_manager = dp.full_name and c.date = dp.date
    --change 1062022 to take the last contractor
    qualify case when od.id is null then 1 else row_number() over (partition by od.id order by od.contractorapp_id desc) end = 1
)

select *,
       total_estimated_cost / nullif(total_revenue, 0) as estimated_margin,
       final_cost / nullif(total_revenue, 0) as final_margin,
       row_number() over (partition by day, full_name order by day) as unique_review
from final_data
order by 1 desc, 2
