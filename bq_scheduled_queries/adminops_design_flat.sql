with
rates as (
    (select
        'Alejandro Tulissi' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Amer Skrobo' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Antonio Ibarra' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Ariana Chavez' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Candy Adames' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Carolina Martinez' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Gabriela Torres' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Milena Bakalova' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Gerardo Gomez' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Hammad Saeed' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Marija Krstanovic' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Daniela Rivera' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Urfan Khan' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Hakeem Radix' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Jimmie Castillo' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Jorge Garcia' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Jovan Krstanovic' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Oscar Ramirez' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Rodolfo Garcia' as full_name,
        6 as hourly_rate)
    union all
    (select
        'Sandra Maksimovic' as full_name,
        6 as hourly_rate)
),

time_in_progress as (
    select
        c.key,
        sum(
            datetime_diff(
                cast(c.created as datetime), cast(coalesce(c.next_timestamp, i.created) as datetime), minute
            )
        ) as time_in_progress_minutes
    from
        (
            select
                c.*,
                i.key,
                rank() over (partition by i.key order by c.created) as rank,
                lag(c.created) over (partition by i.key order by c.created) as next_timestamp
            from jira.changelog as c
            left join jira.issue as i on i.id = c.issue_id
            where
                i.key like 'SBTM%'
                and c.field_id = 'status'
        ) as c
    left join jira.issue as i on i.id = c.issue_id
    where
        c.from_string = 'task in progress'
    group by 1
),

sbtm_data as (
    select
        i.key,
        i.created as date,
        i.status,
        'new request' as type,
        u.name as assignee,
        u.email as assignee_email,
        i.labels,
        i.task_guru,
        i.priority,
        0 as tat_actual,
        0 as sla,
        0 as time_in_progress_minutes,
        0 as cost,
        i.components,
        i.admin_order_number as order_id
    from jira.issue as i
    left join jira.user as u on u.id = i.assignee_id
    where
        i.key like 'SBTM%'
    union all
    select
        i.key,
        date_trunc(cast(i.status_category_change_date as datetime), day) as date,
        i.status,
        'completed' as type,
        u.name as assignee,
        u.email as assignee_email,
        i.labels,
        i.task_guru,
        i.priority,
        (datetime_diff(cast(i.status_category_change_date as datetime), cast(i.created as datetime), minute) / 60) as tat_actual,
        case
            when
                (
                    datetime_diff(cast(i.created as datetime), cast(i.status_category_change_date as datetime), minute) / 60
                ) <= cast(i.turnaround_time_in_hours as int64) then 1
            else 0
        end as sla,
        coalesce(tip.time_in_progress_minutes, 0) as time_in_progress_minutes,
        (
            coalesce(tip.time_in_progress_minutes, 0) / 60
        ) * coalesce(r.hourly_rate, 0) as cost,
        i.components,
        i.admin_order_number as order_id
    from jira.issue as i
    left join time_in_progress as tip on tip.key = i.key
    left join jira.user as u on u.id = i.assignee_id
    left join rates as r on r.full_name = u.name
    where
        i.key like 'SBTM%'
        and i.status = 'COMPLETED TASKS'
),

final as (
    select
        d.key,
        d.status,
        d.type,
        d.assignee,
        d.labels,
        d.task_guru,
        d.priority,
        d.tat_actual,
        d.sla,
        d.time_in_progress_minutes,
        d.cost,
        d.components,
        d.order_id,
        s.full_name,
        s.staff_id,
        s.email,
        s.country,
        s.staff_image,
        date_trunc(date(cast(d.date as timestamp), 'America/Los_Angeles'), day) as date,
        case
            when d.labels in ('sb_delivery_bp', 'sb_delivery') then 'Delivery'
            when d.labels in ('sb_growth_bp', 'sb_growth') then 'Growth'
            when d.labels in ('sb_sales_bp', 'sb_sales') then 'Sales'
            when
                d.components in (
                    '007_Quickbooks Billing Log',
                    '010_Material Logging',
                    '016_Quickbooks Bank Transfer Payments',
                    '031_Home Depot Materials Recon',
                    '036_Waiver Approval',
                    '048_Contractor_Payments_Notifications',
                    '076_Check Deposits',
                    '082_CC receipts',
                    '099_Customer Text Reminders',
                    '110_Material Return Logging',
                    '131_Contractors Weekly Balance',
                    '132_Weekly Text Message Reminder to Installers',
                    '141_ Public Company Tracking Update',
                    '153_Bank Feed Daily'
                ) then 'Finance'
            when d.labels in ('sb_eng-data-prod') then 'Engineering'
            when d.labels in ('sb_hr_bp', 'sb_hr') then 'HR'
            else 'Other'
        end as department,
        case when cs.Team = 'AdminOPS' then cs.Points
        end as points_AdminOps,
        case when cs.Team = 'Design' then cs.Points
        end as points_Design,
        case
            when
                d.labels in (
                    'sb_hr',
                    'sb_sb',
                    'sb_sales',
                    'sb_eng-data-prod',
                    'sb_growth',
                    'sbtm_finance',
                    'sb_delivery',
                    'sb_operations',
                    'sb_estimation',
                    'sb_category'
                ) then 'adminOPS'
            when
                d.labels in (
                    'sb_growth_bp',
                    'sb_delivery_bp',
                    'sb_sales_bp',
                    'sb_blueprints_f8re',
                    'sb_estimation_bp',
                    'sb_est_research',
                    'sb_drawingdata_bp',
                    'sb_hr_design',
                    'sb_smh_bp',
                    'sb_estimation_pd',
                    'sb_eng-data-prod_bp',
                    'sb_design'
                ) then 'Design'
            --this part of code needs to be used after Jimmie fixes labels in Jira. 
            --For now the best approach is through Components (when points are not null)
            else 'other' end as team_by_labels
    from sbtm_data as d
    left join ext_quote.staff_house as s on s.email = d.assignee_email
    left join int_data.adminops_components_spreadsheet as cs on d.components = cs.Components
    where
        d.assignee != 'Ema Pijevcevic'
        and d.status not in ('Canceled')
        and d.components != '030_Feedback Transcript'
        and d.components is not null
)

select
    f.*,
    case
        when points_AdminOps is not null then 'AdminOPS'
        when points_Design is not null then 'Design'
        else 'other' end as team_by_components
from final as f
