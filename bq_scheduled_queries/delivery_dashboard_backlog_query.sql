with date_array as (
    select
        cast(date as timestamp) as date,
        timestamp_add(cast(date as timestamp), interval 86399 second) as end_of_day_ts
    from warehouse.generate_date_series
    where date > '2018-04-15' and date <= current_date
),

test_leads as (
    select
        cl.order_id,
        cl.id as lead_id
    from ergeon.core_lead as cl
    left join ergeon.customers_contact as co on co.id = cl.contact_id
    left join ergeon.core_user as cu on cu.id = co.user_id
    where
        cl.created_at >= '2018-04-16'
        and
        (cl.order_id in
            (50815, 56487, 59225, 59348, 59404, 59666, 59670, 59743, 59753, 59789, 59805, 59813,
                59878, 59908, 59922, 60273, 60283, 60401, 60547, 60589, 60590, 60595, 60596, 60597, 60612)
            or
            lower(cl.full_name) like '%test%' or lower(cl.full_name) like '%fake%'
            or
            lower(co.full_name) like '%test%' or lower(co.full_name) like '%fake%'
            or
            lower(cu.full_name) like '%test%' or lower(cu.full_name) like '%fake%'
            or
            lower(cl.email) like '%+test%' or lower(cl.email) like '%@test.%'
            or
            lower(cu.email) like '%+test%' or lower(cu.email) like '%@test.%')
    qualify row_number() over (partition by cl.order_id order by cl.created_at) = 1
),

won_and_close as (
    select
        ue.order_id,
        ue.won_at as won_date,
        ue.old_region,
        ue.market,
        ue.segment,
        ue.project_manager,
        so.project_status_id,
        ue.pm_id,
        ue.pm_team,
        ue.has_escalation,
        case
            when ue.closed_at is not null and ue.completed_at is null and ue.cancelled_at is not null and ue.cancelled_at < ue.won_at then null
            else ue.closed_at
        end as close_date
    from int_data.order_ue_materialized as ue
    left join ergeon.store_order as so on so.id = ue.order_id
    left join test_leads as t on t.order_id = ue.order_id
    where ue.won_at is not null
        and t.lead_id is null
)

select
    o.order_id,
    o.old_region,
    o.market,
    o.segment,
    o.project_status_id,
    o.has_escalation,
    date(d.date) as date_backlog,
    coalesce(o.project_manager, 'No Project Manager') as project_manager,
    case
        when o.pm_id in (1636, 1698) then 'KAM'
        when o.pm_id is null or o.pm_team is null then 'No House'
        else o.pm_team
    end as house
from date_array as d
left join
    won_and_close as o on
        cast(d.date as date) >= o.won_date and cast(d.date as date) <= coalesce(o.close_date, date_add(current_date, interval 1 day))
qualify rank() over (order by d.date desc) = 1
