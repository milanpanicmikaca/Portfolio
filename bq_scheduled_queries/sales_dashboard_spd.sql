with type as (
    select 'fence' as type union all 
    select 'driveway' as type union all 
    select 'commercial' as type
), mock_data as (
select 
    hd.full_name as cs,
    hd.email, 
    ts.date,
    t.type,
from int_data.hr_dashboard hd
cross join warehouse.generate_date_series ts 
cross join type t
where ladder_name = 'Sales'
and ts.date between '2018-04-15' and current_date()
), onsites as (
    select 
        sa.date,
        cu.full_name as cs,
        case 
            when lower(type) like '%commercial%' or lower(product) like ('%chain%') then 'commercial'
            when lower(product) like '%fence%' and lower(product) not like ('%chain%') then 'fence'
            when lower(product) like '%driveway%' then 'driveway' end as type,
        sum(case when ssat.code = 'physical_onsite' then 1 end) as physical_onsite,
        sum(case when ssat.code = 'remote_onsite' then 1 end) as remote_onsite,   
        sum(case when ssat.code = 'quote_review' then 1 end) as quote_review,
    from ergeon.sales_schedule_appointmentslot sa
        left join ergeon.store_order so on sa.order_id = SO.id
        left join ergeon.sales_schedule_assignmenttype sat on sat.id = so.assignment_type_id
        left join ergeon.sales_schedule_appointmenttype ssat on ssat.id = sa.appointment_type_id   
        left join ergeon.hrm_staff hs on hs.id = so.sales_rep_id   
        left join ergeon.core_user cu on cu.id = hs.user_id
        left join int_data.order_ue_materialized ue on ue.order_id = sa.order_id
    where 
        sa.cancelled_by_id is null
    group by 1, 2, 3
), closes_rev as (
    select distinct
        a.won_at as date,
        a.sales_rep as cs,
        case
            when lower(ue.type) like '%commercial%' or lower(ue.product) like ('%chain%') then 'commercial'
            when lower(ue.product) like '%fence%' and lower(ue.product) not like ('%chain%') then 'fence'
            when lower(ue.product) like '%driveway%' then 'driveway' end as type,
        sum(a.initial_revenue) as revenue,
        count(order_id) as closes,
    from int_data.sales_dashboard_arts a
    left join int_data.order_ue_materialized ue using(order_id)
    where a.sales_rep is not null
    and a.house is not null
    group by 1, 2, 3
), assigned as (
  select 
    sc.assigned_to_id as order_id,
    sc.assigned_at,
    sh.status_id,
    st.code
  from ergeon.core_statushistory sh
    left join ergeon.sales_schedule_csqueue sc on sc.id = sh.object_id
    left join ergeon.core_statustype st on st.id = sh.status_id
  and st.code = 'assigned'
  order by 1 desc
), claimed as (
select 
    date(a.assigned_at, "America/Los_Angeles") as date,
    cu.full_name as cs,
    case 
        when lower(type) like '%commercial%' or lower(product) like ('%chain%') then 'commercial'
        when lower(product) like '%fence%' and lower(product) not like ('%chain%') then 'fence'
        when lower(product) like '%driveway%' then 'driveway' end as type,
    count(ue.order_id) as orders
from int_data.order_ue_materialized ue 
    left join ergeon.store_order so on so.id = ue.order_id
    left join ergeon.hrm_staff hs on hs.id = so.sales_rep_id   
    left join ergeon.core_user cu on cu.id = hs.user_id
    left join assigned a on a.order_id = ue.order_id
group by 1, 2, 3
), csat as (
    select
        date(cast(fb.created_at as timestamp), "America/Los_Angeles") as date,
        cu.full_name as cs,
        case 
            when lower(type) like '%commercial%' or lower(product) like ('%chain%') then 'commercial'
            when lower(product) like '%fence%' and lower(product) not like ('%chain%') then 'fence'
            when lower(product) like '%driveway%' then 'driveway' end as type,
        avg(fb.quoting) as csat,
    from ergeon.feedback_orderfeedback fb
        left join int_data.order_ue_materialized ue on ue.order_id = fb.order_id
        left join ergeon.store_order so on so.id = fb.order_id
        left join ergeon.hrm_staff hs on hs.id = so.sales_rep_id   
        left join ergeon.core_user cu on cu.id = hs.user_id
    group by 1, 2, 3
), escalations as (
    select 
        date(e.reported_at,"America/Los_Angeles") as date,
        cu.full_name as cs,
        case 
            when lower(ue.type) like '%commercial%' or lower(product) like ('%chain%') then 'commercial'
            when lower(product) like '%fence%' and lower(product) not like ('%chain%') then 'fence'
            when lower(product) like '%driveway%' then 'driveway' end as type,
        sum(case when lower(ta.name) like "%sales team%" then 1 end) as escalations,
    from ergeon.store_escalation e
        left join ergeon.store_order so on so.id = e.order_id
        left join ergeon.core_statustype ST on ST.id = e.status_id
        left join ergeon.store_escalation_primary_teams_attributed pa on pa.escalation_id = e.id
        left join ergeon.store_escalationteamattributed ta on ta.id = pa.escalationteamattributed_id AND ta.id = 2
        left join ergeon.hrm_staff hs on hs.id = so.sales_rep_id
        left join ergeon.core_user cu on cu.id = hs.user_id
        left join int_data.order_ue_materialized ue on ue.order_id = so.id
    group by 1, 2, 3
), final as (
select 
    *
from mock_data md 
left join claimed c using (date, cs, type)
left join closes_rev cr using(date, cs, type)
left join onsites os using(date, cs, type)
left join escalations e using(date, cs, type)
left join csat ct using(date, cs, type)
where coalesce (orders, revenue, closes, physical_onsite, remote_onsite, quote_review, escalations, csat) is not null
)
select 
    f.*,
    edb.house,
from final f
left join int_data.employeedb edb on edb.email = f.email