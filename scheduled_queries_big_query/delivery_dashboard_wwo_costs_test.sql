with test_leads as (
    select
        cl.order_id,
        cl.id as lead_id
    from ergeon.core_lead cl
    left join ergeon.customers_contact co on co.id = cl.contact_id
    left join ergeon.core_user cu on cu.id = co.user_id
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

wwo as (
    select
        so.id as order_id,
        ue.completed_at,
        ue.is_completed,
        so.project_status_id,
        ue.project_manager,
        case
            when ue.pm_id in (1636, 1698) then 'KAM'
            else ue.pm_team
        end as house,
        ue.contractor,
        cu3.full_name as team_leader
    from int_data.wwo_ue_materialized ue
    left join ergeon.store_order so on so.id = ue.order_id
    left join test_leads tl on tl.order_id = ue.order_id
    left join ergeon.hrm_staff hs on hs.id = so.project_manager_id
    left join ergeon.hrm_stafflog hsl on hsl.id = hs.current_stafflog_id
    left join ergeon.hrm_team ht on ht.id = hsl.team_id
    left join ergeon.hrm_staff hs2 on hs2.id = ht.lead_id
    left join ergeon.core_user cu3 on cu3.id = hs2.user_id
    where tl.order_id is null
)

select
    date(timestamp(at2.date, 'America/Los_Angeles')) as transaction_date,
    wwo.order_id,
    wwo.completed_at,
    case
        when wwo.project_status_id not in (24, 31, 32) then 1
        else 0
    end as is_active,
    case
        when wwo.project_status_id = 24 then 1
        else 0
    end as is_on_hold,
    wwo.is_completed,
    wwo.project_manager,
    case
        when wwo.house is null or wwo.house in ('House of Mandalorians', 'House of Moonshots', 'Management House') then 'No House'
        else wwo.house
    end as house,
    case
        when wwo.house = 'KAM' then 'Nestor Baca'
        when wwo.house is null or wwo.house in ('House of Mandalorians', 'House of Moonshots', 'Management House') then 'No Team Leader'
        else wwo.team_leader
    end as team_leader,
    contractor,
    case
        when at2.type_id = 1 then at2.amount --materials purchased
        when at2.type_id = 5 then at2.amount --contractor_paid
        when at2.type_id = 7 then at2.amount --warranty (materials)
        when at2.type_id = 14 then -at2.amount -- warranty (customer_discount)
        when at2.type_id = 19 then -at2.amount -- customer_billed
        else 0
    end as transaction_amount
from wwo
left join ergeon.accounting_transaction at2 on wwo.order_id = at2.order_id
--where at2.type_id in (5,7,14,19)
order by 2 desc
