select
    date,
    null as order_id,
    case
        when handyman_name = 'Carlos Quevedo' then 'Carlos A Quevedo'
        when handyman_name = 'Alvaro Meza' then 'Alvaro Meza Rivera'
        else handyman_name
    end as handyman,
    'No House' as house,
    paymount_amount as amount
from googlesheets.handyman_invoices
union all
select
    date(hc.timestamp) as date,
    hc.order_id,
    hc.contractor as handyman,
    ue.pm_team as house,
    hc.amount
from int_data.handyman_costs hc
left join int_data.order_ue_materialized ue on ue.order_id = hc.order_id
