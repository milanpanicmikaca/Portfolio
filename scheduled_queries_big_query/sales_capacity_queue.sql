select 
    tat.name as cs_queue_deal_type,
    count(so.id) as leads,
    min(date(cast(so.created_at as timestamp), "America/Los_Angeles")) as oldest
from ergeon.store_order so
    left join ergeon.tasks_assignmenttype tat on tat.id = so.assignment_type_id
    left join ergeon.core_house ch on ch.id = so.house_id
    left join ergeon.customers_customer cc on cc.id = ch.customer_id
    left join ergeon.customers_contact co on co.id = cc.contact_id
    left join ergeon.core_statustype st on st.id = so.deal_status_id
where
    so.deal_status_id = 4 --New Lead
    and so.sales_rep_id is null -- Not Assigned
    and cc.is_commercial is false
    and tat.name is not null
    and lower(co.full_name) not like '%test%'
group by 1