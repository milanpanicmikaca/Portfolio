with grouped_contractor_signoffs as (
    select order_id, sum(amount) as so_amount
    from
        ergeon.accounting_transaction x left join
        ergeon.accounting_transactiontype type on type.id = x.type_id
    where type.name = 'Contractor Signoff'
    group by 1
)

select
    x.id,
    x.date,
    x.created_at,
    x.amount,
    x.order_id,
    x.contractor_order_id,
    p.name as pay_method,
    type.name as type,
    account.name as account,
    vendor.name as vendor,
    contractor.full_name as contractor,
    ue.completed_at,
    ue.cancelled_at,
    ps.code as status,
    so.so_amount as signed_of_amount,
    'https://api.ergeon.in/public-admin/accounting/transaction/' || x.id || '/change/' as URL,
    case when o.parent_order_id is not null then "WWO" else "No_WWO" end as warranty_orders,
    product_quoted,
    ue.segment,
    cu.full_name as created_by,
    if(x.deleted_at is null, 'non deleted', 'deleted') as deleted,
    extract(date from x.deleted_at at time zone 'America/Los_Angeles') as deleted_at
from
    ergeon.accounting_transaction x left join
    ergeon.accounting_transactiontype type on type.id = x.type_id left join
    ergeon.accounting_account account on account.id = x.account_id left join
    ergeon.accounting_vendor vendor on vendor.id = x.vendor_id left join
    ergeon.accounting_paymethod p on p.id = x.paymethod_id left join
    ergeon.contractor_contractor c on c.id = x.contractor_id left join
    ergeon.contractor_contractorcontact c2 on c2.id = c.contact_id left join
    ergeon.core_user contractor on contractor.id = c2.user_id left join
    ergeon.store_order o on o.id = x.order_id left join
    ergeon.quote_quote q on q.id = o.approved_quote_id left join
    grouped_contractor_signoffs so on so.order_id = x.order_id left join
    ergeon.core_statustype ps on ps.id = o.project_status_id left join
    int_data.order_calculated_fields ue on ue.order_id = x.order_id left join
    ergeon.core_user cu on cu.id = x.created_by_id
where
    x.created_at is not null
