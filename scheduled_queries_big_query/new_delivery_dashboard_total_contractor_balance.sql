with transactions as (
    select
        hc.id as contractor_id,
        sum(case when act.type_id in (1, 2) and order_id is null then act.amount else 0 end) as materials_purchased_returned,
        sum(case when act.type_id = 15 then act.amount else 0 end) as materials_payment,
        sum(case when act.type_id in (20, 21) then act.amount else 0 end) as installer_loan,
        sum(case when act.type_id in (24, 26) then act.amount else 0 end) as reseller_material
    from ergeon.accounting_transaction act
    left join ergeon.contractor_contractor hc on hc.id = act.contractor_id
    left join ergeon.contractor_contractorcontact cc on cc.id = hc.contact_id
    left join ergeon.core_user cu on cu.id = cc.user_id
    where
        ((act.type_id in (1, 2) and order_id is null) --Materials Purchased + Materials Returned
                          --Materials Payment + Installer loan + Installer Loan Payment, Reseller Materials Sold, Tax - Reseller Materials
                          or act.type_id in (15, 20, 21, 24, 26))
        and
        act.deleted_at is null
        and
        cu.full_name is not null
    group by 1
)

select
    cc.id as contractor_id,
    cu.full_name as installer,
    materials_purchased_returned,
    materials_payment,
    installer_loan,
    materials_purchased_returned + materials_payment + installer_loan + reseller_material as total_balance
from ergeon.contractor_contractor cc
left join ergeon.core_statustype cs on cs.id = cc.status_id
left join ergeon.contractor_contractorcontact cc2 on cc2.id = cc.contact_id
left join ergeon.core_user cu on cu.id = cc2.user_id
left join transactions t on t.contractor_id = cc.id
