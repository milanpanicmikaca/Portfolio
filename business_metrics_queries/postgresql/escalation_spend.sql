-- upload to BQ
select
        date_trunc('{period}',at."date")::date as date,
       coalesce(sum(amount),0) as DEL142
from accounting_transaction at
left join accounting_account aa on aa.id = at.account_id 
left join accounting_transactiontype at2 on at2.id = at.type_id
where at2."name" in('Warranty (materials)', 'Warranty (labor)', 'Warranty (customer discounts)')
group by 1
order by 1 desc
