--upload to BQ
select 
    date,
    sum(ar) over (order by date) as FIN002 --cumulative sum ending by that period
from
    (select 
        date,
        sum(cust_billed) - sum(cust_paid) - sum(cust_discounted) - sum(warranty) as ar
    from
      (select 
            date_trunc(t.date, {period}) as date,
            t.order_id, 
            sum(abs(case when t.type_id = 19 then amount else 0 end)) as cust_billed, 
            sum(abs(case when t.type_id = 9 then amount else 0 end)) as cust_paid, 
            sum(abs(case when t.type_id = 10 then amount else 0 end)) as cust_discounted, 
            sum(abs(case when t.type_id = 14 then amount else 0 end)) as warranty,
      from ergeon.accounting_transaction t
      join ergeon.store_order s on s.id = t.order_id
      where 
        t.type_id in (19,9,10,14)
      and 
        t.deleted_at is null
      and 
        s.parent_order_id is null  --wwo excluded
      group by 1,2)
    group by 1)
  order by 1
