-- upload to BQ
create temp function GetAmountOfPeriod(x string) 
  returns int64
  as (case when x = 'day' then 365
           when x = 'week(monday)' then 52
           when x = 'month' then 12
           when x = 'quarter' then 4
           when x = 'year' then 1 end);
with ar_calc as (
    select ar_period,
           sum(ar) over (order by ar_period) as ar --cumulative sum ending by that period
    from
    (select ar_period,
           sum(cust_billed) - sum(cust_paid) - sum(cust_discounted) - sum(warranty) as ar
     from
     (select date_trunc(t.date, {period}) as ar_period,
             t.order_id, 
             sum(case when t.type_id = 19 then amount else 0 end) as cust_billed, 
             sum(case when t.type_id = 9 then amount else 0 end) as cust_paid, 
             sum(abs(case when t.type_id = 10 then amount else 0 end)) as cust_discounted, 
             sum(abs(case when t.type_id = 14 then amount else 0 end)) as warranty,
      from ergeon.accounting_transaction t
      join ergeon.store_order s on s.id = t.order_id
      where t.type_id in (19,9,10,14)
      and t.deleted_at is null
      and s.parent_order_id is null  --wwo excluded
      group by 1,2)
     group by 1)
    order by 1
),
rev_calc as (
    select date_trunc(completed_at, {period}) as rev_period,
           sum(revenue) as tot_rev
    from int_data.order_ue_materialized 
    where completed_at is not null
    and is_warranty_order = false
    group by 1  
)
select a.ar_period as date,
       case when a.ar_period = date_trunc(current_date(),{period}) 
             then round((a.ar*(date_diff(current_date(),date_trunc(current_date(),{period}),day)+1))/nullif(r.tot_rev,0),2) 
             else round((a.ar*(365/GetAmountOfPeriod('{period}')))/nullif(r.tot_rev,0),2) end as FIN001
from ar_calc a
join rev_calc r on r.rev_period = a.ar_period
