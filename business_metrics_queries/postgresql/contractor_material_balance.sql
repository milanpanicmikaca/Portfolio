with
calc_series as
(
 select
        generate_series('2018-06-10', current_date, '1 day')::date as day
),
materials_balance
as
(
select
        at2.date as day,
        order_id,
        amount
from accounting_transaction at2
left join accounting_account aa on aa.id = at2.account_id
left join contractor_contractor hc on at2.contractor_id = hc.id
where at2.contractor_id is not null and at2.deleted_at is null
        and at2.type_id in (15,24,26) -- Materials Payment, Reseller Materials Sold, Tax Reseller Materials
),
calc_final as
(
select
        distinct cs.day,
        coalesce(sum(mb.amount) over (order by cs.day),0) as material_balance
        from calc_series cs
        left join materials_balance mb on mb.day = cs.day
        order by 1 desc
),
rank_1 as
(
select
        date_trunc('{period}', cf.day)::date as date,
        material_balance,
        rank() over (partition by date_trunc('{period}', cf.day)::date order by cf.day) as ranked_days
from calc_final cf
order by 1 desc
),
rank_2 as
(
select
        date,
        material_balance,
        rank() over(partition by date order by ranked_days desc)
from rank_1
),
contractor_material_balance
as
(
select
        date as date,
        material_balance as MAT012
from rank_2
where rank = 1
order by date desc
),
non_bulk_purchases
as
(
select 
        date_trunc('{period}',at2.date)::date as date, 
        sum(amount) as MAT033
from 
        accounting_transaction at2 
where at2.type_id in (1,2) -- Materials Purchased, Materials Returned
  and at2.order_id is not null
  and at2.created_at is not null
  and at2.deleted_at is null
group by 1
)
select 
        mb.date,
        MAT012,
        coalesce(MAT033,0) as MAT033
from 
        contractor_material_balance mb 
        left join non_bulk_purchases bp on bp.date = mb.date
order by 1 desc
