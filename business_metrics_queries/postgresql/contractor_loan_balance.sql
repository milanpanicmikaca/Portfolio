with
        calc_series
as
(
select
        generate_series (('2018-04-16'), (current_date), interval '1 day')::date as day
),
sum_amount
as
(
select 
day, 
sum(case when name in ('Installer Loan', 'Installer Loan Payment') then amount else 0 end) as amount
--name, 
--cu.full_name 
from calc_series cs
left join accounting_transaction at on at.date = cs.day and at.type_id in (20,21)
left join accounting_transactiontype at2 on at.type_id = at2.id
group by 1
order by 1 desc
),
calc_data
as
(
select 
day,
date_trunc('{period}',day)::date as date,
rank() over (partition by date_trunc('{period}',day)::date order by day desc) as rank,
sum(amount) over (order by day) as MAT015
from sum_amount
)
select
        date,
        MAT015
from calc_data
where rank = 1
