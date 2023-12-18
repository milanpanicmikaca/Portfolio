with timeseries as 
(
select 
    date_array as day,
    date_trunc(date_array, {period}) as period, 
    rank() over (partition by date_trunc(date_array,{period}) order by date_array asc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
),
handyman_costs_part_one as --the handyman costs from the initial spreadsheet of delivery team
(
	select
	  date_trunc(date,{period}) as date,
	  sum(paymount_amount) as amount
	from googlesheets.handyman_invoices
	group by 1
),
handyman_costs_part_two as --the handyman costs from the new process kept inside Overhead Costs Spreadsheet
(
	select 
	  date(date_trunc(timestamp,{period})) as date,
	  sum(amount) as amount
	from int_data.handyman_costs
	group by 1
),
wwo_spent as
(
	select
	  date(date_trunc(at2.date,{period})) as date,
	  coalesce(sum(case
		 		          when at2.type_id = 1 then at2.amount --materials purchased
				          when at2.type_id = 5 then at2.amount --contractor_paid
				          when at2.type_id = 7 then at2.amount --warranty (materials)
				          when at2.type_id = 14 then -at2.amount -- warranty (customer_discount)
				          when at2.type_id = 19 then -at2.amount -- customer_billed
				          else 0 end),0) as amount
	from ergeon.accounting_transaction at2
	left join ergeon.store_order so on so.id = at2.order_id
	where at2.type_id in (1,5,7,14,19)
		and so.parent_order_id is not null
		and at2.deleted_at is null
	group by 1
)
select
	t.period as date,
	coalesce(hc1.amount,0) + coalesce(hc2.amount,0) + coalesce(wwo.amount,0) as DEL142
from timeseries t
left join handyman_costs_part_one hc1 on hc1.date = t.period
left join handyman_costs_part_two hc2 on hc2.date = t.period
left join wwo_spent wwo on wwo.date = t.period
where t.period_rank = 1
order by 1 desc
