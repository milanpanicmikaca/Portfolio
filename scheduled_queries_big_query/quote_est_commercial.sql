with
linear_feet as
(
	select
		ql.quote_id as quote_id,
		nullif(sum(case when ql.label in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then ql.quantity else 0 end),0) as linear_feet
	from ergeon.quote_quoteline ql
	group by 1
)
select
	q.id,
	order_id,
	cast(o.pipedrive_deal_key as int) as pd_id,
	title,
	old_total_price as customer_price,
	total_cost,
	date_trunc(q.created_at,minute,'America/Los_Angeles') as created_at,
	cc.full_name as customer_name,
	cs.label as status,
	pm.code as market,
	-- case 
	-- 	when customer_pdf = '' then null
	-- 	else 'https://s3-us-west-2.amazonaws.com/api-ergeon-in/' || left(customer_pdf,strpos(customer_pdf,'#')-2) || '%20%23' || q.id || '%20for%20customer.pdf' 
	-- end as customer_pdf,
	case 
		when installer_pdf = '' then null 
		else 'https://s3-us-west-2.amazonaws.com/api-ergeon-in/' || left(installer_pdf,strpos(installer_pdf,'#')-2) || '%20%23' || q.id || '%20for%20installer.pdf' 
	end as installer_pdf,
	concat('https://admin.ergeon.in/quoting-tool/',order_id,'/quote/',q.id) as admin_link,
	concat('https://api.ergeon.in/public-admin/quote/quote/',q.id,'/change/') as new_admin_link,
	linear_feet,
	rank() over (partition by order_id order by q.created_at) as revision_no
from ergeon.quote_quote q
left join ergeon.store_order o on o.id = q.order_id
left join ergeon.core_house h on h.id = o.house_id
left join ergeon.geo_address ga on ga.id = h.address_id
left join ergeon.geo_county gc on gc.id = ga.county_id
left join ergeon.product_countymarket pcm on pcm.county_id = gc.id 
left join ergeon.product_market pm on pm.id = pcm.market_id
left join ergeon.customers_customer c on c.id = h.customer_id
left join ergeon.customers_contact cc on cc.id = c.contact_id
left join linear_feet lf on lf.quote_id = q.id
left join ergeon.core_statustype cs on cs.id = q.status_id
order by id desc