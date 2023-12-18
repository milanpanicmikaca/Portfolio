with city_order_details as (
select 
    cl.id,
    identifier, 
    case when left(identifier,5) like '+%' then substring(identifier,3,3)
	     when left(identifier,5) like '%[a-z0-9()]%' then substring(identifier,2,3)
         when left(identifier,5) like '%-%' then substring(identifier,1,3)
         when left(identifier,5) like '%^[0-9]%' then substring(identifier,1,3)
    else substring (identifier,2,3) end as city_code
from `bigquerydatabase-270315.ergeon.core_lead` cl
left join `bigquerydatabase-270315.ergeon.store_order` so on so.id = cl.order_id
left join `bigquerydatabase-270315.ergeon.core_house` ch on ch.id = so.house_id
left join `bigquerydatabase-270315.ergeon.geo_address` ga on ch.address_id = ga.id
left join `bigquerydatabase-270315.ergeon.geo_county`gc on gc.id = ga.county_id
left join `bigquerydatabase-270315.ergeon.product_countymarket` pcm on pcm.county_id = gc.id
left join `bigquerydatabase-270315.ergeon.product_market` pm on pm.id = pcm.market_id
left join `bigquerydatabase-270315.ergeon.customers_customer` cc on cc.id = ch.customer_id
left join `bigquerydatabase-270315.ergeon.customers_contact` cco on cco.customer_id = cc.id
left join `bigquerydatabase-270315.ergeon.customers_contactinfo` cci on cci.contact_id = cco.id
where pm.id is null and cc.id is not null
and identifier not like ('%null%') and identifier not like ('%None%')
and cci.type = 'phone' 
),
leads_with_unique_market as
(
select
    distinct ctd.id,
    identifier,
    -- ua.state,
    -- ua.city,
    -- gc.name,
    -- gco.name,
    pm.code,
    city_code,
    count(*) over (partition by ctd.id order by code) as count
from city_order_details as ctd
left join `bigquerydatabase-270315.ext_marketing.us_area_codes` ua on ua.area_code = cast(ctd.city_code as int)
left join `bigquerydatabase-270315.ergeon.geo_city` gc on gc.name = ua.city
left join `bigquerydatabase-270315.ergeon.geo_county` gco on gc.county_id = gco.id
left join `bigquerydatabase-270315.ergeon.product_countymarket` pcm on pcm.county_id = gco.id
left join `bigquerydatabase-270315.ergeon.product_market` pm on pm.id = pcm.market_id
where pm.code is not null
order by ctd.id
)
select
*
from leads_with_unique_market
where count = 1