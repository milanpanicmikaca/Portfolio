-- upload to BQ
with
timeseries as 
(
select 
date_trunc('day', dd)::date as date,
rank() over (partition by date_trunc('{period}', dd)::date order by dd desc) as period_rank
from generate_series ('2018-04-16'::timestamp, current_date, '1 day'::interval) dd
),
zipcode_activation as
(
select
        gz.id,
        date_trunc('day',min(qq.sent_to_customer_at))::date as activated_at
from store_order so
left join core_house ch on ch.id = so.house_id
left join geo_address ga on ch.address_id = ga.id
left join geo_zipcode gz on gz.id = ga.zip_code_id
left join quote_quote qq on qq.order_id = so.id
group by 1
)
select
        date_trunc('{period}', date)::date as date,
        count(*) as MAR114,
        sum(gz.households) as MAR115
from zipcode_activation za
left join timeseries t on za.activated_at <= t.date and current_date >= t.date
left join geo_zipcode gz on gz.id = za.id
where 
        za.activated_at is not null and 
        period_rank = 1
group by 1
order by 1
