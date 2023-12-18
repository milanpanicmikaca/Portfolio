-- upload to BQ
with
timeseries as 
(
select 
date_trunc('day', dd)::date as date,
rank() over (partition by date_trunc('{period}', dd)::date order by dd desc) as period_rank
from generate_series ('2018-04-16'::timestamp, current_date, '1 day'::interval) dd
),
msa_activation as
(
select
        gc.msa_id,
        date_trunc('day',min(qq.sent_to_customer_at))::date as activated_at
from store_order so
left join core_house ch on ch.id = so.house_id
left join geo_address ga on ch.address_id = ga.id
left join geo_county gc on gc.id = ga.county_id
left join quote_quote qq on qq.order_id = so.id
where so.created_at > '2018-04-15'
and sent_to_customer_at is not null and is_cancellation = False
and msa_id is not null
group by 1
)
select
        date_trunc('{period}', date)::date as date,
        count(*) as MAR346
from msa_activation ma
left join timeseries t on ma.activated_at <= t.date and current_date >= t.date
where 
        ma.activated_at is not null and
        period_rank = 1 -- this is a test
        -- this is a second test
group by 1
order by 1
