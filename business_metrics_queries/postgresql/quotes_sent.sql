-- upload to BQ
with
valid_orders as
(
    select
        o.id as valid_order_id
    from core_lead l
    left join store_order o on o.id = l.order_id
    where
        (phone_number is not null or email is not null)
        and full_name not ilike '%test%' and coalesce(email,'') not ilike '%test%'
        and full_name not ilike '%fake%' and coalesce(email,'') not ilike '%fake%'
        and full_name not ilike '%duplicate%'
        and l.created_at >= '2018-05-01'
    group by 1
    order by 1 desc
),
calc_quotes as
(
    select
        quote_quote.id,
        sent_to_customer_at at time zone 'America/Los_Angeles' as sent_period,
        valid_order_id
    from quote_quote
    left join valid_orders vd on vd.valid_order_id = quote_quote.order_id
)
select
    date_trunc( '{period}' , sent_period )::date as date,
    count(distinct valid_order_id) as SAL132, --first_quotes_sent
    count(valid_order_id) as SAL133 --total_quotes_sent
from calc_quotes
where
    valid_order_id is not null
    and sent_period is not null
    and date_trunc('{period}', sent_period)::date >= '2018-01-01'
group by 1
order by 1 desc
limit 4
