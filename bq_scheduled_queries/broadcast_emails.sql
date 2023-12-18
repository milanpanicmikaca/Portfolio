with customer_dimension as (
    select
        customer_id,
        market,
        old_region,
        region,
        channel1 as customer_channel,
        product,
        product_quoted,
        segment
    from int_data.order_ue_materialized
    qualify rank() over(partition by customer_id order by created_ts_at desc, order_id desc) = 1
)

select
    extract(date from b.created_at at time zone 'America/Los_Angeles') as created_at,
    b.name,
    e.name as status,
    cast(split(split(aggregated_data, ',') [offset(0)], ':') [offset(1)] as numeric) as failed_count,
    cast(split(split(aggregated_data, ',') [offset(1)], ':') [offset(1)] as numeric) as opened_count,
    cast(split(split(aggregated_data, ',') [offset(2)], ':') [offset(1)] as numeric) as clicked_count,
    cast(split(split(aggregated_data, ',') [offset(3)], ':') [offset(1)] as numeric) as delivered_count,
    cast(split(split(aggregated_data, ',') [offset(4)], ':') [offset(1)] as numeric) as scheduled_count,
    cast(split(split(aggregated_data, ',') [offset(5)], ':') [offset(1)] as numeric) as complained_count,
    cast(split(split(split(aggregated_data, ',') [offset(6)], ':') [offset(1)], '}') [offset(0)] as numeric) as unsubscribed_count,
    order_deal_statuses,
    cd.*
from ergeon.marketing_notifications_emailevent e
join ergeon.marketing_notifications_broadcastemail b on b.id = e.broadcast_email_id
left join ergeon.marketing_notifications_mailinglist ml on ml.id = b.mailing_list_id
join ergeon.customers_contact cco on cco.id = e.contact_id
join customer_dimension cd on cd.customer_id = cco.customer_id
where
    b.created_at > '2022-01-01'
