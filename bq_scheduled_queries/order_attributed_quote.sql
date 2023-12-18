with first_approved_quote as (
    --find quote id and won date of the first approved quote
    select
        order_id,
        id as first_approved_quote_id,
        approved_at as won_at
    from ergeon.quote_quote
    where approved_at is not null
      and sent_to_customer_at > '2018-04-15'
      and is_cancellation = False
    qualify rank() over(partition by order_id order by approved_at, id) = 1
),

ranking_quotes as (
    --ranking approved/non approved quotes by quote approved/sent
    select
        qq.order_id,
        id,
        won_at,
        if(
            approved_at is null, Null, rank() over(partition by qq.order_id order by coalesce(approved_at, '2000-01-01') desc, id desc)
        ) as approved_rank,
        rank() over(partition by qq.order_id order by sent_to_customer_at desc, id desc) as non_approved_rank
    from ergeon.quote_quote as qq 
    left join first_approved_quote as faq on faq.order_id = qq.order_id
    where sent_to_customer_at is not null
      and created_at > '2018-04-15'
      and is_cancellation = False
),

last_quote_approved_or_sent as (
    --find quote id of the last approved (if approved) or sent to customer quote
    select
        order_id,
        id as quote_id
    from ranking_quotes
    where (won_at is null and non_approved_rank = 1) --last sent quote
      or (won_at is not null and approved_rank = 1) --last approved quote
),

first_quote_sent as (
    select
        order_id,
        id as quote_id,
        sent_to_customer_at as first_quoted_at
    from ergeon.quote_quote
    where sent_to_customer_at is not null
        and created_at > '2018-04-15'
        and is_cancellation = False
    qualify rank() over(partition by order_id order by sent_to_customer_at, id) = 1
),

last_quote_sent as (
    select
        order_id,
        id as quote_id
    from ergeon.quote_quote
    where sent_to_customer_at is not null
        and created_at > '2018-04-15'
        and is_cancellation = False
    qualify rank() over(partition by order_id order by sent_to_customer_at desc, id desc) = 1
),

last_scope_quote_sent as (
    --last quote id with scope = T
    select
        order_id,
        id as quote_id
    from ergeon.quote_quote
    where is_scope_change = True
        and sent_to_customer_at is not null
        and approved_at is null
        and is_cancellation = False
        and created_at > '2018-04-15'
    qualify rank() over(partition by order_id order by sent_to_customer_at desc, id desc) = 1
),

quote_requested_time as (
    --find quote requested by joining all historical draft quotes in order to find the first draft
    select
        fqs.order_id,
        datetime(
            coalesce(draft3.preparation_requested_at, draft2.preparation_requested_at, draft.preparation_requested_at), 'America/Los_Angeles'
        ) as quote_requested_ts_at
    from first_quote_sent as fqs
    left join ergeon.quote_quote as fq on fqs.quote_id = fq.id
    left join ergeon.quote_quote as draft on fq.copied_from_quote_id = draft.id
    left join ergeon.quote_quote as draft2 on draft.copied_from_quote_id = draft2.id
    left join ergeon.quote_quote as draft3 on draft2.copied_from_quote_id = draft3.id
),

quoteline_catalog as (
    --find the catalog type for each quoteline
    select
        ql.id,
        --first look at the quote style, then catalog_id, then catalog_type_id
        ql.description,
        coalesce(sct.type_id, ct.type_id, ql.catalog_type_id) as type_id,
        coalesce(sct.name, ct.name) as name,
        coalesce(ql.catalog_id, qs.catalog_id) as catalog_id
    from ergeon.quote_quoteline as ql
    left join ergeon.quote_quotestyle as qs on qs.id = ql.quote_style_id
    left join ergeon.product_catalog as sct on sct.id = qs.catalog_id
    left join ergeon.product_catalog as ct on ct.id = ql.catalog_id
    left join ergeon.quote_quote as qq on ql.quote_id = qq.id
    where is_cancellation = False
),

discounts_per_quote_sent as (
    --extra discounts/fees per quote (Discounts are used to calculate all stages of margin leakage table on Ad-Hoc)
    select
        ql.quote_id,
        sum(case when pct.item like 'sales%' then abs(ql.price) else 0 end) as sales_discount,
        sum(case when pct.item like 'mktg%' then abs(ql.price) else 0 end) as mktg_discount,
        sum(case when pct.item like 'delivery%' then abs(ql.price) else 0 end) as delivery_discount,
        sum(case when pct.item like 'pricing%' then abs(ql.price) else 0 end) as pricing_discount,
        sum(case when pct.id = 121 then abs(ql.price) else 0 end) as supply_discount,
        sum(case when pct.id = 81 then abs(ql.cost) else 0 end) as installer_pay,
        sum(case when (qlt.catalog_id in (282, 283, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 494, 505)
                                          or (qlt.catalog_id = 504 and lower(qlt.description) like 'small project overhead')
            ) then ql.price else 0 end) as small_project_overhead,
        sum(case when lower(name) like '%additional man hours%' then ql.price else 0 end) as additional_mh_labor --additional man hour labor
    from ergeon.quote_quoteline as ql
    inner join quoteline_catalog as qlt on qlt.id = ql.id
    left join ergeon.product_catalogtype as pct on pct.id = qlt.type_id
    where
        (pct.category in ('sales_discount') and lower(ql.description) not like ('%sales%surcharge%'))
        or pct.category in ('marketing_discount')
        or pct.item like '%delivery%' or pct.item like '%pricing%' or pct.id = 81 or pct.id = 121
        or (
            qlt.catalog_id in (
                282, 283, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 494, 505
            ) or (qlt.catalog_id = 504 and lower(qlt.description) like 'small project overhead')
        )
        or lower(name) like '%additional man hours%'
    group by 1
),

droplines as (
    --find all quotelines that was dropped from an order
    select
        order_id,
        qdl.quote_line_id
    from ergeon.quote_quote as q
    left join ergeon.quote_quotedropline as qdl on qdl.quote_id = q.id
    where approved_at is not null
        and qdl.quote_line_id is not null
),

project_lines as (
    --find all project lines of an approved order
    select
        q.order_id,
        q.id as quote_id,
        ql.id as quoteline_id,
        ql.description as description,
        price,
        cost
    from ergeon.quote_quote as q
    left join ergeon.quote_quoteline as ql on ql.quote_id = q.id
    left join droplines as dl on dl.quote_line_id = ql.id
    where approved_at is not null
      and dl.quote_line_id is null
),

last_approved_discounts as (
    --extra discounts/fees per approved order 
    select
        pl.order_id,
        sum(case when pct.item like 'sales%' then abs(pl.price) else 0 end) as sales_discount,
        sum(case when pct.item like 'mktg%' then abs(pl.price) else 0 end) as mktg_discount,
        sum(case when pct.item like 'delivery%' then abs(pl.price) else 0 end) as delivery_discount,
        sum(case when pct.item like 'pricing%' then abs(pl.price) else 0 end) as pricing_discount,
        sum(case when pct.id = 121 then abs(pl.price) else 0 end) as supply_discount,
        sum(case when pct.id = 81 then abs(pl.cost) else 0 end) as installer_pay,
        sum(
            case
                when
                    (
                        qlt.catalog_id in (
                            282, 283, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 494, 505
                        ) or (qlt.catalog_id = 504 and lower(qlt.description) like 'small project overhead')
                    ) then pl.price
                else 0
            end
        ) as small_project_overhead,
        sum(case when lower(name) like '%additional man hours%' then pl.price else 0 end) as additional_mh_labor --additional man hour labor
    from project_lines as pl 
    inner join quoteline_catalog as qlt on qlt.id = pl.quoteline_id 
    left join ergeon.product_catalogtype as pct on pct.id = qlt.type_id
    where
        (pct.category in ('sales_discount') and lower(pl.description) not like ('%sales%surcharge%'))
        or pct.category in ('marketing_discount')
        or pct.item like '%delivery%' or pct.item like '%pricing%' or pct.id = 81 or pct.id = 121
        or (
            qlt.catalog_id in (
                282, 283, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 494, 505
            ) or (qlt.catalog_id = 504 and lower(qlt.description) like 'small project overhead')
        )
        or lower(name) like '%additional man hours%'
    group by 1
)

--allocate all costs/dates per order
select
    o.id as order_id,
    quote_requested_ts_at,
    fqs.quote_id as first_quote_id,
    faq.first_approved_quote_id,
    lqas.quote_id as last_quote_id,
    extract(date from faq.won_at at time zone 'America/Los_Angeles') as won_at,
    datetime(faq.won_at, 'America/Los_Angeles') as won_ts_at,
    datetime(fqs.first_quoted_at, 'America/Los_Angeles') as quoted_ts_at,
    extract(date from fqs.first_quoted_at at time zone 'America/Los_Angeles') as quoted_at,
    coalesce(fqq.total_price, 0) as first_quoted_price,
    coalesce(fqq.total_cost, 0) as first_quoted_cost,
    coalesce(fqd.sales_discount, 0) as first_quoted_sales_discount,
    coalesce(fqd.mktg_discount, 0) as first_quoted_mktg_discount,
    coalesce(faqq.total_price, 0) as first_approved_price,
    coalesce(faqq.total_cost, 0) as first_approved_cost,
    coalesce(fad.sales_discount, 0) as first_approved_sales_discount,
    coalesce(fad.mktg_discount, 0) as first_approved_mktg_discount,
    coalesce(fad.delivery_discount, 0) as first_approved_delivery_discount,
    coalesce(fad.pricing_discount, 0) as first_approved_pricing_discount,
    coalesce(o.total_project_price, 0) as last_approved_price,
    coalesce(o.total_project_cost, 0) as last_approved_cost,
    coalesce(lad.sales_discount, 0) as last_approved_sales_discount,
    coalesce(lad.mktg_discount, 0) as last_approved_mktg_discount,
    coalesce(lad.delivery_discount, 0) as last_approved_delivery_discount,
    --if not approved --> price of last quote sent / if approved --> total_project_price + price from last scope quote
    coalesce(lad.pricing_discount, 0) as last_approved_pricing_discount,
    coalesce(lad.supply_discount, 0) as last_approved_supply_discount,
    coalesce(lad.installer_pay, 0) as installer_pay,
    coalesce(
        case when faq.won_at is null then lqq.total_price else o.total_project_price + coalesce(lqqs.total_price, 0) end, 0
    ) as last_quoted_price,
    coalesce(case when faq.won_at is null then lqq.total_cost else o.total_project_cost + coalesce(lqqs.total_cost, 0) end, 0) as last_quoted_cost,
    coalesce(lad.small_project_overhead, 0) as last_approved_small_project_overhead,
    coalesce(lqsd.small_project_overhead, 0) as quoted_small_project_overhead,
    coalesce(lad.additional_mh_labor, 0) as additional_mh_labor
from ergeon.store_order as o
--join order by first quote sent
left join first_quote_sent as fqs on fqs.order_id = o.id
left join ergeon.quote_quote as fqq on fqs.quote_id = fqq.id
left join quote_requested_time as rt on rt.order_id = o.id
--join discounts with first quote sent
left join discounts_per_quote_sent as fqd on fqd.quote_id = fqq.id
--join order by last quote sent
left join last_quote_sent as lqs on lqs.order_id = o.id
left join ergeon.quote_quote as lqq on lqs.quote_id = lqq.id
--join discounts with last quote sent
left join discounts_per_quote_sent as lqsd on lqsd.quote_id = lqq.id
--join order by last scope quote sent
left join last_scope_quote_sent as lqss on lqss.order_id = o.id
left join ergeon.quote_quote as lqqs on lqss.quote_id = lqqs.id
--join order by first approved quote
left join first_approved_quote as faq on faq.order_id = o.id
left join ergeon.quote_quote as faqq on faqq.id = faq.first_approved_quote_id
--join discounts with first approved quote
left join discounts_per_quote_sent as fad on fad.quote_id = faq.first_approved_quote_id
--join discounts with last approved order (which used for margin leakage)
left join last_approved_discounts as lad on lad.order_id = o.id
left join last_quote_approved_or_sent as lqas on o.id = lqas.order_id
