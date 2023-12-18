with qlt as (
    select
        ql.id,
        coalesce(tys.id, ty.id, qlt.id) as catalog_type_id,
        coalesce(tys.item, ty.item, qlt.item) as catalog_type
    from
        ergeon.quote_quoteline ql left join
        ergeon.quote_quotestyle qs on qs.id = ql.quote_style_id left join
        ergeon.product_catalog sct on sct.id = qs.catalog_id left join
        ergeon.product_catalog ct on ct.id = ql.catalog_id left join
        ergeon.product_catalogtype ty on ty.id = ct.type_id left join
        ergeon.product_catalogtype tys on tys.id = sct.type_id left join
        ergeon.product_catalogtype qlt on qlt.id = ql.catalog_type_id left join
        ergeon.quote_quote qq on qq.id = ql.quote_id
    where
        is_cancellation = False
)

select
    market,
    qq.order_id,
    qq.id as quote_id,
    extract(date from qq.created_at at time zone 'America/Los_Angeles') as quote_created_at,
    ql.id as quoteline_id,
    catalog_type,
    unit_price,
    unit_cost,
    price,
    cost,
    1 - (unit_cost / nullif(unit_price, 0)) as margin,
    unit_price / (1 + surcharge) as baseline_price,
    ql.description,
    cco.full_name,
    qlt.catalog_type_id,
    concat('Package - ', rank() over(partition by qq.order_id, quote_id, catalog_type order by ql.id)) as package,
    concat("https://admin.ergeon.in/quoting-tool/", o.id, '/overview') as quoting_tool_link,
    concat('https://api.ergeon.in/public-admin/store/order/', o.id, '/change/') as admin_order_link,
    concat('https://api.ergeon.in/public-admin/quote/quote/', qq.id, '/change/') as admin_quote_link
from ergeon.quote_quote qq
left join ergeon.quote_quoteline ql on ql.quote_id = qq.id
left join ergeon.store_order o on qq.order_id = o.id
left join ergeon.core_house ch on ch.id = o.house_id
left join ergeon.customers_customer cc on cc.id = ch.customer_id
left join ergeon.customers_contact cco on cco.id = cc.contact_id
left join int_data.order_ue_materialized ue on ue.order_id = o.id
left join qlt on qlt.id = ql.id
left join ergeon.geo_address ga on ga.id = ch.address_id
left join ergeon.product_tiercatalogtype tc on tc.tier_id = qq.tier_id and tc.catalog_type_id = qlt.catalog_type_id
where
    is_draft = False
    and qq.is_estimate = False
    and lower(full_name) like '%gold order category%'
    and ql.id is not null
    and catalog_type not in ('full-adjustment', 'fence-labor', 'landscaping-grass-addons', 'fence-gate-custom')
order by 1, 2 desc, 3, 5 --I can't use 4 fields for the sorting in Google Data Studio, therefore I order the dataset here
