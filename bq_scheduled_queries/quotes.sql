with min_lead_service as (
    select
        lead_id,
        min(cls.id) as first_lead_service
    from
        ergeon.core_lead_services cls
    group by 1
),

leads as (
    select
        order_id,
        min(id) as lead_id
    from ergeon.core_lead l
    where l.created_at > '2018-04-15'
    group by 1
),

qlt as (
    select
        ql.id,
        coalesce(sct.name, ct.name) as package,
        coalesce(tys.item, ty.item, qlt.item) as item,
        array_to_string(coalesce(cc1.code, cc.code), ",") as code,
        array_to_string(coalesce(cc1.schema, cc.schema), ",") as schema,
        coalesce(tys.service_id, ty.service_id, qlt.service_id) as service_id
    from
        ergeon.quote_quoteline ql left join
        ergeon.quote_quotestyle qs on qs.id = ql.quote_style_id left join
        ergeon.product_catalog sct on sct.id = qs.catalog_id left join
        ergeon.product_catalog ct on ct.id = ql.catalog_id left join
        ergeon.product_catalogtype ty on ty.id = ct.type_id left join
        ergeon.product_catalogtype tys on tys.id = sct.type_id left join
        ergeon.product_catalogtype qlt on qlt.id = ql.catalog_type_id left join
        ergeon.calc_config cc on cc.id = ql.config_id left join
        ergeon.calc_config cc1 on cc1.id = qs.config_id
    where
        coalesce(sct.name, ct.name) is not null
),

quotes as (
    select
        ql.id as quoteline_id, quote_id, q.order_id, qlt.item as type,
        extract(date from ql.created_at at time zone 'America/Los_Angeles') as quoteline_date,
        ql.quantity,
        ql.price,
        extract(date from q.sent_to_customer_at at time zone 'America/Los_Angeles') as quoted_at,
        extract(date from q.approved_at at time zone 'America/Los_Angeles') as approved_at,
        ls.label as lead_service,
        qs.label as quoted_service,
        ci.name as city,
        st.name as state,
        r.name as region, m.code as market,
        '<' || ql.description || '>' as description,
        calc_input,
        json_extract_array(calc_input, "$.sides") as sides,
        json_extract_array(calc_input, "$.gates") as gates,
        json_extract_array(calc_input, "$.polygons") as polygons,
        package, replace(qlt.code, '"', '') as code, qlt.schema,
        concat("https://api.ergeon.in/public-admin/quote/quoteline/", ql.id, "/change/") as quoteline_link,
        concat("https://api.ergeon.in/public-admin/quote/quote/", quote_id, "/change/") as quote_link
    from
        ergeon.quote_quoteline ql join
        ergeon.quote_quote q on q.id = ql.quote_id join
        ergeon.store_order o on o.id = q.order_id join
        ergeon.core_house h on h.id = o.house_id join
        ergeon.geo_address a on a.id = h.address_id join
        ergeon.geo_city ci on ci.id = a.city_id join
        ergeon.geo_state st on st.id = ci.state_id join
        ergeon.product_countymarket cm on cm.county_id = a.county_id join
        ergeon.product_market m on m.id = cm.market_id join
        ergeon.product_region r on r.id = m.region_id join
        qlt on qlt.id = ql.id left join
        leads on leads.order_id = o.id left join
        ergeon.core_lead l on l.id = leads.lead_id left join
        min_lead_service ml on l.id = ml.lead_id left join
        ergeon.core_lead_services cls on cls.id = ml.first_lead_service left join
        ergeon.product_service ls on ls.id = cls.service_id left join
        ergeon.product_service qs on qs.id = qlt.service_id
)

select * from quotes where quoteline_date is not null and code is not null
