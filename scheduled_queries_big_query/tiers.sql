with customer_surcharge as (
    select
        id as customer_tier_id,
        name,
        extract(date from t.created_at at time zone 'America/Los_Angeles') as created_at,
        coalesce(surcharge, deprecated_price_surcharge) as price
    from
        ergeon.product_tier t
    where
        status_id in (63, 64)
        and type = 'customer'
),

contractor_surcharge as (
    select
        id as contractor_tier_id,
        name,
        status_id,
        extract(date from t.created_at at time zone 'America/Los_Angeles') as created_at,
        coalesce(surcharge, deprecated_cost_surcharge) as cost
    from
        ergeon.product_tier t
    where
        status_id in (63, 64)
        and type = 'subcontractor'
),

tier_attributes as (
    select
        customer_tier_id,
        contractor_tier_id,
        name as tier,
        con.created_at,
        price,
        cost,
        (1 - (1 + cost * 1.0) / (1 + price)) as margin,
        case
            when status_id = 64 then "inactive"
            when status_id = 63 then "active"
        end as status
    from
        customer_surcharge left join
        contractor_surcharge con using (name)
),

product_tier as (
    select
        tz.tier_id,
        array_to_string(array_agg(distinct p.short_name), ',') as product
    from
        ergeon.product_tierzipcode tz join
        ergeon.geo_zipcode z on z.id = tz.zip_code_id join
        ergeon.product_tierproduct tp on tp.tier_id = tz.tier_id left join
        ergeon.store_product p on p.id = tp.product_id
    group by 1
),

geo as (
    select
        tz.tier_id,
        array_to_string(array_agg(distinct m.code), ', ') as market,
        array_to_string(array_agg(distinct cn.name), ', ') as county,
        array_to_string(array_agg(distinct st.name), ', ') as state
    from
        ergeon.product_tierzipcode tz join
        ergeon.geo_zipcode gz on gz.id = tz.zip_code_id left join
        ergeon.geo_county cn on cn.id = gz.county_id left join
        ergeon.geo_state st on st.id = cn.state_id left join
        ergeon.product_countymarket pcnm on pcnm.county_id = cn.id left join
        ergeon.product_market m on m.id = pcnm.market_id
    group by 1
),

catalog_type as (
    select
        ptc.tier_id,
        array_to_string(array_agg(distinct item), ', ') as type
    from
        ergeon.product_tiercatalogtype ptc join
        ergeon.product_catalogtype pc on pc.id = ptc.catalog_type_id
    where
        pc.deleted_at is null
    group by 1
)

select
    t.*, product, market, county, state, type,
    'https://api.ergeon.in/public-admin/product/tier/' || t.contractor_tier_id || '/change/' as contractor_tier_url,
    'https://api.ergeon.in/public-admin/product/tier/' || t.customer_tier_id || '/change/' as customer_tier_url
from
    tier_attributes t left join
    product_tier p on p.tier_id = t.contractor_tier_id left join
    geo g on g.tier_id = t.contractor_tier_id left join
    catalog_type c on c.tier_id = t.contractor_tier_id
