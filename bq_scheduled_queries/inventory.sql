select
    l.id,
    l.product_id,
    m.code as market,
    r.name as region,
    l.unit_price,
    l.quantity_available,
    p.name,
    c.description,
    l.store_id,
    s.company_id,
    p.key,
    a.formatted_address,
    p.config_id,
    extract(date from l.created_at at time zone 'America/Los_Angeles') as created_at,
    case
        when m.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) then 'North California'
        when m.id in (6, 5, 14, 7, 1, 12, 11) then 'South California'
        when m.code like '%-TX-%' then 'Texas'
        when m.code like '%-GA-%' then 'Georgia'
        when m.code like '%-MD-%' then 'Maryland'
        when m.code like '%-PA-%' then 'Pennsylvania'
        when m.code like '%-VA-%' then 'Virginia'
        when m.code like '%-FL-%' then 'Florida'
        else 'Other'
    end as old_region,
    split(split(c.description, '2dim-code=') [safe_offset(1)], ';') [offset(0)] as two_dim_code,
    split(split(c.description, '3dim-code=') [safe_offset(1)], ';') [offset(0)] as three_dim_code,
    split(split(c.description, 'ergeon-code=') [safe_offset(1)], ';') [offset(0)] as ergeon_code,
    split(split(c.description, 'lumber-code=') [safe_offset(1)], ';') [offset(0)] as lumber_code,
    split(split(c.description, 'wood-species=') [safe_offset(1)], ';') [offset(0)] as wood_species,
    split(split(c.description, 'color-finish=') [safe_offset(1)], ';') [offset(0)] as color_finish,
    split(split(c.description, 'lumber-grade=') [safe_offset(1)], ';') [offset(0)] as lumber_grade,
    split(split(c.description, 'compression-strength=') [safe_offset(1)], ';') [offset(0)] as compression_strength,
    'https://www.homedepot.com/p//' || p.key as homedepot_link,
    'https://api.ergeon.in/public-admin/inventory/availabilitylog/' || l.id || '/change/' as admin_link
from
    ergeon.inventory_availabilitylog as l inner join
    ergeon.inventory_product as p on p.id = l.product_id inner join
    ergeon.calc_config as c on c.id = p.config_id inner join
    ergeon.inventory_store as s on s.id = l.store_id inner join
    ergeon.geo_address as a on a.id = s.address_id inner join
    ergeon.product_countymarket as cm on cm.county_id = a.county_id inner join
    ergeon.product_market as m on m.id = cm.market_id inner join
    ergeon.product_region as r on r.id = m.region_id
