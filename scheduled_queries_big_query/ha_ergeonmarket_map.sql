with ergeon_market_to_ha_msa as (
    with m1 as (
        select
            pm.id as ergeon_market_id,
            hmsa.ha_msa_id,
            sum(gz.population) as pop
        from
            ergeon.product_market pm left join
            ergeon.product_countymarket pcm on pcm.market_id = pm.id left join
            ergeon.geo_county gc on gc.id = pcm.county_id left join
            ergeon.geo_zipcode gz on gz.county_id = gc.id left join
            int_data.ha_msazipcode ez on ez.ergeon_zipcode = gz.code left join
            int_data.ha_msa hmsa on hmsa.ha_msa_id = ez.ha_msa_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            rank() over (partition by ergeon_market_id order by pop desc) as rank
        from m1
    )

    select
        ergeon_market_id,
        ha_msa_id
    from m2 where rank = 1
),

ergeon_market_to_ergeon_msa as (
    with m1 as (
        select
            pm.id as ergeon_market_id,
            emsa.id as ergeon_msa_id,
            sum(gc.population) as pop
        from
            ergeon.product_market pm left join
            ergeon.product_countymarket pcm on pcm.market_id = pm.id left join
            ergeon.geo_county gc on gc.id = pcm.county_id left join
            ergeon.geo_msa emsa on emsa.id = gc.msa_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            rank() over (partition by ergeon_market_id order by pop desc) as rank
        from m1
    )

    select
        ergeon_market_id,
        ergeon_msa_id
    from m2 where rank = 1
)

select *
from
    ergeon_market_to_ha_msa left join
    ergeon_market_to_ergeon_msa using (ergeon_market_id)
