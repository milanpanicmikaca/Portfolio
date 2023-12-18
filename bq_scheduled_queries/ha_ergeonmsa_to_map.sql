with ergeon_msa_to_ha_msa as (
    with m1 as (
        select
            emsa.id as ergeon_msa_id,
            ez.ha_msa_id,
            sum(gz.population) as pop
        from
            ergeon.geo_msa emsa left join
            ergeon.geo_county gc on gc.msa_id = emsa.id left join
            ergeon.geo_zipcode gz on gz.county_id = gc.id left join
            int_data.ha_msazipcode ez on ez.ergeon_zipcode = gz.code left join
            int_data.ha_msa hmsa on hmsa.ha_msa_id = ez.ha_msa_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            rank() over (partition by ergeon_msa_id order by pop desc) as rank
        from m1
    )

    select
        ergeon_msa_id,
        ha_msa_id
    from m2 where rank = 1
),

ergeon_msa_to_ergeon_market as (
    with m1 as (
        select
            emsa.id as ergeon_msa_id,
            pm.id as ergeon_market_id,
            sum(gc.population) as pop
        from
            ergeon.geo_msa emsa left join
            ergeon.geo_county gc on gc.msa_id = emsa.id left join
            ergeon.product_countymarket pcm on pcm.county_id = gc.id left join
            ergeon.product_market pm on pm.id = pcm.market_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            rank() over (partition by ergeon_msa_id order by pop desc) as rank
        from m1
    )

    select
        ergeon_msa_id,
        ergeon_market_id
    from m2 where rank = 1
)

select *
from
    ergeon_msa_to_ha_msa left join
    ergeon_msa_to_ergeon_market using (ergeon_msa_id)
