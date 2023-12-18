--ha msa to ergeon market
with ha_msa_to_ergeonmarket as (
    with m1 as (
        select
            hm.ha_msa_id,
            pm.id as ergeon_market_id,
            sum(gc.population) as pop
        from
            int_data.ha_msa hm left join
            int_data.ha_msazipcode ez on ez.ha_msa_id = hm.ha_msa_id left join
            ergeon.geo_zipcode gz on gz.code = ez.ergeon_zipcode left join
            ergeon.geo_county gc on gc.id = gz.county_id left join
            ergeon.product_countymarket pcm on pcm.county_id = gc.id left join
            ergeon.product_market pm on pm.id = pcm.market_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            --the next ergeon market is created to take the next most populated ergeon market in each ha msa, 
            --if the most populated one is the 'null' market
            lead(m1.ergeon_market_id) over (partition by ha_msa_id order by pop desc) as next_market_id,
            rank() over (partition by ha_msa_id order by pop desc) as rank
        from m1
    )

    select
        ha_msa_id,
        coalesce(ergeon_market_id, next_market_id) as ergeon_market_id
    from m2 where rank = 1
),

--ha msa to ergeon msa
ha_msa_to_ergeon_msa as (
    with m1 as (
        select
            hmsa.ha_msa_id,
            emsa.id as ergeon_msa_id,
            sum(gz.population) as pop
        from
            int_data.ha_msa hmsa left join
            int_data.ha_msazipcode ez on ez.ha_msa_id = hmsa.ha_msa_id left join
            ergeon.geo_zipcode gz on gz.code = ez.ergeon_zipcode left join
            ergeon.geo_county gc on gc.id = gz.county_id left join
            ergeon.geo_msa emsa on emsa.id = gc.msa_id
        group by 1, 2
    ),

    m2 as (
        select
            m1.*,
            rank() over (partition by ha_msa_id order by pop desc) as rank
        from m1
    )

    select
        ha_msa_id,
        ergeon_msa_id
    from m2 where rank = 1
)

select *
from
    ha_msa_to_ergeonmarket left join
    ha_msa_to_ergeon_msa using (ha_msa_id)
