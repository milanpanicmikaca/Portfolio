with market_launch as (
    select
        code as market,
        mp.market_id,
        min(mp.launch_date) as launch_date
    from
        ergeon.product_marketproduct mp left join
        ergeon.product_market m on m.id = mp.market_id
    where
        is_active = True
        and mp.launch_date is not null
    group by 1, 2
),

ha_data as (
    select
        date,
        case
            when ha_task_name like 'Chain Link Fence - Install%' then '/Fence Installation/Install a Chain Link Fence'
            when ha_task_name like 'Vinyl or PVC Fence - Install%' then '/Fence Installation/Install a Vinyl or PVC Fence'
            when ha_task_name like 'Wood Fence - Install%' then '/Fence Installation/Install a Wood Fence'
            when ha_task_name like 'Wood Fence - Repair%' then '/Fence Installation/Repair or Partially Replace a Wood Fence'
            when ha_task_name like 'Landscape - Install Synthetic Grass for%' then '/Landscaping/Install Artificial Grass'
        end as product,
        ergeon_market_code as market,
        ergeon_old_region_name as old_region,
        ergeon_region_name as region,
        market_id,
        volume
    from
        int_data.monthly_msa_data m left join
        market_launch ml on ml.market = m.ergeon_market_code
    where
        (ha_task_name like 'Chain Link Fence - Install%'
            or (
                ha_task_name like 'Vinyl or PVC Fence - Install%' and ergeon_old_region_name <> 'Texas' and (
                    ergeon_old_region_name <> 'North California' or (ergeon_old_region_name = 'North California' and date >= '2023-03-01')
                )
            )
            or ha_task_name like 'Wood Fence - Install%'
            or ha_task_name like 'Wood Fence - Repair%'
            or (ha_task_name like 'Landscape - Install Synthetic Grass for%' and date >= '2022-10-01'))
        and ergeon_market_code is not null
        and ml.launch_date is not null
        and ml.launch_date <= date
        and ergeon_market_code <> 'Unknown'
        and ergeon_old_region_name <> 'Other'
),

order_ue as (
    select
        date_trunc(closedW_at, month) as date,
        region,
        old_region,
        market,
        product,
        order_id
    from
        int_data.order_ue_materialized
    where
        market <> 'Unknown'
    qualify rank() over(partition by date_trunc(closedW_at, month), region, old_region, market, product order by closedW_at, order_id) = 1
),

final_ha_volume as (
    select
        date,
        region,
        old_region,
        market,
        product,
        sum(volume) as volume
    from
        ha_data
    group by 1, 2, 3, 4, 5
),

population_data as (
    select
        pm.code as market,
        sum(gc.population) as pop
    from
        int_data.ha_msa hm left join
        int_data.ha_msazipcode ez on ez.ha_msa_id = hm.ha_msa_id left join
        ergeon.geo_zipcode gz on gz.code = ez.ergeon_zipcode left join
        ergeon.geo_county gc on gc.id = gz.county_id left join
        ergeon.product_countymarket pcm on pcm.county_id = gc.id left join
        ergeon.product_market pm on pm.id = pcm.market_id
    where
        market_id in (8, 31)
    group by 1
),

population_pa_sf as (
    select
        sum(if(market = 'PA-CA-PA', pop, 0)) / sum(pop) as pa_perc,
        sum(if(market = 'PA-CA-SF', pop, 0)) / sum(pop) as sf_perc
    from
        population_data
)

select
    date,
    region,
    old_region,
    market,
    product,
    if(market = 'PA-CA-SF', sf_perc * volume, volume) as volume,
    order_id
from
    final_ha_volume left join
    order_ue using (date, region, old_region, market, product) cross join
    population_pa_sf
union all
select
    ue.date,
    ue.region,
    ue.old_region,
    ue.market,
    ue.product,
    if(ha.market = 'PA-CA-SF', pa_perc * volume, volume) as volume,
    order_id
from
    order_ue ue join
    final_ha_volume ha on ha.date = ue.date and ue.product = ha.product and ha.market = 'PA-CA-SF' cross join
    population_pa_sf
where
    ue.market = 'PA-CA-PA'
    and ha.market = 'PA-CA-SF'
order by 1 desc
