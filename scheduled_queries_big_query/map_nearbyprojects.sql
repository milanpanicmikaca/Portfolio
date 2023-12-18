with won_deals as (
    select 
        ue.won_at,
        so.id as order_id,
        formatted_address as address,
        st_geogpoint(ue.longitude,ue.latitude) as location,
        cs.code,
        cs.type,
        ue.market,
        ue.total_length,
        p.name as product,
        ue.last_approved_price as final_revenue,
    from ergeon.store_order so
    left join ergeon.core_house ch on ch.id = so.house_id
    left join ergeon.geo_address ga on ga.id = ch.address_id
    left join ergeon.core_statustype cs on cs.id = so.project_status_id
    left join int_data.order_ue_materialized ue on ue.order_id = so.id
    left join ergeon.store_product p on p.id = so.product_id
    where ue.order_status = 'Won'
      and ue.completed_at is null
    )
    select
        s.won_at,
        s.order_id,
        s.market,
        s2.address as child_address,
        s2.order_id as child_order,
        s2.location as child_location,
        s2.final_revenue,
        s2.product,
        s2.total_length as child_lf,
        s.type as status_type,
        s.code as status_code,
        trunc(st_distance(s.location, s2.location)/1000,2) as distance_km,
    from won_deals s
    join won_deals s2 on st_dwithin(s.location, s2.location, 13000) --distance in meters
    order by order_id