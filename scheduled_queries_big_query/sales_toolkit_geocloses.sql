(with completed_orders as (
    select 
        coalesce(a.won_at,date(so.created_at,"America/Los_Angeles")) as date,
        so.id as order_id,
        formatted_address as address,
        st_geogpoint(a.longitude,a.latitude) as location,
        "completed order" as type
    from ergeon.store_order so
    left join ergeon.core_house ch on ch.id = so.house_id
    left join ergeon.geo_address ga on ga.id = ch.address_id
    left join ergeon.core_statustype cs on cs.id = so.project_status_id
    left join int_data.sales_dashboard_arts a on a.order_id = so.id
    where cs.code = 'project_completed'
    )
    select
        s.order_id,
        s2.address as child_address,
        s2.order_id as child_order,
        s2.location as child_location,
        s.type,
    from completed_orders s
    join completed_orders s2 on st_dwithin(s.location, s2.location, 500) --and date_diff(s2.date, s.date, day) between 0 and 90
    )
UNION ALL
(with open_orders as (
    select 
        date(so.created_at,"America/Los_Angeles") as date,
        so.id as order_id,
        formatted_address as address,
        st_geogpoint(longitude,latitude) as location,
        "open order" as type
    from ergeon.store_order so
    left join ergeon.core_house ch on ch.id = so.house_id
    left join ergeon.geo_address ga on ga.id = ch.address_id
    left join ergeon.core_statustype cs on cs.id = so.deal_status_id
    where cs.code not in ('lost','on_hold', 'new')
    )
    select
        s.order_id,
        s2.address as child_address,
        s2.order_id as child_order,
        s2.location as child_location,
        s.type,
    from open_orders s
    join open_orders s2 on st_dwithin(s.location, s2.location, 500) --and date_diff(s2.date, s.date, day) between 0 and 90
)