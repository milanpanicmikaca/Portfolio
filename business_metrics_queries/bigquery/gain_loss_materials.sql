-- make changes to eng-15409 on MAT004 metrics
with
    timeseries as 
    (
    select 
        date_trunc(date_array,{period}) as date,
        from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
        group by 1
    ),
    booked as (
    select 
        date_trunc(sm.date,{period}) as date,
        count(*) as no_of_orders_b,
        count(distinct contractor) as unique_contractor,
        sum(extended_price) as gmv_booked_materials_b,
        sum(case when sm.extended_price - sm.extended_cost > 0 then sm.extended_price - sm.extended_cost else 0 end) as booked_gain,
        sum(case when sm.extended_price - sm.extended_cost < 0 then sm.extended_price - sm.extended_cost else 0 end) as booked_loss,
    from googlesheets.supply_material sm
    group by 1
    ),
    delivered as (
    select 
        date_trunc(sm.delivered_at,{period}) as date,
        count(*) as no_of_orders_d,
        sum(extended_price) as gmv_booked_materials_d,
        sum(case when sm.extended_price - sm.extended_cost > 0 then sm.extended_price - sm.extended_cost else 0 end) as delivered_gain,
        sum(case when sm.extended_price - sm.extended_cost < 0 then sm.extended_price - sm.extended_cost else 0 end) as delivered_loss,
        avg(date_diff(delivered_at,date,day)) as days_to_deliver,
    from googlesheets.supply_material sm
    group by 1
    ),
    qb_delivered as (
    select 
        date_trunc(sm.invoice_date,{period}) as date,
        sum(extended_price) as gmv_booked_materials_d,
    from int_data.qb_income_by_customer sm
    group by 1
    )
select
    t.date as date,
    coalesce(b.no_of_orders_b,0) as MAT001,
    coalesce(b.gmv_booked_materials_b,0) as MAT002,
    coalesce(b.booked_gain,0) as MAT003,
    round(coalesce((b.booked_gain+b.booked_loss)/b.gmv_booked_materials_b,0),2) as MAT004,
    coalesce(b.unique_contractor,0) as MAT005,
    coalesce(d.no_of_orders_d,0) as MAT006,
    coalesce(d.gmv_booked_materials_d,0) as MAT007,
    coalesce(qb.gmv_booked_materials_d,0) as MAT034,
    coalesce(d.delivered_gain,0) as MAT008,
    coalesce((d.delivered_gain+d.delivered_loss)/d.gmv_booked_materials_d,0) as MAT009,
    coalesce(d.days_to_deliver,0) as MAT013,
    coalesce(b.booked_loss,0) as MAT016,
    coalesce(d.delivered_loss,0) as MAT017
from timeseries t
    left join booked b on b.date = t.date
    left join delivered d on d.date = t.date
    left join qb_delivered qb on qb.date = t.date 
    
