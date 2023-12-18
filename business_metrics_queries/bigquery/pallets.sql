-- upload to BQ
with
    timeseries as 
    (
    select 
        date_trunc(date_array,{period}) as date,
        from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
        group by 1
    ),
    pallet as (
    select 
        date_trunc(pt.ordered_date,{period}) as date,
        sum (case when status = 'Material Ordered' then 1 else 0 end) as materials_ordered,
        sum (if (status = 'Material Ordered', case when date_diff (current_date,ordered_date,day) < 15 then 1 else 0 end,0)) as aging_materials_15d,
        sum (if (status = 'Material Ordered', case when date_diff (current_date,ordered_date,day) between 16 and 30 then 1 else 0 end,0)) as aging_materials_30d,
        sum (if (status = 'Material Ordered', case when date_diff (current_date,ordered_date,day) between 31 and 60 then 1 else 0 end,0)) as aging_materials_60d,
        sum (if (status = 'Material Ordered', case when date_diff (current_date,ordered_date,day) > 61 then 1 else 0 end,0)) as aging_materials_90d,
        sum (case when (status = 'Ready at Store' or status = 'Ready for Pickup') then 1 else 0 end) as materials_ready,
        sum (if ((status = 'Ready at Store' or status = 'Ready for Pickup'), case when date_diff (current_date,ordered_date,day) < 15 then 1 else 0 end,0)) as aging_ready_15,
        sum (if ((status = 'Ready at Store' or status = 'Ready for Pickup'), case when date_diff (current_date,ordered_date,day) between 16 and 30 then 1 else 0 end,0)) as aging_ready_30,
        sum (if ((status = 'Ready at Store' or status = 'Ready for Pickup'), case when date_diff (current_date,ordered_date,day) between 31 and 60 then 1 else 0 end,0)) as aging_ready_60,
        sum (if ((status = 'Ready at Store' or status = 'Ready for Pickup'), case when date_diff (current_date,ordered_date,day) > 61 then 1 else 0 end,0)) as aging_ready_90,
        sum (case when vendor = 'Home Depot' then 1 else 0 end) as pallets_home_depot,
        sum (case when vendor = 'Lowes' then 1 else 0 end) as pallets_lowes,
        sum (case when vendor not in ('Home Depot', 'Lowes') then 1 else 0 end) as pallets_others,
    from googlesheets.pallet_tracker pt
    group by 1
        )
select
    t.date as date,
    coalesce(p.materials_ordered,0) as MAT018,
    aging_materials_15d as MAT019,
    aging_materials_30d as MAT020,
    aging_materials_60d as MAT021,
    aging_materials_90d as MAT022,
    --aging_materials_plus90d,
    materials_ready as MAT023,
    aging_ready_15 as MAT024,
    aging_ready_30 as MAT025,
    aging_ready_60 as MAT026,
    aging_ready_90 as MAT027,
    pallets_home_depot as MAT030,
    pallets_lowes as MAT031,
    pallets_others as MAT032,
from timeseries t
    left join pallet p on p.date = t.date
    