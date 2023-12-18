with formatted_numbers as (
    select
        date_add(quarter, interval 1 day) as quarter,
        o.* except(quarter),
        case
            when type = 'currency' then concat("$ ", format("%'d", cast(round(baseline_value, 0) as integer)))
            when type = 'percent' then format('%s%%', cast(round((baseline_value) * 100, 2) as string))
            when type = 'number' then cast(round(baseline_value, 0) as string)
            when type = 'float_1' then cast(round(baseline_value, 1) as string)
            when type = 'float_2' then cast(round(baseline_value, 2) as string)
            when type = 'blank' then null
        end as formatted_baseline,
        case
            when type = 'currency' then concat("$ ", format("%'d", cast(round(current_value, 0) as integer)))
            when type = 'percent' then format('%s%%', cast(round((current_value) * 100, 2) as string))
            when type = 'number' then cast(round(current_value, 0) as string)
            when type = 'float_1' then cast(round(current_value, 1) as string)
            when type = 'float_2' then cast(round(current_value, 2) as string)
            when type = 'blank' then null
        end as formatted_current,
        case
            when type = 'currency' then concat("$ ", format("%'d", cast(round(target_value, 0) as integer)))
            when type = 'percent' then format('%s%%', cast(round((target_value) * 100, 2) as string))
            when type = 'number' then cast(round(target_value, 0) as string)
            when type = 'float_1' then cast(round(target_value, 1) as string)
            when type = 'float_2' then cast(round(target_value, 2) as string)
            when type = 'blank' then null
        end as formatted_target
    from int_data.okr_database o
)

select * from formatted_numbers
