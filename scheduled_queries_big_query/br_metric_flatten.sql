begin transaction;

/* All needed periods for BR: 29 days, 18 weeks, 14 months, 11 quarters, 6 years*/
create temp table periods as
select
    'day' as period_type,
    *
from unnest(generate_date_array(current_date('America/Los_Angeles') - 28, current_date('America/Los_Angeles'), interval 1 day)) as period
union all
--last 18 weeks
select
    'week' as period_type,
    *
from
    unnest(
        generate_date_array(
            date_sub(date_trunc(current_date('America/Los_Angeles'), isoweek), interval 17 week), current_date('America/Los_Angeles'), interval 1 week
        )
    ) as period
union all
--last 14 months
select
    'month' as period_type,
    *
from
    unnest(
        generate_date_array(
            date_sub(date_trunc(current_date('America/Los_Angeles'), month), interval 13 month), current_date('America/Los_Angeles'), interval 1 month
        )
    ) as period
union all
--last 11 quarters
select
    'quarter' as period_type,
    *
from
    unnest(
        generate_date_array(
            date_sub(date_trunc(current_date('America/Los_Angeles'), quarter), interval 10 quarter),
            current_date('America/Los_Angeles'),
            interval 1 quarter
        )
    ) as period
union all
--last 6 years 
select
    'year' as period_type,
    *
from
    unnest(
        generate_date_array(
            date_sub(date_trunc(current_date('America/Los_Angeles'), year), interval 5 year), current_date('America/Los_Angeles'), interval 1 year
        )
    ) as period;

/* Current period's ratio of actual days over expected days
holidays are excluded from calculation and weekends are weighed as halfday
e.g. for 24th of October 2022 it will be 15 (working days) + 4 (weekends) / 21 (working days) + 5 (weekends) = 19/26 = 0.73 */
create temp table current_period_ratio as
with holidays as (
    select holiday as date_holiday from (
        select
            *,
            case
                when day = 31 and month = 12 then list_of_days -- New Year
                when day = 1 and month = 1 then list_of_days -- New Year
                /*(case when day_of_week not in ('Saturday','Sunday') then list_of_days
                when day_of_week = 'Saturday' then prev
                when day_of_week = 'Sunday' then next end)*/
                when day = 4 and month = 7 then list_of_days --Independence Day
                /*(case when day_of_week not in ('Saturday','Sunday') then list_of_days
                when day_of_week = 'Saturday' then prev
                when day_of_week = 'Sunday' then next end)*/
                when day = 24 and month = 12 then list_of_days --Christmas
                when day = 25 and month = 12 then list_of_days --Christmas
                /*(case when day_of_week not in ('Saturday','Sunday') then list_of_days
                when day_of_week = 'Saturday' then prev
                when day_of_week = 'Sunday' then next end)*/
                when month = 9 and day_of_week = 'Monday' and rn_asc = 1 then list_of_days  --first Monday of month, Labor Day
                when month = 5 and day_of_week = 'Monday' and rn_desc = 1 then list_of_days --last Monday of month, Memorial Day
                when month = 11 and day_of_week = 'Thursday' and rn_asc = 4 then list_of_days --fourth Thursday of month, Thanksgiving Day
            end as holiday
        from
            (select
                list_of_days,
                extract(day from list_of_days) as day,
                extract(month from list_of_days) as month,
                format_datetime('%A', list_of_days) as day_of_week,
                lag(list_of_days) over (order by list_of_days) as prev,
                lead(list_of_days) over (order by list_of_days) as next,
                row_number() over (partition by date_trunc(list_of_days, month), format_datetime('%A', list_of_days)) as rn_asc,
                row_number() over (
                    partition by date_trunc(list_of_days, month), format_datetime('%A', list_of_days) order by list_of_days desc
                ) as rn_desc
                from
                    (select *
                        from
                            unnest(
                                generate_date_array(date('2023-01-01'), last_day(current_date('America/Los_Angeles'), year), interval 1 day)
                            ) as list_of_days
                    )
            )
        order by list_of_days)
    where holiday is not null
)

select
    period,
    period_type,
    sum(case when type = 'numerator' then day_weight else 0 end) / nullif(sum(case when type = 'denominator' then day_weight else 0 end), 0) as ratio
from (
        select
            *,
            case when
                (case when extract(dayofweek from list_of_days) = 1 then 7
                                   else extract(dayofweek from list_of_days) - 1 end) in (6, 7) then 0.5
                else 1 end as day_weight
        from (
            --numerator - actual days and denominator - anticipated days in a period
            --month
            select
                *,
                'numerator' as type,
                date_trunc(current_date('America/Los_Angeles'), month) as period,
                'month' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), month), current_date('America/Los_Angeles') - 1, interval 1 day
                    )
                ) as list_of_days
            union all
            select
                *,
                'denominator' as type,
                date_trunc(current_date('America/Los_Angeles'), month) as period,
                'month' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), month), last_day(current_date('America/Los_Angeles'), month), interval 1 day
                    )
                ) as list_of_days
            union all
            --quarter
            select
                *,
                'numerator' as type,
                date_trunc(current_date('America/Los_Angeles'), quarter) as period,
                'quarter' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), quarter), current_date('America/Los_Angeles') - 1, interval 1 day
                    )
                ) as list_of_days
            union all
            select
                *,
                'denominator' as type,
                date_trunc(current_date('America/Los_Angeles'), quarter) as period,
                'quarter' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), quarter),
                        last_day(current_date('America/Los_Angeles'), quarter),
                        interval 1 day
                    )
                ) as list_of_days
            union all
            --year
            select
                *,
                'numerator' as type,
                date_trunc(current_date('America/Los_Angeles'), year) as period,
                'year' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), year), current_date('America/Los_Angeles') - 1, interval 1 day
                    )
                ) as list_of_days
            union all
            select
                *,
                'denominator' as type,
                date_trunc(current_date('America/Los_Angeles'), year) as period,
                'year' as period_type
            from
                unnest(
                    generate_date_array(
                        date_trunc(current_date('America/Los_Angeles'), year), last_day(current_date('America/Los_Angeles'), year), interval 1 day
                    )
                ) as list_of_days
        ) as dates
    ) as dates_with_weights
where list_of_days not in (select date_holiday from holidays) --holidays
group by period, period_type;

/* All actuals, targets, target percentage achieved and ratio for all periods */
create temp table values_data as
with cross_joined_metrics as (
    select
        m.id,
        m.code,
        m.short_label,
        m.is_up,
        m.is_cumulative,
        t.period_type,
        t.period
    from warehouse.br_metric as m
    cross join periods as t
    where m.deleted_at is null
)

select
    t.code,
    t.short_label,
    t.period_type,
    t.period,
    if(v.value is null, 0, v.value) as value,
    if(mt.value is null, 0, mt.value) as target,
    case
        when t.is_up is true then
            coalesce((if(v.value is null, 0, v.value) / nullif(if(mt.value is null, 0, mt.value), 0)), 0)
        when t.is_up is false then
            coalesce((if(mt.value is null, 0, mt.value) / nullif(if(v.value is null, 0, v.value), 0)), 0)
        else 0 end as target_perc,
    t.is_up,
    t.is_cumulative,
    r.ratio
from cross_joined_metrics as t
left join warehouse.br_period as p on p.type = t.period_type
    and p.starting_at = t.period
left join warehouse.br_metric_value as v on v.metric_id = t.id
    and v.period_id = p.id
left join warehouse.br_metric_target as mt on mt.metric_id = t.id
    and mt.period_id = p.id
left join current_period_ratio as r on r.period = t.period
    and r.period_type = t.period_type;

/* Projected values for current period */
create temp table current_projection as
with current_day_values as (
    select
        code,
        value
    from values_data
    where period_type = 'day'
        and period = date_trunc(current_date('America/Los_Angeles'), day)
),

last_months_data as (
    select
        code,
        current_month_proj,
        last2months,
        last11months
    from (
        select
            v.code,
            v.period,
            v.ratio,
            coalesce((v.value - c.value) / nullif(v.ratio, 0), 0) as current_month_proj,
            sum(v.value) over (partition by v.code order by v.period rows between 2 preceding and 1 preceding) as last2months,
            sum(v.value) over (partition by v.code order by v.period rows between unbounded preceding and 1 preceding) as last11months
        from values_data as v
        left join current_day_values as c on c.code = v.code
        where
            date_trunc(v.period, year) = date_trunc(current_date(), year)
            and
            v.period_type = 'month'
    )
    where ratio is not null
),

last_quarters_data as (
    select
        code,
        current_quarter_proj,
        last3quarters
    from (
        select
            v.code,
            v.period,
            v.ratio,
            coalesce((v.value - c.value) / nullif(v.ratio, 0), 0) as current_quarter_proj,
            sum(v.value) over (partition by v.code order by v.period rows between unbounded preceding and 1 preceding) as last3quarters
        from values_data as v
        left join current_day_values as c on c.code = v.code
        where
            date_trunc(v.period, year) = date_trunc(current_date(), year)
            and
            v.period_type = 'quarter'
    )
    where ratio is not null
),

projection_period_calc as (
    select
        d.code,
        d.short_label,
        d.period_type,
        d.period,
        d.target_perc,
        d.is_up,
        d.is_cumulative,
        c.value as current_day_value,
        ratio,
        if(d.value is null, 0, d.value) as value,
        if(d.target is null, 0, d.target) as target,
        case
            when
                period_type = 'year' and extract(
                    month from current_date('America/Los_Angeles')
                ) = 12 then coalesce(m.last11months + m.current_month_proj, 0)
            when
                period_type = 'year' and extract(
                    quarter from current_date('America/Los_Angeles')
                ) = 4 then coalesce(q.last3quarters + q.current_quarter_proj, 0)
            when
                period_type = 'quarter' and date_diff(
                    date_trunc(current_date('America/Los_Angeles'), month), period, month
                ) = 2 then coalesce(m.last2months + m.current_month_proj, 0)
            else coalesce((d.value - c.value) / nullif(d.ratio, 0), 0)
        end as projection_value
    from values_data as d
    left join last_months_data as m on m.code = d.code
    left join last_quarters_data as q on q.code = d.code
    left join current_day_values as c on c.code = d.code
    where ratio is not null
)

select
    d.code,
    d.short_label,
    d.period_type,
    d.period,
    d.value,
    d.target,
    d.target_perc,
    case
        when d.is_cumulative is true then d.value
        when d.is_cumulative is false then d.projection_value
        else 0
    end as projection,
    case
        when d.is_up is true and d.is_cumulative is true then coalesce(d.value / nullif(d.target, 0), 0)
        when d.is_up is false and d.is_cumulative is true then coalesce(d.target / nullif(d.value, 0), 0)
        when d.is_up is true and d.is_cumulative is false then coalesce(d.projection_value / nullif(d.target, 0), 0)
        when d.is_up is false and d.is_cumulative is false then coalesce(d.target / nullif(d.projection_value, 0), 0)
        else 0
    end as project_perc,
    current_day_value,
    ratio
from projection_period_calc as d;

insert into int_data_tests.br_metric_flatten_proj
select
    *except (ratio),
    current_datetime('America/Los_Angeles') as load_dt,
    ratio
from current_projection;

delete from warehouse.br_metric_flatten where true;
insert into warehouse.br_metric_flatten
--periods headers
select
    '' as code,
    '' as short_label,
    period_type,
    string_agg(cast(period as string) order by period asc) as list_of_values
from periods
group by 1, 2, 3
union all
--last 29 days - only actual
select
    code,
    short_label,
    period_type,
    string_agg(cast(value as string) order by period asc) as list_of_values
from values_data
where period_type = 'day'
group by 1, 2, 3
union all
--last 18 weeks - only actual
select
    code,
    short_label,
    period_type,
    string_agg(cast(value as string) order by period asc) as list_of_values
from values_data
where period_type = 'week'
group by 1, 2, 3
union all
--last 14 months 
select
    code,
    short_label,
    period_type,
    string_agg(cast(value as string) order by period asc, sequence asc) as list_of_values
from (
        --last 13 months (excluding current month) - %/target/actual 
        select
            code,
            short_label,
            period_type,
            period,
            target_perc as value,
            '1' as sequence
        from values_data
        where period_type = 'month'
            and period != date_trunc(current_date('America/Los_Angeles'), month)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '2' as sequence
        from values_data
        where period_type = 'month'
            and period != date_trunc(current_date('America/Los_Angeles'), month)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '3' as sequence
        from values_data
        where period_type = 'month'
            and period != date_trunc(current_date('America/Los_Angeles'), month)
        --for current month - actual/projection/%/target
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '1' as sequence
        from values_data
        where period_type = 'month'
            and period = date_trunc(current_date('America/Los_Angeles'), month)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            projection as value,
            '2' as sequence
        from current_projection
        where period_type = 'month'
            and period = date_trunc(current_date('America/Los_Angeles'), month)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            project_perc as value,
            '3' as sequence
        from current_projection
        where period_type = 'month'
            and period = date_trunc(current_date('America/Los_Angeles'), month)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '4' as sequence
        from values_data
        where period_type = 'month'
            and period = date_trunc(current_date('America/Los_Angeles'), month)
)
group by 1, 2, 3
union all
--last 11 quarters 
select
    code,
    short_label,
    period_type,
    string_agg(cast(value as string) order by period asc, sequence asc) as list_of_values
from (
        --last 10 quarters (excluding current quarter) - %/target/actual 
        select
            code,
            short_label,
            period_type,
            period,
            target_perc as value,
            '1' as sequence
        from values_data
        where period_type = 'quarter'
            and period != date_trunc(current_date('America/Los_Angeles'), quarter)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '2' as sequence
        from values_data
        where period_type = 'quarter'
            and period != date_trunc(current_date('America/Los_Angeles'), quarter)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '3' as sequence
        from values_data
        where period_type = 'quarter'
            and period != date_trunc(current_date('America/Los_Angeles'), quarter)
        --for current quarter - actual/projection/%/target
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '1' as sequence
        from values_data
        where period_type = 'quarter'
            and period = date_trunc(current_date('America/Los_Angeles'), quarter)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            projection as value,
            '2' as sequence
        from current_projection
        where period_type = 'quarter'
            and period = date_trunc(current_date('America/Los_Angeles'), quarter)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            project_perc as value,
            '3' as sequence
        from current_projection
        where period_type = 'quarter'
            and period = date_trunc(current_date('America/Los_Angeles'), quarter)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '4' as sequence
        from values_data
        where period_type = 'quarter'
            and period = date_trunc(current_date('America/Los_Angeles'), quarter)
)
group by 1, 2, 3
union all
--last 6 years
select
    code,
    short_label,
    period_type,
    string_agg(cast(value as string) order by period asc, sequence asc) as list_of_values
from (
        --last 5 years (excluding current year) - %/target/actual 
        select
            code,
            short_label,
            period_type,
            period,
            target_perc as value,
            '1' as sequence
        from values_data
        where period_type = 'year'
            and period != date_trunc(current_date('America/Los_Angeles'), year)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '2' as sequence
        from values_data
        where period_type = 'year'
            and period != date_trunc(current_date('America/Los_Angeles'), year)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '3' as sequence
        from values_data
        where period_type = 'year'
            and period != date_trunc(current_date('America/Los_Angeles'), year)
        --for current year - actual/projection/%/target
        union all
        select
            code,
            short_label,
            period_type,
            period,
            value,
            '1' as sequence
        from values_data
        where period_type = 'year'
            and period = date_trunc(current_date('America/Los_Angeles'), year)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            projection as value,
            '2' as sequence
        from current_projection
        where period_type = 'year'
            and period = date_trunc(current_date('America/Los_Angeles'), year)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            project_perc as value,
            '3' as sequence
        from current_projection
        where period_type = 'year'
            and period = date_trunc(current_date('America/Los_Angeles'), year)
        union all
        select
            code,
            short_label,
            period_type,
            period,
            target as value,
            '4' as sequence
        from values_data
        where period_type = 'year'
            and period = date_trunc(current_date('America/Los_Angeles'), year)
)
group by 1, 2, 3;

drop table periods;
drop table current_period_ratio;
drop table values_data;
drop table current_projection;

commit transaction;
