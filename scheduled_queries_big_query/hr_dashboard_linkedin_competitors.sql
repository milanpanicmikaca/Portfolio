with raw_data as (
    select date_trunc(month, month) as month, *except(month) from ext_marketing.linkedin_competitors_history
    union all
    select date_trunc(month, month) as month, *except(month) from ext_marketing.linkedin_competitors
    where date_trunc(month, month) <= date_trunc(current_date(), month)
),

raw_data_ext as (
    select
        * except(followers, employees),
        last_value(
            followers ignore nulls
        ) over (partition by company_name order by month rows between unbounded preceding and current row) as followers,
        employees,
        case when month > date_sub(date_trunc(current_date(), month), interval 12 month) then 1 else 0 end as flag_last12m,
        case when date_trunc(month, quarter) = date_trunc(current_date(), quarter) then 1 else 0 end as flag_last_quarter
    from raw_data
)

select
    *,
    lag(followers) over (partition by company_name order by month) as prev_followers,
    lag(employees) over (partition by company_name order by month) as prev_employees
from raw_data_ext
where month >= date(2019, 1, 1)
