with split_month_pto as (
    --pto that are splitted in multiple months
    select
        id,
        start_date,
        first_day_back,
        days_requested,
        staff_id,
        last_day(start_date) as end_month,
        date_add(first_day_back, interval -1 day) as end_date, --exclude day that member comes back
        date_trunc(first_day_back, month) as start_month
    from ergeon.hrm_stafftimeoffrequest
    where status_id = 48
        and start_date <= current_date
        and date_trunc(start_date, month) <> date_trunc(first_day_back, month)
        and start_date >= '2023-03-01'
        and deleted_at is null
),

split_pto as (
    --week days from start date until end of month
    select
        id,
        start_date as day,
        date_trunc(start_date, month) as month,
        coalesce(count(*), 0) as weekdays
    from split_month_pto
    cross join unnest(generate_date_array(start_date, end_month)) as date
    where
        extract(dayofweek from date) not in (1, 7)
    group by 1, 2, 3

    union all
    --week days from start of month until last day of pto
    select
        id,
        first_day_back as day,
        start_month,
        coalesce(count(*), 0) as weekdays
    from split_month_pto
    cross join unnest(generate_date_array(start_month, end_date)) as date
    where
        extract(dayofweek from date) not in (1, 7)
    group by 1, 2, 3
),

pto_split_aggr as (
    select
        pto.id,
        pto.start_date,
        pto.first_day_back,
        pto.days_requested,
        sps.weekdays + spe.weekdays as total_weekdays, --won't be needed. This is the total weekdays of pto (excluding weekends)
        round((sps.weekdays / (sps.weekdays + spe.weekdays)) * pto.days_requested, 1) as pto_start, --pto for month pto started 
        round((spe.weekdays / (sps.weekdays + spe.weekdays)) * pto.days_requested, 1) as pto_end, --pto for month pto ended
        staff_id
    from split_month_pto pto
    --join by month pto start
    left join split_pto sps on sps.id = pto.id
        and sps.day = pto.start_date
    --join by month pto end
    left join split_pto spe on spe.id = pto.id
        and spe.day = pto.first_day_back
--example, if I got 3 days pto. starts = 31 March return date 5 April so until 4 April. Then I count 1 day in March and 2 in April
),

pto_aggr as (
    --aggregate by using start_date
    select
        staff_id,
        date_trunc(start_date, month) as month,
        sum(if(pto_start is not null, pto_start, days_requested)) as pto_days
    from pto_split_aggr
    group by 1, 2

    union all
    --aggregate by using first_day_back when we capture pto days on 2nd month of splitted pto
    select
        staff_id,
        date_trunc(first_day_back, month) as month,
        sum(pto_end) as pto_days
    from pto_split_aggr
    where pto_end is not null
    group by 1, 2

    union all
    --aggregate rest ptos that are for only one month
    select
        staff_id,
        date_trunc(start_date, month) as month,
        sum(days_requested) as pto_days
    from ergeon.hrm_stafftimeoffrequest
    where status_id = 48
        and start_date <= current_date
        and date_trunc(start_date, month) = date_trunc(first_day_back, month)
        and start_date >= '2023-03-01'
        and deleted_at is null
    group by 1, 2
)

select
    staff_id,
    month,
    sum(pto_days) as pto_days
from pto_aggr
where month >= '2023-04-01'
group by 1, 2
