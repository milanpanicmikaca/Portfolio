with timeseries as (
    select
        date_trunc(date, day) as date
    from `bigquerydatabase-270315.warehouse.generate_date_series`
    where date_trunc(date, day) >= '2018-04-01'
        and date_trunc(date, day) <= date_trunc(current_date('America/Los_Angeles'), day)
),

-- daily non categorical data for wins from order_ue (no order_id)
order_ue_data as (
    select *
    --count(distinct order_id) wins, 
    --sum(first_approved_price) as rev, 
    from int_data.order_ue_materialized
    where won_at is not null
),

cross_join as (
    select
        t.date,
        order_id,
        segment
    from timeseries t
    cross join order_ue_data
),

date_join as (
    select
        cj.*,
        ued.order_id as ue_order_id,
        ued.segment as ue_segment,
        ued.* except (order_id, segment)
    --coalesce(fw.wins,0) as wins,
    --coalesce(fw.rev,0) as rev,
    from cross_join cj
    left join order_ue_data as ued on ued.won_at = cj.date
                                      and ued.order_id = cj.order_id
                                      and ued.segment = cj.segment
),

trailing_data as (
    select
        *,
        count(ue_order_id) over (partition by order_id order by date asc rows between 6 preceding and current row) as wins_7d,
        count(ue_order_id) over (partition by order_id order by date asc rows between 27 preceding and current row) as wins_28d,
        sum(first_approved_price) over (partition by order_id order by date asc rows between 6 preceding and current row) as rev_7d,
        sum(first_approved_price) over (partition by order_id order by date asc rows between 27 preceding and current row) as rev_28d
    from date_join
)

select *,
       wins_28d * 13 / 12 as win_mrr,
       rev_28d * 13 / 12 as rev_mrr,
       rev_7d / nullif(wins_7d, 0) as size_7d,
       rev_28d / nullif(wins_28d, 0) as size_28d
from trailing_data
