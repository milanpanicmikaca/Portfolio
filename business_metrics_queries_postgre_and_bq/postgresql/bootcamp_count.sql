-- upload to BQ
with
timeseries as
(
        select generate_series (('2018-04-16'), (current_date), interval '1 day') as day
),
stafflog as
(
select
        rank() over (partition by staff_id order by hs.created_at desc) as rank,
        hs.created_at at time zone 'PDT' as effective_ts,
        hs.full_name,
        hs.staff_id,
        hs.change_type,
        lead(change_type) over(partition by staff_id order by hs.created_at, hs.id) as next_change_type,
        hp.internal_title,
        hl.name as ladder_name,
        coalesce(lead(hs.created_at at time zone 'PDT') over (partition by staff_id order by hs.created_at,hs.id),current_date) as next_change_date
from hrm_stafflog hs
left join hrm_staffposition hp on hp.id = hs.position_id
left join hrm_staff hr on hr.id = hs.staff_id
left join hrm_ladder hl on hl.id = hp.ladder_id
where is_staff is true
order by full_name, effective_date
),
calc_data
as
(
select
        *,
        case when (date_trunc('day', effective_ts) <= day and day <= next_change_date) then 1 else 0 end as is_current
from timeseries
cross join stafflog
where change_type <> 'left'
and internal_title like '%Bootcamp%'
),
ranked_data
as
(
select
        day,
        sum(is_current) as bootcamp_count,
        sum(case when internal_title like '%Senior Customer Specialist%' then 1 else 0 end) as senior_csr_bootcamp_count,
        sum(case when (ladder_name = 'Quality Assurance Engineering'
                                or ladder_name = 'Engineering') then 1 else 0 end) as engineer_bootcamp_count,
        rank() over (partition by date_trunc('{period}',day) order by day desc) as rank
from calc_data 
where is_current = 1
group by 1
order by day desc
)
select
        date_trunc('{period}',day)::date as date,
        bootcamp_count as HRS103,
        senior_csr_bootcamp_count as HRS112,
        engineer_bootcamp_count as HRS115
from ranked_data
where rank = 1
