-- upload to BQ
with
calc_staff_log
as
(
select
        hsl.id,
        hsl.full_name,
        hsl.staff_id,
        hsl.change_type,
        hsl.effective_date as start_date,
        hsp.internal_title as title,
        hd.name as department,
        hl.name as ladder_name,
        case when 
                lag (internal_title) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id) ilike '%bootcamp%' and 
                lag (change_type) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id) = 'hired' and
                change_type = 'changed' then 'hired' 
        when 
                change_type = 'hired' and internal_title not ilike '%bootcamp%' then 'hired'
        when change_type = 'left' and internal_title not ilike '%bootcamp%' then 'left' end as real_status
from hrm_stafflog hsl 
left join hrm_staff hs on hs.id = hsl.staff_id
left join hrm_staffposition hsp on hsp.id = hsl.position_id
left join hrm_ladder hl on hl.id = hsp.ladder_id
left join hrm_department hd on hd.id = hl.department_id
where
        hs.is_staff is true
        --and hsl.effective_date > '2018-04-15' 
        --and hsp.internal_title not ilike '%bootcamp%'
        and hsl.full_name <> 'Yannis Karamanlakis'
        and lower(full_name) not like '%test%'
        and (internal_title ilike '%bootcamp%' and change_type = 'changed') is not true 
)
select
        date_trunc ('{period}', start_date)::date as date,
        count (real_status) filter (where real_status = 'hired') as HRS101,
        count (real_status) filter (where real_status = 'left') as HRS102
from calc_staff_log
group by 1
