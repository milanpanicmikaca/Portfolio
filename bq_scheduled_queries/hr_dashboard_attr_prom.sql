with timeseries as (
    select 
        date_trunc(date_array,day) as date,
        department as department, 
        title,
        ladder_name,
        br_code,
        from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array, int_data.hr_dashboard h
        group by 1, 2, 3, 4, 5
), positions as (
    select
        hsl.effective_date,
        hsl.staff_id,
        hsp.level,
        hsp.internal_title as title, 
        hd.name as department,
        hsl.change_type,
        hl.name as ladder_name,
        lag(hsp.level) over (partition by staff_id order by effective_date) as previous_level,
        lag(hd.name) over (partition by staff_id order by effective_date) as previous_dept,
        lag(hsp.internal_title) over (partition by staff_id order by effective_date) as previous_title,
        lag(hl.name) over (partition by staff_id order by effective_date) as previous_ladder,
        coalesce((lead (effective_date) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id)), current_date()) as end_date,
    from ergeon.hrm_stafflog hsl
        left join ergeon.hrm_staff hs on hs.id = hsl.staff_id
        left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
        left join ergeon.hrm_ladder hl on hl.id = hsp.ladder_id
        left join ergeon.hrm_department hd on hd.id = hl.department_id
        left join ergeon.core_user u on u.id = hs.user_id
    where lower(u.email) not like '%test%'
), staff_c as (
select 
    t.date,
    t.department,
    t.title,
    t.ladder_name,
    count(*) as staff,
from timeseries t
left join positions p on cast(p.effective_date as date) <= cast(t.date as date) 
    and cast(p.end_date as date) >= cast(t.date as date)
    and t.department = p.department
    and t.title = p.title
    and t.ladder_name = p.ladder_name
where change_type <> 'left'
and lower(p.title) not like '%bootcamp%'
group by 1, 2, 3, 4
order by 1 desc
),hires as (
    select
        hire_date as date,
        department,
        title,
        ladder_name,
        count(*) as hires
    from int_data.hr_dashboard h
    where hire_date is not null
    group by 1, 2, 3, 4
), term as (
    select
        term_date as date,
        department, 
        title,
        ladder_name,
        count(*) as term
    from int_data.hr_dashboard h
    where term_date is not null
    group by 1, 2, 3, 4
), promotions as (
    select 
        effective_date as date,
        department,
        title,
        ladder_name,
        sum(case when previous_level < level then 1 end) as promotions
    from positions  
    where previous_level < level
    group by 1, 2, 3, 4
),attrition_calc as (
    select
        *,
        rank() over (partition by staff_id, effective_date order by effective_date) as rank
    from positions p
    where lower(title) not like '%bootcamp%'
    and change_type <> 'left'
    and previous_level is not null
    and previous_level < level
), attrition as (
    select 
    effective_date as date,
    previous_dept as department,
    previous_title as title,
    previous_ladder as ladder_name,
    count(*) as attrition,
    from attrition_calc
    where previous_dept <> department
    and rank = 1
    group by 1, 2, 3, 4
), final as (
select 
    ts.date,
    ts.department,
    ts.ladder_name,
    ts.title,
    case
      when lower(title) like '%estimation%'and lower(title) not like '%bootcamp%' then 'HRS106' --quoter
      when lower(title) in ('estimation team specialist bootcamp') then 'HRS122' --quote bootcamp
      when lower(title) in ('junior estimation team specialist') then 'HRS123' -- junior estimator
      when lower(title) in ('estimation team specialist','senior estimation team specialist') then 'HRS124' -- Sepacialist
      when lower(title) in ('expert estimator') then 'HRS125' --expert,  
      when lower(title) in ('construction subject matter expert: estimation') then 'HRS126'  --subject matter expert,
      when lower(title) in('estimation team lead','estimation associate team lead','estimation house head') then 'HRS127' --leaders
      when lower(ladder_name) = 'design' then 'HRS107' --designer
      when lower(title) = 'onsite estimator' and lower(title) not like '%bootcamp%' then 'HRS109' --photographer
      when lower(title) like '%project manager%' and lower(title) not like '%senior%' and not lower(title) like '%team%' then 'HRS110'
      when lower(title) like '%customer specialist%' and lower(title) not like '%bootcamp%' and lower(title) not like '%junior%' 
        or lower(title) like 'senior launch customer specialist' and lower(title) not like '%bootcamp%' then 'HRS105' --cs_count
      when lower(title) = 'key accounts manager' then 'HRS118' --KAM
      when lower(title) = 'field account manager' then 'HRS119' --FAM
      when lower(title) = 'sales house head' then 'HRS120' --HH
      when lower(title) like '%customer service%' and lower(title) not like '%bootcamp%' then 'HRS108'--csr
      when lower(title) like 'quality assurance analyst'
        or lower(title) like 'senior quality assurance analyst' then 'HRS114'--QA
      when s.ladder_name = 'Quality Assurance Engineering' or s.ladder_name = 'Engineering' then 'HRS116' --Engineering count
      when lower(title) = 'regional field manager' then 'HRS117'--RM
      else null end as br_code,
    staff,
    hires,
    term,
    promotions,
    attrition,
    from timeseries ts
    left join staff_c s using(date, department, title, ladder_name)
    left join hires h using(date, department, title, ladder_name)
    left join term t using(date, department, title, ladder_name) 
    left join promotions p using(date, department, title, ladder_name)
    left join attrition a using(date, department, title, ladder_name)

    where coalesce(staff, hires, term, promotions, attrition) is not null
) 
select 
f.*,
bm.short_label,
from final f
left join warehouse.br_metric bm on bm.code = f.br_code
order by 1 desc, 2, 3, 4