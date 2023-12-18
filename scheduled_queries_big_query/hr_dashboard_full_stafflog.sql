with hrm_stafflog as (
select 
  * 
from ergeon.hrm_stafflog s 
--where full_name = 'Afaq Ahmad' 
qualify rank() over (partition by staff_id, effective_date order by id desc) = 1
order by effective_date, id
), stafflog as (
    select
        hsl.id,
        hsl.effective_date,
        --coalesce(lead(hsl.effective_date) over (partition by hsl.staff_id order by hsl.effective_date),current_date()) as end_date,
        hsl.staff_id,
        hsl.full_name,
        coalesce(lag(hsl.change_type) over (partition by hsl.staff_id order by hsl.effective_date),"-") as prev_change,
        hsl.change_type,
        coalesce(lag(hsp.level) over (partition by hsl.staff_id order by hsl.effective_date),0) as prev_level,
        hsp.level,
        coalesce(lag(hsp.internal_title) over (partition by hsl.staff_id order by hsl.effective_date),"-") as prev_position,
        hsp.internal_title as position,
        coalesce(lag(hl.name) over (partition by hsl.staff_id order by hsl.effective_date),"-") as prev_ladder,
        hl.name as ladder,
        coalesce(lag(hd.name) over (partition by hsl.staff_id order by hsl.effective_date),"-") as prev_department,
        hd.name as department,
    from hrm_stafflog hsl 
        left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
        left join ergeon.hrm_ladder hl on hl.id = hsp.ladder_id
        left join ergeon.hrm_department hd on hd.id = hl.department_id
), final as (
select
    id,
    effective_date,
    staff_id,
    full_name,
    change_type,
    --prev_position,
    position,
    ladder,
    department,
        case
      when lower(position) like '%estimation%'and lower(position) not like '%bootcamp%' then 'HRS106' --quoter
      when lower(position) in ('estimation team specialist bootcamp') then 'HRS122' --quote bootcamp
      when lower(position) in ('junior estimation team specialist') and change_type <>'left' then 'HRS123' -- junior estimator
      when lower(position) in ('estimation team specialist','senior estimation team specialist') and change_type <>'left' then 'HRS124' -- Sepacialist
      when lower(position) in ('expert estimator') then 'HRS125' --expert,  
      when lower(position) in ('construction subject matter expert: estimation') and change_type <>'left' then 'HRS126'  --subject matter expert,
      when lower(position) in('estimation team lead','estimation associate team lead','estimation house head') then 'HRS127' --leaders
      when lower(ladder) = 'design' then 'HRS107' --designer
      when lower(position) = 'onsite estimator' and lower(position) not like '%bootcamp%' then 'HRS109' --photographer
      when lower(position) like '%project manager%' and lower(position) not like '%bootcamp%' and lower(position) not like '%senior%' and not lower(position) like '%team%' and lower(department) like 'operations' then 'HRS110'
      when lower(position) like '%customer specialist%' and lower(position) not like '%bootcamp%' and lower(position) not like '%junior%' 
        or lower(position) like 'senior launch customer specialist' and lower(position) not like '%bootcamp%' then 'HRS105' --cs_count
      when lower(position) = 'key accounts manager' then 'HRS118' --KAM
      when lower(position) = 'field account manager' then 'HRS119' --FAM
      when lower(position) = 'sales house head' then 'HRS120' --HH
      when lower(position) like '%customer service%' and lower(position) not like '%bootcamp%' then 'HRS108'--csr
      when lower(position) like 'quality assurance analyst'
        or lower(position) like 'senior quality assurance analyst' then 'HRS114'--QA
      when ladder = 'Engineering' and lower(position) not like '%bootcamp%' then 'HRS116' --Engineering count
      when lower(position) = 'regional field manager' then 'HRS117'--RM
    else null end as br_code,
    case when lower(position) like '%bootcamp%' and change_type <> 'left' then 1 else 0 end as is_bootcamp,
    case when (lower(prev_position) like '%bootcamp%' or prev_position = '-') and lower(position) not like '%bootcamp%' and change_type <> 'left' and prev_level <= coalesce(level,1) then effective_date end as hire_date,
    case when (lower(prev_position) like '%bootcamp%' or prev_position = '-') and lower(position) not like '%bootcamp%' and change_type <> 'left' and prev_level <= coalesce(level,1) then 1 else 0 end as is_hire,
    case when level > prev_level and lower(prev_position) not like '%bootcamp%' and lower(prev_position) <> '-' and lower(position) not like '%bootcamp%' then 1 else 0 end as is_promotion,
    case when change_type = 'left' and lower(position) not like '%bootcamp%' then effective_date end as turnover_date,
    case when change_type = 'left' and lower(position) not like '%bootcamp%' then 1 else 0 end as is_turnover,
    case when prev_department <> department and change_type <> 'left' and prev_department <> '-' then 1 else 0 end as is_attrition
from stafflog s
)
select 
  f.*,
  bm.short_label,
  case
		when is_bootcamp = 1 then 'bootcamp'
		when is_hire = 1 then 'hire'
		when is_promotion = 1 then 'promotion'
		when is_attrition = 1 then 'attrition'
		when is_turnover = 1 then 'turnover'
  else null end as type
from final f
left join warehouse.br_metric bm on bm.code = f.br_code
where not (is_bootcamp = 0 and is_hire = 0 and is_promotion = 0 and is_attrition = 0 and is_turnover = 0)
order by full_name, effective_date, id