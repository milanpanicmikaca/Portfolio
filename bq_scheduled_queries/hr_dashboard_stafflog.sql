with staff as (
    select
        hsl.id,
        u.full_name,
        hsl.email,
        hsl.staff_id,
        hsl.position_id,
        gc.name as country,
        u2.full_name as manager,
        hsp.internal_title,
        hd.name as department,
        hl.name as ladder_name,
    from ergeon.hrm_staff hs
    left join ergeon.hrm_stafflog hsl on hsl.id = hs.current_stafflog_id
    left join ergeon.geo_address ga on ga.id = hsl.address_id
    left join ergeon.geo_country gc on gc.id = ga.country_id
    left join ergeon.core_user u on u.id = hs.user_id
    left join ergeon.hrm_staff hs2 on hs2.id = hsl.manager_id
    left join ergeon.core_user u2 on u2.id = hs2.user_id
    left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
    left join ergeon.hrm_ladder hl on hl.id = hsp.ladder_id
    left join ergeon.hrm_department hd on hd.id = hl.department_id
    where hsl.id is not null
), first_changes as (
  select 
    hsl.staff_id,
    min(case when lower(hsp.internal_title) not like '%bootcamp%' and change_type = 'left' then effective_date else null end) as first_term_date,
    min(case when lower(hsp.internal_title) not like '%bootcamp%' and change_type <> 'left' then hsl.effective_date end) as first_hire_date,
    min(case when lower(hsp.internal_title) like '%bootcamp%' then hsl.effective_date end) as first_bootcamp_date,
    min(case when lower(hsp.internal_title) not like '%bootcamp%' and change_type <> 'left' and commitment = 'ft' then hsl.effective_date end) as first_ft_date,
  from ergeon.hrm_stafflog hsl
  left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
  group by 1
), last_changes as (
  select
    hsl.staff_id,
    max(case when lower(hsp.internal_title) not like '%bootcamp%' and change_type = 'left' then effective_date else null end) as last_term_date,
    max(case when lower(hsp.internal_title) like '%bootcamp%' then hsl.effective_date end) as last_bootcamp_date,
  from ergeon.hrm_stafflog hsl
  left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
  left join first_changes fc using (staff_id)
  group by 1
), last_hire_date as (
  select 
  hsl.staff_id,
  min(case when lower(hsp.internal_title) not like '%bootcamp%' and change_type <> 'left' then hsl.effective_date end) as last_hire_date,
  from ergeon.hrm_stafflog hsl
  left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
  left join first_changes fc using (staff_id)
  where effective_date > first_term_date
  group by 1
), change_date_final as (
select 
  s.staff_id,
  s.full_name,
  fc.first_hire_date,
  lhd.last_hire_date,
  fc.first_term_date,
  lc.last_term_date,
  --s.* except (id, staff_id,position_id),
  case when ifnull(lhd.last_hire_date,fc.first_hire_date) >= fc.first_hire_date then ifnull(lhd.last_hire_date,fc.first_hire_date) end as hire_date,
  case when ifnull(lc.last_term_date,fc.first_term_date) >= fc.first_term_date then ifnull(lc.last_term_date,fc.first_term_date) end as term_date,
  case when fc.first_term_date < ifnull(lc.last_term_date,fc.first_term_date) then first_term_date end as previous_term_date,
  case when ifnull(lc.last_bootcamp_date,fc.first_bootcamp_date) >= fc.first_bootcamp_date then ifnull(lc.last_bootcamp_date,fc.first_bootcamp_date) end as bootcamp_date,
  first_ft_date,
from staff s
  left join first_changes fc using (staff_id)
  left join last_changes lc using (staff_id)
  left join last_hire_date lhd using (staff_id)
), change_date as (
select 
  staff_id,
  full_name,
  hire_date,
  term_date,
  bootcamp_date,
  from change_date_final c
)
    select
        hsl.id,
        hsl.effective_date,
        u.full_name,
        hsl.email,
        hsl.staff_id,
        hsl.position_id,
        gc.name as country,
        u2.full_name as manager,
        hsp.internal_title,
        hd.name as department,
        hl.name as ladder_name,
        case when lower(hsp.internal_title) like "%bootcamp%" then 1 else 0 end as is_bootcamp,
        case 
            when hsp.level > coalesce(lag(hsp.level) over (partition by hsl.staff_id order by hsl.effective_date),0) 
            and lower(lag(hsp.internal_title) over (partition by hsl.staff_id order by hsl.effective_date)) not like '%bootcamp%' then 1 else 0 end as is_promotion,
        case 
            when hsp.level > coalesce(lag(hsp.level) over (partition by hsl.staff_id order by hsl.effective_date),0) 
            and lower(lag(hsp.internal_title) over (partition by hsl.staff_id order by hsl.effective_date)) not like '%bootcamp%' 
            and hd.name <> lag(hd.name) over (partition by hsl.staff_id order by hsl.effective_date) then 1 else 0 
        end as is_attrition,
        case 
            when hsl.change_type <> 'left' 
            and row_number() over(partition by hsl.staff_id order by effective_date desc, hsl.id desc) = 1 then 1 else 0 
        end as is_active,
        c.hire_date,
        c.term_date
    from ergeon.hrm_stafflog hsl 
    left join ergeon.hrm_staff hs on hs.id = hsl.staff_id
    left join ergeon.geo_address ga on ga.id = hsl.address_id
    left join ergeon.geo_country gc on gc.id = ga.country_id
    left join ergeon.core_user u on u.id = hs.user_id
    left join ergeon.hrm_staff hs2 on hs2.id = hsl.manager_id
    left join ergeon.core_user u2 on u2.id = hs2.user_id
    left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
    left join ergeon.hrm_ladder hl on hl.id = hsp.ladder_id
    left join ergeon.hrm_department hd on hd.id = hl.department_id
    left join change_date c on c.staff_id = hsl.staff_id
    where hsl.id is not null
    order by 3, 2 
