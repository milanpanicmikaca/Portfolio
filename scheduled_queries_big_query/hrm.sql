with stafflog as (
    select
        l1.*, rank() over(partition by l1.staff_id order by l1.effective_date) as rank
    from
        ergeon.hrm_stafflog l1
    left join ergeon.hrm_stafflog l2 on l2.id = l1.prior_log_id
    where
        (l1.prior_log_id is null or l2.position_id <> l1.position_id or l2.commitment <> l1.commitment or l1.change_type = 'left')
),

stafflog_data as (
    select
        hsl.*,
        case
            when change_type = 'left' then effective_date
            else coalesce(lead(effective_date) over (partition by staff_id order by rank asc), '2030-01-01')
        end as end_date,
        coalesce(lag(hsp.level) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id), 0) as prev_level,
        coalesce(lag(hsp.internal_title) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id), "-") as prev_position,
        coalesce(lag(hl.name) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id), "-") as prev_ladder,
        coalesce(lag(hd.name) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id), "-") as prev_department,
        coalesce(lag(hsl.commitment) over (partition by hsl.staff_id order by hsl.effective_date, hsl.id), "-") as prev_commitment
    from
        stafflog hsl left join
        ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id left join
        ergeon.hrm_ladder hl on hl.id = hsp.ladder_id left join
        ergeon.hrm_department hd on hd.id = hl.department_id
),

last_change_list as (
    select staff_id, max(effective_date) as last_changed_at from stafflog_data group by 1
),

hire_list as (
    select
        staff_id,
        effective_date as hire_date
    from
        ergeon.hrm_stafflog sl left join
        ergeon.hrm_staffposition p on p.id = sl.position_id
    where
        lower(internal_title) not like '%bootcamp%'
        and commitment <> 'pb'
    qualify rank() over(partition by staff_id order by sl.id) = 1
),

current_data as (
    select
        s.id as staff_id,
        p.level as current_level,
        sl.contract as current_contract,
        team.name as current_team,
        mgrcu.full_name as current_manager,
        co.name as current_country
    from
        ergeon.hrm_staff s join
        ergeon.hrm_stafflog sl on sl.id = s.current_stafflog_id join
        ergeon.hrm_staffposition p on p.id = sl.position_id left join
        ergeon.hrm_staff mgr on mgr.id = s.manager_id left join
        ergeon.core_user mgrcu on mgrcu.id = mgr.user_id left join
        ergeon.hrm_team team on team.id = sl.team_id left join
        ergeon.geo_address ad on ad.id = sl.address_id left join
        ergeon.geo_country co on co.id = ad.country_id
    where s.is_staff
)

select
    u.full_name,
    sd.effective_date as started_at,
    sd.end_date,
    sd.effective_date as changed_at,
    date_diff(case when sd.change_type = 'left' then null
        when end_date = '2030-01-01' then current_date
        else end_date end, sd.effective_date, day) as days_spent,
    sd.id as stafflog_id,
    sd.staff_id,
    d.name as department,
    la.name as ladder,
    internal_title,
    p.level,
    sd.commitment,
    sd.contract,
    sd.pay_method,
    sd.change_type,
    team.name as team,
    mgrsl.full_name as manager,
    co.name as country,
    u.email,
    cs.change_type as current_change_type,
    concat("https://api.ergeon.in/public-admin/hrm/staff/", s.id, "/change/") as url,
    concat("https://api.ergeon.in/public-admin/hrm/staff/", sd.id, "/change/") as sl_url,
    rank() over(partition by sd.staff_id order by sd.effective_date desc, sd.id desc) as rank,
    hire_date,
    last_changed_at,
    case
        when lower(internal_title) like '%bootcamp%' then 'bootcamp'
        when
            (
                lower(prev_position) like '%bootcamp%' or prev_position = '-'
            ) and lower(internal_title) not like '%bootcamp%' and sd.change_type <> 'left' and prev_level <= coalesce(level, 1) then 'hire'
        when
            level > prev_level and lower(
                prev_position
            ) not like '%bootcamp%' and lower(prev_position) <> '-' and lower(internal_title) not like '%bootcamp%' then 'promotion'
        when sd.change_type = 'left' and lower(internal_title) not like '%bootcamp%' then 'turnover'
        when prev_department <> d.name and sd.change_type <> 'left' and prev_department <> '-' then 'attrition'
        when
            level is null and prev_level = 0 and prev_position <> internal_title and lower(
                prev_position
            ) not like '%bootcamp%' and lower(prev_position) <> '-' and lower(internal_title) not like '%bootcamp%' then 'promotion'
        when prev_commitment <> sd.commitment then 'commitment'
        when (prev_ladder <> la.name or prev_position <> internal_title) and sd.change_type <> 'left' and prev_ladder <> '-' then 'attrition'
        else null
    end as type,
    prev_level,
    prev_position,
    prev_commitment,
    s.shipping_address,
    current_level,
    current_contract,
    current_team,
    current_manager,
    current_country
from
    stafflog_data sd join
    ergeon.hrm_staff s on sd.staff_id = s.id join
    ergeon.core_user u on s.user_id = u.id join
    ergeon.hrm_staffposition p on p.id = sd.position_id join
    ergeon.hrm_ladder la on la.id = p.ladder_id join
    ergeon.hrm_department d on d.id = la.department_id left join
    ergeon.hrm_staff mgr on mgr.id = sd.manager_id left join
    ergeon.hrm_stafflog mgrsl on mgrsl.id = mgr.current_stafflog_id left join
    ergeon.hrm_team team on team.id = sd.team_id left join
    ergeon.geo_address ad on ad.id = sd.address_id left join
    ergeon.geo_country co on co.id = ad.country_id left join
    ergeon.hrm_stafflog cs on cs.id = s.current_stafflog_id left join
    last_change_list lc on sd.staff_id = lc.staff_id left join
    hire_list hl on sd.staff_id = hl.staff_id left join
    current_data cd on cd.staff_id = s.id
where s.is_staff
