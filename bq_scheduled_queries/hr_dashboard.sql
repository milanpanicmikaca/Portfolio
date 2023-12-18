with staff_log as (
    select
        h.* except (shipping_address, started_at),
        h.started_at as start_date,
        sl.position_id,
        sl.team_id,
        sl.manager_id,
        u3.full_name as team_lead,
        hs.shipping_address,
        case
            when lower(h.internal_title) like '%estimation%' and lower(h.internal_title) not like '%bootcamp%' then 'HRS106' --quoter
            when lower(h.internal_title) in ('estimation team specialist bootcamp') then 'HRS122' --quote bootcamp
            when lower(h.internal_title) in ('junior estimation team specialist') and h.change_type != 'left' then 'HRS123' -- junior estimator
            -- Sepacialist
            when
                lower(
                    h.internal_title
                ) in ('estimation team specialist', 'senior estimation team specialist') and h.change_type != 'left' then 'HRS124'
            when lower(h.internal_title) in ('expert estimator') then 'HRS125' --expert,
            --subject matter expert,
            when lower(h.internal_title) in ('construction subject matter expert: estimation') and h.change_type != 'left' then 'HRS126'
            when lower(h.internal_title) in('estimation team lead', 'estimation associate team lead', 'estimation house head') then 'HRS127' --leaders
            when lower(h.ladder) = 'design' then 'HRS107' --designer
            when lower(h.internal_title) = 'onsite estimator' and lower(h.internal_title) not like '%bootcamp%' then 'HRS109' --photographer
            when
                lower(
                    h.internal_title
                ) like '%project manager%' and lower(h.internal_title) not like '%senior%' and not lower(h.internal_title) like '%team%' then 'HRS110'
            when
                lower(
                    h.internal_title
                ) like '%customer specialist%' and lower(h.internal_title) not like '%bootcamp%' and lower(h.internal_title) not like '%junior%'
                --cs_count
                or lower(h.internal_title) like 'senior launch customer specialist' and lower(h.internal_title) not like '%bootcamp%' then 'HRS105'
            when lower(h.internal_title) = 'key accounts manager' then 'HRS118' --KAM
            when lower(h.internal_title) = 'field account manager' then 'HRS119' --FAM
            when lower(h.internal_title) = 'sales house head' then 'HRS120' --HH
            when lower(h.internal_title) like '%customer service%' and lower(h.internal_title) not like '%bootcamp%' then 'HRS108'--csr
            when lower(h.internal_title) like 'quality assurance analyst'
                or lower(h.internal_title) like 'senior quality assurance analyst' then 'HRS114'--QA
            when h.ladder = 'Quality Assurance Engineering' or h.ladder = 'Engineering' then 'HRS116' --Engineering count
            when lower(h.internal_title) = 'regional field manager' then 'HRS117'--RM 
        end as br_code
    from useful_sql.hrm as h
    left join ergeon.hrm_staff as hs on hs.id = h.staff_id
    left join ergeon.hrm_stafflog as sl on sl.id = hs.current_stafflog_id
    left join ergeon.hrm_team as t on t.name = h.current_team
    left join ergeon.hrm_staff as hs3 on hs3.id = t.lead_id
    left join ergeon.core_user as u3 on u3.id = hs3.user_id

),

date_changes as (
    select
        staff_id,
        count(case when type = 'bootcamp' then 1 end) as bootcamps,
        count(case when type = 'hire' then 1 end) as hire,
        count(case when type = 'promotion' then 1 end) as promotions,
        count(case when type = 'attrition' then 1 end) as attritions,
        count(case when type = 'turnover' then 1 end) as turnovers,
        count(case when type = 'commitment' then 1 end) as turnovers,
        min(case when type = 'bootcamp' then start_date end) as bootcamp_date,
        max(case when type = 'hire' then start_date end) as hire_date,
        max(case when type = 'promotion' then start_date end) as promotion_date,
        max(case when type = 'attrition' then start_date end) as attrition_date,
        max(case when type = 'turnover' or current_change_type = 'left' then start_date end) as term_date,
        max(case when type = 'commitment' then start_date end) as commitment_date
    from staff_log
    --where type is not null
    --and staff_id = 2066
    group by 1
),

final as (
    select
        s.staff_id,
        s.full_name,
        s.email,
        s.start_date,
        s.end_date,
        s.internal_title as title,
        s.position_id,
        s.department,
        s.ladder as ladder_name,
        s.current_country as country,
        s.hire_date,
        dc.term_date,
        dc.bootcamp_date,
        dc.promotions,
        s.br_code,
        s.prev_position as previous_title,
        s.last_changed_at as last_change_date,
        s.team_lead,
        s.team_id,
        s.current_team as house,
        s.manager,
        s.manager_id,
        s.change_type,
        bm.short_label,
        s.shipping_address,
        s.rank,
        case when dc.bootcamp_date is not null and s.hire_date is null and dc.term_date is null then 1 end as is_current_bc,
        date_diff(coalesce(dc.term_date, current_date()), s.hire_date, day) as tenure,
        date_diff(coalesce(dc.term_date, current_date()), dc.hire_date, month) as months_in_position,
        case
            when
                lower(
                    s.internal_title
                ) like '%bootcamp%' and dc.bootcamp_date is not null and dc.hire_date is null and dc.term_date is null then 'active_bootcamp'
            when
                lower(
                    s.internal_title
                ) like '%bootcamp%' and dc.bootcamp_date is not null and dc.hire_date is null and dc.term_date is not null then 'failed_bootcamp'
            when s.current_change_type = 'left' then 'turnover'
            else 'active' end as status,
        case when cast(s.start_date as date) <= current_date() and cast(s.end_date as date) >= current_date() then 1 end as current_hc
    from staff_log as s
    left join date_changes as dc using (staff_id)
    left join warehouse.br_metric as bm on bm.code = s.br_code
    order by s.full_name
)

select
    staff_id,
    full_name,
    email,
    title,
    position_id,
    department,
    ladder_name,
    country,
    hire_date,
    term_date,
    bootcamp_date,
    is_current_bc,
    tenure,
    promotions,
    br_code,
    previous_title,
    last_change_date,
    months_in_position,
    team_lead,
    team_id,
    house,
    manager,
    manager_id,
    change_type,
    short_label,
    shipping_address
from final
where rank = 1
order by full_name
