with
    timeseries as (
        select 
        date_trunc(date_array,{period}) as date,
        from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
        group by 1
    ),
    adminops as (
        select
        date_trunc(date, {period}) as date,
        sum(case when team_by_components = 'Design' then 1 else 0 end) as design_tasks,
        sum(case when team_by_components = 'AdminOPS' then 1 else 0 end) as adminops_tasks,
        sum(case when team_by_components = 'Design' and type = 'new request' then 1 else 0 end) as design_new_requests,
        sum(case when team_by_components = 'Design' and type = 'completed' then 1 else 0 end) as design_completed_requests,
        sum(case when team_by_components = 'AdminOPS' and type = 'new request' then 1 else 0 end) as adminops_new_requests,
        sum(case when team_by_components = 'AdminOPS' and type = 'completed' then 1 else 0 end) as adminops_completed_requests,
        avg(case when team_by_components = 'Design' and type = 'completed' then tat_actual else null end) as design_tat,
        avg(case when team_by_components = 'AdminOPS' and type = 'completed' then tat_actual else null end) as adminops_tat,
        date_diff(current_date,max(case when team_by_components = 'Design' and type = 'new request' then date else null end),day) as oldest_design,
        date_diff(current_date,max(case when team_by_components = 'AdminOPS' and type = 'new request' then date else null end),day) as oldest_adminops,
        from ext_quote.adminops_design_flat df
        --where
        --department not in ('HR','Finance')
        --and lower(labels) not like '%hr%'
        group by 1
    )
select
    t.date,
    coalesce(adminops_new_requests,0) as CTN001,
    coalesce(adminops_completed_requests,0) as CTN002,
    coalesce(adminops_tat,0) as CTN003,
    coalesce(oldest_adminops,0) as CTN004,
    coalesce(design_new_requests,0) as CTN005,
    coalesce(design_completed_requests,0) as CTN006,
    coalesce(design_tat,0) as CTN007,
    coalesce(oldest_design,0) as CTN008,
    coalesce(adminops_tasks,0) as CTN009,
    coalesce(design_tasks,0) as CTN010
    from timeseries t
    left join adminops a on a.date = t.date
