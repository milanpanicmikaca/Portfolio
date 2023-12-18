with cancellation_initial_data as (
    select
        ue.cancelled_at as date,
        coalesce(ue.project_manager, 'No Project Manager') as project_manager,
        ue.market,
        case
            when ue.pm_id = 1923 then 'Launch'
            when ue.pm_id = 2120 then 'Falcons Tribe'
            when ue.pm_id = 2015 then 'Rodeo Rangers'
            when ue.pm_id = 2041 then 'House of Liberty'
            when ue.pm_id = 2278 then 'Vikings Crew'
            when ue.pm_id = 2014 then 'Hollywood Stars'
            when ue.pm_id = 1997 then 'House of Ninjas'
            when ue.pm_id in (1636, 1698) then 'KAM'
            when
                ue.pm_team not in (
                    'Vikings Crew', 'House of Ninjas', 'Rodeo Rangers', 'House of Liberty', 'Hollywood Stars', 'Falcons Tribe', 'KAM', 'Launch'
                ) or ue.pm_team is null then 'No House'
            else ue.pm_team
        end as house,
        case
            when ue.pm_id = 1923 then 'Launch'
            when ue.pm_id in (1636, 1698) then 'Nestor Baca'
            when ue.pm_team is null then 'No Team Leader'
            else cu.full_name
        end as team_leader,
        ue.first_approved_price as cancelled_revenue
    from int_data.order_ue_materialized ue
    left join ergeon.hrm_staff hs on hs.id = ue.pm_id
    left join ergeon.hrm_stafflog hsl on hsl.id = hs.current_stafflog_id
    left join ergeon.hrm_team ht on ht.id = hsl.team_id
    left join ergeon.hrm_staff hs2 on hs2.id = ht.lead_id
    left join ergeon.core_user cu on cu.id = hs2.user_id
    where ue.cancelled_at is not null
),

cancellation as (
    select
        * except(cancelled_revenue),
        sum(cancelled_revenue) as cancelled_revenue
    from cancellation_initial_data
    group by 1, 2, 3, 4, 5
),

--first_approve_price initials
fap_initial_data as (
    select
        ue.won_at as date,
        coalesce(ue.project_manager, 'No Project Manager') as project_manager,
        ue.market,
        case
            when ue.pm_id = 1923 then 'Launch'
            when ue.pm_id = 2120 then 'Falcons Tribe'
            when ue.pm_id = 2015 then 'Rodeo Rangers'
            when ue.pm_id = 2041 then 'House of Liberty'
            when ue.pm_id = 2278 then 'Vikings Crew'
            when ue.pm_id = 2014 then 'Hollywood Stars'
            when ue.pm_id = 1997 then 'House of Ninjas'
            when ue.pm_id in (1636, 1698) then 'KAM'
            when
                ue.pm_team not in (
                    'Vikings Crew', 'House of Ninjas', 'Rodeo Rangers', 'House of Liberty', 'Hollywood Stars', 'Falcons Tribe', 'KAM', 'Launch'
                ) or ue.pm_team is null then 'No House'
            else ue.pm_team
        end as house,
        case
            when ue.pm_id = 1923 then 'Launch'
            when ue.pm_id in (1636, 1698) then 'Nestor Baca'
            when ue.pm_team is null then 'No Team Leader'
            else cu.full_name
        end as team_leader,
        ue.first_approved_price as initial_approved_price
    from int_data.order_ue_materialized ue
    left join ergeon.hrm_staff hs on hs.id = ue.pm_id
    left join ergeon.hrm_stafflog hsl on hsl.id = hs.current_stafflog_id
    left join ergeon.hrm_team ht on ht.id = hsl.team_id
    left join ergeon.hrm_staff hs2 on hs2.id = ht.lead_id
    left join ergeon.core_user cu on cu.id = hs2.user_id
),

fap as (
    select
        * except(initial_approved_price),
        sum(initial_approved_price) as initial_approved_price
    from fap_initial_data
    group by 1, 2, 3, 4, 5
)

select
    coalesce(f.date, c.date) as date,
    coalesce(f.project_manager, c.project_manager) as project_manager,
    coalesce(f.market, c.market) as market,
    coalesce(f.house, c.house) as house,
    coalesce(f.team_leader, c.team_leader) as team_leader,
    coalesce(cancelled_revenue, 0) as cancelled_revenue,
    coalesce(initial_approved_price, 0) as initial_approved_price
from fap f
full join cancellation c using (date, project_manager, house, market)
