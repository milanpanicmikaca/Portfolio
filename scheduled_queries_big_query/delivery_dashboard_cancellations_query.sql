with initial_approved_prive as (
    select
        ue.won_at as date,
        ue.order_id,
        ue.project_manager,
        case
            when pm_id = 1923 then 'Launch'
            when pm_id = 2120 then 'Falcons Tribe'
            when pm_id = 2015 then 'Rodeo Rangers'
            when pm_id = 2041 then 'House of Liberty'
            when pm_id = 2278 then 'Vikings Crew'
            when pm_id = 2014 then 'Hollywood Stars'
            when pm_id = 1997 then 'House of Ninjas'
            when pm_id in (1636, 1698) then 'KAM'
            else pm_team
        end as house,
        ue.contractor,
        ue.market,
        ue.first_approved_price
    from int_data.order_ue_materialized ue
    where won_at is not null
),

cancellations as (
    select
        ue.closedW_at as date,
        ue.order_id,
        ue.project_manager,
        case
            when pm_id in (1636, 1698) then 'KAM'
            when pm_id is null or pm_team is null then 'No House'
            else pm_team
        end as house,
        ue.contractor,
        ue.market,
        ue.first_approved_price
    from int_data.order_ue_materialized ue
    where ue.order_status = 'Cancelled - Won'
)

select
    coalesce(i.date, c.date) as date,
    coalesce(i.order_id, c.order_id) as order_id,
    case
        when coalesce(i.project_manager, c.project_manager) is null then 'No Project Manager'
        else coalesce(i.project_manager, c.project_manager)
    end as project_manager,
    case
        when coalesce(i.house, c.house) is null then 'No House'
        else coalesce(i.house, c.house)
    end as house,
    case
        when coalesce(i.contractor, c.contractor) is null then 'No Contractor'
        else coalesce(i.contractor, c.contractor)
    end as contractor,
    coalesce(i.market, c.market) as market,
    coalesce(i.first_approved_price, 0) as initial_revenue,
    coalesce(c.first_approved_price, 0) as cancelled_revenue
from initial_approved_prive i
full join cancellations c on c.order_id = i.order_id and c.date = i.date
