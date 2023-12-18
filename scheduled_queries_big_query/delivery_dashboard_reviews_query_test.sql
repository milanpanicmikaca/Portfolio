with contractor_order as (
    select
        co.order_id,
        full_name as contractor
    from ergeon.contractor_contractororder co
    left join ergeon.contractor_contractor hc on hc.id = co.contractor_id
    left join ergeon.contractor_contractorcontact cc on cc.id = hc.contact_id
    left join ergeon.core_user u on u.id = cc.user_id
        and co.status_id in (3, 13, 66)
    qualify row_number() over (partition by order_id order by co.id desc) = 1
)

select
    fr.id as review_id,
    fr.order_id,
    date(date_trunc(fr.posted_at, day, 'America/Los_Angeles')) as posted_at,
    cu.full_name as project_manager,
    case
        when hs.id in (1636, 1698) then 'KAM'
        when ht.name is null then 'No House'
        else ht.name
    end as house,
    case
        when hs.id in (1636, 1698) then 'Nestor Baca'
        when ht.name is null then 'No Team Leader'
        else cu2.full_name
    end as team_leader,
    case
        when c.contractor is null then 'No Contractor'
        else c.contractor
    end as contractor,
    fr.score
from ergeon.feedback_review fr
left join ergeon.hrm_staff hs on hs.id = fr.delivery_staff_attributed_id
left join ergeon.core_user cu on cu.id = hs.user_id
left join ergeon.hrm_stafflog hsl on hsl.id = hs.current_stafflog_id
left join ergeon.hrm_team ht on ht.id = hsl.team_id
left join ergeon.hrm_staff hs2 on hs2.id = ht.lead_id
left join ergeon.core_user cu2 on cu2.id = hs2.user_id
left join contractor_order c on c.order_id = fr.order_id
where fr.delivery_staff_attributed_id is not null
