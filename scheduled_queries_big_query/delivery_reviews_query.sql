with pm_data as (
    select
        date_trunc(date(fr.posted_at, "America/Los_Angeles"), day) as day,
        fr.order_id,
        tl.staff_id,
        tl.full_name,
        tl.team_leader,
        coalesce(tl.house, 'Carmen Mendez') as team_lead,
        case when fr.id is not null then 1 else 0 end as reviews_count,
        case when fr.score <= 3 then 1 else 0 end as bad_reviews_count,
        case when fr.score > 3 then 1 else 0 end as good_reviews_count,
        fr.score
    from ergeon.feedback_review fr
    left join ergeon.marketing_localaccount mnl on mnl.id = fr.account_id
    left join ergeon.marketing_channel mnc on mnc.id = mnl.channel_id
    left join ergeon.hrm_staff hs on fr.delivery_staff_attributed_id = hs.id
    left join int_data.delivery_team_lead tl on tl.staff_id = hs.id
    --left join ergeon.core_user cu on hs.user_id = cu.id 
    where tl.full_name is not null
    order by 1 desc
),

contractor_data as (
    select
        cco.order_id,
        cu.full_name as contractor_name,
        cu2.full_name as pm_assigned
    from ergeon.contractor_contractororder cco
    left join ergeon.contractor_contractor cc on cc.id = cco.contractor_id
    left join ergeon.contractor_contractorcontact ccc on ccc.id = cc.contact_id
    left join ergeon.core_user cu on cu.id = ccc.user_id
    left join ergeon.hrm_staff hs on hs.id = cc.project_manager_id
    left join ergeon.core_user cu2 on cu2.id = hs.user_id
    qualify row_number() over (partition by cco.order_id order by cco.id desc) = 1
)

select
    a.day,
    a.staff_id,
    a.full_name as project_manager,
    a.team_leader as team_lead_contractor,
    b.contractor_name,
    b.pm_assigned,
    sum(a.reviews_count) as reviews_count,
    sum(a.bad_reviews_count) as bad_reviews_count,
    sum(a.good_reviews_count) as good_reviews_count,
    avg(a.score) as avg_stars
from pm_data a
left join contractor_data b on a.order_id = b.order_id
group by 1, 2, 3, 4, 5, 6
