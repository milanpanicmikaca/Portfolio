with installer_esc as (
    select
        date_escalated,
        installer,
        count(id) as installer_escalations
    from int_data.inst_escalations
    group by 1, 2
),

oleads as (
    select
        order_id,
        min(l.id) as lead_id
    from ergeon.core_lead l
    where l.created_at >= '2018-04-16'
    group by 1
),

calc_data as (
    (
        select
            so.id,
            so.completed_at,
            rank() over (partition by so.id order by qq.approved_at desc) as rank
        from ergeon.store_order so
        left join ergeon.quote_quote qq on so.id = qq.order_id
        where completed_at is not null and qq.approved_at is not null
            and qq.approved_at > completed_at
    )
    union all
    (
        select *
        from
            (
                select
                    so.id,
                    so.completed_at,
                    rank() over (partition by so.id order by qq.approved_at desc) as rank
                from ergeon.store_order so
                left join ergeon.quote_quote qq on so.id = qq.order_id
                where completed_at is not null and qq.approved_at is not null
                    and qq.approved_at <= so.completed_at
            ) as approved_before_completion_queries
        where rank = 1
    )
),

initial_completion_data as (
    select
        cast(date_trunc(lp.completed_at, day, 'America/Los_Angeles') as date) as completion_date,
        lp.id as order_id
    from calc_data lp
    left join ergeon.store_order o on lp.id = o.id
    left join ergeon.quote_quote q on q.id = o.approved_quote_id
    left join oleads l on l.order_id = o.id
    left join ergeon.core_lead cl on cl.id = l.lead_id
    left join ergeon.customers_contact co on co.id = cl.contact_id
    left join ergeon.core_user cu on cu.id = co.user_id
    --left join cancelled_projects cp on cp.order_id = lp.id
    where
        o.completed_at is not null
        and o.parent_order_id is null
        --and cp.cancelled_at is null
        and q.approved_at >= '2018-04-15'
        and o.id not in (50815, 56487, 59225, 59348, 59404, 59666, 59670, 59743, 59753,
            59789, 59805, 59813, 59878, 59908, 59922, 60273, 60283, 60401, 60547, 60589, 60590, 60595, 60596, 60597, 60612)
        and upper(coalesce(cl.full_name, '') || coalesce(co.full_name, '') || coalesce(cu.full_name, '')) not like '%[TEST]%'
        and lower(coalesce(cl.email, '') || coalesce(cu.email, '')) not like '%+test%'
    qualify row_number() over (partition by cast(date_trunc(lp.completed_at, day, 'America/Los_Angeles') as date), lp.id) = 1
),

contractor_completion_data as (
    select
        completion_date,
        cu.full_name as installer,
        tl.house,
        cc.order_id
    from ergeon.contractor_contractororder cc
    left join initial_completion_data pc on pc.order_id = cc.order_id
    left join ergeon.contractor_contractor cc2 on cc2.id = cc.contractor_id
    left join ergeon.contractor_contractorcontact cc3 on cc3.id = cc2.contact_id
    left join ergeon.core_user cu on cu.id = cc3.user_id
    left join int_data.delivery_team_lead tl on tl.staff_id = cc2.project_manager_id
    where cc.order_id in (select order_id
        from initial_completion_data)
    qualify row_number() over (partition by cc.order_id order by cc.id desc) = 1
),

date_group_comp as (
    select
        completion_date,
        count(order_id) as completed_projects
    from contractor_completion_data
    group by 1
),

house_data as (
    select
        installer,
        house
    from contractor_completion_data
    qualify row_number() over (partition by installer, house) = 1
),

final_inst_esc_data as (
    select
        ie.date_escalated as date,
        ie.installer,
        hd.house,
        ie.installer_escalations as value,
        'installer_escalation' as category
    from installer_esc ie
    left join house_data hd on hd.installer = ie.installer
),

final_completion_data as (
    select
        cd.completion_date as date,
        cd.completed_projects as value,
        'projects_completed' as category
    from date_group_comp cd
    order by 1 desc
)

select
    date,
    installer,
    house,
    value,
    category
from final_inst_esc_data
union all
select
    date,
    installer,
    house,
    value,
    category
from final_completion_data
cross join (select installer, house from final_inst_esc_data qualify row_number() over (partition by installer, house) = 1)
