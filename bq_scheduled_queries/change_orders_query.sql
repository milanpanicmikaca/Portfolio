-- data from the change order spreadsheet
with delivery_requote_requests as (
    select
        *,
        cast(trim(regexp_extract(trim(admin_link), r'quote\/(\d+)')) as bignumeric) as quote_id,
        case
            when length(trim(admin_link)) in (5, 6) then cast(trim(admin_link) as bignumeric)
            else cast(trim(regexp_extract(trim(admin_link), r'/(\d+)/')) as bignumeric)
        end as order_id
    from
        googlesheets.delivery_requote_requests
),

delivery_change_order_quote_ids as (
    select
        dr.order_id,
        dr.added_at,
        dr.request_stage,
        dr.requote_main_reason,
        dr.description,
        dr.request_slack_link,
        qq.id as quote_id,
        rank() over (
            partition by dr.order_id, dr.added_at order by timestamp_diff(qq.created_at, cast(dr.added_at as timestamp), second), qq.id
        ) as rank_diff,
        count(dr.order_id) over (partition by dr.order_id) as total_revisions
    from
        delivery_requote_requests as dr left join
        ergeon.quote_quote as qq on qq.order_id = dr.order_id
    where cast(dr.added_at as timestamp) < qq.created_at
    qualify
        rank() over (partition by dr.order_id, dr.added_at order by timestamp_diff(qq.created_at, cast(dr.added_at as timestamp), second), qq.id) = 1
),

min_approved_quote as (
    select
        ue.order_id,
        qq.id as quote_id,
        qq.approved_at as approved_date
    from
        int_data.order_ue_materialized as ue inner join
        ergeon.quote_quote as qq on qq.order_id = ue.order_id
    where qq.approved_at is not null
    qualify rank() over (partition by qq.order_id order by qq.approved_at) = 1
),

app_revisions as (
    select
        qq.order_id,
        sum(case when qq.approved_at > maq.approved_date and (lower(qq.title) not like '%scop%') then 1 else 0 end) as approved_revisions
    from
        ergeon.quote_quote as qq left join
        min_approved_quote as maq on maq.order_id = qq.order_id
    where qq.approved_at is not null and qq.sent_to_customer_at is not null
    group by 1
),

data as (
    select
        ue.won_at,
        ue.completed_at,
        ue.order_id,
        --ue.cancelled_at,
        qq.id as quote_id,
        --date(qq.created_at,"America/Los_Angeles") as quote_created_at,
        dc2.request_slack_link,
        --dc.request_stage,
        dc2.description,
        dc2.requote_main_reason,
        ue.last_quoted_dept,
        cco.full_name as customer_name,
        sh.house,
        ue.contractor as business_name,
        ar.approved_revisions,
        maq.quote_id as min_app_quote,
        --needed to create the pre installations revisions with positive difference betwwen the win and the revision creation (pre installation stage)
        coalesce(ue.completed_at, ue.cancelled_at) as closed_at,
        regexp_extract(ue.geo, '/.*/(.*)/') as market,
        concat('https://admin.ergeon.in/quoting-tool/', so.id, '/quote/', maq.quote_id) as admin_link_approved,
        --case when dc.request_stage = 'post_installation' then ue.order_id else null end as post_installation_order_id,
        --case when dc.request_stage = 'during_installation' then ue.order_id else null end as during_installation_order_id,
        concat('https://admin.ergeon.in/quoting-tool/', so.id, '/quote/', qq.id) as admin_link_revision,
        date_diff(date(qq.created_at, 'America/Los_Angeles'), ue.won_at, day) as days_since_win,
        case
            when
                dc.request_stage = 'pre_installation' then rank() over (
                    partition by ue.order_id, dc.request_stage order by dc.added_at, cco.full_name
                )
        end as rank_pre_installation,
        case when dc.request_stage = 'pre_installation' then ue.order_id end as pre_installation_order_id,
        case when date(qq.created_at, 'America/Los_Angeles') >= ue.won_at then ue.order_id end as change_order_id,
        --case when coalesce(ue.completed_at,ue.cancelled_at) is not null then ue.order_id else 0 end as is_closed,
        case
            when dc.total_revisions <= 1 then dc.total_revisions
            else dc.total_revisions - 1
        end as total_revisions,
        trim(cu.full_name) as quoter,
        case
            when ue.product_quoted like '%Driveway%' then 'Hardscape'
            when ue.product_quoted like '%Chain Link Fence' then 'Chain Link'
            when ue.product_quoted like '%Vinyl or PVC Fence' then 'Vinyl'
            when ue.product_quoted like '%Wood Fence' then 'Wooden'
            else 'Other'
        end as product,
        case when ue.cancelled_at is null then ue.order_id end as is_won_net,
        case when (lower(qq.title) like '%scop%') then 1 else 0 end as is_scoping
    --row_number() over (partition by ue.order_id, qq.id order by qq.id) as rank_per_quote,
    from int_data.order_ue_materialized as ue
    left join ergeon.quote_quote as qq on qq.order_id = ue.order_id and qq.approved_at is not null
    left join delivery_change_order_quote_ids as dc on dc.quote_id = qq.id
    left join delivery_change_order_quote_ids as dc2 on dc2.order_id = ue.order_id --new
    left join ergeon.store_order as so on so.id = ue.order_id
    left join ergeon.core_house as ch on ch.id = so.house_id
    left join ergeon.customers_customer as cc on ch.customer_id = cc.id
    left join ergeon.customers_contact as cco on cco.customer_id = cc.id
    left join min_approved_quote as maq on maq.order_id = ue.order_id
    left join ergeon.core_user as cu on cu.id = qq.sent_to_customer_by_id
    left join ergeon.hrm_staff as st on st.user_id = cu.id
    left join ext_quote.staff_house as sh on sh.staff_id = st.id
    left join ergeon.hrm_contractor as ct on ct.id = so.contractor_id
    left join app_revisions as ar on ar.order_id = ue.order_id
    where ue.won_at is not null and qq.sent_to_customer_at is not null
    qualify row_number() over (partition by ue.order_id, qq.id order by ue.order_id, qq.id) = 1
),

approved_revisions_admin as (
    select
        order_id as project_approved_revision,
        quote_id as quote_approved_revision
    from data
    where quote_id != min_app_quote and is_scoping = 0
    qualify row_number() over (partition by order_id order by quote_id) = 1
),

final_data as (
    select
        d.*,
        ara.*,
        ara2.project_approved_revision as non_scoping_order,
        ara2.quote_approved_revision as non_scoping_quote,
        row_number() over (partition by d.order_id order by d.quote_id) as rank_quote -- same quotes
    from data as d
    left join approved_revisions_admin as ara on ara.project_approved_revision = d.order_id
                                                 and ara.quote_approved_revision = d.quote_id
    left join approved_revisions_admin as ara2 on ara2.project_approved_revision = d.order_id
)

select
    * except(project_approved_revision, quote_approved_revision),
    case
        when rank_quote = 2 then non_scoping_order
    end as project_approved_revision,
    case
        when rank_quote = 2 then non_scoping_quote
    end as quote_approved_revision,
    case
        when rank_quote = 2 then concat('https://admin.ergeon.in/quoting-tool/', non_scoping_order, '/quote/', non_scoping_quote)
    end as non_scoping_admin_link_revision
from final_data
