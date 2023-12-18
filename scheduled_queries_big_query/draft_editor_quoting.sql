with ql_count as (
    select
        qq.id as quote_id,
        count(ql.id) as ql_count
    from `ergeon.quote_quote`qq left join
        `ergeon.quote_quoteline`ql on qq.id = ql.quote_id
    group by 1
)

select
    qq.id as quote_id,
    qq.order_id,
    is_draft,
    is_scope_change,
    qq.is_estimate,
    total_cost,
    total_price,
    qc.ql_count as number_of_quote_lines,
    hs2.full_name as sales_rep,
    quoter as estimator,
    cu.full_name as created_by,
    l.name as department,
    date(cast(qq.created_at as timestamp), "America/Los_Angeles") as created_at,
    cu1.full_name as prepared_by,
    case when is_draft = true and qq.is_estimate = false then 'Draft quote'
        when is_draft = false and qq.is_estimate = true then 'Estimate quote'
        when is_draft = false and qq.is_estimate = false then 'Regular quote'
        else 'Other' end as quote_type,
    case when lower(calc_input) like ('%cad_objects%') then 'Draft Editor' else 'Legacy' end as quoting_tool,
    case when qq.cancelled_at is not null then 'Cancelled' else 'Active' end as quote_status,
    case
        when is_scope_change is true then (
            case
                when lower(title) like '%scop%' and method = 'measured' then 'scoping_task'
                else 'change_of_order'
            end)
        else 'regular_quote'
    end as scope_change_type,
    case when so.parent_order_id is not null then 'WWO' else 'not_WWO' end as wwo,
    concat('https://api.ergeon.in/public-admin/store/order/', qq.order_id, '/change/') as Public_Admin_URL,
    product_quoted
from
    ergeon.quote_quote qq left join
    ergeon.core_user cu on qq.created_by_id = cu.id left join
    ergeon.hrm_staff hs on qq.preparation_completed_by_id = hs.id left join
    ergeon.core_user cu1 on hs.user_id = cu1.id left join
    ergeon.store_order so on qq.order_id = so.id left join
    ergeon.hrm_staff hs1 on hs1.id = so.sales_rep_id left join
    ergeon.hrm_stafflog hs2 on hs2.id = hs1.current_stafflog_id left join
    ql_count qc on qq.id = qc.quote_id left join
    ergeon.hrm_staff h on h.user_id = cu.id left join --mapping for department from qq.created_by_id
    ergeon.hrm_stafflog sl on sl.id = h.current_stafflog_id left join --mapping for department from qq.created_by_id
    ergeon.hrm_staffposition p on p.id = sl.position_id left join --mapping for department from qq.created_by_id
    ergeon.hrm_ladder l on l.id = p.ladder_id left join--mapping for department from qq.created_by_id
    int_data.order_ue_materialized ue on qq.order_id = ue.order_id --product filter
where
    qq.created_at > '2022-11-01'
