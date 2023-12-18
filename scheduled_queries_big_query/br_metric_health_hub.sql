with
extract_array_from_tabs as ( -- pulling metrics in array from each br app tab
    select
        JSON_EXTRACT_ARRAY(content, '$.Summary.rows') as metrics_array,
        'Summary' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
    union all
    select
        JSON_EXTRACT_ARRAY(content, '$.Fence.rows') as metrics_array,
        'Fence' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
    union all
    select
        JSON_EXTRACT_ARRAY(content, '$.Turf.rows') as metrics_array,
        'Turf' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
    union all
    select
        JSON_EXTRACT_ARRAY(content, '$.Driveway.rows') as metrics_array,
        'Driveway' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
    union all
    select
        JSON_EXTRACT_ARRAY(content, '$.By Market - Fence.rows') as metrics_array,
        'By Market - Fence' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
    union all
    select
        JSON_EXTRACT_ARRAY(content, '$.By Market - Driveway.rows') as metrics_array,
        'By Market - Driveway' as br_app_location
    from warehouse.br_report where deleted_at is null and slug = 'business-review'
),

metrics_in_metadata_json as ( --taking the name of the metric through unnesting array values
    select
        br_app_location,
        JSON_EXTRACT_SCALAR(flattened_array, '$.name') as metric
    from extract_array_from_tabs,
        UNNEST(extract_array_from_tabs.metrics_array) as flattened_array
    where JSON_EXTRACT_SCALAR(flattened_array, '$.rowClass') = 'metric'
),

br_metrics_values as ( -- getting the last date when metric was modified (also if it was deleted)
    select
        EXTRACT(date from brv.modified_at) as modified_at,
        brv.metric_id,
        brm.code,
        brm.short_label,
        MAX(EXTRACT(date from brv.modified_at)) as last_modified,
        case
            when MAX(EXTRACT(date from brv.modified_at)) >= DATE_SUB(CURRENT_DATE(), interval 7 day) then 'OK' else 'Values Outdated'
        end as metric_getting_value,
        brm.deleted_at
    from warehouse.br_metric_value as brv
    left join warehouse.br_metric as brm on brv.metric_id = brm.id
    group by 1, 2, 3, 4, 7
    qualify ROW_NUMBER() over (partition by brv.metric_id order by modified_at desc) = 1
),

br_metric_queries as ( --getting the br query location (query is also included)
    select
        metric_id,
        query,
        query_string
    from `warehouse.br_query`,
        UNNEST(metric_ids) as metric_id
    order by query
)

select
    bm.code as in_admin,
    f.code as in_br_metric_flatten,
    m.metric as in_metadata_json,
    m.br_app_location,
    v.metric_getting_value,
    v.short_label,
    v.last_modified,
    v.deleted_at,
    case when q.query is null and LOWER(bm.source) like '%calc%' then 'calculated_metrics_view' else q.query end as source,
    case when bm.code is null then 1 else 0 end as missing_admin_count,
    case when f.code is null then 1 else 0 end as missing_flatten_count,
    case when m.metric is null then 1 else 0 end as missing_json_count,
    case when v.metric_getting_value = 'Values Outdated' then 1 else 0 end as outdated_values_count,
    case when q.query is null and LOWER(bm.source) not like '%calc%' then 1 else 0 end as missing_query_count,
    case when v.metric_getting_value is null then 1 else 0 end as not_getting_any_value_count,
    case when v.metric_getting_value is null then 'Empty Values' else 'OK' end as metric_is_not_getting_any_value
from warehouse.br_metric as bm
left join metrics_in_metadata_json as m on m.metric = bm.code
-- to avoid duplications: no matter which period_type is selected
left join warehouse.br_metric_flatten as f on f.code = bm.code and f.period_type = 'day'
left join br_metrics_values as v on v.code = bm.code
left join br_metric_queries as q on q.metric_id = v.metric_id
