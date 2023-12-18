with flattened_labels as
(
select
    au.protopayload_auditlog.resourceName as id,
    string_agg(labels.key, ",") as keys,
    string_agg(labels.value, ",") as values
FROM warehouse.cloudaudit_googleapis_com_data_access au
cross join unnest(au.protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.labels) as labels
group by 1
),
flattened_tables as
(
select
    au.protopayload_auditlog.resourceName as id,
    string_agg(concat(tables.datasetId, ".",tables.tableId)) as tables
FROM warehouse.cloudaudit_googleapis_com_data_access au
cross join unnest(au.protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables) as tables
group by 1
)
SELECT  
	protopayload_auditlog.authenticationInfo.principalEmail as user,
  au.timestamp,
	cast(5.0*
		(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes/POWER(2,40))
    as numeric
		) as queryCostInUSD,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
    cast(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes /POWER(2,40) as numeric) as tb,
    SAFE_DIVIDE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,(TIMESTAMP_DIFF(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime, protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.JobStatistics.startTime, MILLISECOND))) as avg_job_slots_used, 
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query,
    case when fl.values like '%connected_sheets%' and fl.values like '%apps-script%' then 'connected_sheets_apps_script'
         when fl.values like '%connected_sheets' and fl.values like '%schedule%' then 'connected_sheets_scheduled'
         when fl.values like '%scheduled_query%' then 'scheduled_query'
         when fl.values like '%legacy%' then 'legacy_connector'
         when fl.values is null then 'API_call|direct_query' end as type,
    case when ft.tables like '%ergeon%' then true else false end as is_ergeon,
    case when ft.tables like '%jira%' then true else false end as is_jira,
    case when ft.tables like '%pipedrive%' then true else false end as is_pipedrive,
    case when ft.tables like '%warehouse%' then true else false end as is_warehouse,
    case when ft.tables like '%ext_marketing%' then true else false end as is_ext_marketing,
    case when ft.tables like '%ext_quote%' then true else false end as is_ext_quote,
    case when ft.tables like '%ext_delivery%' then true else false end as is_ext_delivery,
    case when protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query like '%SELECT clmn%' then true else false end as is_datastudio,
    case when protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query like '%pivot%' then true else false end as is_calculated_pivot
FROM warehouse.cloudaudit_googleapis_com_data_access au
left join flattened_labels fl on fl.id = au.protopayload_auditlog.resourceName
left join flattened_tables as ft on ft.id = au.protopayload_auditlog.resourceName
WHERE protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName="query_job_completed"
AND 
	protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes 
				IS NOT NULL