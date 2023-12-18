select
  cast(issue_id as int64) as issue_id,
  key,
  timestamp_millis(timestamp_micro) as created_at,
  items.field as field,
  items.field_type as field_type,
  items.field_id as field_id,
  items.from_id as from_id,
  items.from_string as from_string,
  items.to_id as to_id,
  items.to_string as to_string
from
(
  select
    issue_id,
    k.key,
    timestamp_micro,
    array
    (
      select as struct
        regexp_replace(json_extract(items, '$.field'), r'([\'\"])', '') as field,
        regexp_replace(json_extract(items, '$.fieldtype'), r'([\'\"])', '') as field_type,
        regexp_replace(json_extract(items, '$.fieldId'), r'([\'\"])', '') as field_id,
        regexp_replace(json_extract(items, '$.from'), r'([\'\"])', '') as from_id,
        regexp_replace(json_extract(items, '$.fromString'), r'([\'\"])', '') as from_string,
        regexp_replace(json_extract(items, '$.to'), r'([\'\"])', '') as to_id,
        regexp_replace(json_extract(items, '$.toString'), r'([\'\"])', '') as to_string
      from unnest (json_extract_array(changelog, "$.items")) as items
    ) as items
  from
  (
    select
      rank() over (partition by json_extract_scalar(json, '$.issue.id') order by timestamp_micro desc) as rank,
      json_extract_scalar(json, '$.issue.id') as issue_id,
      json_extract_scalar(json, '$.issue.key') as key,
      timestamp_micro,
      json_extract(json,'$.changelog') as changelog
    from jira.log
  ) as k
  inner join jira.SD_roadmap_issues sd on sd.key = k.key
  where
    changelog is not null
  )
  left join unnest(items) as items