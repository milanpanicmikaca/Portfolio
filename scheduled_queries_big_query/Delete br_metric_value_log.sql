DELETE
FROM
  warehouse.br_metric_value_log
WHERE
  ' metric_id:' || CAST(metric_id AS string) || ' period_id: ' || CAST(period_id AS string) || ' modified_at: ' || CAST(modified_at AS string) IN (
   SELECT
    ' metric_id:' || CAST(metric_id AS string) || ' period_id: ' || CAST(period_id AS string) || ' modified_at: ' || CAST(modified_at AS string)
  FROM
    warehouse.br_metric_value_log
  QUALIFY ROW_NUMBER() OVER (PARTITION BY metric_id, period_id ORDER BY modified_at DESC) > 1)
  AND modified_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE);