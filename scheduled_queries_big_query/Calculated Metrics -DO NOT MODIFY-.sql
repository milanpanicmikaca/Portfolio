CALL `bigquerydatabase-270315.x.pivot` (
  'bigquerydatabase-270315.warehouse.temp' # source table
  , 'bigquerydatabase-270315.warehouse.metric_pivot' # destination table
  , ['period_id'] # row_ids
  , 'code' # pivot_col_name
  , 'metric_value' # pivot_col_value
  , 10000 # max_columns
  , 'SUM' # aggregation
  , '' # optional_limit
);
INSERT `bigquerydatabase-270315.warehouse.br_metric_value_log`
select * from warehouse.calculated_metrics_view;
