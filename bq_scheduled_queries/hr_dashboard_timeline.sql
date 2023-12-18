select 
  m.short_label,
  m.code,
  p.starting_at as date,
  p.type,
  v.value
from warehouse.br_metric_value v
left join warehouse.br_period p on p.id = v.period_id
left join warehouse.br_metric m on m.id = v.metric_id
where p.type in ('quarter', 'month', 'year')
and code in ('HRS101', 'HRS102', 'HRS103', 'HRS104')