select 
	bm.is_up,
	bm.code,
	bmv.value,
  round(bmv.value,0) as value_rounded,
  round(bmv.value/1000, 0) as value_rounded_k,
	bmt.value as target,
	bp.starting_at,
	bp.type
from warehouse.br_metric_value bmv
left join warehouse.br_period bp
on bmv.period_id = cast(bp.id as int64)
inner join warehouse.br_metric bm 
on bmv.metric_id = cast(bm.id as int64)
left join warehouse.br_metric_target bmt on bmt.metric_id = bm.id and 
bmt.period_id = bp.id
where 
bm.is_active is true 
order by bp.starting_at desc, bm.code;