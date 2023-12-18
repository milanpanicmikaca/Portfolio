with metrics as (
select
bp.type as period,starting_at,ending_at,
bmv.modified_at, 
bm.modified_at,
cast(bmv.modified_at as date) as date,
code,
short_label,
value
from warehouse.br_metric_value bmv
left join warehouse.br_metric bm on bm.id = bmv.metric_id
left join warehouse.br_period bp on bp.id = period_id
),
closed_projects as (
select * from metrics 
where code = 'SAL111' 
order by starting_at desc
),
infrastructure_cost as (
select * from metrics
where code ='ENG110' 
order by starting_at desc
),eng_headcount as (
  select
  * from metrics 
  where code = 'HRS116'
)
select
cp.starting_at,
ic.value/cp.value as cpp,
ehc.value as headcount
from closed_projects cp 
left join infrastructure_cost ic on ic.starting_at = cp.starting_at and ic.period = cp.period
left join eng_headcount ehc on ehc.starting_at = cp.starting_at and ehc.period = cp.period
where cp.period = 'day'
order by 1 desc