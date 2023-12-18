with quote_lineinfo as (
select 
  order_id,
  quote_id,
  created_at,
  picket_build,
  frame_style,
  length,
  row_number() over (partition by order_id order by length desc) as rown_ql
from int_data.order_ql_materialized
where coalesce(picket_build,frame_style) is not null
), quote_info as (
select distinct
  qi.*,
from quote_lineinfo qi
left join ergeon.quote_quote qq on qq.id = qi.quote_id
where rown_ql = 1
)
select 
  q.* except (rown_ql),
  od.quoted_at,
  od.segment,
  od.full_name,
  od.market,
  od.region,
from quote_info q
left join int_data.sales_dashboard_od od on od.order_id = q.order_id