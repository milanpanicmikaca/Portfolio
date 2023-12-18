with staff_positions as 
(
select
  q.id,
  sent_to_customer_at,
  department as dep_name,
  case when rank() over (partition by order_id order by sent_to_customer_at,q.id) = 1 then 'new' else 'requote' end as completion_type
from 
  ergeon.quote_quote q left join
  ergeon.hrm_staff s on s.user_id = q.sent_to_customer_by_id left join
  useful_sql.hrm hrm on hrm.staff_id = if(q.sent_to_customer_at < '2022-07-04',s.id, q.preparation_completed_by_id) and date(q.sent_to_customer_at) between hrm.started_at and hrm.end_date left join --before 4th of July 2022 we were using diferent methodology
  ergeon.store_order o on o.id = q.order_id
where 
  sent_to_customer_at >= '2018-04-16' 
  and is_scope_change is null --excluding change of orders
  and is_cancellation = False --cancelled can be marked as complete
  and parent_order_id is null --excluding WWO
  and is_estimate = False --excluding estimate quotes (this can be excluded with sent_to_customer_at)
  and department in ('Sales','Construction')
qualify rank() over(partition by q.id order by hrm.started_at desc) = 1 --date of position change can overlap and create duplicate
),
qd as 
(
select
  date_trunc(extract( date from datetime(cast(sent_to_customer_at as timestamp), "America/Los_Angeles")),{period}) as date,
  count(*) as total_completed,
  sum(case when dep_name = 'Sales' then 1 else 0 end) as sales_count,
  sum(case when dep_name = 'Construction' then 1 else 0 end) as construction_count,
  sum(case when completion_type = 'new' and dep_name = 'Sales' then 1 else 0 end) as new_completed_sales,
  sum(case when completion_type = 'requote' and dep_name = 'Sales' then 1 else 0 end) as requotes_sales,
  sum(case when completion_type = 'new' and dep_name = 'Construction' then 1 else 0 end) as new_completed_construction,
  sum(case when completion_type = 'requote' and dep_name = 'Construction' then 1 else 0 end) as requotes_construction
from 
  staff_positions
group by 1
)
select
  date,
  coalesce(total_completed,0) as CTN044,
  coalesce(sales_count,0) as CTN045,
  coalesce(construction_count,0) as CTN046,
  coalesce(new_completed_sales,0) as CTN047,
  coalesce(requotes_sales,0) as CTN048,
  coalesce(new_completed_construction,0) as CTN049,
  coalesce(requotes_construction,0) as CTN050
from 
  qd
