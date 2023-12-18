with 
-- need to get unique staff members to generate balance array per staff member in transactions
distinct_staff as
(
SELECT
  s.id as staff_id
from
  ergeon.hrm_staff s join 
  ergeon.hrm_stafflog sl on sl.id = s.current_stafflog_id join
  ergeon.hrm_staffposition p on p.id = sl.position_id join
  ergeon.hrm_ladder la on la.id = p.ladder_id join 
  ergeon.hrm_department d on d.id = la.department_id
where 
  s.is_staff 
  and sl.change_type in ('hired','changed') 
  and (d.name = 'Sales' or (d.name = 'Operations' and la.name = 'Project Management'))
),
balance as
(
  select
    -- add 86399 seconds to make timestamp with hh:mm:ss = 23:59:59
    timestamp_add(cast(date_array as timestamp), interval 86399 second) as date,
    null as order_id,
    null as review_id,
    staff_id,
    CONCAT('Review Commission|Balance as of ',cast(timestamp_add(cast(date_array as timestamp), interval 86399 second) as date)) as description,
    null as amount,
    'balance' as type
  -- First balance after initial recording of transactions
  from unnest(GENERATE_DATE_ARRAY('2022-05-29', current_date, INTERVAL 1 week)) AS date_array,
  distinct_staff
),
-- union over all sources of transactions in ledger
final_union as 
(
  select
    *,
    row_number() over (order by date) as row_number
    from
  (
    select
      *
    from compensation_system.sales_review_commissions_trx
    union all
    select
        *
    from compensation_system.sales_review_adjustments_trx
    union all
    select
      *
    from balance
    union all
    select
      *
    from compensation_system.sales_review_payments
  )
)
select
  cast(date as date) as date,
  order_id,
  review_id,
  cu.full_name,
  description,
  type,
  amount,
  sum(coalesce(amount,0)) over (partition by f.staff_id order by row_number) as current_balance,
  f.staff_id,
  hs.upwork_key
from 
  final_union f
  left join ergeon.hrm_staff hs on hs.id = f.staff_id
  left join ergeon.core_user cu on cu.id = hs.user_id  
  left join ergeon.hrm_stafflog sl on sl.id = hs.current_stafflog_id
where 
  hs.is_staff 
  and ((sl.change_type = 'left' and date_trunc(cast(sl.effective_date as date),week) >= cast(date as date)) or sl.change_type <> 'left')
order by 4,1