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
  and (d.name = 'Sales' or (d.name = 'Operations' and internal_title like '%Project Manager%' and internal_title not like '%Team%'))
),
balance as
(
  select
    -- add 86399 seconds to make timestamp with hh:mm:ss = 23:59:59
    timestamp_add(cast(date_array as timestamp), interval 86399 second) as date,
    null as order_id,
    null as escalation_id,
    staff_id,
    CONCAT('Won Commission|Balance as of ',cast(timestamp_add(cast(date_array as timestamp), interval 86399 second) as date)) as description,
    null as amount,
    'balance' as type
  -- First balance after initial recording of transactions
  from unnest(GENERATE_DATE_ARRAY('2022-07-03', current_date, INTERVAL 1 week)) AS date_array,
  distinct_staff
),
final_union as 
(
  select
    *,
    row_number() over (order by date) as row_number
    from
  (
    select
      *
    from compensation_system.sales_won_commissions_trx
    union all
    select
      *
    from compensation_system.sales_completed_commissions_trx
    union all
    select
      *
    from compensation_system.sales_escalation_commissions_trx
    union all
    select
      *
    from compensation_system.sales_stainingupsell_commissions_trx
    union all
    select
        *
    from compensation_system.sales_commission_adjustments_trx
    union all
    select
      *
    from balance
    union all
    select
      *
    from compensation_system.sales_commission_payments
      )
)
select
  cast(date as date) as date,
  f.order_id,
  escalation_id,
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
  left join compensation_system.commissioning_threshold t on t.order_id = f.order_id and is_eligible_for_bonus = 1 --only orders that pass the commission threshold after
where 
  hs.is_staff 
  and (t.is_eligible_for_bonus is not null or type in ('commission payment','balance','staining upsell'))
  and ((sl.change_type = 'left' and date_trunc(cast(sl.effective_date as date),week) >= cast(date as date)) or sl.change_type <> 'left')
order by 4,1