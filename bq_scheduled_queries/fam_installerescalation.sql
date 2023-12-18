with oleads as (
	select 
    order_id,
    min(l.id) as lead_id
	from ergeon.core_lead l
	where l.created_at >= '2018-04-16'
	group by 1
), calc_data as (
  select 
    ue.order_id,
    ue.completed_at as completion_date,
  from int_data.order_ue_materialized ue
    left join oleads l on l.order_id = ue.order_id
    left join ergeon.core_lead cl on cl.id = l.lead_id
    left join ergeon.customers_contact co on co.id = cl.contact_id
    left join ergeon.core_user cu on cu.id = co.user_id
  where completed_at is not null and won_at is not null
    and ue.order_id not in 
      (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,
      59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
    and 
  		(upper(cl.full_name) not like '%[TEST]%' or
         upper(co.full_name) not like '%[TEST]%' or
         upper(cu.full_name) not like '%[TEST]%')
    and 
  		(lower(cl.email) not like '%+test%' or
         lower(cu.email) not like '%+test%')
), info_rank as (
  select 
    completion_date as date,
    cu.full_name as installer,
    tl.house,
    cc.order_id as id,
    'order_completed' as type,
    1 as value,
    row_number() over (partition by cc.order_id order by cc.id desc) as rank_info
  from ergeon.contractor_contractororder cc
    left join calc_data pc on pc.order_id = cc.order_id
    left join ergeon.contractor_contractor cc2 on cc2.id = cc.contractor_id
    left join ergeon.contractor_contractorcontact cc3 on cc3.id = cc2.contact_id
    left join ergeon.core_user cu on cu.id = cc3.user_id
    left join int_data.hr_dashboard tl on tl.staff_id = cc2.project_manager_id
  where cc.order_id in (select order_id from calc_data)
)
select 
  * except(rank_info)
from info_rank
where rank_info = 1

union all

select
  ie.date,
  u.full_name as installer,
  tl.house,
  ie.id,
  'installer_escalation' as type,
  1 as value
from int_data.inst_escalations_spreadsheet ie
left join int_data.hr_dashboard tl on tl.email = ie.project_manager
left join ergeon.contractor_contractor cc on cc.id = ie.id
left join ergeon.contractor_contractorcontact co on co.id = cc.contact_id
left join ergeon.core_user u on u.id = co.user_id