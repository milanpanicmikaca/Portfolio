with active_contractors as
(
 select 
  cc.id as contractor_id, 
  cu2.full_name as installer, 
  cu2.email as installer_email, 
  cu.full_name as owner_name
 from ergeon.contractor_contractor cc
 left join ergeon.hrm_staff hs on hs.id = cc.regional_manager_id
 left join ergeon.core_user cu on cu.id = hs.user_id
 left join ergeon.contractor_contractorcontact ccc on ccc.id = cc.contact_id
 left join ergeon.core_user cu2 on cu2.id = ccc.user_id
 where end_date is null and cc.deleted_at is null
),
oleads as 
(
	select 
    order_id,
    min(l.id) as lead_id
	from ergeon.core_lead l
	where l.created_at >= '2018-04-16'
	group by 1
),
order_to_contractor as
(
  select 
    ue.order_id,
    ue.completed_at,
    ue.revenue,
    cco.contractor_id
  from int_data.order_ue_materialized ue
  left join ergeon.contractor_contractororder cco on cco.order_id = ue.order_id
  left join oleads l on l.order_id = ue.order_id
  left join ergeon.core_lead cl on cl.id = l.lead_id
  left join ergeon.customers_contact co on co.id = cl.contact_id
  left join ergeon.core_user cu on cu.id = co.user_id
  where ue.completed_at is not null --cco.status_id = 13  and 
    and ue.won_at is not null
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
  qualify row_number() over (partition by ue.order_id order by cco.id desc) = 1
),
fam_recruited as
(
  select 
    p.email, 
  from pipedrive.person p 
    left join ergeon.core_user u on u.email = p.email
    left join pipedrive.deal d on d.person_id = p.id
    left join pipedrive.stage s on s.id = d.stage_id
    left join pipedrive.pipeline pl on pl.id = s.pipeline_id
  where pl.name = 'FAM'
  and u.email is not null
)
select 
  o.order_id,
  o.completed_at,
  a.installer,
  case 
    when a.owner_name is null then a.installer 
    else a.owner_name 
  end as owner_name,
  o.revenue,
  f.email as fam_email
from order_to_contractor o
left join active_contractors a on a.contractor_id = o.contractor_id
left join fam_recruited f on f.email = a.installer_email
where installer is not null
order by 1
-- qualify row_number() over (partition by ue.order_id order by cco.id desc) = 1