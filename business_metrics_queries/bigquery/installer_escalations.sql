-- upload to BQ
with timeseries as (
select 
    date_trunc(date_array,{period}) as date,
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as date_array
), oleads as (
	select 
    order_id,
    min(l.id) as lead_id
	from ergeon.core_lead l
	where l.created_at >= '2018-04-16'
	group by 1
), calc_data as (
  select 
    ue.order_id,
    ue.completed_at,
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
    pc.completed_at,
    row_number() over (partition by cc.order_id order by cc.id desc) as rank_info
  from ergeon.contractor_contractororder cc
  join calc_data pc on pc.order_id = cc.order_id
), completed_projects as (
  select 
    date_trunc(completed_at, {period}) as date,
    count(*) as completed_projects
  from info_rank
  where rank_info = 1
  group by 1 
), installer_escalation as (
  select
    date_trunc(ie.date, {period}) as date,
    count(*) as installer_escallations,
  from int_data.inst_escalations_spreadsheet ie
  group by 1
)
select
  t.date, 
  ie.installer_escallations / nullif(cp.completed_projects,0) as DEL419
  from timeseries t
  left join completed_projects cp on cp.date = t.date
  left join installer_escalation ie on ie.date = t.date
  where period_rank = 1
