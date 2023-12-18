with timeseries as 
(
  select 
    date_array as day,
    rank() over (partition by date_trunc(date_array,{period}) order by date_array desc) as period_rank
  from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as date_array
),
test_leads as 
(
	select 
    cl.order_id,
    cl.id as lead_id  
	from ergeon.core_lead cl
  left join ergeon.customers_contact co on co.id = cl.contact_id
  left join ergeon.core_user cu on cu.id = co.user_id
	where 
    cl.created_at >= '2018-04-16'
  and
    (cl.order_id in 
        (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,
        59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
  or  
    lower(cl.full_name) like '%test%' or lower(cl.full_name) like '%fake%'
  or 
    lower(co.full_name) like '%test%'or lower(co.full_name) like '%fake%'
  or
    lower(cu.full_name) like '%test%' or lower(cu.full_name) like '%fake%'
  or
    lower(cl.email) like '%+test%' or lower(cl.email) like '%@test.%'
  or 
    lower(cu.email) like '%+test%' or lower(cu.email) like '%@test.%')
  qualify row_number() over (partition by cl.order_id order by cl.created_at) = 1
),
won_and_close as
(
  select
    ue.order_id,
    ue.won_at as won_date,
    case 
      when ue.closed_at is not null and ue.completed_at is null and ue.cancelled_at is not null and ue.cancelled_at < ue.won_at and so.deal_status_id <> 9 then null
      else closed_at
    end as close_date,
    ue.product_quoted,
    ue.old_region,
    ue.market
  from int_data.order_ue_materialized ue
  left join ergeon.store_order so on so.id = ue.order_id
  left join test_leads t on t.order_id = ue.order_id
  where 
    ue.won_at is not null
  and
    t.order_id is null
)
,data_ as(
select
  day,
  sub.*
from
(
  select 
    c.*,
    GENERATE_DATE_ARRAY(cast(won_date as date), current_date(), INTERVAL 1 day) AS date_array
  from won_and_close c
)sub
cross join UNNEST(date_array) AS day
),cte as(
select 
  order_id,
  day,
  product_quoted,
  old_region,
  market,
  case when day <= coalesce(close_date, current_date()) then 1 else 0 end as backlog
from data_
),final_cte as (
select
  day,
  sum(backlog) as DEL230,
  sum(case when product_quoted like '/Fence Installation/%' then backlog else 0 end) as DEL230F,
  sum(case when product_quoted like '/Driveway Installation/%' then backlog else 0 end) as DEL230D,
  sum(case when product_quoted like '/Landscaping/%'then backlog else 0 end) as DEL230T,
  -----Fence by region/market-------
  sum(case when product_quoted like '/Fence Installation/%' and old_region = 'North California' then backlog else 0 end) as DEL449F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-EB' then backlog else 0 end) as DEL450F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-SF' then backlog else 0 end) as DEL564F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-NB' then backlog else 0 end) as DEL451F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-SA' then backlog else 0 end) as DEL452F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-ST' then backlog else 0 end) as DEL453F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-FR' then backlog else 0 end) as DEL454F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-WA' then backlog else 0 end) as DEL455F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-SJ' then backlog else 0 end) as DEL456F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-PA' then backlog else 0 end) as DEL457F,
  sum(case when product_quoted like '/Fence Installation/%' and old_region = 'South California' then backlog else 0 end) as DEL458F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-SV' then backlog else 0 end) as DEL459F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-OC' then backlog else 0 end) as DEL460F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-LA' then backlog else 0 end) as DEL461F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-VC' then backlog else 0 end) as DEL462F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-CA-SD' then backlog else 0 end) as DEL463F,
  sum(case when product_quoted like '/Fence Installation/%' and old_region = 'Texas' then backlog else 0 end) as DEL464F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-TX-DL' then backlog else 0 end) as DEL465F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-TX-FW' then backlog else 0 end) as DEL466F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-TX-HT' then backlog else 0 end) as DEL467F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-TX-SA' then backlog else 0 end) as DEL468F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-TX-AU' then backlog else 0 end) as DEL469F,
  sum(case when product_quoted like '/Fence Installation/%' and old_region = 'Georgia' then backlog else 0 end) as DEL470F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-GA-AT' then backlog else 0 end) as DEL471F,
  sum(case when product_quoted like '/Fence Installation/%' and old_region in ('Maryland', 'Pennsylvania', 'Virginia') then backlog else 0 end) as DEL472F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-MD-BL' then backlog else 0 end) as DEL473F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-MD-DC' then backlog else 0 end) as DEL474F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-PA-PH' then backlog else 0 end) as DEL475F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-VA-AR' then backlog else 0 end) as DEL476F,
  sum(case when product_quoted like '/Fence Installation/%' and old_region = 'Florida' then backlog else 0 end) as DEL477F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-FL-MI' then backlog else 0 end) as DEL478F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-FL-OR' then backlog else 0 end) as DEL479F,
  sum(case when product_quoted like '/Fence Installation/%' and market like 'WN-IL-%' then backlog else 0 end) as DEL568F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-IL-CH' then backlog else 0 end) as DEL579F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-IL-NA' then backlog else 0 end) as DEL590F,
  sum(case when product_quoted like '/Fence Installation/%' and market like '%-IL-LA' then backlog else 0 end) as DEL601F,
  -----Turf by region/market-------
  sum(case when product_quoted like '/Landscaping%' and old_region = 'North California' then backlog else 0 end) as DEL449T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-EB' then backlog else 0 end) as DEL450T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-NB' then backlog else 0 end) as DEL451T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-SA' then backlog else 0 end) as DEL452T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-ST' then backlog else 0 end) as DEL453T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-FR' then backlog else 0 end) as DEL454T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-WA' then backlog else 0 end) as DEL455T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-SJ' then backlog else 0 end) as DEL456T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-PA' then backlog else 0 end) as DEL457T,
  sum(case when product_quoted like '/Landscaping%' and old_region = 'South California' then backlog else 0 end) as DEL458T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-SV' then backlog else 0 end) as DEL459T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-OC' then backlog else 0 end) as DEL460T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-LA' then backlog else 0 end) as DEL461T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-VC' then backlog else 0 end) as DEL462T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-CA-SD' then backlog else 0 end) as DEL463T,
  sum(case when product_quoted like '/Landscaping%' and old_region = 'Texas' then backlog else 0 end) as DEL464T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-TX-DL' then backlog else 0 end) as DEL465T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-TX-FW' then backlog else 0 end) as DEL466T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-TX-HT' then backlog else 0 end) as DEL467T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-TX-SA' then backlog else 0 end) as DEL468T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-TX-AU' then backlog else 0 end) as DEL469T,
  sum(case when product_quoted like '/Landscaping%' and old_region = 'Georgia' then backlog else 0 end) as DEL470T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-GA-AT' then backlog else 0 end) as DEL471T,
  sum(case when product_quoted like '/Landscaping%' and old_region in ('Maryland', 'Pennsylvania', 'Virginia') then backlog else 0 end) as DEL472T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-MD-BL' then backlog else 0 end) as DEL473T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-MD-DC' then backlog else 0 end) as DEL474T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-PA-PH' then backlog else 0 end) as DEL475T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-VA-AR' then backlog else 0 end) as DEL476T,
  sum(case when product_quoted like '/Landscaping%' and old_region = 'Florida' then backlog else 0 end) as DEL477T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-FL-MI' then backlog else 0 end) as DEL478T,
  sum(case when product_quoted like '/Landscaping%' and market like '%-FL-OR' then backlog else 0 end) as DEL479T
from cte
group by 1
)
select 
  date_trunc(t.day,{period}) as date,
  c.*except(day) 
from timeseries t
left join final_cte c on c.day = t.day
where t.period_rank = 1 
