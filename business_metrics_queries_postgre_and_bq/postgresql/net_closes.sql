-- upload to BQ
with
calc_lead as
(
        select
                so.id as order_id,
                min(cl.id) as lead_id
        from store_order so
        left join core_lead cl on cl.order_id = so.id
        group by 1
),
min_lead_service as --in cases of a lead with multiple services we grab the one with the smallest ID
(
select
	lead_id,
	min(cls.id) as first_lead_service
from
	core_lead_services cls
group by 1
),
physical_orders as
(
        select
                a.order_id,
                  min(a.date) as day,
                1 as is_physical_onsite
        from schedule_appointment a
        left join store_order o on o.id = a.order_id
        left join schedule_appointmenttype ssat on ssat.id = a.appointment_type_id
        where
                        ssat.code = 'physical_onsite'
                and a.cancelled_at is null
        group by 1
),
returning_customers as
(
select
        o.id as order_id,
        rank() over (partition by c.id order by o.created_at asc) as rank_order
from store_order o
left join core_house h on h.id = o.house_id
left join customers_customer c on c.id = h.customer_id
where
        o.created_at > '2018-04-15' and o.approved_quote_id is not null
),
tier_change_orders as
(
        select
                qq.id,
                coalesce(tier_id <> lag(tier_id) over (partition by order_id order by approved_at),false) as tier_change
        from quote_quote qq
        where qq.approved_at is not null 
        and tier_id is not null
        and is_cancellation = False
        order by 1,approved_at
),
approved_before_change_orders as
(
        select
                so.id as order_id,
                qq.id as quote_id,
                rank() over (partition by so.id order by qq.approved_at desc) as rank_approved_before_change_orders
        from store_order so
        left join quote_quote qq on qq.order_id = so.id
        where qq.is_scope_change is null
        and qq.approved_at is not null
        and is_cancellation = False
),
last_approved_quotes as 
(
  select 
    o.id as order_id,
    completed_at as cancelled_at,
    is_cancellation,
    rank() over(partition by o.id order by approved_at desc,q.id desc) as approved_rank
  from 
    store_order o join 
    quote_quote q on q.order_id = o.id 
  where 
    q.created_at >= '2018-04-16'
    and approved_at is not null
),
cancelled_project as 
(
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
calc_data as
(
        (
        select
                so.id as order_id,
                case when is_scope_change is true
                then sum(qq.total_price) over (partition by (case when is_scope_change is true or ac.quote_id is not null then so.id end) order by qq.approved_at)
                else qq.total_price end as total_price,
                qq.approved_at as transaction_ts,
                'approved_quote' as type,
                tc.tier_change,
                qq.created_at,
                qq.is_scope_change
        from store_order so
        left join quote_quote qq on qq.order_id = so.id
        left join tier_change_orders tc on tc.id = qq.id
        left join approved_before_change_orders ac on ac.quote_id = qq.id and rank_approved_before_change_orders = 1
        where qq.approved_at is not null
        and qq.approved_at >= '2018-04-15'
        and parent_order_id is null
        and qq.is_cancellation = False
        and so.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
                                        59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
        )
        union all
        --cancelled after won 18/08/2022
        (
     	select
        	so.id as order_id,
        	0 as total_price,
                --so.cancelled_at as transaction_ts, --will be deteled
                coalesce(cp.cancelled_at,so.cancelled_at) as transaction_ts,
        	'cancellation' as type,
        	false as tier_change,
        	null as created_at,
        	false as is_scope_change
	from store_order so
	left join quote_quote qq on qq.order_id = so.id
        left join cancelled_project cp on cp.order_id = so.id
	where qq.approved_at is not null --will be deleted
	and qq.approved_at >= '2018-04-15'
	and so.parent_order_id is null
	and so.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
			59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
	--and so.cancelled_at is not null --will be deleted
	and coalesce(cp.cancelled_at,so.cancelled_at) >= qq.approved_at --will be deleted
        --and so.completed_at is null --will be deleted
        --and is_cancellation = True
        )
),
calc_transaction_initial as
(
select
        cd.*,
        rank() over (partition by cd.order_id order by transaction_ts) as rank,
        case when type = 'approved_quote' then
        (rank() over (partition by (case when type = 'approved_quote' then cd.order_id else null end) order by transaction_ts)) end as rank_approved,
    	total_price - coalesce((lag(total_price) over (partition by cd.order_id order by transaction_ts)),0) as transaction_amount,
        po.is_physical_onsite,
        rc.rank_order,
        so.product_id,
        pm.id as market_id,
        pm.code as market_code,
        case when pcnm.market_id = 1 then 'CS-SD'
        when pcnm.market_id = 2 then 'CN-EB'
        when pcnm.market_id = 3 then 'CN-SA'
        when pcnm.market_id = 4 then 'CN-WA'
        when pcnm.market_id = 5 then 'CS-OC'
        when pcnm.market_id = 6 then 'CS-LA'
        when pcnm.market_id = 7 then 'CS-VC'
        when pcnm.market_id = 8 then 'CN-SF'
        when pcnm.market_id = 9 then 'CN-NB'
        when pcnm.market_id = 10 then 'CN-FR'
        when pcnm.market_id = 11 then 'CS-CC'
        when pcnm.market_id = 12 then 'CS-CV'
        when pcnm.market_id = 13 then 'CN-NC'
        when pcnm.market_id = 14 then 'CS-SV'
        when pcnm.market_id = 16 then 'TX-DL'
        when pcnm.market_id = 17 then 'TX-FW'
        when pcnm.market_id = 18 then 'TX-HT'
        when pcnm.market_id = 19 then 'TX-SA'
        when pcnm.market_id = 20 then 'GA-AT'
        when pcnm.market_id = 21 then 'MD-DC'
        when pcnm.market_id = 22 then 'MD-BL'
        when pcnm.market_id = 29 then 'CN-ST'
        when pcnm.market_id = 30 then 'CN-SJ'
        when pcnm.market_id = 31 then 'CN-PA'
        when pcnm.market_id = 32 then 'TX-AU'
        when pcnm.market_id = 33 then 'PA-PH'
        when pcnm.market_id = 35 then 'VA-AR'
        when pcnm.market_id = 24 then 'FL-MI'
        when pcnm.market_id = 26 then 'FL-OR'
        when pcnm.market_id = 43 then 'WA-SE'
        when pcnm.market_id = 42 then 'WN-CH'
        when pcnm.market_id = 57 then 'WN-NA'
        when pcnm.market_id = 58 then 'WN-LA'
        else null end as code,
        pm.region_id as region_id,
        cc.is_commercial::integer,
        ps.label as service_category,
       case when type = 'cancellation' then
       row_number() over (partition by (case when type = 'cancellation' then cd.order_id else null end) order by transaction_ts) end as rank_cancelled
from calc_data cd
left join physical_orders po on po.order_id = cd.order_id
left join returning_customers rc on rc.order_id = cd.order_id
left join store_order so on so.id = cd.order_id
left join core_house h on h.id = so.house_id
left join customers_customer cc on cc.id = h.customer_id
left join geo_address ga on ga.id = h.address_id
left join geo_county cn on cn.id = ga.county_id
left join product_countymarket pcnm on pcnm.county_id = cn.id
left join product_market pm on pm.id = pcnm.market_id
left join calc_lead cl on cl.order_id = so.id
left join core_lead l on l.id = cl.lead_id
left join min_lead_service ml on l.id = ml.lead_id
left join core_lead_services cls on cls.id = ml.first_lead_service
left join product_service ps on ps.id = cls.service_id
left join customers_contact cco on cco.id = l.contact_id
left join core_user cu on cu.id = cco.user_id
where coalesce(l.full_name,'')||coalesce(cco.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
and coalesce(l.email,'')||coalesce(cu.email,'') not ilike '%+test%'
order by order_id desc, transaction_ts
),
calc_transaction as
(
	select 
		ct1.*, 
		ct2.total_price as cancelled_price
	from calc_transaction_initial ct1
	left join calc_transaction_initial ct2 on ct2.order_id = ct1.order_id and ct1.rank_cancelled = ct2.rank	
),
calc_cancelled_orders as
(
select
	date_trunc('{period}', transaction_ts at time zone 'America/Los_Angeles')::date as date,
	count(order_id) as cancelled_projects,
	count(case when product_id = 105 and is_commercial = 0 and service_category ilike '%vinyl%' then order_id else null end) as vinyl_fence_cancelled_projects,
    count(case when product_id = 105 and is_commercial = 0 then order_id else null end) as fence_cancelled_projects,
    count(case when is_commercial = 1 then order_id else null end) as commercial_cancelled_projects,
    count(case when is_physical_onsite = 1 then order_id else null end) as cancelled_physical_projects,
    count(case when product_id = 105 and is_physical_onsite = 1  and is_commercial = 0 then order_id else null end) as cancelled_fence_physical_projects,
    count(case when is_commercial = 1 and is_physical_onsite = 1 then order_id else null end) as cancelled_commercial_physical_projects,
    count(case when product_id = 105 and code like '%CN-%' and is_commercial = 0 then order_id else null end) as norcal_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-EB' and is_commercial = 0 then order_id else null end) as eb_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-SF' and is_commercial = 0 then order_id else null end) as sf_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'GA-AT' and is_commercial = 0 then order_id else null end) as at_cancelled_fence_projects,
    count(case when product_id = 105 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') and is_commercial = 0 then order_id else null end) as sbsf_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-NB' and is_commercial = 0 then order_id else null end) as nb_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-SA' and is_commercial = 0 then order_id else null end) as sac_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-FR' and is_commercial = 0 then order_id else null end) as fr_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%CS-%' and is_commercial = 0 then order_id else null end) as sc_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CS-SV' and is_commercial = 0 then order_id else null end) as sv_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CS-OC' and is_commercial = 0 then order_id else null end) as oc_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CS-LA' and is_commercial = 0 then order_id else null end) as la_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CS-VC' and is_commercial = 0 then order_id else null end) as vc_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'TX-DL' and is_commercial = 0 then order_id else null end) as dl_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'TX-SA' and is_commercial = 0 then order_id else null end) as sa_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'TX-HT' and is_commercial = 0 then order_id else null end) as ht_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'TX-AU' and is_commercial = 0 then order_id else null end) as au_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'TX-FW' and is_commercial = 0 then order_id else null end) as fw_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%TX-%' and is_commercial = 0 then order_id else null end) as tx_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CS-SD' and is_commercial = 0 then order_id else null end) as sd_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-WA' and is_commercial = 0 then order_id else null end) as wa_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-SJ' and is_commercial = 0 then order_id else null end) as sj_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-PA' and is_commercial = 0 then order_id else null end) as pa_cancelled_fence_projects,
    count(case when product_id = 105 and (code like '%MD-%' or code like '%PA-%' or code like '%VA-%') and is_commercial = 0 then order_id else null end) as ne_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'CN-ST' and is_commercial = 0 then order_id else null end) as st_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%MD-%' and is_commercial = 0 then order_id else null end) as md_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'MD-BL' and is_commercial = 0 then order_id else null end) as bl_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'MD-DC' and is_commercial = 0 then order_id else null end) as dc_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%PA-%' and is_commercial = 0 then order_id else null end) as pen_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'PA-PH' and is_commercial = 0 then order_id else null end) as ph_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%GA-%' and is_commercial = 0 then order_id else null end) as ga_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%VA-%' and is_commercial = 0 then order_id else null end) as va_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'VA-AR' and is_commercial = 0 then order_id else null end) as ar_cancelled_fence_projects,
    count(case when product_id = 105 and code like '%FL-%' and is_commercial = 0 then order_id else null end) as fl_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'FL-MI' and is_commercial = 0 then order_id else null end) as mi_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'FL-OR' and is_commercial = 0 then order_id else null end) as or_cancelled_fence_projects,
    count(case when product_id = 105 and code like 'PA-WA-%' and is_commercial = 0 then order_id else null end) as pa_wa_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'WA-SE' and is_commercial = 0 then order_id else null end) as se_cancelled_fence_projects,
    count(case when product_id = 105 and code like 'WN-%' and is_commercial = 0 then order_id else null end) as wn_il_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'WN-CH' and is_commercial = 0 then order_id else null end) as wn_ch_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'WN-NA' and is_commercial = 0 then order_id else null end) as wn_na_cancelled_fence_projects,
    count(case when product_id = 105 and code = 'WN-LA' and is_commercial = 0 then order_id else null end) as wn_la_cancelled_fence_projects,
    --Turf
    count(case when product_id = 132 and is_commercial = 0 then order_id else null end) as turf_cancelled_projects,
    count(case when product_id = 132 and is_physical_onsite = 1  and is_commercial = 0 then order_id else null end) as cancelled_turf_physical_projects,
    --Hardscape
    count(case when product_id = 34 and is_commercial = 0 then order_id else null end) as driveway_cancelled_projects,
    count(case when product_id = 34 and code like '%TX-%' and is_commercial = 0 then order_id else null end) as tx_cancelled_driveway_projects,
    count(case when product_id = 34 and is_physical_onsite = 1  and is_commercial = 0 then order_id else null end) as cancelled_driveway_physical_projects,
    count(case when product_id = 34 and code like '%CN-%' and is_commercial = 0 then order_id else null end) as norcal_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-EB' and is_commercial = 0 then order_id else null end) as eb_cancelled_driveway_projects,
    count(case when product_id = 34 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') and is_commercial = 0 then order_id else null end) as sbsf_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-NB' and is_commercial = 0 then order_id else null end) as nb_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-SA' and is_commercial = 0 then order_id else null end) as sac_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-FR' and is_commercial = 0 then order_id else null end) as fr_cancelled_driveway_projects,
    count(case when product_id = 34 and code like '%CS-%' and is_commercial = 0 then order_id else null end) as sc_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CS-SV' and is_commercial = 0 then order_id else null end) as sv_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CS-OC' and is_commercial = 0 then order_id else null end) as oc_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CS-LA' and is_commercial = 0 then order_id else null end) as la_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'TX-DL' and is_commercial = 0 then order_id else null end) as dl_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'TX-FW' and is_commercial = 0 then order_id else null end) as fw_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CS-SD' and is_commercial = 0 then order_id else null end) as sd_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-WA' and is_commercial = 0 then order_id else null end) as wa_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-SJ' and is_commercial = 0 then order_id else null end) as sj_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-PA' and is_commercial = 0 then order_id else null end) as pa_cancelled_driveway_projects,
    count(case when product_id = 34 and code = 'CN-ST' and is_commercial = 0 then order_id else null end) as st_cancelled_driveway_projects
from calc_transaction ct
where type = 'cancellation'
and rank_cancelled = 1
group by 1
),
calc_revenue
as
(
select
        date_trunc('{period}',transaction_ts at time zone 'America/Los_Angeles')::date as date,
        -- Net Revenue
                --Fence, Commercial and Totals
        sum(transaction_amount) as net_revenue,
        sum(case when product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as net_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and service_category ilike '%vinyl%' then transaction_amount else 0 end) as net_vinyl_fence_revenue,
        sum(case when is_commercial = 1 then transaction_amount else 0 end) as net_commercial_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%CN-%' then transaction_amount else 0 end) as net_norcal_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-EB' then transaction_amount else 0 end) as net_eb_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-SF' then transaction_amount else 0 end) as net_sf_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'GA-AT' then transaction_amount else 0 end) as net_at_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') then transaction_amount else 0 end) as net_sbsf_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-NB' then transaction_amount else 0 end) as net_nb_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-SA' then transaction_amount else 0 end) as net_sac_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-FR' then transaction_amount else 0 end) as net_fr_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%CS-%' then transaction_amount else 0 end) as net_sc_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CS-SV' then transaction_amount else 0 end) as net_sv_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CS-OC' then transaction_amount else 0 end) as net_oc_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CS-LA' then transaction_amount else 0 end) as net_la_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CS-VC' then transaction_amount else 0 end) as net_vc_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'TX-DL' then transaction_amount else 0 end) as net_dl_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'TX-FW' then transaction_amount else 0 end) as net_fw_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%TX-%' then transaction_amount else 0 end) as net_tx_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CS-SD' then transaction_amount else 0 end) as net_sd_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-WA' then transaction_amount else 0 end) as net_wa_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-SJ' then transaction_amount else 0 end) as net_sj_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-PA' then transaction_amount else 0 end) as net_pa_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'CN-ST' then transaction_amount else 0 end) as net_st_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'TX-SA' then transaction_amount else 0 end) as net_sa_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'TX-HT' then transaction_amount else 0 end) as net_ht_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'TX-AU' then transaction_amount else 0 end) as net_au_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and (code like '%MD-%' or code like '%PA-%' or code like '%VA-%') then transaction_amount else 0 end) as net_ne_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%MD-%' then transaction_amount else 0 end) as net_md_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'MD-BL' then transaction_amount else 0 end) as net_bl_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'MD-DC' then transaction_amount else 0 end) as net_dc_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'PA-PH' then transaction_amount else 0 end) as net_ph_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%PA-%' then transaction_amount else 0 end) as net_pen_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%GA-%' then transaction_amount else 0 end) as net_ga_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%VA-%' then transaction_amount else 0 end) as net_va_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'VA-AR' then transaction_amount else 0 end) as net_ar_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like '%FL-%' then transaction_amount else 0 end) as net_fl_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'FL-MI' then transaction_amount else 0 end) as net_mi_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'FL-OR' then transaction_amount else 0 end) as net_or_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like 'PA-WA-%' then transaction_amount else 0 end) as net_pa_wa_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'WA-SE' then transaction_amount else 0 end) as net_se_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code like 'WN-%' then transaction_amount else 0 end) as net_wn_il_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'WN-CH' then transaction_amount else 0 end) as net_wn_ch_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'WN-NA' then transaction_amount else 0 end) as net_wn_na_fence_revenue,
        sum(case when product_id = 105 and is_commercial = 0 and code = 'WN-LA' then transaction_amount else 0 end) as net_wn_la_fence_revenue,
                -- Turf
        sum(case when product_id = 132 and is_commercial = 0 then transaction_amount else 0 end) as net_turf_revenue,
                --Hardscape
        sum(case when product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as net_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code like '%TX-%' then transaction_amount else 0 end) as net_tx_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code like '%CN-%' then transaction_amount else 0 end) as net_norcal_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-EB' then transaction_amount else 0 end) as net_eb_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') then transaction_amount else 0 end) as net_sbsf_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-NB' then transaction_amount else 0 end) as net_nb_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-SA' then transaction_amount else 0 end) as net_sac_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-FR' then transaction_amount else 0 end) as net_fr_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code like '%CS-%' then transaction_amount else 0 end) as net_sc_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CS-SV' then transaction_amount else 0 end) as net_sv_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CS-OC' then transaction_amount else 0 end) as net_oc_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CS-LA' then transaction_amount else 0 end) as net_la_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CS-VC' then transaction_amount else 0 end) as net_vc_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'TX-DL' then transaction_amount else 0 end) as net_dl_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'TX-FW' then transaction_amount else 0 end) as net_fw_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CS-SD' then transaction_amount else 0 end) as net_sd_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-WA' then transaction_amount else 0 end) as net_wa_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-SJ' then transaction_amount else 0 end) as net_sj_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-PA' then transaction_amount else 0 end) as net_pa_driveway_revenue,
        sum(case when product_id = 34 and is_commercial = 0 and code = 'CN-ST' then transaction_amount else 0 end) as net_st_driveway_revenue,
        -- Adjusted projects
        count(distinct case when type = 'approved_quote' and tier_change is false and rank_approved > 1 and abs(transaction_amount) <> 0 then order_id else null end) as adjusted_projects,
        count(distinct case when type = 'approved_quote' and tier_change is false and rank_approved > 1 and abs(transaction_amount) <> 0 and product_id = 105 and is_commercial = 0 then order_id else null end) as fence_adjusted_projects,
        count(distinct case when type = 'approved_quote' and tier_change is false and rank_approved > 1 and abs(transaction_amount) <> 0 and product_id = 132 and is_commercial = 0 then order_id else null end) as turf_adjusted_projects,
        count(distinct case when type = 'approved_quote' and tier_change is false and rank_approved > 1 and abs(transaction_amount) <> 0 and product_id = 34 and is_commercial = 0 then order_id else null end) as driveway_adjusted_projects,
        count(distinct case when type = 'approved_quote' and tier_change is false and rank_approved > 1 and abs(transaction_amount) <> 0 and is_commercial = 1 then order_id else null end) as commercial_adjusted_projects,
        -- Cancelled Revenue
        sum(case when type = 'cancellation' and rank_cancelled = 1 then abs(cancelled_price) else 0 end) as cancellations,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 then abs(cancelled_price) else 0 end) as cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 132 then abs(cancelled_price) else 0 end) as cancellations_turf,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 34 then abs(cancelled_price) else 0 end) as cancellations_driveway,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 1 then abs(cancelled_price) else 0 end) as cancellations_commercial,
        -- By Market Fence Cancelled Revenue
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 2 then abs(cancelled_price) else 0 end) as cn_eb_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 9 then abs(cancelled_price) else 0 end) as cn_nb_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 3 then abs(cancelled_price) else 0 end) as cn_sa_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 8 then abs(cancelled_price) else 0 end) as cn_sf_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 29 then abs(cancelled_price) else 0 end) as cn_st_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 10 then abs(cancelled_price) else 0 end) as cn_fr_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 4 then abs(cancelled_price) else 0 end) as cn_wa_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 30 then abs(cancelled_price) else 0 end) as cn_sj_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 31 then abs(cancelled_price) else 0 end) as cn_pa_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 14 then abs(cancelled_price) else 0 end) as cs_sv_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 5 then abs(cancelled_price) else 0 end) as cs_oc_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 6 then abs(cancelled_price) else 0 end) as cs_la_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 7 then abs(cancelled_price) else 0 end) as cs_vc_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 1 then abs(cancelled_price) else 0 end) as cs_sd_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 16 then abs(cancelled_price) else 0 end) as tx_dl_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 17 then abs(cancelled_price) else 0 end) as tx_fw_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 18 then abs(cancelled_price) else 0 end) as tx_ht_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 19 then abs(cancelled_price) else 0 end) as tx_sa_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 32 then abs(cancelled_price) else 0 end) as tx_au_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 20 then abs(cancelled_price) else 0 end) as ga_at_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 22 then abs(cancelled_price) else 0 end) as md_bl_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 21 then abs(cancelled_price) else 0 end) as md_dc_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 33 then abs(cancelled_price) else 0 end) as pa_ph_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 35 then abs(cancelled_price) else 0 end) as va_ar_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 24 then abs(cancelled_price) else 0 end) as fl_mi_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 26 then abs(cancelled_price) else 0 end) as fl_or_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%CN-%' then abs(cancelled_price) else 0 end) as nc_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%CS-%' then abs(cancelled_price) else 0 end) as sc_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%TX-%' then abs(cancelled_price) else 0 end) as tx_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%GA-%' then abs(cancelled_price) else 0 end) as ga_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and region_id in (5,7,8) then abs(cancelled_price) else 0 end) as ne_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%FL-%' then abs(cancelled_price) else 0 end) as fl_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like '%PA-WA-%' then abs(cancelled_price) else 0 end) as pa_wa_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 43 then abs(cancelled_price) else 0 end) as wa_se_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and code like 'WN-%' then abs(cancelled_price) else 0 end) as wn_il_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 42 then abs(cancelled_price) else 0 end) as wn_ch_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 57 then abs(cancelled_price) else 0 end) as wn_na_cancellations_fence,
        sum(case when type = 'cancellation' and rank_cancelled = 1 and is_commercial = 0 and product_id = 105 and market_id = 58 then abs(cancelled_price) else 0 end) as wn_la_cancellations_fence,
        -- Adjusted Revenue
        sum(case when type = 'approved_quote' and rank_approved > 1 then transaction_amount else 0 end) as adjustments,
        sum(case when type = 'approved_quote' and rank_approved > 1 and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as fence_adjustments,
        sum(case when type = 'approved_quote' and rank_approved > 1 and product_id = 132 and is_commercial = 0 then transaction_amount else 0 end) as turf_adjustments,
        sum(case when type = 'approved_quote' and rank_approved > 1 and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as driveway_adjustments,
        sum(case when type = 'approved_quote' and rank_approved > 1 and is_commercial = 1 then transaction_amount else 0 end) as commercial_adjustments,
        -- Initial projects closed (net projects = initial_projects - cancelled_projects)
                --Fence, Commercial and Totals
        count(case when type = 'approved_quote' and rank_approved = 1 then order_id else null end) as initial_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 then order_id else null end) as initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and service_category ilike '%vinyl%' then order_id else null end) as initial_vinyl_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and is_commercial = 1 then order_id else null end) as initial_commercial_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%CN-%' then order_id else null end) as norcal_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-EB' then order_id else null end) as eb_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-SF' then order_id else null end) as sf_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'GA-AT' then order_id else null end) as at_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') then order_id else null end) as sbsf_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-NB' then order_id else null end) as nb_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-SA' then order_id else null end) as sac_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-FR' then order_id else null end) as fr_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%CS-%' then order_id else null end) as sc_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CS-SV' then order_id else null end) as sv_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CS-OC' then order_id else null end) as oc_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CS-LA' then order_id else null end) as la_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CS-VC' then order_id else null end) as vc_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'TX-DL' then order_id else null end) as dl_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'TX-FW' then order_id else null end) as fw_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%TX-%' then order_id else null end) as tx_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CS-SD' then order_id else null end) as sd_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-WA' then order_id else null end) as wa_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-SJ' then order_id else null end) as sj_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-PA' then order_id else null end) as pa_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'CN-ST' then order_id else null end) as st_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'TX-SA' then order_id else null end) as sa_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'TX-HT' then order_id else null end) as ht_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'TX-AU' then order_id else null end) as au_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and (code like '%MD-%' or code like '%PA-%' or code like '%VA-%') then order_id else null end) as ne_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%MD-%' then order_id else null end) as md_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'MD-BL' then order_id else null end) as bl_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'MD-DC' then order_id else null end) as dc_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%PA-%' then order_id else null end) as pen_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'PA-PH' then order_id else null end) as ph_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%GA-%' then order_id else null end) as ga_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%VA-%' then order_id else null end) as va_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'VA-AR' then order_id else null end) as ar_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like '%FL-%' then order_id else null end) as fl_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'FL-MI' then order_id else null end) as mi_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'FL-OR' then order_id else null end) as or_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like 'PA-WA-%' then order_id else null end) as pa_wa_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'WA-SE' then order_id else null end) as se_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code like 'WN-%' then order_id else null end) as wn_il_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'WN-CH' then order_id else null end) as wn_ch_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'WN-NA' then order_id else null end) as wn_na_initial_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and code = 'WN-LA' then order_id else null end) as wn_la_initial_fence_projects_closed,
                --Turf
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 132 and is_commercial = 0 then order_id else null end) as initial_turf_projects_closed,
                --Hardscape
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 then order_id else null end) as initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code like '%TX-%' then order_id else null end) as tx_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code like '%CN-%' then order_id else null end) as norcal_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-EB' then order_id else null end) as eb_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code in ('CN-WA','CN-WA','CN-PA','CN-SF') then order_id else null end) as sbsf_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-NB' then order_id else null end) as nb_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-SA' then order_id else null end) as sac_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-FR' then order_id else null end) as fr_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code like '%CS-%' then order_id else null end) as sc_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CS-SV' then order_id else null end) as sv_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CS-OC' then order_id else null end) as oc_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CS-LA' then order_id else null end) as la_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'TX-DL' then order_id else null end) as dl_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'TX-FW' then order_id else null end) as fw_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CS-SD' then order_id else null end) as sd_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-WA' then order_id else null end) as wa_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-SJ' then order_id else null end) as sj_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-PA' then order_id else null end) as pa_initial_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and code = 'CN-ST' then order_id else null end) as st_initial_driveway_projects_closed,
    -- Initial projects closed with a physical_onsite (net projects with physical onsite = initial projects with physical_onsite - cancelled_projects with physical onsite)
        count(case when type = 'approved_quote' and rank_approved = 1 and is_physical_onsite = 1 then order_id else null end) initial_physical_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 105 and is_commercial = 0 and is_physical_onsite = 1 then order_id else null end) as initial_physical_fence_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 132 and is_commercial = 0 and is_physical_onsite = 1 then order_id else null end) as initial_physical_turf_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and product_id = 34 and is_commercial = 0 and is_physical_onsite = 1 then order_id else null end) as initial_physical_driveway_projects_closed,
        count(case when type = 'approved_quote' and rank_approved = 1 and is_commercial = 1 and is_physical_onsite = 1 then order_id else null end) as initial_physical_commercial_projects_closed,
        -- Net revenue for returning customers
        sum(case when rank_order > 1 then transaction_amount else 0 end) as net_returning_customer_revenue,
        sum(case when rank_order > 1 and is_commercial = 0 and product_id = 105 then transaction_amount else 0 end) as net_fence_returning_customer_revenue,
        sum(case when rank_order > 1 and is_commercial = 0 and product_id = 132 then transaction_amount else 0 end) as net_turf_returning_customer_revenue,
        sum(case when rank_order > 1 and is_commercial = 0 and product_id = 34 then transaction_amount else 0 end) as net_driveway_returning_customer_revenue,
        sum(case when rank_order > 1 and is_commercial = 1 then transaction_amount else 0 end) as net__commercial_returning_customer_revenue
from calc_transaction
group by 1
)
select
        cr.date,
                --Fence, Commercial and Totals
        net_revenue as SAL108,
        net_fence_revenue as SAL108F,
        net_vinyl_fence_revenue as SAL461F,
        net_commercial_revenue as SAL108C,
        net_norcal_fence_revenue as SAL480F,
        net_eb_fence_revenue as SAL301F,
        net_sf_fence_revenue as SAL1123F,
        net_at_fence_revenue as SAL703F,
        net_sbsf_fence_revenue as SAL302F,
        net_nb_fence_revenue as SAL303F,
        net_sac_fence_revenue as SAL304F,
        net_fr_fence_revenue as SAL305F,
        net_sc_fence_revenue as SAL374F,
        net_sv_fence_revenue as SAL420F,
        net_oc_fence_revenue as SAL424F,
        net_la_fence_revenue as SAL428F,
        net_vc_fence_revenue as SAL488F,
        net_dl_fence_revenue as SAL551F,
        net_fw_fence_revenue as SAL564F,
        net_tx_fence_revenue as SAL584F,
        net_sd_fence_revenue as SAL573F,
        net_wa_fence_revenue as SAL607F,
        net_sj_fence_revenue as SAL608F,
        net_pa_fence_revenue as SAL609F,
        net_st_fence_revenue as SAL610F,
        net_sa_fence_revenue as SAL721F,
        net_ht_fence_revenue as SAL712F,
        net_au_fence_revenue as SAL730F,
        net_md_fence_revenue as SAL782F,
        net_ne_fence_revenue as SAL967F,
        net_bl_fence_revenue as SAL736F,
        net_dc_fence_revenue as SAL756F,
        net_ph_fence_revenue as SAL797F,
        net_pen_fence_revenue as SAL823F,
        net_ga_fence_revenue as SAL846F,
        net_va_fence_revenue as SAL886F,
        net_ar_fence_revenue as SAL860F,
        net_fl_fence_revenue as SAL907F,
        net_mi_fence_revenue as SAL922F,
        net_or_fence_revenue as SAL942F,
        net_pa_wa_fence_revenue as SAL1080F,
        net_se_fence_revenue as SAL1049F,
        net_wn_il_fence_revenue as SAL1152F,
        net_wn_ch_fence_revenue as SAL1169F,
        net_wn_na_fence_revenue as SAL1192F,
        net_wn_la_fence_revenue as SAL1215F,
               --Turf
        net_turf_revenue as SAL108T,
                --Hardscape
        net_driveway_revenue as SAL108D,
        net_tx_driveway_revenue as SAL584D,
        net_norcal_driveway_revenue as SAL480D,
        net_eb_driveway_revenue as SAL301D,
        net_sbsf_driveway_revenue as SAL302D,
        net_nb_driveway_revenue as SAL303D,
        net_sac_driveway_revenue as SAL304D,
        net_fr_driveway_revenue as SAL305D,
        net_sc_driveway_revenue as SAL374D,
        net_sv_driveway_revenue as SAL420D,
        net_oc_driveway_revenue as SAL424D,
        net_la_driveway_revenue as SAL428D,
        net_vc_driveway_revenue as SAL488D,
        net_dl_driveway_revenue as SAL551D,
        net_fw_driveway_revenue as SAL564D,
        net_sd_driveway_revenue as SAL573D,
        net_wa_driveway_revenue as SAL607D,
        net_sj_driveway_revenue as SAL608D,
        net_pa_driveway_revenue as SAL609D,
        net_st_driveway_revenue as SAL610D,
                --Fence, Total and Commercial
        initial_projects_closed - coalesce(cancelled_projects,0) as SAL109,--net_projects_closed
        initial_fence_projects_closed - coalesce(fence_cancelled_projects, 0) as SAL109F, --net_fence_projects_closed
        initial_vinyl_fence_projects_closed - coalesce(vinyl_fence_cancelled_projects, 0) as SAL462F, --net_vinyl_fence_projects_closed
        initial_commercial_projects_closed - coalesce(commercial_cancelled_projects,0) as SAL109C, --net_commercial_projects_closed
        norcal_initial_fence_projects_closed - coalesce(norcal_cancelled_fence_projects,0) as SAL478F, --net_norcal_fence_projects_closed
        eb_initial_fence_projects_closed - coalesce(eb_cancelled_fence_projects,0) as SAL296F, --net_eb_fence_projects_closed
        sf_initial_fence_projects_closed - coalesce(sf_cancelled_fence_projects,0) as SAL1122F, --net_sf_fence_projects_closed
        at_initial_fence_projects_closed - coalesce(at_cancelled_fence_projects,0) as SAL701F, --net_at_fence_projects_closed
        sbsf_initial_fence_projects_closed - coalesce(sbsf_cancelled_fence_projects,0) as SAL297F, --net_sbsf_fence_projects_closed
        nb_initial_fence_projects_closed - coalesce(nb_cancelled_fence_projects,0) as SAL298F, --net_nb_fence_projects_closed
        sac_initial_fence_projects_closed - coalesce(sac_cancelled_fence_projects,0) as SAL299F, --net_sac_fence_projects_closed
        fr_initial_fence_projects_closed - coalesce(fr_cancelled_fence_projects,0) as SAL300F, --net_fr_fence_projects_closed
        sc_initial_fence_projects_closed - coalesce(sc_cancelled_fence_projects,0) as SAL372F, --net_eb_fence_projects_closed
        sv_initial_fence_projects_closed - coalesce(sv_cancelled_fence_projects,0) as SAL418F, --net_sv_fence_projects_closed
        oc_initial_fence_projects_closed - coalesce(oc_cancelled_fence_projects,0) as SAL422F, --net_oc_fence_projects_closed
        la_initial_fence_projects_closed - coalesce(la_cancelled_fence_projects,0) as SAL426F, --net_la_fence_projects_closed
        vc_initial_fence_projects_closed - coalesce(vc_cancelled_fence_projects,0) as SAL489F, --net_vc_fence_projects_closed
        dl_initial_fence_projects_closed - coalesce(dl_cancelled_fence_projects,0) as SAL549F, --net_dl_fence_projects_closed
        fw_initial_fence_projects_closed - coalesce(fw_cancelled_fence_projects,0) as SAL562F, --net_fw_fence_projects_closed
        tx_initial_fence_projects_closed - coalesce(tx_cancelled_fence_projects,0) as SAL582F, --net_tx_fence_projects_closed
        sd_initial_fence_projects_closed - coalesce(sd_cancelled_fence_projects,0) as SAL571F, --net_sd_fence_projects_closed
        wa_initial_fence_projects_closed - coalesce(wa_cancelled_fence_projects,0) as SAL611F, --net_wa_fence_projects_closed
        sj_initial_fence_projects_closed - coalesce(sj_cancelled_fence_projects,0) as SAL612F, --net_sj_fence_projects_closed
        pa_initial_fence_projects_closed - coalesce(pa_cancelled_fence_projects,0) as SAL613F, --net_pa_fence_projects_closed
        st_initial_fence_projects_closed - coalesce(wa_cancelled_fence_projects,0) as SAL614F, --net_st_fence_projects_closed
        sa_initial_fence_projects_closed - coalesce(sa_cancelled_fence_projects,0) as SAL719F, --net_sa_fence_projects_closed
        ht_initial_fence_projects_closed - coalesce(ht_cancelled_fence_projects,0) as SAL710F, --net_ht_fence_projects_closed
        au_initial_fence_projects_closed - coalesce(au_cancelled_fence_projects,0) as SAL728F, --net_au_fence_projects_closed
        md_initial_fence_projects_closed - coalesce(md_cancelled_fence_projects,0) as SAL780F, --net_md_fence_projects_closed
        ne_initial_fence_projects_closed - coalesce(ne_cancelled_fence_projects,0) as SAL965F, --net_ne_fence_projects_closed
        bl_initial_fence_projects_closed - coalesce(bl_cancelled_fence_projects,0) as SAL735F, --net_bl_fence_projects_closed
        dc_initial_fence_projects_closed - coalesce(dc_cancelled_fence_projects,0) as SAL755F, --net_dc_fence_projects_closed
        pen_initial_fence_projects_closed - coalesce(pen_cancelled_fence_projects,0) as SAL821F, --net_ph_fence_projects_closed
        ph_initial_fence_projects_closed - coalesce(ph_cancelled_fence_projects,0) as SAL796F, --net_ph_fence_projects_closed
        ga_initial_fence_projects_closed - coalesce(ga_cancelled_fence_projects,0) as SAL844F, --net_ga_fence_projects_closed
        va_initial_fence_projects_closed - coalesce(va_cancelled_fence_projects,0) as SAL884F, --net_va_fence_projects_closed
        ar_initial_fence_projects_closed - coalesce(ar_cancelled_fence_projects,0) as SAL859F, --net_ar_fence_projects_closed
        fl_initial_fence_projects_closed - coalesce(fl_cancelled_fence_projects,0) as SAL905F, --net_fl_fence_projects_closed
        mi_initial_fence_projects_closed - coalesce(mi_cancelled_fence_projects,0) as SAL920F, --net_mi_fence_projects_closed
        or_initial_fence_projects_closed - coalesce(or_cancelled_fence_projects,0) as SAL940F, --net_mi_fence_projects_closed
        pa_wa_initial_fence_projects_closed - coalesce(pa_wa_cancelled_fence_projects,0) as SAL1078F, --net_pa_wa_fence_projects_closed
        se_initial_fence_projects_closed - coalesce(se_cancelled_fence_projects,0) as SAL1048F, --net_se_fence_projects_closed
        wn_il_initial_fence_projects_closed - coalesce(wn_il_cancelled_fence_projects,0) as SAL1150F, --net_illinois_fence_projects_closed
        wn_ch_initial_fence_projects_closed - coalesce(wn_ch_cancelled_fence_projects,0) as SAL1168F, --net_wn_il_ch_fence_projects_closed
        wn_na_initial_fence_projects_closed - coalesce(wn_na_cancelled_fence_projects,0) as SAL1191F, --net_wn_il_na_fence_projects_closed
        wn_la_initial_fence_projects_closed - coalesce(wn_la_cancelled_fence_projects,0) as SAL1214F, --net_wn_il_la_fence_projects_closed
                --Turf
        initial_turf_projects_closed - coalesce(turf_cancelled_projects, 0) as SAL109T, --net_turf_projects_closed
                --Hardscape
        initial_driveway_projects_closed - coalesce(driveway_cancelled_projects,0) as SAL109D, --net_driveway_projects_closed
        norcal_initial_driveway_projects_closed - coalesce(norcal_cancelled_driveway_projects,0) as SAL478D, --net_norcal_driveway_projects_closed
        eb_initial_driveway_projects_closed - coalesce(eb_cancelled_driveway_projects,0) as SAL296D, --net_eb_driveway_projects_closed
        sbsf_initial_driveway_projects_closed - coalesce(sbsf_cancelled_driveway_projects,0) as SAL297D, --net_sbsf_driveway_projects_closed
        nb_initial_driveway_projects_closed - coalesce(nb_cancelled_driveway_projects,0) as SAL298D, --net_nb_driveway_projects_closed
        sac_initial_driveway_projects_closed - coalesce(sac_cancelled_driveway_projects,0) as SAL299D, --net_sac_driveway_projects_closed
        fr_initial_driveway_projects_closed - coalesce(fr_cancelled_driveway_projects,0) as SAL300D, --net_fr_driveway_projects_closed
        sc_initial_driveway_projects_closed - coalesce(sc_cancelled_driveway_projects,0) as SAL372D, --net_eb_driveway_projects_closed
        sv_initial_driveway_projects_closed - coalesce(sv_cancelled_driveway_projects,0) as SAL418D, --net_sv_driveway_projects_closed
        oc_initial_driveway_projects_closed - coalesce(oc_cancelled_driveway_projects,0) as SAL422D, --net_oc_driveway_projects_closed
        la_initial_driveway_projects_closed - coalesce(la_cancelled_driveway_projects,0) as SAL426D, --net_la_driveway_projects_closed
        dl_initial_driveway_projects_closed - coalesce(dl_cancelled_driveway_projects,0) as SAL549D, --net_dl_driveway_projects_closed
        fw_initial_driveway_projects_closed - coalesce(fw_cancelled_driveway_projects,0) as SAL562D, --net_fw_driveway_projects_closed
        tx_initial_driveway_projects_closed - coalesce(tx_cancelled_driveway_projects,0) as SAL582D, --net_tx_fence_projects_closed
        sd_initial_driveway_projects_closed - coalesce(sd_cancelled_driveway_projects,0) as SAL571D, --net_sd_driveway_projects_closed
        wa_initial_driveway_projects_closed - coalesce(wa_cancelled_driveway_projects,0) as SAL611D, --net_wa_driveway_projects_closed
        sj_initial_driveway_projects_closed - coalesce(sj_cancelled_driveway_projects,0) as SAL612D, --net_sj_driveway_projects_closed
        pa_initial_driveway_projects_closed - coalesce(pa_cancelled_driveway_projects,0) as SAL613D, --net_pa_driveway_projects_closed
        st_initial_driveway_projects_closed - coalesce(wa_cancelled_driveway_projects,0) as SAL614D, --net_st_driveway_projects_closed
        cancellations as SAL112,
        cancellations_fence as SAL112F,
        cancellations_turf as SAL112T,
        cancellations_driveway as SAL112D,
        cancellations_commercial as SAL112C,
        cn_eb_cancellations_fence as SAL979F,
        cn_nb_cancellations_fence as SAL986F,
        cn_sa_cancellations_fence as SAL980F,
        cn_st_cancellations_fence as SAL997F,
        cn_sf_cancellations_fence as SAL985F,
        cn_fr_cancellations_fence as SAL987F,
        cn_wa_cancellations_fence as SAL981F,
        cn_sj_cancellations_fence as SAL998F,
        cn_pa_cancellations_fence as SAL999F,
        cs_sv_cancellations_fence as SAL988F,
        cs_oc_cancellations_fence as SAL982F,
        cs_la_cancellations_fence as SAL983F,
        cs_vc_cancellations_fence as SAL984F,
        cs_sd_cancellations_fence as SAL978F,
        tx_dl_cancellations_fence as SAL989F,
        tx_fw_cancellations_fence as SAL990F,
        tx_ht_cancellations_fence as SAL991F,
        tx_sa_cancellations_fence as SAL992F,
        tx_au_cancellations_fence as SAL1000F,
        ga_at_cancellations_fence as SAL993F,
        md_bl_cancellations_fence as SAL995F,
        wa_se_cancellations_fence as SAL1065F,
        wn_il_cancellations_fence as SAL1138F,
        wn_ch_cancellations_fence as SAL1184F,
        wn_na_cancellations_fence as SAL1207F,
        wn_la_cancellations_fence as SAL1230F,
        md_dc_cancellations_fence as SAL994F,
        pa_ph_cancellations_fence as SAL1001F,
        va_ar_cancellations_fence as SAL1002F,
        fl_mi_cancellations_fence as SAL996F,
        fl_or_cancellations_fence as SAL1003F,
        nc_cancellations_fence as SAL1004F,
        sc_cancellations_fence as SAL1005F,
        tx_cancellations_fence as SAL1006F,
        ga_cancellations_fence as SAL1007F,
        ne_cancellations_fence as SAL1008F,
        fl_cancellations_fence as SAL1009F,
        pa_wa_cancellations_fence as SAL1067F,
        cancelled_projects as SAL114,
        fence_cancelled_projects as SAL114F,
        turf_cancelled_projects as SAL114T,
        driveway_cancelled_projects as SAL114D,
        commercial_cancelled_projects as SAL114C,
        adjustments as SAL116,
        fence_adjustments as SAL116F,
        turf_adjustments as SAL116T,
        driveway_adjustments as SAL116D,
        commercial_adjustments as SAL116C,
        adjusted_projects as SAL117,
        fence_adjusted_projects as SAL117F,
        turf_adjusted_projects as SAL117T,
        driveway_adjusted_projects as SAL117D,
        commercial_adjusted_projects as SAL117C,
        initial_physical_projects_closed - coalesce(cancelled_physical_projects,0) as SAL167,--net_physical_projects_closed
        initial_physical_fence_projects_closed - coalesce(cancelled_fence_physical_projects, 0) as SAL167F, --net_physical_fence_projects_closed
        initial_physical_turf_projects_closed - coalesce(cancelled_turf_physical_projects, 0) as SAL167T, --net_physical_turf_projects_closed
        initial_physical_driveway_projects_closed - coalesce(cancelled_driveway_physical_projects,0) as SAL167D, --net_physical_driveway_projects_closed
        initial_physical_commercial_projects_closed - coalesce(cancelled_commercial_physical_projects,0) as SAL167C,--net_physical_commercial_projects_closed
        coalesce(net_returning_customer_revenue/nullif(net_revenue,0),0) as SAL134,
        coalesce(net_fence_returning_customer_revenue/nullif(net_fence_revenue,0),0) as SAL134F,
        coalesce(net_turf_returning_customer_revenue/nullif(net_turf_revenue,0),0) as SAL134T,
        coalesce(net_driveway_returning_customer_revenue/nullif(net_driveway_revenue,0),0) as SAL134D,
        coalesce(net__commercial_returning_customer_revenue/nullif(net_commercial_revenue,0),0) as SAL134C
from calc_revenue cr
left join calc_cancelled_orders co on co.date = cr.date
order by 1 desc
