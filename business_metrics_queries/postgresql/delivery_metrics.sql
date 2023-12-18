-- upload to BQ
with
oleads as
( -- making sure we only grab one lead per order - the first one
	select order_id,
              case
                          when cv.event_object ->> 'utm_source' ilike '%yelp%' then 'Paid/Yelp' 
                          when cv.event_object ->> 'utm_source' ilike '%home%advisor%' and cv.event_object ->> 'utm_campaign' ilike 'ads' then 'Paid/Home Advisor/Ads' 
                          when cv.event_object ->> 'utm_source' ilike '%home%advisor%' then 'Paid/Home Advisor' 
                          when cv.event_object ->> 'utm_source' ilike '%thumbtack%' then 'Paid/Thumbtack' 
                          when cv.event_object ->> 'utm_source' ilike '%borg%' then 'Paid/Borg' 
                          when cv.event_object ->> 'utm_source' ilike '%dodge%' then 'Paid/Misc/Dodge' 
                          when cv.event_object ->> 'utm_source' ilike '%nextdoor%' then 'Paid/Nextdoor' 
                          when cv.event_object ->> 'utm_source' ilike '%lawson%'  then 'Paid/Misc/Lawson' 
                          when cv.event_object ->> 'utm_source' ilike '%bark%'  then 'Paid/Bark' 
                          when cv.event_object ->> 'utm_source' ilike '%facebook%' or 
                               cv.event_object ->> 'browser_name' ilike '%instagram%' or
                               cv.event_object ->> 'utm_source' ilike '%instagram%' then 
                                case 
                                        when cv.event_object ->> 'utm_campaign' is not null and cv.event_object ->> 'initial_landing_page' ilike '%ergeon.com/blog/%' then 'Non Paid/Facebook'
                                        when cv.event_object ->> 'initial_landing_page' ilike '%ergeon.com/' or cv.event_object ->> 'initial_landing_page' ilike '%ergeon.com/?fbclid%' then 'Non Paid/Facebook'
                                        else 'Paid/Facebook'  
                                end
                          when cv.event_object ->> 'utm_source' ilike '%directmail%'  then 'Non Paid/Misc/Direct Mail' 
                          when cv.event_object ->> 'utm_source' ilike '%email%' or 
                               cv.event_object ->> 'initial_referrer' ilike '%android.gm%' then 'Non Paid/Misc/Email Marketing' 
                          when cv.event_object ->> 'utm_source' ilike '%google%' or 
                               cv.event_object ->> 'utm_source' ilike '%bing%' or
                               cv.event_object ->> 'utm_source' ilike '%gmb%' then
                            case
                                      when cv.event_object ->> 'utm_campaign' ilike '%gls%' then 'Paid/Google/GLS'
                                      when cv.event_object ->> 'initial_landing_page' ilike '%gclid%' or
                                      	   cv.event_object ->> 'initial_landing_page' ilike '%gbraid%' or
                                      	   cv.event_object ->> 'initial_landing_page' ilike '%wbraid%' or
                                          (cv.event_object ->> 'utm_campaign' ilike '%ads%' and
                                           cv.event_object ->> 'utm_medium' ilike '%call%') then 'Paid/Google/Ads'
                                      when cv.event_object ->> 'utm_campaign' ilike '%gmb%' or
                                      	   cv.event_object ->> 'utm_source' ilike '%gmb%' then 'Non Paid/Google/GMB'
                                      when cv.event_object ->> 'utm_campaign' ilike '%ls%brand%' then 'Non Paid/Google/Direct'
                                      when cv.event_object ->> 'initial_landing_page' is null and cv.event_object ->> 'utm_medium' ilike '%website%' then 'Non Paid/Google/SEO'
                                      when cv.event_object ->> 'initial_landing_page' is null then 'Non Paid/Google/Direct'
                                      when cv.event_object ->> 'initial_landing_page' ilike '%ergeon.com/' then 'Non Paid/Google/Direct'
                                      when cv.event_object ->> 'initial_landing_page' ilike '%ergeon.com/%' then 'Non Paid/Google/SEO'
                                      else 'Non Paid/Misc/Google/Unknown'
                            end
                          when cv.event_object ->> 'utm_source' ilike '%direct%' then 'Non Paid/Direct'
                          when cv.event_object ->> 'utm_source' ilike '%sdr%' then 'Paid/Misc/SDR'
                          else 'Non Paid/Misc/Unknown'
                end as channel,
    min(l.id) as lead_id
	from core_lead l
	left join customers_visitoraction cv on cv.id = l.visitor_action_id
	where l.created_at >= '2018-04-16'
	group by 1,2
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
-- first approved date of an order
first_approved_date as 
( 
select 
  order_id,
  min(approved_at) as approved 
from 
  quote_quote 
where 
  sent_to_customer_at is not null 
  and created_at > '2018-04-15' 
  and is_cancellation = False
group by 1
),
quotes_sent_or_approved as -- ranking sent or approved quotes by ascending to get first sent or approved
(
select 
  qq.order_id,
  id, 
  approved,
  case when approved_at is null then null else rank() over(partition by qq.order_id order by coalesce(approved_at,'2100-01-01')) end as approved_rank, ----making sure that every null approved_at will have the lowest rank
  rank() over(partition by qq.order_id order by sent_to_customer_at,id) as non_approved_rank
from 
  quote_quote qq left join 
  first_approved_date a on a.order_id = qq.order_id
where 
  sent_to_customer_at is not null 
  and created_at > '2018-04-15'
  and is_cancellation = False
),
first_approved_or_sent_quotes as --ranking descending starting by the first approved quote (if approved) or the first quote sent
(
select 
  order_id,id as quote_id
from 
  quotes_sent_or_approved 
where 
  (approved is null and non_approved_rank = 1) or (approved is not null and approved_rank = 1)
),
services as
( --collecting all services for each quoteline
  select 
    ql.id, 
    coalesce(tys.service_id,ty.service_id,qlt.service_id) as service_id
  from 
    quote_quoteline ql left join 
    quote_quotestyle qs on qs.id = ql.quote_style_id left join
    product_catalog sct on sct.id = qs.catalog_id left join
    product_catalog ct on ct.id = ql.catalog_id left join
    product_catalogtype ty on ty.id = ct.type_id left join
    product_catalogtype tys on tys.id = sct.type_id left join 
    product_catalogtype qlt on qlt.id = ql.catalog_type_id left join
    quote_quote qq on qq.id = ql.quote_id 
  where
    is_cancellation = False
),
first_approved_quote_services as
( --rank service of an order according to the total price of each quoted service
select 
  order_id,service_id, 
  rank() over (partition by order_id order by sum(price) desc,coalesce(service_id,10000)) as rank --making sure that every null service_id will have the lowest rank in case of the same price
from 
  first_approved_or_sent_quotes ofq join 
  quote_quoteline ql on ql.quote_id = ofq.quote_id left join 
  services qlt on qlt.id = ql.id
group by 1,2
),
first_approved_quote_service as 
( --Grab the service id with the highest total amount of quoted price for each order 
select 
  order_id,service_id 
from 
  first_approved_quote_services 
where 
  rank = 1
),
prod_serv as ( --Get product_quoted
        select
	        o.id as order_id,
	        channel,
  		    case 
              when os.label is not null then osp.id
              when s.label is not null then sp.id
  		        else p.id
	        end as product_quoted,
		/*case
  		        when os.label is not null then os.label
  		        when p.id = 105 then 'Install a Wood Fence'
	        end as service_quoted*/        
--/Product/Service according to quoted product and service
  case 
    when os.label is not null then os.label
    when s.label is not null then s.label
    when p.id = 105 then 'Install a Wood Fence'
    when p.id = 132 then 'Install Artificial Grass'
    when p.id = 34 then 'Install Concrete Driveways & Floors'
  end as service_quoted
  from
                store_order o
        left join oleads ol on ol.order_id = o.id
        left join first_approved_quote_service ofs on ofs.order_id = o.id
        left join product_service os on os.id = ofs.service_id
        left join store_product osp on osp.id = os.product_id
        left join min_lead_service ml on ol.lead_id = ml.lead_id
        left join core_lead_services cls on cls.id = ml.first_lead_service
        left join product_service s on s.id = cls.service_id
        left join store_product sp on s.product_id = sp.id 
        left join store_product p on p.id = o.product_id
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
cancelled_projects as 
(
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
calc_cost as
(
        select
            date_trunc('{period}', o.completed_at at time zone 'America/Los_Angeles')::date as date,
            sum(co.total_cost) as total_cost,
            sum(case when product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as total_cost_fence,
            sum(case when product_quoted = 132 and is_commercial is false then co.total_cost else 0 end) as total_cost_turf,
            sum(case when product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as total_cost_driveway,
            sum(case when is_commercial is true then co.total_cost else 0 end) as total_cost_commercial,
            -- By Market
                --Fence
            sum(case when pm.id in (2,10,9,3,29,4,31,30,8,13) and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as norcal_fence_cost,
            sum(case when pcnm.market_id = 2 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as eb_fence_cost,
            sum(case when pcnm.market_id = 8 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as sf_fence_cost,
            sum(case when pcnm.market_id = 20 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as at_fence_cost,
            sum(case when pcnm.market_id = 18 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as ht_fence_cost,
            sum(case when pcnm.market_id = 32 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as au_fence_cost,
            sum(case when pcnm.market_id in (4,30,31,8) and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as sbsf_fence_cost,
            sum(case when pcnm.market_id = 9 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as nb_fence_cost,
            sum(case when pcnm.market_id = 10 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as fr_fence_cost,
            sum(case when pcnm.market_id = 3 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as sac_fence_cost,
            sum(case when pm.id in (6,5,14,7,1,12,11) and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as sc_fence_cost,
            sum(case when pcnm.market_id = 14 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as sv_fence_cost,
            sum(case when pcnm.market_id = 5 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as oc_fence_cost,
            sum(case when pcnm.market_id = 6 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as la_fence_cost,
            sum(case when pcnm.market_id = 7 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as vc_fence_cost,
            sum(case when pcnm.market_id = 16 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as dl_fence_cost,
            sum(case when pcnm.market_id = 17 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as fw_fence_cost,
            sum(case when pcnm.market_id = 1 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as sd_fence_cost,
            sum(case when pm.code like '%-TX-%' and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as tx_fence_cost,
            sum(case when pcnm.market_id = 4 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as wa_fence_cost,
            sum(case when pcnm.market_id = 30 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as sj_fence_cost,
            sum(case when pcnm.market_id = 31 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as pa_fence_cost,
            sum(case when pcnm.market_id = 29 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as st_fence_cost,
            sum(case when pm.code like '%-MD-%' and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as maryland_fence_cost,
            sum(case when (pm.code like '%-MD-%' or pm.code like '%-PA-%' or pm.code like '%-VA-%') and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as north_east_fence_cost,
            sum(case when pcnm.market_id = 22 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as bl_fence_cost,
            sum(case when pcnm.market_id = 21 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as dc_fence_cost,
            sum(case when pm.code like '%-PA-%' and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as pen_fence_cost,
            sum(case when pcnm.market_id = 33 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as ph_fence_cost,
            sum(case when pm.code like '%-VA-%' and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as va_fence_cost,
            sum(case when pcnm.market_id = 35 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as ar_fence_cost,
            sum(case when pm.code like '%-FL-%' and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as fl_fence_cost,
            sum(case when pcnm.market_id = 24 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as mi_fence_cost,
            sum(case when pcnm.market_id = 26 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as or_fence_cost,
            sum(case when pcnm.market_id = 43 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as se_fence_cost,
            sum(case when pm.code like '%-WA-%' and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as pa_wa_fence_cost,
            sum(case when pcnm.market_id = 19 and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as sa_fence_cost,
            sum(case when pm.code like '%WN-IL-%' and product_quoted = 105 and is_commercial is false then co.total_cost else 0 end) as wn_il_fence_cost,
            sum(case when pcnm.market_id = 42 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as wn_ch_fence_cost,
            sum(case when pcnm.market_id = 57 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as wn_na_fence_cost,
            sum(case when pcnm.market_id = 58 and is_commercial is false and product_quoted = 105 then co.total_cost else 0 end) as wn_la_fence_cost,
                --Hardscape
            sum(case when pm.id in (2,10,9,3,29,4,31,30,8,13) and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as norcal_driveway_cost,
            sum(case when pcnm.market_id = 2 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as eb_driveway_cost,
            sum(case when pcnm.market_id = 20 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as at_driveway_cost,
            sum(case when pcnm.market_id = 18 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as ht_driveway_cost,
            sum(case when pcnm.market_id in (4,30,31,8) and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as sbsf_driveway_cost,
            sum(case when pcnm.market_id = 9 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as nb_driveway_cost,
            sum(case when pcnm.market_id = 10 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as fr_driveway_cost,
            sum(case when pcnm.market_id = 3 and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as sac_driveway_cost,
            sum(case when pm.id in (6,5,14,7,1,12,11) and is_commercial is false and product_quoted = 34 then co.total_cost else 0 end) as sc_driveway_cost,
            sum(case when pcnm.market_id = 14 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as sv_driveway_cost,
            sum(case when pcnm.market_id = 5 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as oc_driveway_cost,
            sum(case when pcnm.market_id = 6 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as la_driveway_cost,
            sum(case when pcnm.market_id = 7 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as vc_driveway_cost,
            sum(case when pcnm.market_id = 16 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as dl_driveway_cost,
            sum(case when pcnm.market_id = 17 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as fw_driveway_cost,
            sum(case when pcnm.market_id = 1 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as sd_driveway_cost,
            sum(case when pm.code like '%-TX-%' and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as tx_driveway_cost,
            sum(case when pcnm.market_id = 4 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as wa_driveway_cost,
            sum(case when pcnm.market_id = 30 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as sj_driveway_cost,
            sum(case when pcnm.market_id = 31 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as pa_driveway_cost,
            sum(case when pcnm.market_id = 29 and product_quoted = 34 and is_commercial is false then co.total_cost else 0 end) as st_driveway_cost
        from contractor_contractororder co
        left join store_order o on co.order_id = o.id
        left join quote_quote q on q.id = o.approved_quote_id
        left join core_house h on h.id = o.house_id
        left join customers_customer cc on cc.id = h.customer_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_county cn on cn.id = ga.county_id
        left join product_countymarket pcnm on pcnm.county_id = cn.id
        left join product_market pm on pm.id = pcnm.market_id
        left join oleads l on l.order_id = o.id
        left join core_lead cl on cl.id = l.lead_id
        left join customers_contact cco on cco.id = cl.contact_id
        left join core_user cu on cu.id = cco.user_id
        left join prod_serv ps on ps.order_id = o.id
        left join cancelled_projects cp on cp.order_id = co.order_id
        where
            co.status_id = 13
            and o.completed_at is not null
            and cp.order_id is null
                and q.approved_at >= '2018-04-15'
                and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
                                                                59789,59805,59813,59878,59908,59922,60273,60283,60401,
                                                                60547,60589,60590,60595,60596,60597,60612)
                and coalesce(cl.full_name,'')||coalesce(cco.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
                and coalesce(cl.email,'')||coalesce(cu.email,'') not ilike '%+test%'
          group by 1
    order by 1 desc
),
calc_data
as
(
        (
        select
                so.id,
                so.completed_at,
                qq.approved_at,
                so.total_project_price,
                qq2.approved_at as last_approved_at,
                qq2.total_price as price_approved_quote,
                rank() over (partition by so.id order by qq.approved_at desc)
        from store_order so
        left join quote_quote qq on so.id = qq.order_id
        left join quote_quote qq2 on so.approved_quote_id = qq2.id
        left join cancelled_projects cp on cp.order_id = so.id
        where so.completed_at is not null and qq.approved_at is not null and cp.order_id is null
        and qq.approved_at > completed_at
        )
        union all
        (
        select
                *
        from
        (
                select
                        so.id,
                        so.completed_at,
                        qq.approved_at ,
                        so.total_project_price,
                        qq2.approved_at as last_approved_at,
                        qq2.total_price  as price_approved_quote,
                        rank() over (partition by so.id order by qq.approved_at desc)
                from store_order so
                left join quote_quote qq on so.id = qq.order_id
                left join quote_quote qq2 on so.approved_quote_id = qq2.id
                left join cancelled_projects cp on cp.order_id = so.id
                where so.completed_at is not null and qq.approved_at is not null and cp.order_id is null
                and qq.approved_at  <= so.completed_at
        ) as approved_before_completion_queries
        where rank = 1
        )
),
lag_price
as
(
select
        *,
        lag(total_project_price) over (partition by id order by approved_at) as price_previous_quote
from calc_data
order by 3 desc, rank
)
,
calc_order_transactions
as
(
        select
                lp.id,
                lp.completed_at,
                case when lp.approved_at > lp.completed_at then lp.approved_at else lp.completed_at end as transaction_date,
                case when price_previous_quote is not null then lp.total_project_price - price_previous_quote else lp.total_project_price end as transaction_amount,
                ps.product_quoted as product_id,
                ps.service_quoted as service_category,
                cc.is_commercial::integer,
                ssa.name as queue_name,
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
                o.parent_order_id,
                ps.channel
        from lag_price lp
        left join store_order o on lp.id = o.id
        left join quote_quote q on q.id = o.approved_quote_id
        left join core_house h on h.id = o.house_id
        left join customers_customer cc on cc.id = h.customer_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_county cn on cn.id = ga.county_id
        left join product_countymarket pcnm on pcnm.county_id = cn.id
        left join product_market pm on pm.id = pcnm.market_id
        left join oleads l on l.order_id = o.id
        left join core_lead cl on cl.id = l.lead_id
        left join prod_serv ps on ps.order_id = o.id
        left join tasks_assignmenttype ssa on ssa.id = o.assignment_type_id
        left join customers_contact co on co.id = cl.contact_id
        left join core_user cu on cu.id = co.user_id
        left join cancelled_projects cp on cp.order_id = lp.id
        where
                o.completed_at is not null
                and cp.order_id is null
                and q.approved_at >= '2018-04-15'
                and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
                                                        59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
                and coalesce(cl.full_name,'')||coalesce(co.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
                and coalesce(cl.email,'')||coalesce(cu.email,'') not ilike '%+test%'
),
calc_revenue
as
(
select
        date_trunc('{period}', transaction_date at time zone 'America/Los_Angeles')::date as date,
        sum(transaction_amount) as revenue_billed,
        sum(case when parent_order_id is not null then transaction_amount else 0 end) as warranty_revenue_billed,
        sum(case when product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as fence_revenue_billed,
        sum(case when product_id = 105 and is_commercial = 0 and parent_order_id is not null then transaction_amount else 0 end) as warranty_fence_revenue_billed,
        sum(case when product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as driveway_revenue_billed,
        sum(case when product_id = 34 and is_commercial = 0 and parent_order_id is not null then transaction_amount else 0 end) as warranty_driveway_revenue_billed,
        sum(case when product_id = 132 and is_commercial = 0 then transaction_amount else 0 end) as turf_revenue_billed,
        sum(case when product_id = 132 and is_commercial = 0 and parent_order_id is not null then transaction_amount else 0 end) as warranty_turf_revenue_billed,
        sum(case when service_category = 'Install a Wood Fence' and is_commercial = 0 then transaction_amount else 0 end) as fence_installation_revenue_billed,
        sum(case when service_category ilike '%vinyl%' and is_commercial = 0 then transaction_amount else 0 end) as vinyl_fence_revenue_billed,
        sum(case when is_commercial = 1 then transaction_amount else 0 end) as commercial_revenue_billed,
        -- By Market metrics
                --Fence
        sum(case when code like '%CN-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as norcal_fence_revenue_billed,
        sum(case when code = 'CN-EB' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as eb_fence_revenue_billed,
        sum(case when code = 'CN-SF' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as sf_fence_revenue_billed,
        sum(case when code in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sbsf_fence_revenue_billed,
        sum(case when code = 'CN-NB' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as nb_fence_revenue_billed,
        sum(case when code = 'CN-SA' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sac_fence_revenue_billed,
        sum(case when code = 'CN-FR' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as fr_fence_revenue_billed,
        sum(case when code = 'CN-WA' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as wa_fence_revenue_billed,
        sum(case when code = 'CN-SJ' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sj_fence_revenue_billed,
        sum(case when code = 'CN-PA' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as pa_fence_revenue_billed,
        sum(case when code = 'CN-ST' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as st_fence_revenue_billed,
        sum(case when code like '%CS-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sc_fence_revenue_billed,
        sum(case when code = 'CS-SV' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sv_fence_revenue_billed,
        sum(case when code = 'CS-OC' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as oc_fence_revenue_billed,
        sum(case when code = 'CS-LA' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as la_fence_revenue_billed,
        sum(case when code = 'CS-VC' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as vc_fence_revenue_billed,
        sum(case when code = 'CS-SD' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sd_fence_revenue_billed,
        sum(case when code like '%TX-%' and product_id = 105  and is_commercial = 0 then transaction_amount else 0 end) as tx_fence_revenue_billed,
        sum(case when code = 'TX-SA' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as sa_fence_revenue_billed,
        sum(case when code = 'TX-DL' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as dl_fence_revenue_billed,
        sum(case when code = 'TX-FW' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as fw_fence_revenue_billed,
        sum(case when code = 'TX-HT' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as ht_fence_revenue_billed,
        sum(case when code = 'TX-AU' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as au_fence_revenue_billed,
        sum(case when code like '%GA-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as georgia_fence_revenue_billed,
        sum(case when code = 'GA-AT' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as at_fence_revenue_billed,
        sum(case when code like '%MD-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as maryland_fence_revenue_billed,
        sum(case when (code like '%MD-%' or code like '%PA-%' or code like '%VA-%') and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as north_east_fence_revenue_billed,
        sum(case when code = 'MD-BL' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as bl_fence_revenue_billed,
        sum(case when code = 'MD-DC' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as dc_fence_revenue_billed,
        sum(case when code like '%PA-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as pen_fence_revenue_billed,
        sum(case when code = 'PA-PH' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as ph_fence_revenue_billed,
        sum(case when code like '%VA-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as va_fence_revenue_billed,
        sum(case when code = 'VA-AR' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as ar_fence_revenue_billed,
        sum(case when code like '%FL-%' and product_id = 105 and is_commercial = 0 then transaction_amount else 0 end) as fl_fence_revenue_billed,
        sum(case when code = 'FL-MI' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as mi_fence_revenue_billed,
        sum(case when code = 'FL-OR' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as or_fence_revenue_billed,
        sum(case when code = 'WA-SE' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as se_fence_revenue_billed,
        sum(case when code like '%WA-%' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as pa_wa_fence_revenue_billed,
        sum(case when code like 'WN-%' and product_id = 105  and is_commercial = 0 then transaction_amount else 0 end) as wn_il_fence_revenue_billed,
        sum(case when code = 'WN-CH' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as wn_ch_fence_revenue_billed,
        sum(case when code = 'WN-NA' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as wn_na_fence_revenue_billed,
        sum(case when code = 'WN-LA' and product_id = 105 and is_commercial = 0  then transaction_amount else 0 end) as wn_la_fence_revenue_billed,
                --Driveway
        sum(case when code like '%CN-%' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as norcal_driveway_revenue_billed,
        sum(case when code = 'CN-EB' and product_id = 34 and is_commercial = 0  then transaction_amount else 0 end) as eb_driveway_revenue_billed,
        sum(case when code = 'GA-AT' and product_id = 34 and is_commercial = 0  then transaction_amount else 0 end) as at_driveway_revenue_billed,
        sum(case when code = 'TX-HT' and product_id = 34 and is_commercial = 0  then transaction_amount else 0 end) as ht_driveway_revenue_billed,
        sum(case when code in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sbsf_driveway_revenue_billed,
        sum(case when code = 'CN-NB' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as nb_driveway_revenue_billed,
        sum(case when code = 'CN-SA' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sac_driveway_revenue_billed,
        sum(case when code = 'CN-FR' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as fr_driveway_revenue_billed,
        sum(case when code like '%CS-%' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sc_driveway_revenue_billed,
        sum(case when code = 'CS-SV' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sv_driveway_revenue_billed,
        sum(case when code = 'CS-OC' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as oc_driveway_revenue_billed,
        sum(case when code = 'CS-LA' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as la_driveway_revenue_billed,
        sum(case when code = 'CS-VC' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as vc_driveway_revenue_billed,
        sum(case when code like '%TX-%' and product_id = 34  and is_commercial = 0 then transaction_amount else 0 end) as tx_driveway_revenue_billed,
        sum(case when code = 'TX-SA' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sa_driveway_revenue_billed,
        sum(case when code = 'TX-DL' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as dl_driveway_revenue_billed,
        sum(case when code = 'TX-FW' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as fw_driveway_revenue_billed,
        sum(case when code = 'CS-SD' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sd_driveway_revenue_billed,
        sum(case when code = 'CN-WA' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as wa_driveway_revenue_billed,
        sum(case when code = 'CN-SJ' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as sj_driveway_revenue_billed,
        sum(case when code = 'CN-PA' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as pa_driveway_revenue_billed,
        sum(case when code = 'CN-ST' and product_id = 34 and is_commercial = 0 then transaction_amount else 0 end) as st_driveway_revenue_billed
from calc_order_transactions
where ((parent_order_id is not null and transaction_amount > 0) or parent_order_id is null)
group by 1
order by 1 desc
),
calc_projects
as
(
select
        date_trunc('{period}', completed_at at time zone 'America/Los_Angeles')::date as date,
        count(distinct id) as projects_billed,
        count(distinct (case when product_id = 105 and is_commercial = 0 then id else null end)) as fence_projects_billed,
        count(distinct (case when product_id = 34 and is_commercial = 0 then id else null end)) as driveway_projects_billed,
        count(distinct (case when product_id = 132 and is_commercial = 0 then id else null end)) as turf_projects_billed,
        count(distinct (case when is_commercial = 1 then id else null end)) as commercial_projects_billed,
        count(distinct (case when service_category ilike '%vinyl%' and is_commercial = 0 then id else null end)) as vinyl_fence_projects_billed,
        -- By Market metrics
        		--All Products
        count(distinct (case when code like '%CN-%'  then id else null end)) as norcal_projects_billed,
        count(distinct (case when code = 'CN-EB' then id else null end)) as eb_projects_billed,
        count(distinct (case when code in ('CN-WA','CN-SJ','CN-PA','CN-SF') then id else null end)) as sbsf_projects_billed,
        count(distinct (case when code = 'CN-NB' then id else null end)) as nb_projects_billed,
        count(distinct (case when code = 'CN-SA' then id else null end)) as sac_projects_billed,
        count(distinct (case when code = 'CN-FR' then id else null end)) as fr_projects_billed,
        count(distinct (case when code = 'CN-SF' then id else null end)) as sf_projects_billed,
        count(distinct (case when code = 'CN-WA' then id else null end)) as wa_projects_billed,
        count(distinct (case when code = 'CN-SJ' then id else null end)) as sj_projects_billed,
        count(distinct (case when code = 'CN-PA' then id else null end)) as pa_projects_billed,
        count(distinct (case when code = 'CN-ST' then id else null end)) as st_projects_billed,
        count(distinct (case when code like '%CS-%' then id else null end)) as sc_projects_billed,
        count(distinct (case when code = 'CS-SV' then id else null end)) as sv_projects_billed,
        count(distinct (case when code = 'CS-OC' then id else null end)) as oc_projects_billed,
        count(distinct (case when code = 'CS-LA' then id else null end)) as la_projects_billed,
        count(distinct (case when code = 'CS-VC' then id else null end)) as vc_projects_billed,
        count(distinct (case when code = 'CS-SD' then id else null end)) as sd_projects_billed,
        count(distinct (case when code like '%TX-%' then id else null end)) as tx_projects_billed,
        count(distinct (case when code = 'TX-DL' then id else null end)) as dl_projects_billed,
        count(distinct (case when code = 'TX-SA' then id else null end)) as sa_projects_billed,
        count(distinct (case when code = 'TX-FW' then id else null end)) as fw_projects_billed,
        count(distinct (case when code = 'TX-HT' then id else null end)) as ht_projects_billed,
        count(distinct (case when code = 'TX-AU' then id else null end)) as au_projects_billed,
        count(distinct (case when code like '%GA-%' then id else null end)) as georgia_projects_billed,
        count(distinct (case when code = 'GA-AT' then id else null end)) as at_projects_billed,
        count(distinct (case when code like '%MD-%' then id else null end)) as maryland_projects_billed,
        count(distinct (case when code = 'MD-BL' then id else null end)) as bl_projects_billed,
        count(distinct (case when code = 'MD-DC' then id else null end)) as dc_projects_billed,
        count(distinct (case when code like '%PA-%' then id else null end)) as pen_projects_billed,
        count(distinct (case when code = 'PA-PH' then id else null end)) as ph_projects_billed,
        count(distinct (case when code like '%VA-%' then id else null end)) as va_projects_billed,
        count(distinct (case when code = 'VA-AR' then id else null end)) as ar_projects_billed,
        count(distinct (case when code like '%FL-%' then id else null end)) as fl_projects_billed,
        count(distinct (case when code = 'FL-MI' then id else null end)) as mi_projects_billed,
        count(distinct (case when code = 'FL-OR' then id else null end)) as or_projects_billed,
        count(distinct (case when code like '%PA-WA-%'  then id else null end)) as pa_wa_projects_billed,
        count(distinct (case when code = 'WA-SE' then id else null end)) as se_projects_billed,
        count(distinct (case when code like 'WN-%' then id else null end)) as wn_il_projects_billed,
        count(distinct (case when code = 'WN-CH' then id else null end)) as wn_ch_projects_billed,
        count(distinct (case when code = 'WN-NA' then id else null end)) as wn_na_projects_billed,
        count(distinct (case when code = 'WN-LA' then id else null end)) as wn_la_projects_billed,
        		--Channel
        count(distinct (case when channel = 'Paid/Yelp' then id else null end)) as yelp_projects_billed,
        count(distinct (case when channel ilike 'Paid/Google%' then id else null end)) as google_projects_billed,
        count(distinct (case when channel ilike 'Paid/Home Advisor%' then id else null end)) as ha_projects_billed,
        count(distinct (case when channel = 'Paid/Thumbtack' then id else null end)) as tt_projects_billed,
        count(distinct (case when channel = 'Non Paid/BBB' then id else null end)) as bbb_projects_billed,
                --Fence
        count(distinct (case when code like '%CN-%' and product_id = 105 and is_commercial = 0 then id else null end)) as norcal_fence_projects_billed,
        count(distinct (case when code = 'CN-EB' and product_id = 105 and is_commercial = 0 then id else null end)) as eb_fence_projects_billed,
        count(distinct (case when code in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 105 and is_commercial = 0 then id else null end)) as sbsf_fence_projects_billed,
        count(distinct (case when code = 'CN-NB' and product_id = 105 and is_commercial = 0 then id else null end)) as nb_fence_projects_billed,
        count(distinct (case when code = 'CN-SA' and product_id = 105 and is_commercial = 0 then id else null end)) as sac_fence_projects_billed,
        count(distinct (case when code = 'CN-FR' and product_id = 105 and is_commercial = 0 then id else null end)) as fr_fence_projects_billed,
        count(distinct (case when code = 'CN-WA' and product_id = 105 and is_commercial = 0 then id else null end)) as wa_fence_projects_billed,
        count(distinct (case when code = 'CN-SJ' and product_id = 105 and is_commercial = 0 then id else null end)) as sj_fence_projects_billed,
        count(distinct (case when code = 'CN-PA' and product_id = 105 and is_commercial = 0 then id else null end)) as pa_fence_projects_billed,
        count(distinct (case when code = 'CN-ST' and product_id = 105 and is_commercial = 0 then id else null end)) as st_fence_projects_billed,
        count(distinct (case when code = 'CN-SF' and product_id = 105 and is_commercial = 0 then id else null end)) as sf_fence_projects_billed,
        count(distinct (case when code like '%CS-%' and product_id = 105 and is_commercial = 0 then id else null end)) as sc_fence_projects_billed,
        count(distinct (case when code = 'CS-SV' and product_id = 105 and is_commercial = 0 then id else null end)) as sv_fence_projects_billed,
        count(distinct (case when code = 'CS-OC' and product_id = 105 and is_commercial = 0 then id else null end)) as oc_fence_projects_billed,
        count(distinct (case when code = 'CS-LA' and product_id = 105 and is_commercial = 0 then id else null end)) as la_fence_projects_billed,
        count(distinct (case when code = 'CS-VC' and product_id = 105 and is_commercial = 0 then id else null end)) as vc_fence_projects_billed,
        count(distinct (case when code = 'CS-SD' and product_id = 105 and is_commercial = 0 then id else null end)) as sd_fence_projects_billed,
        count(distinct (case when code like '%TX-%' and product_id = 105 and is_commercial = 0 then id else null end)) as tx_fence_projects_billed,
        count(distinct (case when code = 'TX-DL' and product_id = 105 and is_commercial = 0 then id else null end)) as dl_fence_projects_billed,
        count(distinct (case when code = 'TX-SA' and product_id = 105 and is_commercial = 0 then id else null end)) as sa_fence_projects_billed,
        count(distinct (case when code = 'TX-FW' and product_id = 105 and is_commercial = 0 then id else null end)) as fw_fence_projects_billed,
        count(distinct (case when code = 'TX-HT' and product_id = 105 and is_commercial = 0 then id else null end)) as ht_fence_projects_billed,
        count(distinct (case when code = 'TX-AU' and product_id = 105 and is_commercial = 0 then id else null end)) as au_fence_projects_billed,
        count(distinct (case when code like '%GA-%' and product_id = 105 and is_commercial = 0 then id else null end)) as georgia_fence_projects_billed,
        count(distinct (case when code = 'GA-AT' and product_id = 105 and is_commercial = 0 then id else null end)) as at_fence_projects_billed,
        count(distinct (case when code like '%MD-%' and product_id = 105 and is_commercial = 0 then id else null end)) as maryland_fence_projects_billed,
        count(distinct (case when (code like '%MD-%' or code like '%PA-%' or code like '%VA-%') and product_id = 105 and is_commercial = 0 then id else null end)) as north_east_fence_projects_billed,
        count(distinct (case when code = 'MD-BL' and product_id = 105 and is_commercial = 0 then id else null end)) as bl_fence_projects_billed,
        count(distinct (case when code = 'MD-DC' and product_id = 105 and is_commercial = 0 then id else null end)) as dc_fence_projects_billed,
        count(distinct (case when code like '%PA-%' and product_id = 105 and is_commercial = 0 then id else null end)) as pen_fence_projects_billed,
        count(distinct (case when code = 'PA-PH' and product_id = 105 and is_commercial = 0 then id else null end)) as ph_fence_projects_billed,
        count(distinct (case when code like '%VA-%' and product_id = 105 and is_commercial = 0 then id else null end)) as va_fence_projects_billed,
        count(distinct (case when code = 'VA-AR' and product_id = 105 and is_commercial = 0 then id else null end)) as ar_fence_projects_billed,
        count(distinct (case when code like '%FL-%' and product_id = 105 and is_commercial = 0 then id else null end)) as fl_fence_projects_billed,
        count(distinct (case when code = 'FL-MI' and product_id = 105 and is_commercial = 0 then id else null end)) as mi_fence_projects_billed,
        count(distinct (case when code = 'FL-OR' and product_id = 105 and is_commercial = 0 then id else null end)) as or_fence_projects_billed,
        count(distinct (case when code = 'WA-SE' and product_id = 105 and is_commercial = 0 then id else null end)) as se_fence_projects_billed,
        count(distinct (case when code like '%WA-%' and product_id = 105 and is_commercial = 0 then id else null end)) as pa_wa_fence_projects_billed,
        count(distinct (case when code like 'WN-%' and product_id = 105 and is_commercial = 0 then id else null end)) as wn_il_fence_projects_billed,
        count(distinct (case when code = 'WN-CH' and product_id = 105 and is_commercial = 0 then id else null end)) as wn_ch_fence_projects_billed,
        count(distinct (case when code = 'WN-NA' and product_id = 105 and is_commercial = 0 then id else null end)) as wn_na_fence_projects_billed,
        count(distinct (case when code = 'WN-LA' and product_id = 105 and is_commercial = 0 then id else null end)) as wn_la_fence_projects_billed,
        --Driveway
        count(distinct (case when code like '%CN-%' and product_id = 34 and is_commercial = 0 then id else null end)) as norcal_driveway_projects_billed,
        count(distinct (case when code = 'CN-EB' and product_id = 34 and is_commercial = 0 then id else null end)) as eb_driveway_projects_billed,
        count(distinct (case when code = 'GA-AT' and product_id = 34 and is_commercial = 0 then id else null end)) as at_driveway_projects_billed,
        count(distinct (case when code = 'TX-HT' and product_id = 34 and is_commercial = 0 then id else null end)) as ht_driveway_projects_billed,
        count(distinct (case when code in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 34 and is_commercial = 0 then id else null end)) as sbsf_driveway_projects_billed,
        count(distinct (case when code = 'CN-NB' and product_id = 34 and is_commercial = 0 then id else null end)) as nb_driveway_projects_billed,
        count(distinct (case when code = 'CN-SA' and product_id = 34 and is_commercial = 0 then id else null end)) as sac_driveway_projects_billed,
        count(distinct (case when code = 'CN-FR' and product_id = 34 and is_commercial = 0 then id else null end)) as fr_driveway_projects_billed,
        count(distinct (case when code like '%CS-%' and product_id = 34 and is_commercial = 0 then id else null end)) as sc_driveway_projects_billed,
        count(distinct (case when code = 'CS-SV' and product_id = 34 and is_commercial = 0 then id else null end)) as sv_driveway_projects_billed,
        count(distinct (case when code = 'CS-OC' and product_id = 34 and is_commercial = 0 then id else null end)) as oc_driveway_projects_billed,
        count(distinct (case when code = 'CS-LA' and product_id = 34 and is_commercial = 0 then id else null end)) as la_driveway_projects_billed,
        count(distinct (case when code = 'CS-VC' and product_id = 34 and is_commercial = 0 then id else null end)) as vc_driveway_projects_billed,
        count(distinct (case when code like '%TX-%' and product_id = 34 and is_commercial = 0 then id else null end)) as tx_driveway_projects_billed,
        count(distinct (case when code = 'TX-DL' and product_id = 34 and is_commercial = 0 then id else null end)) as dl_driveway_projects_billed,
        count(distinct (case when code = 'TX-FW' and product_id = 34 and is_commercial = 0 then id else null end)) as fw_driveway_projects_billed,
        count(distinct (case when code = 'CS-SD' and product_id = 34 and is_commercial = 0 then id else null end)) as sd_driveway_projects_billed,
        count(distinct (case when code = 'CN-WA' and product_id = 34 and is_commercial = 0 then id else null end)) as wa_driveway_projects_billed,
        count(distinct (case when code = 'CN-SJ' and product_id = 34 and is_commercial = 0 then id else null end)) as sj_driveway_projects_billed,
        count(distinct (case when code = 'CN-PA' and product_id = 34 and is_commercial = 0 then id else null end)) as pa_driveway_projects_billed,
        count(distinct (case when code = 'CN-ST' and product_id = 34 and is_commercial = 0 then id else null end)) as st_driveway_projects_billed
from calc_order_transactions
where parent_order_id is null
group by 1
order by 2 desc
)
select
        cr.date,
        -- Billed projects ------------------------------------------------------
        		--All Products
        eb_projects_billed as DEL151,
        at_projects_billed as DEL203,
        ht_projects_billed as DEL207,
        au_projects_billed as DEL211,
        sbsf_projects_billed as DEL152,
        nb_projects_billed as DEL153,
        sac_projects_billed as DEL154,
        fr_projects_billed as DEL155,
        sf_projects_billed as DEL402,
        sc_projects_billed as DEL166,
        sv_projects_billed as DEL174,
        oc_projects_billed as DEL177,
        la_projects_billed as DEL180,
        vc_projects_billed as DEL220,
        tx_projects_billed as DEL242,
        dl_projects_billed as DEL225,
        fw_projects_billed as DEL231,
        sa_projects_billed as DEL281,
        sd_projects_billed as DEL235,
        wa_projects_billed as DEL255,
        sj_projects_billed as DEL256,
        pa_projects_billed as DEL257,
        st_projects_billed as DEL258,
        georgia_projects_billed as DEL360,
        maryland_projects_billed as DEL343,
        bl_projects_billed as DEL214,
        dc_projects_billed as DEL337,
        ph_projects_billed as DEL349,
        pen_projects_billed as DEL355,
        ar_projects_billed as DEL366,
        va_projects_billed as DEL372,
        fl_projects_billed as DEL377,
        mi_projects_billed as DEL381,
        or_projects_billed as DEL396,
        se_projects_billed as DEL410,
        pa_wa_projects_billed as DEL416,
        wn_il_projects_billed as DEL565,
        wn_ch_projects_billed as DEL572,
        wn_na_projects_billed as DEL583,
        wn_la_projects_billed as DEL594,
        		--Channel
        yelp_projects_billed as DEL390,
        google_projects_billed as DEL391,
        ha_projects_billed as DEL392,
        tt_projects_billed as DEL393,
        bbb_projects_billed as DEL394,
                --Fence, Commercial and Total
        projects_billed as DEL111,
        fence_projects_billed as DEL111F,
        vinyl_fence_projects_billed as DEL186F,
        fence_installation_revenue_billed as DEL183,
        commercial_projects_billed as DEL111C,
        norcal_fence_projects_billed as DEL197F,
        eb_fence_projects_billed as DEL151F,
        at_fence_projects_billed as DEL203F,
        ht_fence_projects_billed as DEL207F,
        au_fence_projects_billed as DEL211F,
        sbsf_fence_projects_billed as DEL152F,
        nb_fence_projects_billed as DEL153F,
        sac_fence_projects_billed as DEL154F,
        fr_fence_projects_billed as DEL155F,
        sf_fence_projects_billed as DEL402F,
        sc_fence_projects_billed as DEL166F,
        sv_fence_projects_billed as DEL174F,
        oc_fence_projects_billed as DEL177F,
        la_fence_projects_billed as DEL180F,
        vc_fence_projects_billed as DEL220F,
        tx_fence_projects_billed as DEL242F,
        dl_fence_projects_billed as DEL225F,
        fw_fence_projects_billed as DEL231F,
        sa_fence_projects_billed as DEL281F,
        sd_fence_projects_billed as DEL235F,
        wa_fence_projects_billed as DEL255F,
        sj_fence_projects_billed as DEL256F,
        pa_fence_projects_billed as DEL257F,
        st_fence_projects_billed as DEL258F,
        georgia_fence_projects_billed as DEL360F,
        maryland_fence_projects_billed as DEL343F,
        north_east_fence_projects_billed as DEL403F,
        bl_fence_projects_billed as DEL214F,
        dc_fence_projects_billed as DEL337F,
        ph_fence_projects_billed as DEL349F,
        pen_fence_projects_billed as DEL355F,
        ar_fence_projects_billed as DEL366F,
        va_fence_projects_billed as DEL372F,
        fl_fence_projects_billed as DEL377F,
        mi_fence_projects_billed as DEL381F,
        or_fence_projects_billed as DEL396F,
        norcal_projects_billed as DEL197,
        se_fence_projects_billed as DEL410F,
        pa_wa_fence_projects_billed as DEL416F,
        wn_il_fence_projects_billed as DEL565F,
        wn_ch_fence_projects_billed as DEL572F,
        wn_na_fence_projects_billed as DEL583F,
        wn_la_fence_projects_billed as DEL594F,
                --Hardscape
        driveway_projects_billed as DEL111D,
        norcal_driveway_projects_billed as DEL197D,
        eb_driveway_projects_billed as DEL151D,
        sbsf_driveway_projects_billed as DEL152D,
        nb_driveway_projects_billed as DEL153D,
        sac_driveway_projects_billed as DEL154D,
        fr_driveway_projects_billed as DEL155D,
        sc_driveway_projects_billed as DEL166D,
        sv_driveway_projects_billed as DEL174D,
        oc_driveway_projects_billed as DEL177D,
        la_driveway_projects_billed as DEL180D,
        vc_driveway_projects_billed as DEL220D,
        tx_driveway_projects_billed as DEL242D,
        dl_driveway_projects_billed as DEL225D,
        fw_driveway_projects_billed as DEL231D,
        sd_driveway_projects_billed as DEL235D,
        wa_driveway_projects_billed as DEL255D,
        sj_driveway_projects_billed as DEL256D,
        pa_driveway_projects_billed as DEL257D,
        st_driveway_projects_billed as DEL258D,
                        --Turf
        turf_projects_billed as DEL111T,
        -- Billed revenue -------------------------------------------------------
            --Fence, Commercial and Total
        revenue_billed as DEL112,
        warranty_revenue_billed as DEL229,
        warranty_fence_revenue_billed as DEL229F,
        fence_revenue_billed as DEL112F,
        vinyl_fence_revenue_billed as DEL187F,
        commercial_revenue_billed as DEL112C,
        norcal_fence_revenue_billed as DEL198F,
        eb_fence_revenue_billed as DEL156F,
        at_fence_revenue_billed as DEL200F,
        ht_fence_revenue_billed as DEL204F,
        au_fence_revenue_billed as DEL208F,
        sbsf_fence_revenue_billed as DEL157F,
        nb_fence_revenue_billed as DEL158F,
        sac_fence_revenue_billed as DEL159F,
        fr_fence_revenue_billed as DEL160F,
        sc_fence_revenue_billed as DEL167F,
        sv_fence_revenue_billed as DEL175F,
        oc_fence_revenue_billed as DEL178F,
        la_fence_revenue_billed as DEL181F,
        vc_fence_revenue_billed as DEL221F,
        dl_fence_revenue_billed as DEL226F,
        tx_fence_revenue_billed as DEL243F,
        sa_fence_revenue_billed as DEL282F,
        fw_fence_revenue_billed as DEL232F,
        sd_fence_revenue_billed as DEL236F,
        wa_fence_revenue_billed as DEL259F,
        sj_fence_revenue_billed as DEL260F,
        pa_fence_revenue_billed as DEL261F,
        st_fence_revenue_billed as DEL262F,
        georgia_fence_revenue_billed as DEL361F,
        maryland_fence_revenue_billed as DEL344F,
        north_east_fence_revenue_billed as DEL404F,
        bl_fence_revenue_billed as DEL215F,
        dc_fence_revenue_billed as DEL338F,
        ph_fence_revenue_billed as DEL350F,
        pen_fence_revenue_billed as DEL356F,
        va_fence_revenue_billed as DEL373F,
        ar_fence_revenue_billed as DEL367F,
        fl_fence_revenue_billed as DEL378F,
        mi_fence_revenue_billed as DEL382F,
        or_fence_revenue_billed as DEL397F,
        se_fence_revenue_billed as DEL411F,
        pa_wa_fence_revenue_billed as DEL417F,
        wn_il_fence_revenue_billed as DEL604F,
        wn_ch_fence_revenue_billed as DEL573F,
        wn_na_fence_revenue_billed as DEL584F,
        wn_la_fence_revenue_billed as DEL595F,
            --Hardscape
        warranty_driveway_revenue_billed as DEL229D,
        driveway_revenue_billed as DEL112D,
        norcal_driveway_revenue_billed as DEL198D,
        eb_driveway_revenue_billed as DEL156D,
        sbsf_driveway_revenue_billed as DEL157D,
        nb_driveway_revenue_billed as DEL158D,
        sac_driveway_revenue_billed as DEL159D,
        fr_driveway_revenue_billed as DEL160D,
        sc_driveway_revenue_billed as DEL167D,
        sv_driveway_revenue_billed as DEL175D,
        oc_driveway_revenue_billed as DEL178D,
        la_driveway_revenue_billed as DEL181D,
        vc_driveway_revenue_billed as DEL221D,
        dl_driveway_revenue_billed as DEL226D,
        tx_driveway_revenue_billed as DEL243D,
        sa_driveway_revenue_billed as DEL282D,
        fw_driveway_revenue_billed as DEL232D,
        sd_driveway_revenue_billed as DEL236D,
        wa_driveway_revenue_billed as DEL259D,
        sj_driveway_revenue_billed as DEL260D,
        pa_driveway_revenue_billed as DEL261D,
        st_driveway_revenue_billed as DEL262D,
            -- Turf
        turf_revenue_billed as DEL112T,
        -- Gross profit ---------------------------------------------------------
            --Fence, Commercial and Total
        revenue_billed - coalesce(total_cost,0) as PRO119,
        fence_revenue_billed - coalesce(total_cost_fence,0) as PRO119F,
        commercial_revenue_billed - coalesce(total_cost_commercial,0) as PRO119C,
        eb_fence_revenue_billed - coalesce(eb_fence_cost,0) as DEL188F,
        sf_fence_revenue_billed - coalesce(sf_fence_cost,0) as DEL562F,
        at_fence_revenue_billed - coalesce(at_fence_cost,0) as DEL202F,
        ht_fence_revenue_billed - coalesce(ht_fence_cost,0) as DEL206F,
        au_fence_revenue_billed - coalesce(au_fence_cost,0) as DEL210F,
        sbsf_fence_revenue_billed - coalesce(sbsf_fence_cost,0) as DEL189F,
        nb_fence_revenue_billed - coalesce(nb_fence_cost,0) as DEL190F,
        sac_fence_revenue_billed - coalesce(sac_fence_cost,0) as DEL191F,
        fr_fence_revenue_billed - coalesce(fr_fence_cost,0) as DEL192F,
        sc_fence_revenue_billed - coalesce(sc_fence_cost,0) as DEL193F,
        sv_fence_revenue_billed - coalesce(sv_fence_cost,0) as DEL194F,
        oc_fence_revenue_billed - coalesce(oc_fence_cost,0) as DEL195F,
        la_fence_revenue_billed - coalesce(la_fence_cost,0) as DEL196F,
        vc_fence_revenue_billed - coalesce(vc_fence_cost,0) as DEL222F,
        dl_fence_revenue_billed - coalesce(dl_fence_cost,0) as DEL228F,
        fw_fence_revenue_billed - coalesce(fw_fence_cost,0) as DEL234F,
        sa_fence_revenue_billed - coalesce(sa_fence_cost,0) as DEL284F,
        sd_fence_revenue_billed - coalesce(sd_fence_cost,0) as DEL238F,
        tx_fence_revenue_billed - coalesce(tx_fence_cost,0) as DEL245F,
        wa_fence_revenue_billed - coalesce(wa_fence_cost,0) as DEL263F,
        sj_fence_revenue_billed - coalesce(sj_fence_cost,0) as DEL264F,
        pa_fence_revenue_billed - coalesce(pa_fence_cost,0) as DEL265F,
        st_fence_revenue_billed - coalesce(st_fence_cost,0) as DEL266F,
        bl_fence_revenue_billed - coalesce(bl_fence_cost,0) as DEL217F,
        dc_fence_revenue_billed - coalesce(dc_fence_cost,0) as DEL340F,
        ph_fence_revenue_billed - coalesce(ph_fence_cost,0) as DEL352F,
        ar_fence_revenue_billed - coalesce(ar_fence_cost,0) as DEL369F,
        mi_fence_revenue_billed - coalesce(mi_fence_cost,0) as DEL384F,
        or_fence_revenue_billed - coalesce(or_fence_cost,0) as DEL399F,
        se_fence_revenue_billed - coalesce(se_fence_cost,0) as DEL413F,
        wn_il_fence_revenue_billed - coalesce(wn_il_fence_cost,0) as DEL566F,
        wn_ch_fence_revenue_billed - coalesce(wn_ch_fence_cost,0) as DEL574F,
        wn_na_fence_revenue_billed - coalesce(wn_na_fence_cost,0) as DEL585F,
        wn_la_fence_revenue_billed - coalesce(wn_la_fence_cost,0) as DEL596F,
            --Hardscape
        driveway_revenue_billed - coalesce(total_cost_driveway,0) as PRO119D,
        eb_driveway_revenue_billed - coalesce(eb_driveway_cost,0) as DEL188D,
        sbsf_driveway_revenue_billed - coalesce(sbsf_driveway_cost,0) as DEL189D,
        nb_driveway_revenue_billed - coalesce(nb_driveway_cost,0) as DEL190D,
        sac_driveway_revenue_billed - coalesce(sac_driveway_cost,0) as DEL191D,
        fr_driveway_revenue_billed - coalesce(fr_driveway_cost,0) as DEL192D,
        sc_driveway_revenue_billed - coalesce(sc_driveway_cost,0) as DEL193D,
        sv_driveway_revenue_billed - coalesce(sv_driveway_cost,0) as DEL194D,
        oc_driveway_revenue_billed - coalesce(oc_driveway_cost,0) as DEL195D,
        la_driveway_revenue_billed - coalesce(la_driveway_cost,0) as DEL196D,
        vc_driveway_revenue_billed - coalesce(vc_driveway_cost,0) as DEL222D,
        dl_driveway_revenue_billed - coalesce(dl_driveway_cost,0) as DEL228D,
        fw_driveway_revenue_billed - coalesce(fw_driveway_cost,0) as DEL234D,
        sd_driveway_revenue_billed - coalesce(sd_driveway_cost,0) as DEL238D,
        tx_driveway_revenue_billed - coalesce(tx_driveway_cost,0) as DEL245D,
        wa_driveway_revenue_billed - coalesce(wa_driveway_cost,0) as DEL263D,
        sj_driveway_revenue_billed - coalesce(sj_driveway_cost,0) as DEL264D,
        pa_driveway_revenue_billed - coalesce(pa_driveway_cost,0) as DEL265D,
        st_driveway_revenue_billed - coalesce(st_driveway_cost,0) as DEL266D,
            --Turf
        turf_revenue_billed - coalesce(total_cost_turf,0) as PRO119T,
        -- Gross Margin ---------------------------------------------------------
            --Fence, Commercial and Total
        coalesce((revenue_billed - coalesce(total_cost,0))/nullif(revenue_billed,0),0) as PRO107,
        coalesce((fence_revenue_billed - coalesce(total_cost_fence,0))/nullif(fence_revenue_billed,0),0) as PRO107F,
        coalesce((commercial_revenue_billed - coalesce(total_cost_commercial,0))/nullif(commercial_revenue_billed,0),0) as PRO107C,
        coalesce((norcal_fence_revenue_billed - coalesce(norcal_fence_cost,0))/nullif(norcal_fence_revenue_billed,0),0) as DEL199F,
        coalesce((eb_fence_revenue_billed - coalesce(eb_fence_cost,0))/nullif(eb_fence_revenue_billed,0),0) as DEL161F,
        coalesce((sf_fence_revenue_billed - coalesce(sf_fence_cost,0))/nullif(sf_fence_revenue_billed,0),0) as DEL561F,
        coalesce((at_fence_revenue_billed - coalesce(at_fence_cost,0))/nullif(at_fence_revenue_billed,0),0) as DEL201F,
        coalesce((ht_fence_revenue_billed - coalesce(ht_fence_cost,0))/nullif(ht_fence_revenue_billed,0),0) as DEL205F,
        coalesce((au_fence_revenue_billed - coalesce(au_fence_cost,0))/nullif(au_fence_revenue_billed,0),0) as DEL209F,
        coalesce((sbsf_fence_revenue_billed - coalesce(sbsf_fence_cost,0))/nullif(sbsf_fence_revenue_billed,0),0) as DEL162F,
        coalesce((nb_fence_revenue_billed - coalesce(nb_fence_cost,0))/nullif(nb_fence_revenue_billed,0),0) as DEL163F,
        coalesce((sac_fence_revenue_billed - coalesce(sac_fence_cost,0))/nullif(sac_fence_revenue_billed,0),0) as DEL164F,
        coalesce((fr_fence_revenue_billed - coalesce(fr_fence_cost,0))/nullif(fr_fence_revenue_billed,0),0) as DEL165F,
        coalesce((sc_fence_revenue_billed - coalesce(sc_fence_cost,0))/nullif(sc_fence_revenue_billed,0),0) as DEL168F,
        coalesce((sv_fence_revenue_billed - coalesce(sv_fence_cost,0))/nullif(sv_fence_revenue_billed,0),0) as DEL176F,
        coalesce((oc_fence_revenue_billed - coalesce(oc_fence_cost,0))/nullif(oc_fence_revenue_billed,0),0) as DEL179F,
        coalesce((la_fence_revenue_billed - coalesce(la_fence_cost,0))/nullif(la_fence_revenue_billed,0),0) as DEL182F,
        coalesce((vc_fence_revenue_billed - coalesce(vc_fence_cost,0))/nullif(vc_fence_revenue_billed,0),0) as DEL223F,
        coalesce((dl_fence_revenue_billed - coalesce(dl_fence_cost,0))/nullif(dl_fence_revenue_billed,0),0) as DEL227F,
        coalesce((fw_fence_revenue_billed - coalesce(fw_fence_cost,0))/nullif(fw_fence_revenue_billed,0),0) as DEL233F,
        coalesce((sd_fence_revenue_billed - coalesce(sd_fence_cost,0))/nullif(sd_fence_revenue_billed,0),0) as DEL237F,
        coalesce((tx_fence_revenue_billed - coalesce(tx_fence_cost,0))/nullif(tx_fence_revenue_billed,0),0) as DEL244F,
        coalesce((wa_fence_revenue_billed - coalesce(wa_fence_cost,0))/nullif(wa_fence_revenue_billed,0),0) as DEL267F,
        coalesce((sj_fence_revenue_billed - coalesce(sj_fence_cost,0))/nullif(sj_fence_revenue_billed,0),0) as DEL268F,
        coalesce((pa_fence_revenue_billed - coalesce(pa_fence_cost,0))/nullif(pa_fence_revenue_billed,0),0) as DEL269F,
        coalesce((st_fence_revenue_billed - coalesce(st_fence_cost,0))/nullif(st_fence_revenue_billed,0),0) as DEL270F,
        coalesce((sa_fence_revenue_billed - coalesce(sa_fence_cost,0))/nullif(sa_fence_revenue_billed,0),0) as DEL283F,
        coalesce((maryland_fence_revenue_billed - coalesce(maryland_fence_cost,0))/nullif(maryland_fence_revenue_billed,0),0) as DEL345F,
        coalesce((north_east_fence_revenue_billed - coalesce(north_east_fence_cost,0))/nullif(north_east_fence_revenue_billed,0),0) as DEL405F,
        coalesce((bl_fence_revenue_billed - coalesce(bl_fence_cost,0))/nullif(bl_fence_revenue_billed,0),0) as DEL216F,
        coalesce((dc_fence_revenue_billed - coalesce(dc_fence_cost,0))/nullif(dc_fence_revenue_billed,0),0) as DEL339F,
        coalesce((pen_fence_revenue_billed - coalesce(pen_fence_cost,0))/nullif(pen_fence_revenue_billed,0),0) as DEL357F,
        coalesce((ph_fence_revenue_billed - coalesce(ph_fence_cost,0))/nullif(ph_fence_revenue_billed,0),0) as DEL351F,
        coalesce((va_fence_revenue_billed - coalesce(va_fence_cost,0))/nullif(va_fence_revenue_billed,0),0) as DEL374F,
        coalesce((ar_fence_revenue_billed - coalesce(ar_fence_cost,0))/nullif(ar_fence_revenue_billed,0),0) as DEL368F,
        coalesce((fl_fence_revenue_billed - coalesce(fl_fence_cost,0))/nullif(fl_fence_revenue_billed,0),0) as DEL379F,
        coalesce((mi_fence_revenue_billed - coalesce(mi_fence_cost,0))/nullif(mi_fence_revenue_billed,0),0) as DEL383F,
        coalesce((or_fence_revenue_billed - coalesce(or_fence_cost,0))/nullif(or_fence_revenue_billed,0),0) as DEL398F,
        coalesce((se_fence_revenue_billed - coalesce(se_fence_cost,0))/nullif(se_fence_revenue_billed,0),0) as DEL412F,
        coalesce((pa_wa_fence_revenue_billed - coalesce(pa_wa_fence_cost,0))/nullif(pa_wa_fence_revenue_billed,0),0) as DEL418F,
        coalesce((wn_il_fence_revenue_billed - coalesce(wn_ch_fence_cost,0))/nullif(wn_il_fence_revenue_billed,0),0) as DEL566F,
        coalesce((wn_ch_fence_revenue_billed - coalesce(wn_ch_fence_cost,0))/nullif(wn_ch_fence_revenue_billed,0),0) as DEL575F,
        coalesce((wn_na_fence_revenue_billed - coalesce(wn_na_fence_cost,0))/nullif(wn_na_fence_revenue_billed,0),0) as DEL586F,
        coalesce((wn_la_fence_revenue_billed - coalesce(wn_la_fence_cost,0))/nullif(wn_la_fence_revenue_billed,0),0) as DEL597F,
           --Hardscape
        coalesce((driveway_revenue_billed - coalesce(total_cost_driveway,0))/nullif(driveway_revenue_billed,0),0) as PRO107D,
        coalesce((norcal_driveway_revenue_billed - coalesce(norcal_driveway_cost,0))/nullif(norcal_driveway_revenue_billed,0),0) as DEL199D,
        coalesce((eb_driveway_revenue_billed - coalesce(eb_driveway_cost,0))/nullif(eb_driveway_revenue_billed,0),0) as DEL161D,
        coalesce((sbsf_driveway_revenue_billed - coalesce(sbsf_driveway_cost,0))/nullif(sbsf_driveway_revenue_billed,0),0) as DEL162D,
        coalesce((nb_driveway_revenue_billed - coalesce(nb_driveway_cost,0))/nullif(nb_driveway_revenue_billed,0),0) as DEL163D,
        coalesce((sac_driveway_revenue_billed - coalesce(sac_driveway_cost,0))/nullif(sac_driveway_revenue_billed,0),0) as DEL164D,
        coalesce((fr_driveway_revenue_billed - coalesce(fr_driveway_cost,0))/nullif(fr_driveway_revenue_billed,0),0) as DEL165D,
        coalesce((sc_driveway_revenue_billed - coalesce(sc_driveway_cost,0))/nullif(sc_driveway_revenue_billed,0),0) as DEL168D,
        coalesce((sv_driveway_revenue_billed - coalesce(sv_driveway_cost,0))/nullif(sv_driveway_revenue_billed,0),0) as DEL176D,
        coalesce((oc_driveway_revenue_billed - coalesce(oc_driveway_cost,0))/nullif(oc_driveway_revenue_billed,0),0) as DEL179D,
        coalesce((la_driveway_revenue_billed - coalesce(la_driveway_cost,0))/nullif(la_driveway_revenue_billed,0),0) as DEL182D,
        coalesce((vc_driveway_revenue_billed - coalesce(vc_driveway_cost,0))/nullif(vc_driveway_revenue_billed,0),0) as DEL223D,
        coalesce((dl_driveway_revenue_billed - coalesce(dl_driveway_cost,0))/nullif(dl_driveway_revenue_billed,0),0) as DEL227D,
        coalesce((fw_driveway_revenue_billed - coalesce(fw_driveway_cost,0))/nullif(fw_driveway_revenue_billed,0),0) as DEL233D,
        coalesce((sd_driveway_revenue_billed - coalesce(sd_driveway_cost,0))/nullif(sd_driveway_revenue_billed,0),0) as DEL237D,
        coalesce((tx_driveway_revenue_billed - coalesce(tx_driveway_cost,0))/nullif(tx_driveway_revenue_billed,0),0) as DEL244D,
        coalesce((wa_driveway_revenue_billed - coalesce(wa_driveway_cost,0))/nullif(wa_driveway_revenue_billed,0),0) as DEL267D,
        coalesce((sj_driveway_revenue_billed - coalesce(sj_driveway_cost,0))/nullif(sj_driveway_revenue_billed,0),0) as DEL268D,
        coalesce((pa_driveway_revenue_billed - coalesce(pa_driveway_cost,0))/nullif(pa_driveway_revenue_billed,0),0) as DEL269D,
        coalesce((st_driveway_revenue_billed - coalesce(st_driveway_cost,0))/nullif(st_driveway_revenue_billed,0),0) as DEL270D,
           --Turf
        coalesce((turf_revenue_billed - coalesce(total_cost_turf,0))/nullif(turf_revenue_billed,0),0) as PRO107T
from calc_revenue cr
left join calc_cost c on c.date = cr.date
left join calc_projects cp on cp.date = cr.date where cr.date >= '2022-01-01'
