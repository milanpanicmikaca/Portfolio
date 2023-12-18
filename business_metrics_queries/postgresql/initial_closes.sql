with
        calc_returning_customers
as
(
        select
                so.id as order_id,
                rank() over (partition by cc.id order by so.created_at) as rank_order_customer
        from store_order so
        left join core_house ch on ch.id = so.house_id
        left join customers_customer cc on cc.id = ch.customer_id
        where so.approved_quote_id is not null
),
        calc_customer_lead_id
as
(
        select
                so.id as order_id,
                min(cl2.id) as id
        from store_order so
        left join core_house ch on ch.id = so.house_id
        left join customers_customer cc on cc.id = ch.customer_id
        left join core_lead cl2 on cl2.contact_id = cc.contact_id
        group by 1
),
        calc_core_lead_id
as
(
        select
                so.id as order_id,
                min(cl.id) as id
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
        lead_attribution
as
(
select
        cl.id,
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
                end as channel
from core_lead cl
left join customers_visitoraction cv on cv.id = cl.visitor_action_id
),
calc_lead
as
(
        select
                so.id as order_id,
                col.id as core_lead_id,
                cul.id as customer_lead_id,
                case when crc.rank_order_customer > 1 then 'Non Paid/Returning Customers'
                         when la.channel <> 'Non Paid/Misc/Unknown' then la.channel
                         else la2.channel end as channel_attributed,
                case when crc.rank_order_customer > 1 then cul.id
                         when la.channel <> 'Non Paid/Misc/Unknown' then col.id
                         else cul.id end as final_lead_id
        from store_order so
        left join calc_core_lead_id col on col.order_id = so.id
        left join calc_customer_lead_id cul on cul.order_id = so.id
        left join lead_attribution la on la.id = col.id
        left join lead_attribution la2 on la2.id = cul.id
        left join calc_returning_customers crc on crc.order_id = col.order_id and crc.rank_order_customer > 1
        order by 1 desc
),
calc_lead_final
as
(
        select
                cl.order_id,
                channel_attributed as channel,
                final_lead_id,
                case
                          when ps.label = 'Install a Wood Fence' then 'WF'
                          when ps.label = 'Install a Chain Link Fence' then 'CLF'
                          when ps.label = 'Install Concrete Driveways & Floors' then 'CD'
                          when ps.label = 'Install Concrete Patios, Walks and Steps' then 'CP'
                          when ps.label = 'Install Brick or Stone Patios, Walks, and Steps' then 'BP'
                          when ps.label = 'Install Stamped Concrete' then 'SC'
                          when ps.label = 'Install Interlocking Pavers for Driveways & Floors' then 'IPD'
                          when ps.label = 'Install Interlocking Pavers for Patios, Walks and Steps' then 'IPP'
                          when ps.label = 'Install Brick or Stone Driveways & Floors' then 'BD'
                          when ps.label = 'Repair or Partially Replace a Wood Fence' then 'RWF'
                          when ps.label = 'Install Asphalt Paving' then 'AP'
                          when ps.label  ilike '%vinyl%' then 'VIN'
                          else 'OTHER'
                  end as service_category,
                l.full_name as lead_full_name,
                co.full_name as customer_full_name,
                l.email as lead_email,
                cu.email as core_user_email,
                cu.full_name as core_user_full_name
        from calc_lead cl
        left join core_lead l on l.id = cl.final_lead_id
        left join customers_visitoraction cv on cv.id = l.visitor_action_id
        left join min_lead_service ml on l.id = ml.lead_id
        left join core_lead_services cls on cls.id = ml.first_lead_service
        left join product_service ps on ps.id = cls.service_id
        left join customers_contact co on co.id = l.contact_id
        left join core_user cu on cu.id = co.user_id
),
clean_approved_orders as
(
        select
                q.id as quote_id,
                q.approved_at,
                q.cancelled_at,
                q.order_id,
                q.total_price,
                o.product_id,
                pm.code as market_code,
                case when pcm.market_id = 1 then 'CS-SD'
                        when pcm.market_id = 2 then 'CN-EB'
                        when pcm.market_id = 3 then 'CN-SA'
                        when pcm.market_id = 4 then 'CN-WA'
                        when pcm.market_id = 5 then 'CS-OC'
                        when pcm.market_id = 6 then 'CS-LA'
                        when pcm.market_id = 7 then 'CS-VC'
                        when pcm.market_id = 8 then 'CN-SF'
                        when pcm.market_id = 9 then 'CN-NB'
                        when pcm.market_id = 10 then 'CN-FR'
                        when pcm.market_id = 11 then 'CS-CC'
                        when pcm.market_id = 12 then 'CS-CV'
                        when pcm.market_id = 13 then 'CN-NC'
                        when pcm.market_id = 14 then 'CS-SV'
                        when pcm.market_id = 16 then 'TX-DL'
                        when pcm.market_id = 17 then 'TX-FW'
                        when pcm.market_id = 18 then 'TX-HT'
                        when pcm.market_id = 19 then 'TX-SA'
                        when pcm.market_id = 20 then 'GA-AT'
                        when pcm.market_id = 21 then 'MD-DC'
                        when pcm.market_id = 22 then 'MD-BL'
                        when pcm.market_id = 29 then 'CN-ST'
                        when pcm.market_id = 30 then 'CN-SJ'
                        when pcm.market_id = 31 then 'CN-PA'
                        when pcm.market_id = 32 then 'TX-AU'
                        when pcm.market_id = 33 then 'PA-PH'
                        when pcm.market_id = 35 then 'VA-AR'
                        when pcm.market_id = 24 then 'FL-MI'
                        when pcm.market_id = 26 then 'FL-OR'
                        when pcm.market_id = 43 then 'WA-SE'
                        when pcm.market_id = 42 then 'WN-CH'
                        when pcm.market_id = 57 then 'WN-NA'
                        when pcm.market_id = 58 then 'WN-LA'
                        else null end as market,
                pm.region_id as region_id,
                cc.is_commercial::integer,
                                clf.channel,
                                clf.service_category
        from
        (
                select
                        q.*,
                        rank() over (partition by q.order_id order by q.approved_at) as rank
                from quote_quote q
                where
                        q.approved_at is not null
                        and q.created_at > '2018-04-15'
        ) as q
        left join store_order o on o.id = q.order_id
        left join core_house h on h.id = o.house_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_county gc on gc.id = ga.county_id
        left join product_countymarket pcm on pcm.county_id = gc.id
        left join product_market pm on pm.id = pcm.market_id
        left join calc_lead_final clf on clf.order_id = o.id
        left join customers_customer cc on cc.id = h.customer_id
        where
                rank = 1
                        and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
                                        59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
                                        and o.parent_order_id is null
                and coalesce(clf.lead_full_name,'')||coalesce(clf.customer_full_name,'')||coalesce(clf.core_user_full_name,'') not ilike '%[TEST]%'
                and coalesce(clf.lead_email,'')||coalesce(clf.core_user_email,'') not ilike '%+test%'
)
select
    date_trunc('{period}', approved_at at time zone 'America/Los_Angeles')::date as date,
    -- All Closes
    count(*) as SAL111, -- Initial Projects Closed
    sum(total_price) as SAL110, -- Initial Revenue Closed
     -- Fence Closes
    sum(case when product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL111F, -- Initial Fence Projects Closed
    sum(case when product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL110F, -- Initial Fence Revenue Closed
     -- Vinyl Fence Closes
    sum(case when product_id = 105 and is_commercial = 0 and service_category = 'VIN' then 1 else 0 end) as SAL460F, -- Initial Vinyl Fence Projects Closed
    sum(case when product_id = 105 and is_commercial = 0 and service_category = 'VIN' then total_price else 0 end) as SAL459F, -- Initial Vinyl Fence Revenue Closed
     -- Turf Closes
    sum(case when product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL111T, -- Initial Turf Projects Closed
    sum(case when product_id = 132 and is_commercial = 0 then total_price else 0 end) as SAL110T, -- Initial Turf Revenue Closed
     -- Driveway Closes
    sum(case when product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL111D, -- Initial Driveway Projects Closed
    sum(case when product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL110D, -- Initial Driveway Revenue Closed
     -- Commercial Closes
    sum(case when is_commercial = 1 then 1 else 0 end) as SAL111C, -- Initial Commercial Projects Closed
    sum(case when is_commercial = 1 then total_price else 0 end) as SAL110C, -- Initial Commercial Revenue Closed
    -- Closes by Market
    sum(case when market like '%CN-%' then 1 else 0 end) as SAL469, --norcal_closes
    sum(case when market = 'CN-EB'then 1 else 0 end) as SAL137, --eb_closes
    sum(case when market is null then 1 else 0 end) as SAL142, --na_closes
    sum(case when market = 'CN-NB' then 1 else 0 end) as SAL140, --nb_closes
    sum(case when market = 'CN-SA' then 1 else 0 end) as SAL138, --sac_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA') then 1 else 0 end) as SAL139, --sb_closes
    sum(case when market = 'CN-SF' then 1 else 0 end) as SAL141, --sf_closes
    sum(case when market = 'CN-FR' then 1 else 0 end) as SAL229, --fr_closes
    sum(case when market = 'TX-DL' then 1 else 0 end) as SAL543, --dl_closes
--    sum(case when market = 'TX-FW' then 1 else 0 end) as SAL561, --fw_closes
--    sum(case when market like '%CS-%' then 1 else 0 end) as SAL371, --socal_closes
--    sum(case when market = 'CS-SV' then 1 else 0 end) as SAL417--sv_closes
--    sum(case when market = 'CS-OC' then 1 else 0 end) as SAL421--oc_closes
--    sum(case when market = 'CS-LA' then 1 else 0 end) as SAL425--la_closes
--    sum(case when market = 'CS-VC' then 1 else 0 end) as SAL490--vc_closes
    sum(case when market = 'CN-WA' then 1 else 0 end) as SAL615,--wa_closes
    sum(case when market = 'CN-SJ' then 1 else 0 end) as SAL616,--sj_closes
    sum(case when market = 'CN-PA' then 1 else 0 end) as SAL617,--pa_closes
    sum(case when market = 'CN-ST' then 1 else 0 end) as SAL618,--st_closes
    sum(case when market like '%TX-%' then 1 else 0 end) as SAL581F, --TX Fence Initial Projects closed
    sum(case when market = 'MD-BL'then 1 else 0 end) as SAL731, --bl_closes
    sum(case when market = 'MD-DC'then 1 else 0 end) as SAL751, --dc_closes
    sum(case when market = 'VA-AR'then 1 else 0 end) as SAL855, --ar_closes
    sum(case when market = 'FL-MI'then 1 else 0 end) as SAL917, --mi_closes
    sum(case when market = 'FL-OR'then 1 else 0 end) as SAL937, --mi_closes
    sum(case when market = 'WA-SE'then 1 else 0 end) as SAL1042, --se_closes
    sum(case when market like 'WN-%' then 1 else 0 end) as SAL1141, --wn_il_closes
    -- Commercial Closes by Market
    sum(case when market like '%CS-%' and is_commercial = 1 then 1 else 0 end) as SAL472C, --nc_commercial_closes
    sum(case when market = 'CN-EB'and is_commercial = 1 then 1 else 0 end) as SAL267C, --eb_commercial_closes
    sum(case when market is null and is_commercial = 1 then 1 else 0 end) as SAL273C, --na_commercial_closes
    sum(case when market = 'CN-NB' and is_commercial = 1  then 1 else 0 end) as SAL270C, --nb_commercial_closes
    sum(case when market = 'CN-SA' and is_commercial = 1  then 1 else 0 end) as SAL268C, --sac_commercial_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_commercial = 1  then 1 else 0 end) as SAL269C, --sb_commercial_closes
    sum(case when market = 'CN-SF' and is_commercial = 1  then 1 else 0 end) as SAL271C, --sf_commercial_closes
    sum(case when market = 'CN-FR' and is_commercial = 1  then 1 else 0 end) as SAL272C, --fr_commercial_closes
--    sum(case when market like '%CS-%' and is_commercial = 1 then 1 else 0 end) as SAL371C --socal_closes
--    sum(case when market = 'CS-SV' and is_commercial = 1 then 1 else 0 end) as SAL417C--sv_closes
--    sum(case when market = 'CS-OC' and is_commercial = 1 then 1 else 0 end) as SAL421C--oc_closes
--    sum(case when market = 'CS-LA' and is_commercial = 1 then 1 else 0 end) as SAL425C--la_closes
--    sum(case when market = 'CS-VC' and is_commercial = 1 then 1 else 0 end) as SAL490C--vc_commercial_closes
    sum(case when market = 'CN-WA' and is_commercial = 1 then 1 else 0 end) as SAL615C,--wa_commercial_closes
    sum(case when market = 'CN-SJ' and is_commercial = 1 then 1 else 0 end) as SAL616C,--sj_commercial_closes
    sum(case when market = 'CN-PA' and is_commercial = 1 then 1 else 0 end) as SAL617C,--pa_commercial_closes
    sum(case when market = 'CN-ST' and is_commercial = 1 then 1 else 0 end) as SAL618C,--st_commercial_closes
    sum(case when market like '%MD-%' and is_commercial = 1 then 1 else 0 end) as SAL774C, --md_commercial_closes
    sum(case when market = 'MD-BL'and is_commercial = 1 then 1 else 0 end) as SAL734C, --bl_commercial_closes
    sum(case when market = 'MD-DC'and is_commercial = 1 then 1 else 0 end) as SAL754C, --dc_commercial_closes
    sum(case when market like '%PA-%' and is_commercial = 1 then 1 else 0 end) as SAL815C, --pen_commercial_closes
    sum(case when market = 'PA-PH'and is_commercial = 1 then 1 else 0 end) as SAL795C, --ph_commercial_closes
    sum(case when market like '%GA-%' and is_commercial = 1 then 1 else 0 end) as SAL838C, --ga_commercial_closes
    sum(case when market like '%VA-%' and is_commercial = 1 then 1 else 0 end) as SAL878C, --va_commercial_closes
    sum(case when market like '%FL-%' and is_commercial = 1 then 1 else 0 end) as SAL899C, --fl_commercial_closes
    sum(case when market = 'VA-AR'and is_commercial = 1 then 1 else 0 end) as SAL858C, --ar_commercial_closes
    sum(case when market = 'FL-MI'and is_commercial = 1 then 1 else 0 end) as SAL917C, --mi_commercial_closes
    sum(case when market = 'FL-OR'and is_commercial = 1 then 1 else 0 end) as SAL937C, --or_commercial_closes
    sum(case when market = 'MD-BL'and is_commercial = 1 then 1 else 0 end) as SAL1047C, --se_commercial_closes
    sum(case when market like 'PA-WA-%' and is_commercial = 1 then 1 else 0 end) as SAL1072C, --pa_wa_commercial_closes
    sum(case when market like 'WN-%' and is_commercial = 1 then 1 else 0 end) as SAL1144C, --wn_il_commercial_closes
    sum(case when market = 'WN-CH'and is_commercial = 1 then 1 else 0 end) as SAL1167C, --ch_commercial_closes
    sum(case when market = 'WN-NA'and is_commercial = 1 then 1 else 0 end) as SAL1190C, --na_commercial_closes
    sum(case when market = 'WN-LA'and is_commercial = 1 then 1 else 0 end) as SAL1213C, --la_commercial_closes
    -- Fence Closes by Market
    sum(case when market like '%CN-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL469F, --norcal_fence_closes
    sum(case when market = 'CN-EB'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL137F, --eb_fence_closes
    sum(case when market = 'GA-AT'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL700F, --at_fence_closes
    sum(case when market = 'TX-HT'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL709F, --ht_fence_closes
    sum(case when market = 'TX-AU'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL727F, --au_fence_closes
    sum(case when market = 'TX-SA'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL718F, --sa_fence_closes
    sum(case when market is null and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL142F, --na_fence_closes
    sum(case when market = 'CN-NB' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL140F, --nb_fence_closes
    sum(case when market = 'CN-SA' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL138F, --sac_fence_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA') and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL139F, --sb_fence_closes
    sum(case when market = 'CN-SF' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL141F, --sf_fence_closes
    sum(case when market = 'CN-FR' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL229F, --fr_fence_closes
    sum(case when market like '%CS-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL371F, --socal_fence_closes
    sum(case when market = 'CS-SV' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL417F, --sv_fence_closes
    sum(case when market = 'CS-OC' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL421F, --oc_fence_closes
    sum(case when market = 'CS-LA' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL425F,--la_fence_closes
    sum(case when market = 'CS-VC' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL490F,--vc_fence_closes
    sum(case when market = 'TX-DL' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL543F,--dl_fence_closes
    sum(case when market = 'TX-FW' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL561F,--fw_fence_closes
    sum(case when market = 'CS-SD' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL565F,--sd_fence_closes
    sum(case when market = 'CN-WA' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL615F,--wa_fence_closes
    sum(case when market = 'CN-SJ' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL616F,--sj_fence_closes
    sum(case when market = 'CN-PA' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL617F,--pa_fence_closes
    sum(case when market = 'CN-ST' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL618F,--st_fence_closes
    sum(case when market like '%TX-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL581F, --tx_fence_closes
    sum(case when market like '%MD-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL771F, --maryland_fence_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL957F, --north_east_fence_closes
    sum(case when market = 'MD-BL'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL731F, --bl_fence_closes
    sum(case when market = 'MD-DC'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL751F, --dc_fence_closes
    sum(case when market like '%VA-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL875F, --va_fence_closes
    sum(case when market = 'VA-AR'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL855F, --ar_fence_closes
    sum(case when market like '%FL-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL896F, --fl_fence_closes
    sum(case when market = 'FL-MI'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL917F, --mi_fence_closes
    sum(case when market = 'FL-OR'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL937F, --mi_fence_closes
    sum(case when market like '%PA-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL812F, --pen_fence_closes
    sum(case when market = 'PA-PH'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL792F, --ph_fence_closes
    sum(case when market like '%GA-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL835F, --ga_fence_closes
    sum(case when market like '%WA-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1069F, --pa_wa_fence_closes
    sum(case when market = 'WA-SE'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1042F, --se_fence_closes
    sum(case when market like 'WN-%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1141F, --wn_il_fence_closes
    sum(case when market = 'WN-CH'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1164F, --ch_fence_closes
    sum(case when market = 'WN-NA'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1187F, --na_fence_closes
    sum(case when market = 'WN-LA'and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1210F, --la_fence_closes
    -- Fence Revenue by Market
    sum(case when market like '%CN-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL479F, --norcal_fence_revenue
    sum(case when market = 'CN-EB'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL306F, -- eb_fence_revenue,
    sum(case when market = 'CN-SF'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1231F, -- sf_fence_revenue,
    sum(case when market = 'GA-AT'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL702F, -- at_fence_revenue,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL307F, -- sbsf_fence_revenue,
    sum(case when market = 'CN-NB' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL308F, -- nb_fence_revenue,
    sum(case when market = 'CN-SA' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL309F, -- sac_fence_revenue,
    sum(case when market = 'CN-FR' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL310F, -- fr_fence_revenue,
    sum(case when market like '%CS-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL373F, --socal_fence_revenue
    sum(case when market = 'CS-SV' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL419F, --sv_fence_revenue
    sum(case when market = 'CS-OC' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL423F, --oc_fence_revenue
    sum(case when market = 'CS-LA' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL427F,--la_fence_revenue
    sum(case when market = 'CS-VC' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL491F,--vc_fence_revenue
    sum(case when market = 'TX-DL' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL550F,--dl_fence_revenue
    sum(case when market = 'TX-FW' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL563F,--fw_fence_revenue
    sum(case when market = 'TX-SA' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL720F,--sa_fence_revenue
    sum(case when market = 'TX-HT' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL711F,--ht_fence_revenue
    sum(case when market = 'TX-AU' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL729F,--au_fence_revenue
    sum(case when market = 'CS-SD' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL572F,--sd_fence_revenue
    sum(case when market = 'CN-WA' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL619F,--wa_fence_revenue
    sum(case when market = 'CN-SJ' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL620F,--sj_fence_revenue
    sum(case when market = 'CN-PA' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL621F,--pa_fence_revenue
    sum(case when market = 'CN-ST' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL622F,--st_fence_revenue
    sum(case when market like '%TX-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL583F,--tx_fence_revenue
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL966F, -- ne_fence_revenue
    sum(case when market like '%MD-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL781F, -- md_fence_revenue
    sum(case when market = 'MD-BL'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL737F, -- bl_fence_revenue,
    sum(case when market = 'MD-DC'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL757F, -- dc_fence_revenue,
    sum(case when market like '%PA-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL822F, -- pen_fence_revenue
    sum(case when market = 'PA-PH'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL798F, -- ph_fence_revenue,
    sum(case when market like '%GA-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL845F, -- ga_fence_revenue
    sum(case when market like '%VA-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL885F, -- va_fence_revenue
    sum(case when market = 'VA-AR'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL861F, -- ar_fence_revenue,
    sum(case when market like '%FL-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL906F, -- fl_fence_revenue
    sum(case when market = 'FL-MI'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL923F, -- mi_fence_revenue,
    sum(case when market = 'FL-OR'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL943F, -- mi_fence_revenue,
    sum(case when market like '%WA-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1079F, -- pa_wa_fence_revenue
    sum(case when market = 'WA-SE'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1050F, -- se_fence_revenue,
    sum(case when market like 'WN-%' and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1151F, --wn_il_fence_revenue
    sum(case when market = 'WN-CH'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1170F, --ch_fence_revenue
    sum(case when market = 'WN-NA'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1193F, --na_commercial_closes
    sum(case when market = 'WN-LA'and product_id = 105 and is_commercial = 0 then total_price else 0 end) as SAL1216F, --la_commercial_closes
    -- Turf Closes by Market
    sum(case when market like '%CN-%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL469T, --norcal_turf_clos
    sum(case when market is null and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL142T, --na_turf_closes
    sum(case when market like '%CS-%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL371T, --socal_turf_closes
    sum(case when market = 'CS-SV' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL417T, --sv_turf_closes
    sum(case when market = 'CS-OC' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL421T, --oc_turf_closes
    sum(case when market = 'CS-LA' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL425T,--la_turf_closes
    sum(case when market = 'CS-VC' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL490T,--vc_turf_closes
    sum(case when market = 'CS-SD' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL565T,--sd_turf_closes
    -- Driveway Closes by Market
    sum(case when market like '%CN-%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL469D, --norcal_driveway_closes
    sum(case when market = 'CN-EB'and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL137D, --eb_driveway_closes
    sum(case when market is null and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL142D, --na_driveway_closes
    sum(case when market = 'CN-NB' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL140D, --nb_driveway_closes
    sum(case when market = 'CN-SA' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL138D, --sac_driveway_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA') and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL139D, --sb_driveway_closes
    sum(case when market = 'CN-SF' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL141D, --sf_driveway_closes
    sum(case when market = 'CN-FR' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL229D, --fr_driveway_closes
    sum(case when market like '%CS-%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL371D, --socal_driveway_closes
    sum(case when market = 'CS-SV' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL417D, --sv_driveway_closes
    sum(case when market = 'CS-OC' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL421D, --oc_driveway_closes
    sum(case when market = 'CS-LA' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL425D,--la_driveway_closes
    sum(case when market = 'CS-VC' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL490D,--vc_driveway_closes
    --sum(case when market = 'TX-DL' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL543D,--dl_driveway_closes
    --sum(case when market = 'TX-FW' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL561D,--fw_driveway_closes
    sum(case when market = 'CS-SD' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL565D,--sd_driveway_closes
    sum(case when market like '%TX-%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL581D, --tx_driveway_closes
    sum(case when market = 'CN-WA' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL615D,--wa_driveway_closes
    sum(case when market = 'CN-SJ' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL616D,--sj_driveway_closes
    sum(case when market = 'CN-PA' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL617D,--pa_driveway_closes
    sum(case when market = 'CN-ST' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL618D,--st_driveway_closes
    -- Closes by Product
    sum(case when service_category = 'WF' and is_commercial = 0 then 1 else 0 end) as SAL255, --wood_fence_closes
    sum(case when service_category = 'CLF' and is_commercial = 0 then 1 else 0 end) as SAL256, --chainlink_fence_closes
    sum(case when service_category = 'CD' and is_commercial = 0 then 1 else 0 end) as SAL257, --concrete_driveways_closes
    sum(case when service_category = 'CP' and is_commercial = 0 then 1 else 0 end) as SAL258, --concrete_patios_closes
    sum(case when service_category = 'BP' and is_commercial = 0 then 1 else 0 end) as SAL259, --brick_patios_closes
    sum(case when service_category = 'SC' and is_commercial = 0 then 1 else 0 end) as SAL260, --stamped_concrete_closes
    sum(case when service_category = 'IPD' and is_commercial = 0 then 1 else 0 end) as SAL261, --pavers_driveways_closes
    sum(case when service_category = 'IPP' and is_commercial = 0 then 1 else 0 end) as SAL262, --pavers_patios_closes
    sum(case when service_category = 'BD' and is_commercial = 0 then 1 else 0 end) as SAL263, --brick_driveways_closes
    sum(case when service_category = 'RWF' and is_commercial = 0 then 1 else 0 end) as SAL264, --repair_fence_closes
    sum(case when service_category = 'AP' and is_commercial = 0 then 1 else 0 end) as SAL265, --asphalt_closes
    sum(case when service_category = 'OTHER' and is_commercial = 0 then 1 else 0 end) as SAL266, --other_closes
    -- Closes by Product Fence
    sum(case when service_category = 'OTHER' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL266F, --other_fence_closes
    -- Closes by Product Fence
    sum(case when service_category = 'OTHER' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL266T, --other_turf_closes
    -- Closes by Product Driveway
    sum(case when service_category = 'OTHER' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL266D, --other_driveway_closes
    -- HomeAdvisor Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL481F, -- norcal_ha_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL336F, -- eb_ha_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1129F, -- sf_ha_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL337F, -- sbsf_ha_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL338F, -- nb_ha_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL339F, -- sac_ha_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL340F, -- fr_ha_fence_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL388F, --socal_ha_fence_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL504F, --sv_ha_fence_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL505F, --oc_ha_fence_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL506F,--la_ha_fence_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL507F,--vc_ha_fence_closes
    sum(case when market = 'CN-WA' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL623F, --wa_ha_fence_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL624F, --sj_ha_fence_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL625F,--pa_ha_fence_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL626F,--st_ha_fence_closes
    sum(case when market like '%MD-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL783F, -- md_ha_fence_initial_closes,
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL968F, -- ne_ha_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL743F, -- bl_ha_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL763F, -- dc_ha_fence_initial_closes,
    sum(case when market like '%PA-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL824F, -- pen_ha_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL804F, -- ph_ha_fence_initial_closes,
    sum(case when market like '%GA-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL847F, -- ga_ha_fence_initial_closes,
    sum(case when market like '%VA-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL887F, -- va_ha_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL867F, -- ar_ha_fence_initial_closes,
    sum(case when market like '%FL-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL908F, -- fl_ha_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL929F, -- mi_ha_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL949F, -- or_ha_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1081F, -- pa_wa_ha_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1056F, -- se_ha_fence_initial_closes,
    sum(case when market like 'WN-%' and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1153F, --ch_il_fence_initial_closes
    sum(case when market = 'WN-CH'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1176F, --ch_ha_fence_initial_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1199F, --na_ha_fence_initial_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1222F, --la_ha_fence_initial_closes
    -- HomeAdvisor Ads Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1091F, -- norcal_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1092F, -- eb_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1137F, -- sf_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1093F, -- nb_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1094F, -- sac_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-ST' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1095F,--st_ha_ads_fence_closes
    sum(case when market = 'CN-FR' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1096F, -- fr_ha_ads_fence_initial_closes,
    sum(case when market = 'CN-WA' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1097F, --wa_ha_ads_fence_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1098F, --sj_ha_ads_fence_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1099F,--pa_ha_ads_fence_closes
    sum(case when market like '%CS-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1100F, --socal_ha_ads_fence_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1101F, --sv_ha_ads_fence_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1102F, --oc_ha_ads_fence_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1103F,--la_ha_ads_fence_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1104F,--vc_ha_ads_fence_closes
    sum(case when market = 'CS-SD' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1105F,--sd_ha_ads_fence_closes
    sum(case when market like '%TX-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1106F, --texas_ha_ads_fence_closes
    sum(case when market = 'TX-DL' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1107F, --dl_ha_ads_fence_closes
    sum(case when market = 'TX-FW' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1108F, --fw_ha_ads_fence_closes
    sum(case when market = 'TX-HT' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1109F,--ht_ha_ads_fence_closes
    sum(case when market = 'TX-SA' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1110F,--sa_ha_ads_fence_closes
    sum(case when market = 'TX-AU' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1111F,--au_ha_ads_fence_closes
    sum(case when market like '%GA-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1112F, -- ga_ha_ads_fence_initial_closes,
    sum(case when market = 'GA-AT' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1113F, -- at_ha_ads_fence_initial_closes,
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1114F, -- ne_ha_ads_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1115F, -- bl_ha_ads_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1116F, -- dc_ha_ads_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1117F, -- ph_ha_ads_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1118F, -- ar_ha_ads_fence_initial_closes,
    sum(case when market like '%FL-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1119F, -- fl_ha_ads_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1120F, -- mi_ha_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1121F, -- or_ha_fence_initial_closes
    sum(case when market like 'WN-%' and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1140F, -- wn_il_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1163F, --ch_ads_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1186F, --na_ads_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1209F, --la_ads_fence_closes
    -- Thumbtack Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL482F, -- norcal_th_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL341F, -- eb_th_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1130F, -- sf_th_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL342F, -- sbsf_th_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL343F, -- nb_th_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL344F, -- sac_th_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL345F, -- fr_th_fence_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL389F, --socal_th_fence_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL508F, --sv_th_fence_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL509F, --oc_th_fence_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL510F,--la_th_fence_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL511F,--vc_th_fence_closes
    sum(case when market = 'CN-WA' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL627F, --wa_th_fence_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL628F, --sj_th_fence_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL629F,--pa_th_fence_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL630F,--st_th_fence_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL969F, -- ne_th_fence_initial_closes,
    sum(case when market like '%MD-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL784F, -- md_th_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL744F, -- bl_th_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL764F, -- dc_th_fence_initial_closes,
    sum(case when market like '%PA-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL825F, -- penn_th_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL805F, -- ph_th_fence_initial_closes,
    sum(case when market like '%GA-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL848F, -- ga_th_fence_initial_closes,
    sum(case when market like '%VA-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL888F, -- va_th_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL868F, -- ar_th_fence_initial_closes,
    sum(case when market like '%FL-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL909F, -- fl_th_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL930F, -- mi_th_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL950F, -- or_th_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1082F, -- pa_wa_th_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1057F, -- se_th_fence_initial_closes,
    sum(case when market like 'WN-%' and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1154F, -- wn_il_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1177F, --ch_th_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1200F, --na_th_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1223F, --la_th_fence_closes
    -- Borg Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL502F, -- norcal_borg_fence_initial_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL503F, -- sb_borg_fence_initial_closes
    sum(case when market = 'CN-EB'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL539F, -- eb_borg_fence_initial_closes
    sum(case when market = 'CN-SF'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1136F, -- sf_borg_fence_initial_closes
    sum(case when market = 'CN-NB' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL540F, -- nb_borg_fence_initial_closes
    sum(case when market = 'CN-SA' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL541F, -- sac_borg_fence_initial_closes
    sum(case when market = 'CN-FR' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL542F, -- fr_borg_fence_initial_closes
    sum(case when market = 'CN-WA'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL631F, -- wa_borg_fence_initial_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL632F, -- sj_borg_fence_initial_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL633F, -- pa_borg_fence_initial_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL634F, -- st_borg_fence_initial_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL976F, -- ne_borg_fence_initial_closes
    sum(case when market like '%MD-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL791F, -- md_borg_fence_initial_closes
    sum(case when market = 'MD-BL'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL750F, -- bl_borg_fence_initial_closes
    sum(case when market = 'MD-DC'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL770F, -- dc_borg_fence_initial_closes
    sum(case when market like '%PA-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL832F, -- pen_borg_fence_initial_closes
    sum(case when market = 'PA-PH'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL811F, -- ph_borg_fence_initial_closes
    sum(case when market like '%VA-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL895F, -- va_borg_fence_initial_closes
    sum(case when market = 'VA-AR'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL874F, -- ar_borg_fence_initial_closes
    sum(case when market like '%FL-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL916F, -- fl_borg_fence_initial_closes
    sum(case when market = 'FL-MI'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL936F, -- mi_borg_fence_initial_closes
    sum(case when market = 'FL-OR'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL956F, -- or_borg_fence_initial_closes
    sum(case when market like 'PA-WA%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1089F, -- pa_wa_borg_fence_initial_closes
    sum(case when market = 'WA-SE'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1064F, -- se_borg_fence_initial_closes
    sum(case when market like 'WN-%' and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1161F, -- wn_il_borg_fence_initial_closes
    sum(case when market = 'WN-CH'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1183F, --ch_borg_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1206F, --na_borg_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1229F, --la_borg_fence_closes
    -- Paid Google Fence Initial Closes
    sum(case when market like '%CN-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL483F, -- norcal_gg_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL346F, -- eb_gg_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1131F, -- sf_gg_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL347F, -- sbsf_gg_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL348F, -- nb_gg_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL349F, -- sac_gg_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL350F, -- fr_gg_fence_initial_closes,
    sum(case when market like '%CS-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL390F, --socal_gg_fence_closes
    sum(case when market = 'CS-SV' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL512F, --sv_gg_fence_closes
    sum(case when market = 'CS-OC' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL513F, --oc_gg_fence_closes
    sum(case when market = 'CS-LA' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL514F,--la_gg_fence_closes
    sum(case when market = 'CS-VC' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL515F,--vc_gg_fence_closes
    sum(case when market = 'CN-WA' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL635F, --pa_gg_fence_closes
    sum(case when market = 'CN-SJ' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL636F, --sj_gg_fence_closes
    sum(case when market = 'CN-PA' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL637F,--pa_gg_fence_closes
    sum(case when market = 'CN-ST' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL638F,--st_gg_fence_closes
    sum(case when market like '%MD-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL785F, -- md_gg_fence_initial_closes,
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL970F, -- ne_gg_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL745F, -- bl_gg_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL765F, -- dc_gg_fence_initial_closes,
    sum(case when market like '%PA-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL826F, -- pen_gg_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL806F, -- ph_gg_fence_initial_closes,
    sum(case when market like '%GA-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL849F, -- ga_gg_fence_initial_closes,
    sum(case when market like '%VA-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL889F, -- va_gg_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL869F, -- ar_gg_fence_initial_closes,
    sum(case when market like '%FL-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL910F, -- fl_gg_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL931F, -- mi_gg_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL951F, -- or_gg_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1083F, -- pa_wa_gg_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1058F, -- se_gg_fence_initial_closes,
    sum(case when market like 'WN-%' and channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1155F, -- wn_il_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1178F, --ch_gg_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1201F, --na_gg_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1224F, --la_gg_fence_closes
    -- Facebook Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL484F, -- norcal_fb_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL351F, -- eb_fb_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1132F, -- sf_fb_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL352F, -- sbsf_fb_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL353F, -- nb_fb_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL354F, -- sac_fb_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL355F, -- fr_fb_fence_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL391F, --socal_fb_fence_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL516F, --sv_fb_fence_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL517F, --oc_fb_fence_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL518F,--la_fb_fence_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL519F,--vc_fb_fence_closes
    sum(case when market = 'CS-WA' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL639F, --wa_fb_fence_closes
    sum(case when market = 'CS-SJ' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL640F, --sj_fb_fence_closes
    sum(case when market = 'CS-PA' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL641F,--pa_fb_fence_closes
    sum(case when market = 'CS-ST' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL642F,--st_fb_fence_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL971F, -- ne_fb_fence_initial_closes,
    sum(case when market like '%MD-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL786F, -- md_fb_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL746F, -- bl_fb_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL766F, -- dc_fb_fence_initial_closes,
    sum(case when market like '%PA-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL827F, -- pen_fb_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL807F, -- ph_fb_fence_initial_closes,
    sum(case when market like '%GA-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL850F, -- ga_fb_fence_initial_closes,
    sum(case when market like '%VA-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL890F, -- va_fb_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL870F, -- ar_fb_fence_initial_closes,
    sum(case when market like '%FL-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL911F, -- fl_fb_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL932F, -- mi_fb_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL952F, -- mi_fb_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1084F, -- pa_wa_fb_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1059F, -- se_fb_fence_initial_closes,
    sum(case when market like 'WN-%' and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1156F, -- wn_il_fb_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1179F, --ch_fb_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1202F, --na_fb_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1225F, --la_fb_fence_closes
    -- Yelp Fence Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL485F, -- norcal_yelp_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL356F, -- eb_yelp_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1133F, -- sf_yelp_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL357F, -- sbsf_yelp_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL358F, -- nb_yelp_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL359F, -- sac_yelp_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL360F, -- fr_yelp_fence_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL392F, --socal_yelp_fence_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL520F, --sv_yelp_fence_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL521F, --oc_yelp_fence_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL522F,--la_yelp_fence_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL523F,--vc_yelp_fence_closes
    sum(case when market = 'CN-WA' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL643F, --wa_yelp_fence_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL644F, --sj_yelp_fence_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL645F,--pa_yelp_fence_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL646F,--st_yelp_fence_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL972F, -- ne_yelp_fence_initial_closes,
    sum(case when market like '%MD-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL787F, -- md_yelp_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL747F, -- bl_yelp_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL767F, -- dc_yelp_fence_initial_closes,
    sum(case when market like '%PA-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL828F, -- pen_yelp_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL808F, -- ph_yelp_fence_initial_closes,
    sum(case when market like '%GA-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL851F, -- ga_yelp_fence_initial_closes,
    sum(case when market like '%VA-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL891F, -- va_yelp_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL871F, -- ar_yelp_fence_initial_closes,
    sum(case when market like '%FL-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL912F, -- fl_yelp_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL933F, -- mi_yelp_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL953F, -- or_yelp_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1085F, -- pa_wa_yelp_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1060F, -- se_yelp_fence_initial_closes,
    sum(case when market like 'WN-%' and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1157F, -- wn_il_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1180F, --ch_yelp_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1203F, --na_yelp_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1226F, --la_yelp_fence_closes
    -- Paid Misc Fence Leads
    sum(case when market like '%CN-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL486F, -- norcal_misc_fence_initial_closes,
    sum(case when market = 'CN-EB'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL361F, -- eb_misc_fence_initial_closes,
    sum(case when market = 'CN-SF'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1134F, -- sf_misc_fence_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF')  and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL362F, -- sbsf_misc_fence_initial_closes,
    sum(case when market = 'CN-NB' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL363F, -- nb_misc_fence_initial_closes,
    sum(case when market = 'CN-SA' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL364F, -- sac_misc_fence_initial_closes,
    sum(case when market = 'CN-FR' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL365F, -- fr_misc_fence_initial_closes
    sum(case when market like '%CS-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL393F, -- socal_misc_fence_initial_closes
    sum(case when market = 'CS-SV' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL524F, --sv_misc_fence_closes
    sum(case when market = 'CS-OC' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL525F, --oc_misc_fence_closes
    sum(case when market = 'CS-LA' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL526F,--la_misc_fence_closes
    sum(case when market = 'CS-VC' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL527F,--vc_misc_fence_closes
    sum(case when market = 'CS-WA' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL647F, --wa_misc_fence_closes
    sum(case when market = 'CS-SJ' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL648F, --sj_misc_fence_closes
    sum(case when market = 'CS-PA' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL649F,--pa_misc_fence_closes
    sum(case when market = 'CS-ST' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL650F,--st_misc_fence_closes
    sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL973F, -- md_misc_fence_initial_closes,
    sum(case when market like '%MD-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL788F, -- md_misc_fence_initial_closes,
    sum(case when market = 'MD-BL'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL748F, -- bl_misc_fence_initial_closes,
    sum(case when market = 'MD-DC'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL768F, -- dc_misc_fence_initial_closes,
    sum(case when market like '%PA-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL829F, -- pen_misc_fence_initial_closes,
    sum(case when market = 'PA-PH'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL809F, -- ph_misc_fence_initial_closes,
    sum(case when market like '%GA-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL852F, -- ga_misc_fence_initial_closes,
    sum(case when market like '%VA-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL892F, -- va_misc_fence_initial_closes,
    sum(case when market = 'VA-AR'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL872F, -- ar_misc_fence_initial_closes,
    sum(case when market like '%FL-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL913F, -- fl_misc_fence_initial_closes,
    sum(case when market = 'FL-MI'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL934F, -- mi_misc_fence_initial_closes,
    sum(case when market = 'FL-OR'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL954F, -- or_misc_fence_initial_closes,
    sum(case when market like 'PA-WA-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1086F, -- pa_wa_misc_fence_initial_closes,
    sum(case when market = 'WA-SE'and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1061F, --se_misc_fence_initial_closes,
    sum(case when market like 'WN-%' and channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1158F, -- wn_il_fence_initial_closes,
    sum(case when market = 'WN-CH'and channel = 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1181F, --ch_misc_fence_closes
    sum(case when market = 'WN-NA'and channel = 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1204F, --na_misc_fence_closes
    sum(case when market = 'WN-LA'and channel = 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1227F, --la_misc_fence_closes
    -- Paid Closes
    sum(case when channel ilike 'Paid%' then 1 else 0 end) as SAL383, -- Paid Closes
    sum(case when channel = 'Paid/Home Advisor' then 1 else 0 end) as SAL280, -- Home Advisor Closes
    sum(case when channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as SAL1090, -- Home Advisor Ads Closes
    sum(case when channel = 'Paid/Thumbtack' then 1 else 0 end) as SAL147, -- Thumbtack Closes
    sum(case when channel ilike 'Paid/Misc%' then 1 else 0 end) as SAL290,-- Misc Paid Closes
    sum(case when channel = 'Paid/Facebook' then 1 else 0 end) as SAL145, -- Facebook Closes
    sum(case when channel ilike 'Paid/Google%' then 1 else 0 end) as SAL148,-- Google Closes
    sum(case when channel = 'Paid/Bark' then 1 else 0 end) as SAL833, -- Bark Closes
    sum(case when channel = 'Paid/Nextdoor' then 1 else 0 end) as SAL250, -- Nextdoor Closes
    sum(case when channel = 'Paid/Yelp' then 1 else 0 end) as SAL146, -- Paid/Yelp Closes
    --Non Paid closes
    sum(case when channel ilike 'Non Paid%' then 1 else 0 end) as SAL149, -- Non Paid Closes
    sum(case when channel = 'Non Paid/Returning Customers' then 1 else 0 end) as SAL384, -- Non Paid/Returning Customer Closes
    sum(case when channel ilike 'Non Paid%GMB' then 1 else 0 end) as SAL385, -- Non Paid/GMB Closes
    sum(case when channel ilike 'Non Paid%SEO' then 1 else 0 end) as SAL386,-- Non Paid/SEO
    sum(case when channel ilike 'Non Paid%Direct' then 1 else 0 end) as SAL977,-- Non Paid/Direct
    sum(case when channel ilike 'Non Paid/Misc%' then 1 else 0 end) as SAL387, --Non Paid/Misc
-- Fence Paid closes
sum(case when channel ilike 'Paid%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL383F, -- Paid Fence Closes
sum(case when channel = 'Paid/Home Advisor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL280F, -- Home Advisor Closes
sum(case when channel = 'Paid/Home Advisor/Ads' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL1090F, -- Home Advisor Ads Closes
sum(case when channel = 'Paid/Thumbtack' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL147F, -- Thumbtack Closes
sum(case when channel = 'Paid/Borg' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL498F, -- Borg closes
sum(case when channel ilike 'Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL290F,-- Misc Paid Closes
sum(case when channel = 'Paid/Facebook' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL145F, -- Facebook Closes
sum(case when channel ilike 'Paid/Google%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL148F, -- Google Closes
sum(case when channel = 'Paid/Bark' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL833F, -- Bark Closes
sum(case when channel = 'Paid/Nextdoor' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL250F, -- Nextdoor Closes
sum(case when channel = 'Paid/Yelp' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL146F, -- Paid/Yelp Closes
-- Fence Non Paid
sum(case when channel ilike 'Non Paid%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL149F, -- Non Paid Fence Closes
sum(case when channel = 'Non Paid/Returning Customers' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL384F, -- Non Paid/ Returning Customer Driveway Closes
sum(case when channel ilike 'Non Paid%GMB' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL385F, -- Non Paid/GMB Closes
sum(case when channel ilike 'Non Paid%SEO' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL386F,-- Non Paid/SEO
sum(case when channel ilike 'Non Paid%Direct' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL977F,-- Non Paid/Direct
sum(case when channel ilike 'Non Paid/Misc%' and product_id = 105 and is_commercial = 0 then 1 else 0 end) as SAL387F, --Non Paid/Misc
-- Turf Paid closes
sum(case when channel ilike 'Paid%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL383T, -- Paid Turf Closes
sum(case when channel = 'Paid/Home Advisor' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL280T, -- Home Advisor Closes
sum(case when channel = 'Paid/Thumbtack' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL147T, -- Thumbtack Closes
sum(case when channel = 'Paid/Borg' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL498T, -- Borg closes
sum(case when channel ilike 'Paid/Misc%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL290T,-- Misc Paid Closes
sum(case when channel = 'Paid/Facebook' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL145T, -- Facebook Closes
sum(case when channel ilike 'Paid/Google%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL148T, -- Google Closes
sum(case when channel = 'Paid/Bark' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL833T, -- Bark Closes
sum(case when channel = 'Paid/Nextdoor' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL250T, -- Nextdoor Closes
sum(case when channel = 'Paid/Yelp' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL146T, -- Paid/Yelp Closes
-- Turf Non Paid
sum(case when channel ilike 'Non Paid%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL149T, -- Non Paid Turf Closes
sum(case when channel = 'Non Paid/Returning Customers' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL384T, -- Non Paid/ Returning Customer Driveway Closes
sum(case when channel ilike 'Non Paid%GMB' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL385T, -- Non Paid/GMB Closes
sum(case when channel ilike 'Non Paid%SEO' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL386T,-- Non Paid/SEO
sum(case when channel ilike 'Non Paid%Direct' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL977T,-- Non Paid/Direct
sum(case when channel ilike 'Non Paid/Misc%' and product_id = 132 and is_commercial = 0 then 1 else 0 end) as SAL387T, --Non Paid/Misc
-- Driveway Paid closes SAL371
sum(case when channel ilike 'Paid%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL383D, -- Paid Driveway Closes
sum(case when channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL280D, -- Home Advisor Closes
sum(case when channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL147D,-- Thumbtack Closes
sum(case when channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL290D,-- Misc Paid Closes
sum(case when channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL145D,-- Facebook Closes
sum(case when channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL148D,-- Google Closes
sum(case when channel = 'Paid/Bark' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL833D,-- Bark Closes
sum(case when channel = 'Paid/Nexdoor' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL250D,-- Nextdoor Closes
-- Driveway Non Paid
sum(case when channel ilike 'Non Paid%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL149D, -- Non Paid Driveway Closes
sum(case when channel = 'Non Paid/Returning Customers' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL384D, -- Non Paid/Returning Customer Fence Closes
sum(case when channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL146D, -- Paid/Yelp Closes
sum(case when channel ilike 'Non Paid%GMB' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL385D, -- Non Paid/GMB Closes
sum(case when channel ilike 'Non Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL387D, --Non Paid/Misc
sum(case when channel ilike 'Non Paid%SEO' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL386D, -- Non Paid/SEO
    sum(case when market like '%CN-%' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL479D, --NorCal_driveway_revenue
    sum(case when market = 'CN-EB'and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL306D, -- eb_driveway_revenue,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL307D, -- sbsf_driveway_revenue,
    sum(case when market = 'CN-NB' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL308D, -- nb_driveway_revenue,
    sum(case when market = 'CN-SA' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL309D, -- sac_driveway_revenue,
    sum(case when market = 'CN-FR' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL310D, -- fr_driveway_revenue,
    sum(case when market like '%CS-%' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL373D, --socal_driveway_revenue
    sum(case when market = 'CS-SV' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL419D, --sv_driveway_revenue
    sum(case when market = 'CS-OC' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL423D, --oc_driveway_revenue
    sum(case when market = 'CS-LA' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL427D,--la_driveway_revenue
    sum(case when market = 'CS-VC' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL491D,--vc_driveway_revenue
    sum(case when market = 'TX-DL' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL550D,--dl_driveway_revenue
    sum(case when market = 'TX-FW' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL563D,--fw_driveway_revenue
    sum(case when market = 'CS-SD' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL572D,--sd_driveway_revenue
    sum(case when market = 'CN-WA' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL619D,--wa_driveway_revenue
    sum(case when market = 'CN-SJ' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL620D,--sj_driveway_revenue
    sum(case when market = 'CN-PA' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL621D,--pa_driveway_revenue
    sum(case when market = 'CN-ST' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL622D,--st_driveway_revenue
    sum(case when market like '%TX-%' and product_id = 34 and is_commercial = 0 then total_price else 0 end) as SAL583D,--tx_driveway_revenue
    -- HomeAdvisor Driveway Initial Closes
    sum(case when market like '%CN-%' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL481D, -- eb_ha_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL336D, -- eb_ha_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL337D, -- sbsf_ha_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL338D, -- nb_ha_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL339D, -- sac_ha_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL340D, -- fr_ha_driveway_initial_closes,
    sum(case when market like '%CS-%' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL388D, --socal_ha_driveway_closes
    sum(case when market = 'CS-SV' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL504D, --sv_ha_driveway_closes
    sum(case when market = 'CS-OC' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL505D, --oc_ha_driveway_closes
    sum(case when market = 'CS-LA' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL506D,--la_ha_driveway_closes
    sum(case when market = 'CS-VC' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL507D,--vc_ha_driveway_closes
    sum(case when market = 'CN-WA' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL623D, --wa_ha_driveway_closes
    sum(case when market = 'CN-SJ' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL624D, --sj_ha_driveway_closes
    sum(case when market = 'CN-PA' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL625D,--pa_ha_driveway_closes
    sum(case when market = 'CN-ST' and channel ilike 'Paid/Home Advisor%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL626D,--st_ha_driveway_closes
    -- Thumbtack Driveway Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL482D, -- eb_th_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL341D, -- eb_th_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL342D, -- sbsf_th_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL343D, -- nb_th_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL344D, -- sac_th_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL345D, -- fr_th_driveway_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL389D, --socal_th_driveway_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL508D, --sv_th_driveway_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL509D, --oc_th_driveway_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL510D,--la_th_driveway_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL511D,--vc_th_driveway_closes
    sum(case when market = 'CN-WA' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL627D, --wa_th_driveway_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL628D, --sj_th_driveway_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL629D,--pa_th_driveway_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Thumbtack' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL630D,--st_th_driveway_closes
    -- Borg Driveway Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL502D, -- norcal_borg_driveway_initial_closes
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL503D, -- sb_borg_driveway_initial_closes
    sum(case when market = 'CN-EB'and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL539D, -- eb_borg_driveway_initial_closes
    sum(case when market = 'CN-NB' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL540D, -- nb_borg_driveway_initial_closes
    sum(case when market = 'CN-SA' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL541D, -- sac_borg_driveway_initial_closes
    sum(case when market = 'CN-FR' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL542D, -- fr_borg_driveway_initial_closes
    sum(case when market = 'CN-WA'and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL631D, -- wa_borg_driveway_initial_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL632D, -- sj_borg_driveway_initial_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL633D, -- pa_borg_driveway_initial_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Borg' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL634D, -- st_borg_driveway_initial_closes
    -- Paid Google Driveway Initial Closes
    sum(case when market like '%CN-%' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL483D, -- eb_gg_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL346D, -- eb_gg_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL347D, -- sbsf_gg_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL348D, -- nb_gg_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL349D, -- sac_gg_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL350D, -- fr_gg_driveway_initial_closes,
    sum(case when market like '%CS-%' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL390D, --socal_gg_driveway_closes
    sum(case when market = 'CS-SV' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL512D, --sv_gg_driveway_closes
    sum(case when market = 'CS-OC' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL513D, --oc_gg_driveway_closes
    sum(case when market = 'CS-LA' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL514D,--la_gg_driveway_closes
    sum(case when market = 'CS-VC' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL515D,--vc_gg_driveway_closes
    sum(case when market = 'CN-WA' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL635D, --pa_gg_driveway_closes
    sum(case when market = 'CN-SJ' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL636D, --sj_gg_driveway_closes
    sum(case when market = 'CN-PA' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL637D,--pa_gg_driveway_closes
    sum(case when market = 'CN-ST' and channel ilike 'Paid/Google%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL638D,--st_gg_driveway_closes
    -- Facebook Driveway Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL484D, -- eb_fb_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL351D, -- eb_fb_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL352D, -- sbsf_fb_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL353D, -- nb_fb_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL354D, -- sac_fb_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL355D, -- fr_fb_driveway_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL391D, --socal_fb_driveway_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL516D, --sv_fb_driveway_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL517D, --oc_fb_driveway_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL518D,--la_fb_driveway_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL519D,--vc_fb_driveway_closes
    sum(case when market = 'CS-WA' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL639D, --wa_fb_driveway_closes
    sum(case when market = 'CS-SJ' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL640D, --sj_fb_driveway_closes
    sum(case when market = 'CS-PA' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL641D,--pa_fb_driveway_closes
    sum(case when market = 'CS-ST' and channel = 'Paid/Facebook' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL642D,--st_fb_driveway_closes
    -- Yelp Driveway Initial Closes
    sum(case when market like '%CN-%' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL485D, -- eb_yelp_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL356D, -- eb_yelp_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL357D, -- sbsf_yelp_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL358D, -- nb_yelp_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL359D, -- sac_yelp_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL360D, -- fr_yelp_driveway_initial_closes,
    sum(case when market like '%CS-%' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL392D, --socal_yelp_driveway_closes
    sum(case when market = 'CS-SV' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL520D, --sv_yelp_driveway_closes
    sum(case when market = 'CS-OC' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL521D, --oc_yelp_driveway_closes
    sum(case when market = 'CS-LA' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL522D,--la_yelp_driveway_closes
    sum(case when market = 'CS-VC' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL523D,--vc_yelp_driveway_closes
    sum(case when market = 'CN-WA' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL643D, --wa_yelp_driveway_closes
    sum(case when market = 'CN-SJ' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL644D, --sj_yelp_driveway_closes
    sum(case when market = 'CN-PA' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL645D,--pa_yelp_driveway_closes
    sum(case when market = 'CN-ST' and channel = 'Paid/Yelp' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL646D,--st_yelp_driveway_closes
    -- Paid Misc Driveway Leads
    sum(case when market like '%CN-%' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL486D, -- eb_misc_driveway_initial_closes,
    sum(case when market = 'CN-EB'and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL361D, -- eb_misc_driveway_initial_closes,
    sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF')  and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL362D, -- sbsf_misc_driveway_initial_closes,
    sum(case when market = 'CN-NB' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL363D, -- nb_misc_driveway_initial_closes,
    sum(case when market = 'CN-SA' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL364D, -- sac_misc_driveway_initial_closes,
    sum(case when market = 'CN-FR' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL365D, -- fr_misc_driveway_initial_closes
    sum(case when market like '%CS-%' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL393D, -- socal_misc_driveway_initial_closes
    sum(case when market = 'CS-SV' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL524D, --sv_misc_driveway_closes
    sum(case when market = 'CS-OC' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL525D, --oc_misc_driveway_closes
    sum(case when market = 'CS-LA' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL526D,--la_misc_driveway_closes
    sum(case when market = 'CS-VC' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL527D,--vc_misc_driveway_closes
    sum(case when market = 'CS-WA' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL647D, --wa_misc_driveway_closes
    sum(case when market = 'CS-SJ' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL648D, --sj_misc_driveway_closes
    sum(case when market = 'CS-PA' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL649D,--pa_misc_driveway_closes
    sum(case when market = 'CS-ST' and channel ilike 'Paid/Misc%' and product_id = 34 and is_commercial = 0 then 1 else 0 end) as SAL650D--st_misc_driveway_closes
from clean_approved_orders
group by 1
