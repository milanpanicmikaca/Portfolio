-- upload to BQ
with
min_lead_service as --in cases of a lead with multiple services we grab the one with the smallest ID
(
select
	lead_id,
	min(cls.id) as first_lead_service
from
	core_lead_services cls
group by 1
),
calc_leads_detailed as
(
        select
            l.id,
            cc.id as c_id,
                       date_trunc('{period}',l.created_at at time zone 'America/Los_Angeles')::date as date,
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
                else null end as market,
                pm.region_id as region_id,
                  case when not cc.is_commercial and l.product_id = 105 then 1 else 0 end as is_fence,   --excludes commerical
                  case when not cc.is_commercial and l.product_id = 34 then 1 else 0 end as is_driveway,   --excludes commerical
                  case when not cc.is_commercial and l.product_id = 132 then 1 else 0 end as is_turf,   --excludes commerical
                  cc.is_commercial::integer,
                  cc.is_key_account::integer,
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
                          when ps.label  ilike '%vinyl%' then 'vinyl'
                          when ps.label = 'Install Artificial Grass' then 'AG'
                          else 'OTHER'
                  end as service_category
         from core_lead l
         left join customers_visitoraction cv on cv.id = l.visitor_action_id
         left join store_order o on o.id = l.order_id
         left join core_house h on h.id = o.house_id
         left join customers_customer cc on cc.id = h.customer_id
         left join geo_address ga on ga.id = h.address_id
         left join geo_county cn on cn.id = ga.county_id
         left join product_countymarket pcnm on pcnm.county_id = cn.id
         left join product_market pm on pm.id = pcnm.market_id
         left join min_lead_service ml on l.id = ml.lead_id
       	 left join core_lead_services cls on cls.id = ml.first_lead_service
         left join product_service ps on ps.id = cls.service_id
          where
                (l.phone_number is not null or l.email is not null)
            and l.full_name not ilike '%test%' and coalesce(l.email,'') not ilike '%test%'
                   and l.full_name not ilike '%fake%' and coalesce(l.email,'') not ilike '%fake%'
            and l.full_name not ilike '%duplicate%'
                   and l.created_at >= '2018-04-16'
)
,leads as
(
select
        date,
        count(*) as MAR101, --leads
        count(distinct(case when is_key_account = 1 then c_id end)) as MAR803C, --key account leads
        sum(case when is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR101F, --fence_leads
        sum(case when is_turf = 1 and is_commercial = 0 then 1 else 0 end) as MAR101T, --turf_leads
        sum(case when is_driveway = 1 then 1 else 0 end) as MAR101D, --driveway_leads,
        sum(case when is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR618F, -- vinyl fence_leads
          -- Leads by Channel
        -- Paid Leads
        sum(case when channel ilike 'Paid%' then 1 else 0 end) as MAR469, -- Paid Leads
        sum(case when channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR328, -- Home Advisor Leads
        sum(case when channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2358, -- Home Advisor Ads Leads
        sum(case when channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR135, -- Thumbtack Leads
        sum(case when channel = 'Paid/Borg' then 1 else 0 end) as MAR682, -- Borg Leads
        sum(case when channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR335,-- Misc Paid Leads
        sum(case when channel = 'Paid/Facebook' then 1 else 0 end) as MAR121, -- Facebook Leads
        sum(case when channel ilike 'Paid/Google%' then 1 else 0 end) as MAR158,-- Google Leads
        sum(case when channel = 'Paid/Bark' then 1 else 0 end) as MAR1286,-- Bark Leads
        sum(case when channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR235,-- Nextdoor Leads
        sum(case when channel = 'Paid/Yelp' then 1 else 0 end) as MAR131, -- Paid/Yelp Leads
        --Non Paid
        sum(case when channel ilike 'Non Paid%' then 1 else 0 end) as MAR125, -- Non Paid Leads
        sum(case when channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR470, -- Non Paid%GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR471,-- Non Paid%SEO
        sum(case when channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2059,-- Non Paid%Direct
        sum(case when channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR472, --Non Paid/Misc
               -- Fence Paid Leads
        sum(case when channel ilike 'Paid%' and is_fence = 1 then 1 else 0 end) as MAR469F, -- Paid Leads
        sum(case when channel = 'Paid/Home Advisor' and is_fence = 1 then 1 else 0 end) as MAR328F, -- Home Advisor Leads
        sum(case when channel = 'Paid/Home Advisor/Ads' and is_fence = 1 then 1 else 0 end) as MAR2358F, -- Home Advisor Ads Leads
        sum(case when channel = 'Paid/Thumbtack' and is_fence = 1 then 1 else 0 end) as MAR135F, -- Thumbtack Leads
        sum(case when channel = 'Paid/Borg' and is_fence = 1 then 1 else 0 end) as MAR682F, -- Borg Leads
        sum(case when channel ilike 'Paid/Misc%' and is_fence = 1 then 1 else 0 end) as MAR335F,-- Misc Paid Leads
        sum(case when channel = 'Paid/Facebook' and is_fence = 1 then 1 else 0 end) as MAR121F, -- Facebook Leads
        sum(case when channel ilike 'Paid/Google%' and is_fence = 1 then 1 else 0 end) as MAR158F, -- Google Leads
        sum(case when channel = 'Paid/Bark' and is_fence = 1 then 1 else 0 end) as MAR1286F, -- Bark Leads
        sum(case when channel = 'Paid/Nextdoor' and is_fence = 1 then 1 else 0 end) as MAR235F, -- Nextdoor Leads
        sum(case when channel = 'Paid/Yelp' and is_fence = 1 then 1 else 0 end) as MAR131F, -- Paid/Yelp Leads
        -- Fence Non Paid
        sum(case when channel ilike 'Non Paid%' and is_fence = 1 then 1 else 0 end) as MAR125F, -- Non Paid Leads
        sum(case when channel ilike 'Non Paid%GMB' and is_fence = 1 then 1 else 0 end) as MAR470F, -- Non Paid%GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and is_fence = 1 then 1 else 0 end) as MAR471F,-- Non Paid%SEO
        sum(case when channel ilike 'Non Paid%Direct' and is_fence = 1 then 1 else 0 end) as MAR2059F,-- Non Paid%Direct
        sum(case when channel ilike 'Non Paid/Misc%' and is_fence = 1 then 1 else 0 end) as MAR472F, --Non Paid/Misc
         -- Vinyl Fence Paid Leads
        sum(case when channel ilike 'Paid%' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR619F, -- Paid Leads
        sum(case when channel = 'Paid/Home Advisor' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR620F, -- Home Advisor Leads
        sum(case when channel = 'Paid/Home Advisor/Ads' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR2458F, -- Home Advisor Ads Leads
        sum(case when channel = 'Paid/Thumbtack' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR621F, -- Thumbtack Leads
        sum(case when channel ilike 'Paid/Misc%' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR622F,-- Misc Paid Leads
        sum(case when channel = 'Paid/Facebook' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR623F, -- Facebook Leads
        sum(case when channel ilike 'Paid/Google%' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR624F, -- Google Leads
        sum(case when channel = 'Paid/Bark' and service_category ilike '%vinyl%' then 1 else 0 end) as MAR1389F,
        sum(case when channel = 'Paid/Nextdoor' and service_category ilike '%vinyl%' then 1 else 0 end) as MAR1390F,
        sum(case when channel = 'Paid/Yelp' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR626F, -- Paid/Yelp Leads
        -- Vinyl Fence Non Paid
        sum(case when channel ilike 'Non Paid%' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR625F, -- Non Paid Leads
        sum(case when channel ilike 'Non Paid%GMB' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR627F, -- Non Paid%GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR628F,-- Non Paid%SEO
        sum(case when channel ilike 'Non Paid%Direct' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR2073F,-- Non Paid%Direct
        sum(case when channel ilike 'Non Paid/Misc%' and is_fence = 1 and service_category ilike '%vinyl%' then 1 else 0 end) as MAR629F, --Non Paid/Misc
        -- Turf Paid Leads
        sum(case when channel ilike 'Paid%' and is_turf = 1 then 1 else 0 end) as MAR469T, -- Paid Turf Leads
        sum(case when channel = 'Paid/Home Advisor' and is_turf = 1 then 1 else 0 end) as MAR328T, -- Home Advisor Leads
        sum(case when channel = 'Paid/Home Advisor/Ads' and is_turf = 1 then 1 else 0 end) as MAR2358T, -- Home Advisor Ads Leads
        sum(case when channel = 'Paid/Thumbtack' and is_turf = 1 then 1 else 0 end) as MAR135T, -- Thumbtack Leads
        sum(case when channel = 'Paid/Borg' and is_turf = 1 then 1 else 0 end) as MAR682T, -- Borg Leads
        sum(case when channel ilike 'Paid/Misc%' and is_turf = 1 then 1 else 0 end) as MAR335T,-- Misc Paid Leads
        sum(case when channel = 'Paid/Facebook' and is_turf = 1 then 1 else 0 end) as MAR121T, -- Facebook Leads
        sum(case when channel ilike 'Paid/Google%' and is_turf = 1 then 1 else 0 end) as MAR158T, -- Google Leads
        sum(case when channel = 'Paid/Bark' and is_turf = 1 then 1 else 0 end) as MAR1286T, -- Bark Leads
        sum(case when channel = 'Paid/Nextdoor' and is_turf = 1 then 1 else 0 end) as MAR235T, -- Nextdoor Leads
        sum(case when channel = 'Paid/Yelp' and is_turf = 1 then 1 else 0 end) as MAR131T, -- Paid/Yelp Leads
         -- Turf Non Paid
        sum(case when channel ilike 'Non Paid%' and is_turf = 1 then 1 else 0 end) as MAR125T, -- Non Paid Leads
        sum(case when channel ilike 'Non Paid%GMB' and is_turf = 1 then 1 else 0 end) as MAR470T, -- Non Paid%GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and is_turf = 1 then 1 else 0 end) as MAR471T,-- Non Paid%SEO
        sum(case when channel ilike 'Non Paid%Direct' and is_turf = 1 then 1 else 0 end) as MAR2059T,-- Non Paid%Direct
        sum(case when channel ilike 'Non Paid/Misc%' and is_turf = 1 then 1 else 0 end) as MAR472T, --Non Paid/Misc
        -- Driveway Paid Leads
        sum(case when channel ilike 'Paid%' and is_driveway = 1 then 1 else 0 end) as MAR469D, -- Paid Leads
        sum(case when channel ilike 'Paid/Home Advisor%' and is_driveway  = 1 then 1 else 0 end) as MAR328D, -- Home Advisor Leads
        sum(case when channel = 'Paid/Thumbtack' and is_driveway = 1 then 1 else 0 end) as MAR135D,-- Thumbtack Leads
        sum(case when channel ilike 'Paid/Misc%' and is_driveway = 1 then 1 else 0 end) as MAR335D,-- Misc Paid Leads
        sum(case when channel = 'Paid/Facebook' and is_driveway = 1 then 1 else 0 end) as MAR121D,-- Facebook Leads
        sum(case when channel ilike 'Paid/Google%' and is_driveway = 1 then 1 else 0 end) as MAR158D,-- Google Leads
        sum(case when channel = 'Paid/Bark' and is_driveway = 1 then 1 else 0 end) as MAR1286D,-- Bark Leads
        sum(case when channel = 'Paid/Nextdoor' and is_driveway = 1 then 1 else 0 end) as MAR235D,-- Nextdoor Leads
        -- Driveway Non Paid
        sum(case when channel ilike 'Non Paid%' and is_driveway = 1 then 1 else 0 end) as MAR125D, -- Non Paid Leads
        sum(case when channel = 'Paid/Yelp' and is_driveway = 1 then 1 else 0 end) as MAR131D, -- Paid/Yelp Leads
        sum(case when channel ilike 'Non Paid%GMB' and is_driveway = 1 then 1 else 0 end) as MAR470D, -- Non Paid%GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and is_driveway = 1 then 1 else 0 end) as MAR471D, -- Non Paid%SEO
        sum(case when channel ilike 'Non Paid/Misc%' and is_driveway = 1 then 1 else 0 end) as MAR472D,--Non Paid/Misc
        --- leads by market
                sum(case when market is null then 1 else 0 end) as MAR170, --na_leads
                sum(case when market like '%CN-%' then 1 else 0 end) as MAR638, --nc_leads
                sum(case when market = 'CN-EB' then 1 else 0 end) as MAR165, --eb_leads
                sum(case when market = 'CN-NB' then 1 else 0 end) as MAR168, --nb_leads
                sum(case when market = 'CN-SA' then 1 else 0 end) as MAR166, --sac_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA') then 1 else 0 end) as MAR167, --sb_leads
                sum(case when market = 'CN-SF' then 1 else 0 end) as MAR169, --sf_leads
                sum(case when market = 'CN-FR' then 1 else 0 end) as MAR193, --fr_leads
                sum(case when market = 'CN-WA' then 1 else 0 end) as MAR804, --wa_leads
                sum(case when market = 'CN-SJ' then 1 else 0 end) as MAR805, --sj_leads
                sum(case when market = 'CN-PA' then 1 else 0 end) as MAR806, --pa_leads
                sum(case when market = 'CN-ST' then 1 else 0 end) as MAR807, --st_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') then 1 else 0 end) as MAR345, --sb_sf_leads
                sum(case when market like '%CS-%' then 1 else 0 end) as MAR476, --sc_leads
                sum(case when market = 'CS-SV' then 1 else 0 end) as MAR538, --sv_leads
                sum(case when market = 'CS-OC' then 1 else 0 end) as MAR539, --oc_leads
                sum(case when market = 'CS-LA' then 1 else 0 end) as MAR540, --la_leads
                sum(case when market = 'CS-VC' then 1 else 0 end) as MAR681, --vc_leads
                sum(case when market = 'CS-SD' then 1 else 0 end) as MAR749, --sd_leads
                sum(case when market like '%TX-%' then 1 else 0 end) as MAR765, --tx_leads
                sum(case when market = 'TX-FW' then 1 else 0 end) as MAR735, --fw_leads
                sum(case when market = 'TX-DL' then 1 else 0 end) as MAR715, --dl_leads
                sum(case when market = 'TX-SA' then 1 else 0 end) as MAR1056, --sa_leads
                sum(case when market = 'TX-HT' then 1 else 0 end) as MAR2020, --ht_leads
                sum(case when market = 'TX-AU' then 1 else 0 end) as MAR2021, --au_leads
                sum(case when market like '%GA-%' then 1 else 0 end) as MAR1293, --ga_leads
                sum(case when market = 'GA-AT' then 1 else 0 end) as MAR2022, --at_leads
                sum(case when market like '%MD-%' then 1 else 0 end) as MAR1191, --md_leads
                sum(case when market = 'MD-BL' then 1 else 0 end) as MAR1113, --bl_leads
                sum(case when market = 'MD-DC' then 1 else 0 end) as MAR1154, --dc_leads
                sum(case when market like '%PA-%' then 1 else 0 end) as MAR1256, --pen_leads
                sum(case when market = 'PA-PH' then 1 else 0 end) as MAR1220, --ph_leads
                sum(case when market like '%VA-%' then 1 else 0 end) as MAR1535, --va_leads
                sum(case when market = 'VA-AR' then 1 else 0 end) as MAR1499, --ar_leads
                sum(case when market like '%FL-%' then 1 else 0 end) as MAR1579, --fl_leads
                sum(case when market = 'FL-MI' then 1 else 0 end) as MAR1609, --mi_leads
                sum(case when market = 'FL-OR' then 1 else 0 end) as MAR1657, --or_leads
                sum(case when market = 'WA-SE' then 1 else 0 end) as MAR2225, --se_leads
                sum(case when market_code like '%PA-WA-%' then 1 else 0 end) as MAR2273, --pa_wa_leads
                sum(case when market_code like 'WN-%' then 1 else 0 end) as MAR2526, --wn_il_leads
                sum(case when market = 'WN-CH' then 1 else 0 end) as MAR2893, --wn-ch_leads
                sum(case when market = 'WN-NA' then 1 else 0 end) as MAR2952, --wn-na_leads
                sum(case when market = 'WN-LA' then 1 else 0 end) as MAR3011, --wn-la_leads
                -- Fence Leads by Market
                sum(case when market is null and is_fence = 1 then 1 else 0 end) as MAR170F, --na_fence_leads
                sum(case when market like '%CN-%' and is_fence = 1 then 1 else 0 end) as MAR638F, --nc_fence_leads
                sum(case when market = 'CN-EB' and is_fence = 1 and is_commercial=0 then 1 else 0 end) as MAR165F, --eb_fence_leads
                sum(case when market = 'CN-NB' and is_commercial=0 and is_fence = 1 then 1 else 0 end) as MAR168F, --nb_fence_leads
                sum(case when market = 'CN-SA' and is_fence = 1 then 1 else 0 end) as MAR166F, --sac_fence_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_commercial=0 and is_fence = 1 then 1 else 0 end) as MAR167F, --sb_fence_leads
                sum(case when market = 'CN-SF' and is_fence = 1 then 1 else 0 end) as MAR169F, --sf_fence_leads
                sum(case when market = 'CN-FR' and is_fence = 1 then 1 else 0 end) as MAR193F, --fr_fence_leads
                sum(case when market = 'CN-WA' and is_fence = 1 then 1 else 0 end) as MAR804F, --wa_fence_leads
                sum(case when market = 'CN-SJ' and is_fence = 1 then 1 else 0 end) as MAR805F, --sj_fence_leads
                sum(case when market = 'CN-PA' and is_fence = 1 then 1 else 0 end) as MAR806F, --pa_fence_leads
                sum(case when market = 'CN-ST' and is_fence = 1 then 1 else 0 end) as MAR807F, --st_fence_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and is_fence = 1 then 1 else 0 end) as MAR345F, --sb_sf_fence_leads
                sum(case when market like '%CS-%' and is_fence = 1 then 1 else 0 end) as MAR476F, --sc_fence_leads
                sum(case when market = 'CS-SV' and is_fence = 1 then 1 else 0 end) as MAR538F, --sv_fence_leads
                sum(case when market = 'CS-OC' and is_fence = 1 then 1 else 0 end) as MAR539F, --oc_fence_leads
                sum(case when market = 'CS-LA' and is_fence = 1 then 1 else 0 end) as MAR540F, --la_fence_leads
                sum(case when market = 'CS-VC' and is_fence = 1 then 1 else 0 end) as MAR681F, --vc_fence_leads
                sum(case when market = 'CS-SD' and is_fence = 1 then 1 else 0 end) as MAR749F, --sd_fence_leads
                sum(case when market like '%TX-%' and is_fence = 1 then 1 else 0 end) as MAR765F, --tx_fence_leads
                sum(case when market = 'TX-FW' and is_fence = 1 then 1 else 0 end) as MAR735F, --fw_fence_leads
                sum(case when market = 'TX-DL' and is_fence = 1 then 1 else 0 end) as MAR715F, --dl_fence_leads
                sum(case when market = 'TX-SA' and is_fence = 1 then 1 else 0 end) as MAR1056F, --sa_fence_leads
                sum(case when market = 'TX-HT' and is_fence = 1 then 1 else 0 end) as MAR2020F, --ht_fence_leads
                sum(case when market = 'TX-AU' and is_fence = 1 then 1 else 0 end) as MAR2021F, --au_fence_leads
                sum(case when market like '%GA-%' and is_fence = 1 then 1 else 0 end) as MAR1293F, --ga_fence_leads
                sum(case when market = 'GA-AT' and is_fence = 1 then 1 else 0 end) as MAR2022F, --at_fence_leads
                sum(case when market like '%MD-%' and is_fence = 1 then 1 else 0 end) as MAR1191F, --md_fence_leads
                sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and is_fence = 1 then 1 else 0 end) as MAR2023F, --ne_fence_leads
                sum(case when market = 'MD-BL' and is_fence = 1 then 1 else 0 end) as MAR1113F, --bl_fence_leads
                sum(case when market = 'MD-DC' and is_fence = 1 then 1 else 0 end) as MAR1154F, --dc_fence_leads
                sum(case when market like '%PA-%' and is_fence = 1 then 1 else 0 end) as MAR1256F, --pen_fence_leads
                sum(case when market = 'PA-PH' and is_fence = 1 then 1 else 0 end) as MAR1220F, --ph_fence_leads
                sum(case when market like '%VA-%' and is_fence = 1 then 1 else 0 end) as MAR1535F, --va_fence_leads
                sum(case when market = 'VA-AR' and is_fence = 1 then 1 else 0 end) as MAR1499F, --ar_fence_leads
                sum(case when market like '%FL-%' and is_fence = 1 then 1 else 0 end) as MAR1579F, --fl_fence_leads
                sum(case when market = 'FL-MI' and is_fence = 1 then 1 else 0 end) as MAR1609F, --mi_fence_leads
                sum(case when market = 'FL-OR' and is_fence = 1 then 1 else 0 end) as MAR1657F, --or_fence_leads
                sum(case when market_code like 'PA-WA-%' and is_fence = 1 then 1 else 0 end) as MAR2273F, --pa_wa_fence_leads
                sum(case when market = 'WA-SE' and is_fence = 1 then 1 else 0 end) as MAR2225F, --se_fence_leads
                sum(case when market_code like 'WN-%' and is_fence = 1 then 1 else 0 end) as MAR2526F, --wn_il_fence_leads
                sum(case when market = 'WN-CH' and is_fence = 1 then 1 else 0 end) as MAR2893F, --wn-ch_fence_leads
                sum(case when market = 'WN-NA' and is_fence = 1 then 1 else 0 end) as MAR2952F, --wn-na_fence_leads
                sum(case when market = 'WN-LA' and is_fence = 1 then 1 else 0 end) as MAR3011F, --wn-la_fence_leads
     -- Turf Leads by Market
                sum(case when market like '%CN-%' and is_turf = 1 then 1 else 0 end) as MAR638T, --nc_turf_leads
                sum(case when market like '%CS-%'  and is_turf = 1 then 1 else 0 end) as MAR476T, --socal_turf_leads
                sum(case when market = 'CS-SV' and is_turf = 1 then 1 else 0 end) as MAR538T, --sv_turf_leads
                sum(case when market = 'CS-OC' and is_turf = 1 then 1 else 0 end) as MAR539T, --oc_turf_leads
                sum(case when market = 'CS-LA' and is_turf = 1 then 1 else 0 end) as MAR540T, --la_turf_leads
                sum(case when market = 'CS-VC' and is_turf = 1 then 1 else 0 end) as MAR681T, --vc_turf_leads
                sum(case when market = 'CS-SD' and is_turf = 1 then 1 else 0 end) as MAR749T, --sd_turf_leads
                sum(case when market is null and is_turf = 1 then 1 else 0 end) as MAR170T, --na_turf_leads
     -- Driveway Leads by Market
                sum(case when market = 'FL-OR' and is_fence = 1 then 1 else 0 end) as MAR1657F, --mi_fence_leads
                -- Driveway Leads by Market
                sum(case when market like '%CN-%' and is_driveway = 1 then 1 else 0 end) as MAR638D, --nc_driveway_leads
                sum(case when market = 'CN-EB' and is_driveway = 1 then 1 else 0 end) as MAR165D, --eb_driveway_leads
                sum(case when market is null and is_driveway = 1 then 1 else 0 end) as MAR170D, --na_driveway_leads
                sum(case when market = 'CN-NB' and is_driveway = 1 then 1 else 0 end) as MAR168D, --nb_driveway_leads
                sum(case when market = 'CN-SA' and is_driveway = 1 then 1 else 0 end) as MAR166D, --sac_driveway_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_driveway = 1 then 1 else 0 end) as MAR167D, --sb_driveway_leads
                sum(case when market = 'CN-SF' and is_driveway = 1 then 1 else 0 end) as MAR169D, --sf_driveway_leads
                sum(case when market = 'CN-FR' and is_driveway = 1 then 1 else 0 end) as MAR193D, --fr_driveway_leads
                sum(case when market = 'TX-DL' and is_driveway = 1 then 1 else 0 end) as MAR715D, --dl_driveway_leads
                sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and is_driveway = 1 then 1 else 0 end) as MAR345D, --sb_sf_driveway_leads
                sum(case when market like '%CS-%' and is_driveway = 1 then 1 else 0 end) as MAR476D, --sc_driveway_leads
                sum(case when market = 'CS-SV' and is_driveway = 1 then 1 else 0 end) as MAR538D, --sv_driveway_leads
                sum(case when market = 'CS-OC' and is_driveway = 1 then 1 else 0 end) as MAR539D, --oc_driveway_leads
                sum(case when market = 'CS-LA' and is_driveway = 1 then 1 else 0 end) as MAR540D, --la_driveway_leads
                sum(case when market = 'CS-VC' and is_driveway = 1 then 1 else 0 end) as MAR681D, --vc_driveway_leads
                sum(case when market like '%TX-%' and is_driveway = 1 then 1 else 0 end) as MAR765D, --tx_driveway_leads
                sum(case when market = 'TX-FW' and is_driveway = 1 then 1 else 0 end) as MAR735D, --fw_driveway_leads
                sum(case when market = 'CS-SD' and is_driveway = 1 then 1 else 0 end) as MAR749D, --sd_driveway_leads
                sum(case when market = 'CN-WA' and is_driveway = 1 then 1 else 0 end) as MAR804D, --wa_driveway_leads
                sum(case when market = 'CN-SJ' and is_driveway = 1 then 1 else 0 end) as MAR805D, --sj_driveway_leads
                sum(case when market = 'CN-PA' and is_driveway = 1 then 1 else 0 end) as MAR806D, --pa_driveway_leads
                sum(case when market = 'CN-ST' and is_driveway = 1 then 1 else 0 end) as MAR807D, --st_driveway_leads
        --
                sum(case when service_category = 'WF'  and is_fence = 1 then 1 else 0 end) as MAR240, --wood_fence_leads
                sum(case when service_category = 'CLF' and is_fence = 1 then 1 else 0 end) as MAR241, --chainlink_fence_leads
                sum(case when service_category = 'CD'  then 1 else 0 end) as MAR242, --concrete_driveways_leads
                sum(case when service_category = 'CP' then 1 else 0 end) as MAR243, --concrete_patios_leads
                sum(case when service_category = 'BP'   then 1 else 0 end) as MAR244, --brick_patios_leads
                sum(case when service_category = 'SC'  then 1 else 0 end) as MAR245, --stamped_concrete_leads
                sum(case when service_category = 'IPD'  then 1 else 0 end) as MAR246, --pavers_driveways_leads
                sum(case when service_category = 'IPP'  then 1 else 0 end) as MAR247, --pavers_patios_leads
                sum(case when service_category = 'BD' then 1 else 0 end) as MAR248, --brick_driveways_leads
                sum(case when service_category = 'RWF' and is_fence = 1 then 1 else 0 end) as MAR249, --repair_fence_leads
                sum(case when service_category = 'AP'  then 1 else 0 end) as MAR250, --asphalt_leads
                sum(case when service_category = 'AG'  then 1 else 0 end) as MAR2210, --artificial turf_leads
                sum(case when service_category = 'OTHER'  then 1 else 0 end) as MAR251, --other_leads
           -- Fence Leads by Product
                sum(case when service_category = 'OTHER' and is_fence = 1 then 1 else 0 end) as MAR251F, --other_fence_leads
           -- Driveway Leads by Product
                sum(case when service_category = 'OTHER' and is_driveway = 1  then 1 else 0 end) as MAR251D --other_driveway_leads
 from calc_leads_detailed
--where is_commercial = 0
group by 1
order by 1 desc
)
, leads_by_market_and_channel as
(select
        date,
        -- Paid Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR641F, -- nc_paid_fence_leads,
        sum(case when market = 'CN-EB' and channel ilike 'Paid%' then 1 else 0 end) as MAR347F, -- eb_paid_fence_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid%' then 1 else 0 end) as MAR358F, -- sbsf_paid_fence_leads,
        sum(case when market = 'CN-SF' and channel ilike 'Paid%' then 1 else 0 end) as MAR2469F, -- sf_paid_fence_leads,
        sum(case when market = 'CN-NB' and channel ilike 'Paid%' then 1 else 0 end) as MAR369F, -- nb_paid_fence_leads,
        sum(case when market = 'CN-SA' and channel ilike 'Paid%' then 1 else 0 end) as MAR380F, -- sac_paid_fence_leads,
        sum(case when market = 'CN-FR' and channel ilike 'Paid%' then 1 else 0 end) as MAR391F, -- fr_paid_fence_leads,
        sum(case when market like '%CS-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR477F, -- sc_paid_fence_leads,
        sum(case when market = 'CS-SV' and channel ilike 'Paid%' then 1 else 0 end) as MAR541F, -- sv_paid_fence_leads,
        sum(case when market = 'CS-OC' and channel ilike 'Paid%' then 1 else 0 end) as MAR552F, -- oc_paid_fence_leads,
        sum(case when market = 'CS-LA' and channel ilike 'Paid%' then 1 else 0 end) as MAR563F, -- la_paid_fence_leads,
        sum(case when market = 'CS-VC' and channel ilike 'Paid%' then 1 else 0 end) as MAR668F, -- vc_paid_fence_leads,
        sum(case when market = 'CS-SD' and channel ilike 'Paid%' then 1 else 0 end) as MAR754F, --sd_paid_fence_leads
        sum(case when market like '%TX-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR766F, -- tx_paid_fence_leads,
        sum(case when market = 'TX-DL' and channel ilike 'Paid%' then 1 else 0 end) as MAR720F, --dl_paid_fence_leads
        sum(case when market = 'TX-FW' and channel ilike 'Paid%' then 1 else 0 end) as MAR736F, --fw_paid_fence_leads
        sum(case when market = 'CN-WA' and channel ilike 'Paid%' then 1 else 0 end) as MAR808F, --wa_paid_fence_leads
        sum(case when market = 'CN-SJ' and channel ilike 'Paid%' then 1 else 0 end) as MAR809F, --sj_paid_fence_leads
        sum(case when market = 'CN-PA' and channel ilike 'Paid%' then 1 else 0 end) as MAR810F, --pa_paid_fence_leads
        sum(case when market = 'CN-ST' and channel ilike 'Paid%' then 1 else 0 end) as MAR811F, --st_paid_fence_leads
        sum(case when market like '%MD-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR1193F, -- md_paid_fence_leads,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Paid%' then 1 else 0 end) as MAR2025F, -- ne_paid_fence_leads,
        sum(case when market = 'MD-BL' and channel ilike 'Paid%' then 1 else 0 end) as MAR1123F, -- bl_paid_fence_leads,
        sum(case when market = 'MD-DC' and channel ilike 'Paid%' then 1 else 0 end) as MAR1164F, -- dc_paid_fence_leads,
        sum(case when market like '%PA-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR1258F, -- penn_paid_fence_leads,
        sum(case when market = 'PA-PH' and channel ilike 'Paid%' then 1 else 0 end) as MAR1228F, -- ph_paid_fence_leads,
        sum(case when market like '%GA-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR1295F, -- ga_paid_fence_leads,
        sum(case when market like '%VA-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR1537F, -- va_paid_fence_leads,
        sum(case when market = 'VA-AR' and channel ilike 'Paid%' then 1 else 0 end) as MAR1507F, -- ar_paid_fence_leads,
        sum(case when market like '%FL-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR1581F, -- fl_paid_fence_leads,
        sum(case when market = 'FL-MI' and channel ilike 'Paid%' then 1 else 0 end) as MAR1617F, -- mi_paid_fence_leads,
        sum(case when market = 'FL-OR' and channel ilike 'Paid%' then 1 else 0 end) as MAR1665F, -- or_paid_fence_leads,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR2275F, -- pa_wa_paid_fence_leads,
        sum(case when market = 'WA-SE' and channel ilike 'Paid%' then 1 else 0 end) as MAR2233F, -- se_paid_fence_leads,
        sum(case when market_code like 'WN-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR2529F, --wn_paid_fence_leads
        sum(case when market = 'WN-CH' and channel ilike 'Paid%' then 1 else 0 end) as MAR2918F, --wn-ch_paid_fence_leads
        sum(case when market = 'WN-NA' and channel ilike 'Paid%' then 1 else 0 end) as MAR2977F, --wn-na_paid_fence_leads
        sum(case when market = 'WN-LA' and channel ilike 'Paid%' then 1 else 0 end) as MAR3036F, --wn-la_paid_fence_leads
        -- Non Paid Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR642F, -- sc_nonpaid_fence_leads,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR348F, -- eb_nonpaid_fence_leads,
        sum(case when market = 'CN-SF' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2470F, -- sf_nonpaid_fence_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%' then 1 else 0 end) as MAR359F, -- sbsf_nonpaid_fence_leads,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR370F, -- nb_nonpaid_fence_leads,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR381F, -- sac_nonpaid_fence_leads,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR392F, -- fr_nonpaid_fence_leads,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR478F, -- nc_nonpaid_fence_leads,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR542F, -- sv_nonpaid_fence_leads,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR553F, -- oc_nonpaid_fence_leads,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR564F, -- la_nonpaid_fence_leads,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR669F, -- vc_nonpaid_fence_leads,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR767F, -- tx_nonpaid_fence_leads,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR726F, -- dl_nonpaid_fence_leads,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR737F, -- fw_nonpaid_fence_leads,
        sum(case when market = 'SD-SD' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR760F, -- sd_nonpaid_fence_leads,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR812F, --wa_Non Paid_fence_leads
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR813F, --sj_Non Paid_fence_leads
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR814F, --pa_Non Paid_fence_leads
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR815F, --st_Non Paid_fence_leads
        sum(case when market like '%MD-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1194F, -- md_nonpaid_fence_leads,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2026F, -- ne_nonpaid_fence_leads,
        sum(case when market = 'MD-BL' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1124F, -- bl_nonpaid_fence_leads,
        sum(case when market = 'MD-DC' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1165F, -- dc_nonpaid_fence_leads,
        sum(case when market like '%PA-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1259F, -- pen_nonpaid_fence_leads,
        sum(case when market = 'PA-PH' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1229F, -- ph_nonpaid_fence_leads,
        sum(case when market like '%GA-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1296F, -- ga_nonpaid_fence_leads,
        sum(case when market like '%VA-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1538F, -- va_nonpaid_fence_leads,
        sum(case when market = 'VA-AR' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1508F, -- ar_nonpaid_fence_leads,
        sum(case when market like '%FL-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1582F, -- fl_nonpaid_fence_leads,
        sum(case when market = 'FL-MI' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1618F, -- mi_nonpaid_fence_leads,
        sum(case when market = 'FL-OR' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1666F, -- mi_nonpaid_fence_leads,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2276F, -- pa_wa_nonpaid_fence_leads,
        sum(case when market = 'WA-SE' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2234F, -- se_nonpaid_fence_leads,
        sum(case when market_code like 'WN-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2560F, --wn_nonpaid_fence_leads
        sum(case when market = 'WN-CH' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2919F, --wn-ch_nonpaid_fence_leads
        sum(case when market = 'WN-NA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR2978F, --wn-na_nonpaid_fence_leads
        sum(case when market = 'WN-LA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR3037F, --wn-la_nonpaid_fence_leads
        -- HomeAdvisor Fence Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR643F, -- sc_ha_fence_leads,
        sum(case when market = 'CN-EB' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR349F, -- eb_ha_fence_leads,
        sum(case when market = 'CN-SF' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2471F, -- sf_ha_fence_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR360F, -- sbsf_ha_fence_leads,
        sum(case when market = 'CN-NB' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR371F, -- nb_ha_fence_leads,
        sum(case when market = 'CN-SA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR382F, -- sac_ha_fence_leads,
        sum(case when market = 'CN-FR' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR393F, -- fr_ha_fence_leads,
        sum(case when market like '%CS-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR479F, -- nc_ha_fence_leads,
        sum(case when market = 'CS-SV' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR543F, -- sv_ha_fence_leads,
        sum(case when market = 'CS-OC' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR554F, -- oc_ha_fence_leads,
        sum(case when market = 'CS-LA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR565F, -- la_ha_fence_leads,
        sum(case when market = 'CS-VC' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR670F, -- vc_ha_fence_leads,
        sum(case when market like '%TX-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR768F, -- tx_ha_fence_leads,
        sum(case when market = 'TX-DL' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR721F, -- dl_ha_fence_leads,
        sum(case when market = 'TX-FW' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR738F, -- fw_ha_fence_leads,
        sum(case when market = 'CS-SD' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR755F, -- sd_ha_fence_leads
        sum(case when market = 'CN-WA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR816F, --wa_ha_fence_leads
        sum(case when market = 'CN-SJ' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR817F, --sj_ha_fence_leads
        sum(case when market = 'CN-PA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR818F, --pa_ha_fence_leads
        sum(case when market = 'CN-ST' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR819F, --st_ha_fence_leads
        sum(case when market like '%MD-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1195F, -- md_ha_fence_leads,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2027F, -- ne_ha_fence_leads,
        sum(case when market = 'MD-BL' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1125F, -- bl_ha_fence_leads,
        sum(case when market = 'MD-DC' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1166F, -- dc_ha_fence_leads,
        sum(case when market like '%PA-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1260F, -- pen_ha_fence_leads,
        sum(case when market = 'PA-PH' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1230F, -- ph_ha_fence_leads,
        sum(case when market like '%GA-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1297F, --ga_ha_fence_leads,
        sum(case when market like '%VA-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1539F, -- va_ha_fence_leads,
        sum(case when market = 'VA-AR' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1509F, -- ar_ha_fence_leads,
        sum(case when market like '%FL-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1583F, -- fl_ha_fence_leads,
        sum(case when market = 'FL-MI' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1619F, -- mi_ha_fence_leads,
        sum(case when market = 'FL-OR' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1667F, -- mi_ha_fence_leads,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2277F, -- pa_wa_ha_fence_leads,
        sum(case when market = 'WA-SE' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2235F, --se_ha_fence_leads,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2591F, --wn_ha_fence_leads
        sum(case when market = 'WN-CH' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2920F, --wn-ch_ha_fence_leads
        sum(case when market = 'WN-NA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR2979F, --wn-na_ha_fence_leads
        sum(case when market = 'WN-LA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR3038F, --wn-la_ha_fence_leads      
        --- HA Ads leads
        sum(case when market like '%CN-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2365F, -- sc_ha_ads_fence_leads,
        sum(case when market = 'CN-EB' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2366F, -- eb_ha_ads_fence_leads,
        sum(case when market = 'CN-SF' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2501F, -- sf_ha_ads_fence_leads,
        sum(case when market = 'CN-NB' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2367F, -- nb_ha_ads_fence_leads,
        sum(case when market = 'CN-SA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2368F, -- sac_ha_ads_fence_leads,
        sum(case when market = 'CN-ST' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2369F, --st_ha_ads_fence_leads
        sum(case when market = 'CN-FR' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2370F, -- fr_ha_ads_fence_leads,
        sum(case when market = 'CN-WA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2371F, --wa_ha_ads_fence_leads
        sum(case when market = 'CN-SJ' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2372F, --sj_ha_ads_fence_leads
        sum(case when market = 'CN-PA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2373F, --pa_ha_ads_fence_leads
        sum(case when market like '%CS-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2374F, -- nc_ha_ads_fence_leads,
        sum(case when market = 'CS-SV' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2375F, -- sv_ha_ads_fence_leads,
        sum(case when market = 'CS-OC' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2376F, -- oc_ha_ads_fence_leads,
        sum(case when market = 'CS-LA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2377F, -- la_ha_ads_fence_leads,
        sum(case when market = 'CS-VC' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2378F, -- vc_ha_ads_fence_leads,
        sum(case when market = 'CS-SD' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2379F, -- sd_ha_ads_fence_leads
        sum(case when market like '%TX-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2380F, -- tx_ha_ads_fence_leads,
        sum(case when market = 'TX-DL' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2381F, -- dl_ha_ads_fence_leads,
        sum(case when market = 'TX-FW' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2382F, -- fw_ha_ads_fence_leads,
        sum(case when market = 'TX-HT' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2383F, -- ht_ha_ads_fence_leads,
        sum(case when market = 'TX-SA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2384F, -- sa_ha_ads_fence_leads,
        sum(case when market = 'TX-AU' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2385F, -- au_ha_ads_fence_leads,
        sum(case when market like '%GA-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2386F, --ga_ha_ads_fence_leads,
        sum(case when market = 'GA-AT' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2387F, -- at_ha_ads_fence_leads,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2388F, -- ne_ha_ads_fence_leads,
        sum(case when market = 'MD-BL' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2389F, -- bl_ha_ads_fence_leads,
        sum(case when market = 'MD-DC' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2390F, -- dc_ha_ads_fence_leads,
        sum(case when market = 'PA-PH' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2391F, -- ph_ha_ads_fence_leads,
        sum(case when market = 'VA-AR' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2392F, -- ar_ha_ads_fence_leads,
        sum(case when market like '%FL-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2393F, -- fl_ha_ads_fence_leads,
        sum(case when market = 'FL-MI' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2394F, -- mi_ha_ads_fence_leads,
        sum(case when market = 'FL-OR' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2395F, -- or_ha_ads_fence_leads,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2459F, -- pa_wa_ha_ads_fence_leads,
        sum(case when market = 'WA-SE' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2460F, --se_ha_ads_fence_leads,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2523F, --wn_il_ha_ads_fence_leads
        sum(case when market = 'WN-CH' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2908F, --wn-ch_ha_ads_fence_leads
        sum(case when market = 'WN-NA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2967F, --wn-na_ha_ads_fence_leads
        sum(case when market = 'WN-LA' and channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR3026F, --wn-la_ha_ads_fence_leads
        -- Thumbtack Fence Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR644F,
        sum(case when market = 'CN-EB' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR350F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2472F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR361F,
        sum(case when market = 'CN-NB' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR372F,
        sum(case when market = 'CN-SA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR383F,
        sum(case when market = 'CN-FR' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR394F,
        sum(case when market like '%CS-%'  and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR480F,
        sum(case when market = 'CS-SV' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR544F,
        sum(case when market = 'TX-DL' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR722F,
        sum(case when market = 'CS-OC' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR555F,
        sum(case when market = 'CS-LA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR566F,
        sum(case when market = 'CS-VC' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR671F,
        sum(case when market like '%TX-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR769F,
        sum(case when market = 'TX-FW' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR739F,
        sum(case when market = 'CS-SD' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR756F,
        sum(case when market = 'CN-WA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR820F,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR821F,
        sum(case when market = 'CN-PA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR822F,
        sum(case when market = 'CN-ST' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR823F,
        sum(case when market like '%MD-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1196F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2028F,
        sum(case when market = 'MD-BL' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1126F,
        sum(case when market = 'MD-DC' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1167F,
        sum(case when market like '%PA-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1261F,
        sum(case when market = 'PA-PH' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1231F,
        sum(case when market like '%GA-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1298F,
        sum(case when market like '%VA-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1540F,
        sum(case when market = 'VA-AR' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1510F,
        sum(case when market like '%FL-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1584F,
        sum(case when market = 'FL-MI' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1620F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1668F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2278F,
        sum(case when market = 'WA-SE' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2236F,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2622F,
        sum(case when market = 'WN-CH' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2921F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR2980F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR3039F,
        -- Borg Fence Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR689F,
        sum(case when market = 'CN-EB' and channel = 'Paid/Borg' then 1 else 0 end) as MAR702F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Borg' then 1 else 0 end) as MAR2493F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Borg' then 1 else 0 end) as MAR690F,
        sum(case when market = 'CN-NB' and channel = 'Paid/Borg' then 1 else 0 end) as MAR703F,
        sum(case when market = 'CN-SA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR704F,
        sum(case when market = 'CN-FR' and channel = 'Paid/Borg' then 1 else 0 end) as MAR705F,
        sum(case when market = 'CN-WA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR824F,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Borg' then 1 else 0 end) as MAR825F,
        sum(case when market = 'CN-PA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR826F,
        sum(case when market = 'CN-ST' and channel = 'Paid/Borg' then 1 else 0 end) as MAR827F,
        sum(case when market like '%MD-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1219F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Borg' then 1 else 0 end) as MAR2051F,
        sum(case when market = 'MD-BL' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1135F,
        sum(case when market = 'MD-DC' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1176F,
        sum(case when market like '%PA-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1284F,
        sum(case when market = 'PA-PH' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1254F,
        sum(case when market like '%GA-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1321F,
        sum(case when market like '%VA-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1563F,
        sum(case when market = 'VA-AR' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1533F,
        sum(case when market like '%FL-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1607F,
        sum(case when market = 'FL-MI' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1643F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1691F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR2301F,
        sum(case when market = 'WA-SE' and channel = 'Paid/Borg' then 1 else 0 end) as MAR2260F,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR2885F,
        sum(case when market = 'WN-CH' and channel = 'Paid/Borg' then 1 else 0 end) as MAR2944F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR3003F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR3062F,
        -- Paid Google Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR651F,
        sum(case when market = 'CN-EB' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR351F,
        sum(case when market = 'CN-SF' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2473F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR362F,
        sum(case when market = 'CN-NB' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR373F,
        sum(case when market = 'CN-SA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR384F,
        sum(case when market = 'CN-FR' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR395F,
        sum(case when market like '%CS-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR481F,
        sum(case when market = 'CS-SV' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR545F,
        sum(case when market = 'CS-OC' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR556F,
        sum(case when market = 'CS-LA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR567F,
        sum(case when market = 'CS-VC' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR672F,
        sum(case when market like '%TX-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR770F,
        sum(case when market = 'TX-DL' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR723F,
        sum(case when market = 'TX-FW' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR740F,
        sum(case when market = 'CS-SD' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR757F,
        sum(case when market = 'CN-WA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR828F,
        sum(case when market = 'CN-SJ' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR829F,
        sum(case when market = 'CN-PA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR830F,
        sum(case when market = 'CN-ST' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR831F,
        sum(case when market like '%MD-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1203F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2035F,
        sum(case when market = 'MD-BL' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1127F,
        sum(case when market = 'MD-DC' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1168F,
        sum(case when market like '%PA-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1268F,
        sum(case when market = 'PA-PH' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1232F,
        sum(case when market like '%GA-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1305F,
        sum(case when market like '%VA-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1547F,
        sum(case when market = 'VA-AR' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1511F,
        sum(case when market like '%FL-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1591F,
        sum(case when market = 'FL-MI' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1621F,
        sum(case when market = 'FL-OR' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1669F,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2285F,
        sum(case when market = 'WA-SE' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2237F,
        sum(case when market_code like 'WN-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2839F,
        sum(case when market = 'WN-CH' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2922F,
        sum(case when market = 'WN-NA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR2981F,
        sum(case when market = 'WN-LA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR3040F,
        -- Facebook Fence Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR645F,
        sum(case when market = 'CN-EB' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR352F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2474F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Facebook' then 1 else 0 end) as MAR363F,
        sum(case when market = 'CN-NB' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR374F,
        sum(case when market = 'CN-SA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR385F,
        sum(case when market = 'CN-FR' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR396F,
        sum(case when market like '%CS-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR482F,
        sum(case when market = 'CS-SV' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR546F,
        sum(case when market = 'CS-OC' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR557F,
        sum(case when market = 'CS-LA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR568F,
        sum(case when market = 'CS-VC' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR673F,
        sum(case when market like '%TX-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR771F,
        sum(case when market = 'TX-DL' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR724F,
        sum(case when market = 'TX-FW' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR741F,
        sum(case when market = 'CS-SD' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR758F,
        sum(case when market = 'CN-WA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR832F,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR833F,
        sum(case when market = 'CN-PA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR834F,
        sum(case when market = 'CN-ST' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR835F,
        sum(case when market like '%MD-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1197F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2029F,
        sum(case when market = 'MD-BL' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1128F,
        sum(case when market = 'MD-DC' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1169F,
        sum(case when market like '%PA-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1262F,
        sum(case when market = 'PA-PH' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1233F,
        sum(case when market like '%GA-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1299F,
        sum(case when market like '%VA-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1541F,
        sum(case when market = 'VA-AR' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1512F,
        sum(case when market like '%FL-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1585F,
        sum(case when market = 'FL-MI' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1622F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1670F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2279F,
        sum(case when market = 'WA-SE' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2238F,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2653F,
        sum(case when market = 'WN-CH' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2923F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR2982F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR3041F,
        -- Paid Bark Fence Leads #added 18/08/2022
	sum(case when market like '%CN-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1323F, --NorCal
	sum(case when market = 'CN-EB' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1329F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2494F,
	sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Bark' then 1 else 0 end) as MAR1335F,
	sum(case when market = 'CN-NB' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1341F,
	sum(case when market = 'CN-SA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1347F,
	sum(case when market = 'CN-ST' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1353F,
	sum(case when market = 'CN-FR' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1359F,
	sum(case when market = 'CN-WA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1365F,
	sum(case when market = 'CN-SJ' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1371F,
	sum(case when market = 'CN-PA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1377F,
	sum(case when market like '%CS-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1383F, --SoCal
	sum(case when market = 'CS-SV' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1391F,
	sum(case when market = 'CS-OC' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1397F,
	sum(case when market = 'CS-LA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1403F,
	sum(case when market = 'CS-VC' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1409F,
	sum(case when market = 'CS-SD' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1415F,
        sum(case when market like '%TX-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1421F, --Texas
	sum(case when market = 'TX-DL' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1427F,
	sum(case when market = 'TX-FW' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1433F,
	sum(case when market = 'TX-HT' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1439F,
	sum(case when market = 'TX-SA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1445F,
	sum(case when market = 'TX-AU' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1451F,
        sum(case when market like '%GA-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1457F, --Georgia
	sum(case when market = 'GA-AT' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1463F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Bark' then 1 else 0 end) as MAR2053F, --North East
	sum(case when market like '%MD-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1469F, --Maryland
	sum(case when market = 'MD-BL' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1475F,
	sum(case when market = 'MD-DC' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1481F,
	sum(case when market like '%PA-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1487F, --Pennsylvania
	sum(case when market = 'PA-PH' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1493F,
        sum(case when market like '%VA-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1566F, --Virginia
	sum(case when market = 'VA-AR' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1572F,
        sum(case when market like '%FL-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1653F, --Florida
        sum(case when market = 'FL-MI' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1655F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Bark' then 1 else 0 end) as MAR1693F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2307F, --Washington
        sum(case when market = 'WA-SE' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2263F,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2504F, --Illinois
        sum(case when market = 'WN-CH' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2887F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR2946F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Bark' then 1 else 0 end) as MAR3005F,
        --Paid Nextdoor Fence Leads #added 18/08/2022
	sum(case when market like '%CN-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1324F,
	sum(case when market = 'CN-EB' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1330F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2495F,
	sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1336F,
	sum(case when market = 'CN-NB' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1342F,
	sum(case when market = 'CN-SA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1348F,
	sum(case when market = 'CN-ST' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1354F,
	sum(case when market = 'CN-FR' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1360F,
	sum(case when market = 'CN-WA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1366F,
	sum(case when market = 'CN-SJ' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1372F,
	sum(case when market = 'CN-PA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1378F,
	sum(case when market like '%CS-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1384F,
	sum(case when market = 'CS-SV' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1392F,
	sum(case when market = 'CS-OC' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1398F,
	sum(case when market = 'CS-LA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1404F,
	sum(case when market = 'CS-VC' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1410F,
	sum(case when market = 'CS-SD' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1416F,
	sum(case when market like '%TX-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1422F,
	sum(case when market = 'TX-DL' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1428F,
	sum(case when market = 'TX-FW' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1434F,
	sum(case when market = 'TX-HT' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1440F,
	sum(case when market = 'TX-SA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1446F,
	sum(case when market = 'TX-AU' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1452F,
	sum(case when market like '%GA-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1458F,
	sum(case when market = 'GA-AT' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1464F,
	sum(case when market like '%MD-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1470F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2054F,
	sum(case when market = 'MD-BL' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1476F,
	sum(case when market = 'MD-DC' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1482F,
	sum(case when market like '%PA-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1488F,
	sum(case when market = 'PA-PH' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1494F,
        sum(case when market like '%VA-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1567F,
        sum(case when market = 'VA-AR' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1573F,
        sum(case when market like '%FL-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1654F,
        sum(case when market = 'FL-MI' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1656F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR1694F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2308F,
        sum(case when market = 'WA-SE' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2264F,
        sum(case when market like 'WN-%' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2505F,
        sum(case when market = 'WN-CH' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2888F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR2947F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR3006F,
        -- Paid Misc Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR646F,
        sum(case when market = 'CN-EB' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR353F,
        sum(case when market = 'CN-SF' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2475F,
        sum(case when  market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR364F,
        sum(case when market = 'CN-NB' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR375F,
        sum(case when market = 'CN-SA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR386F,
        sum(case when market = 'CN-FR' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR397F,
        sum(case when market like '%CS-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR483F,
        sum(case when market = 'CS-SV' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR547F,
        sum(case when market = 'CS-OC' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR558F,
        sum(case when market = 'CS-LA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR569F,
        sum(case when market = 'CS-VC' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR674F,
        sum(case when market like '%TX-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR772F,
        sum(case when market = 'TX-DL' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR725F,
        sum(case when market = 'TX-FW' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR742F,
        sum(case when market = 'CS-SD' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR759F,
        sum(case when market = 'CN-WA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR836F,
        sum(case when market = 'CN-SJ' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR837F,
        sum(case when market = 'CN-PA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR838F,
        sum(case when market = 'CN-ST' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR839F,
        sum(case when market like '%MD-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1198F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2030F,
        sum(case when market = 'MD-BL' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1129F,
        sum(case when market = 'MD-DC' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1170F,
        sum(case when market like '%PA-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1263F,
        sum(case when market = 'PA-PH' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1234F,
        sum(case when market like '%GA-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1300F,
        sum(case when market like '%VA-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1542F,
        sum(case when market = 'VA-AR' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1513F,
        sum(case when market like '%FL-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1586F,
        sum(case when market = 'FL-MI' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1623F,
        sum(case when market = 'FL-OR' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1671F,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2280F,
        sum(case when market = 'WA-SE' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2239F,
        sum(case when market_code like 'WN-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2684F, --Illinois
        sum(case when market = 'WN-CH' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2924F,
        sum(case when market = 'WN-NA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR2983F,
        sum(case when market = 'WN-LA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR3042F,
        -- Yelp Fence Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR647F,
        sum(case when market = 'CN-EB' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR354F,
        sum(case when market = 'CN-SF' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2476F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Yelp' then 1 else 0 end) as MAR365F,
        sum(case when market = 'CN-NB' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR376F,
        sum(case when market = 'CN-SA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR387F,
        sum(case when market = 'CN-FR' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR398F,
        sum(case when market like '%CS-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR484F,
        sum(case when market = 'CS-SV' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR548F,
        sum(case when market = 'CS-OC' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR559F,
        sum(case when market = 'CS-LA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR570F,
        sum(case when market = 'CS-VC' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR675F,
        sum(case when market like '%TX-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR773F,
        sum(case when market = 'TX-DL' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR727F,
        sum(case when market = 'TX-FW' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR743F,
        sum(case when market = 'CS-SD' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR761F,
        sum(case when market = 'CN-WA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR840F,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR841F,
        sum(case when market = 'CN-PA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR842F,
        sum(case when market = 'CN-ST' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR843F,
        sum(case when market like '%MD-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1199F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2031F,
        sum(case when market = 'MD-BL' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1130F,
        sum(case when market = 'MD-DC' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1171F,
        sum(case when market like '%PA-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1264F,
        sum(case when market = 'PA-PH' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1235F,
        sum(case when market like '%GA-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1301F,
        sum(case when market like '%VA-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1543F,
        sum(case when market = 'VA-AR' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1514F,
        sum(case when market like '%FL-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1587F,
        sum(case when market = 'FL-MI' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1624F,
        sum(case when market = 'FL-OR' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1672F,
        sum(case when market_code like 'PA-WA-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2281F,
        sum(case when market = 'WA-SE' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2240F,
        sum(case when market_code like 'WN-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2715F, --Illinois
        sum(case when market = 'WN-CH' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2925F,
        sum(case when market = 'WN-NA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR2984F,
        sum(case when market = 'WN-LA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR3043F,
        -- Non Paid GMB Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR648F,
        sum(case when market = 'CN-EB' and is_commercial=0 and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR355F,
        sum(case when market = 'CN-SF' and is_commercial=0 and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2477F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR366F,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR377F,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR388F,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR399F,
        sum(case when market like '%CS-%'  and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR485F,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR549F,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR560F,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR571F,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR676F,
        sum(case when market like '%TX-%'  and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR774F,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR728F,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR744F,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR762F,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR844F,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR845F,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR846F,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR847F,
        sum(case when market like '%MD-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1200F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2032F,
        sum(case when market = 'MD-BL' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1131F,
        sum(case when market = 'MD-DC' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1172F,
        sum(case when market like '%PA-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1265F,
        sum(case when market = 'PA-PH' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1236F,
        sum(case when market like '%GA-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1302F,
        sum(case when market like '%VA-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1544F,
        sum(case when market = 'VA-AR' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1515F,
        sum(case when market like '%FL-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1588F,
        sum(case when market = 'FL-MI' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1625F,
        sum(case when market = 'FL-OR' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1673F,
        sum(case when market like '%PA-WA-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2282F,
        sum(case when market = 'WA-SE' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2241F,
        sum(case when market_code like 'WN-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2746F, --Illinois
        sum(case when market = 'WN-CH' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2926F,
        sum(case when market = 'WN-NA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR2985F,
        sum(case when market = 'WN-LA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR3044F,
        -- Non Paid SEO Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR649F,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR356F,
        sum(case when market = 'CN-SF' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2478F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR367F,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR378F,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR389F,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR400F,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR486F,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR550F,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR561F,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR572F,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR677F,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR775F,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR729F,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR745F,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR763F,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR848F,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR849F,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR850F,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR851F,
        sum(case when market like '%MD-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1201F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2033F,
        sum(case when market = 'MD-BL' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1132F,
        sum(case when market = 'MD-DC' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1173F,
        sum(case when market like '%PA-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1266F,
        sum(case when market = 'PA-PH' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1237F,
        sum(case when market like '%GA-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1303F,
        sum(case when market like '%VA-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1545F,
        sum(case when market = 'VA-AR' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1516F,
        sum(case when market like '%FL-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1589F,
        sum(case when market = 'FL-MI' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1626F,
        sum(case when market = 'FL-OR' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1674F,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2283F,
        sum(case when market = 'WA-SE' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2242F,
        sum(case when market_code like 'WN-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2777F, --Illinois
        sum(case when market = 'WN-CH' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2927F,
        sum(case when market = 'WN-NA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR2986F,
        sum(case when market = 'WN-LA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR3045F,
        -- Non Paid Direct Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2063F,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2064F,
        sum(case when market = 'CN-SF' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2500F,
        --sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR367F,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2065F,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2066F,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2068F,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2069F,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2070F,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2071F,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2067F,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2072F,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2074F,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2075F,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2076F,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2077F,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2078F,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2079F,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2080F,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2081F,
        sum(case when market = 'TX-HT' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2082F,
        sum(case when market = 'TX-SA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2083F,
        sum(case when market = 'TX-AU' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2084F,
        sum(case when market like '%GA-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2085F,
        sum(case when market = 'GA-AT' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2086F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2087F,
        sum(case when market = 'MD-BL' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2088F,
        sum(case when market = 'MD-DC' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2089F,
        sum(case when market = 'PA-PH' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2090F,
        sum(case when market = 'VA-AR' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2091F,
        sum(case when market like '%FL-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2092F,
        sum(case when market = 'FL-MI' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2093F,
        sum(case when market = 'FL-OR' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2094F,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2309F,
        sum(case when market = 'WA-SE' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2271F,
        sum(case when market_code like 'WN-%' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2510F, --Illinois
        sum(case when market = 'WN-CH' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2905F,
        sum(case when market = 'WN-NA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2964F,
        sum(case when market = 'WN-LA' and channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR3023F,
        -- Non Paid Misc Fence Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR650F,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR357F,
        sum(case when market = 'CN-SF' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2479F,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR368F,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR379F,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR390F,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR401F,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR487F,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR551F,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR562F,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR573F,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR678F,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR776F,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR730F,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR746F,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR764F,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR852F,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR853F,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR854F,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR855F,
        sum(case when market like '%MD-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1202F,
        sum(case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2034F,
        sum(case when market = 'MD-BL' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1133F,
        sum(case when market = 'MD-DC' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1174F,
        sum(case when market like '%PA-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1267F,
        sum(case when market = 'PA-PH' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1238F,
        sum(case when market like '%GA-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1304F,
        sum(case when market like '%VA-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1546F,
        sum(case when market = 'VA-AR' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1517F,
        sum(case when market like '%FL-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1590F,
        sum(case when market = 'FL-MI' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1627F,
        sum(case when market = 'FL-OR' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1675F,
        sum(case when market = 'GA-AT' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR955F,
        sum(case when market = 'GA-AT' and channel = 'Paid/Borg' then 1 else 0 end) as MAR957F,
        sum(case when market = 'GA-AT' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR956F,
        sum(case when market = 'GA-AT' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR958F,
        sum(case when market = 'GA-AT' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR959F,
        sum(case when market = 'GA-AT' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR960F,
        sum(case when market = 'GA-AT' and is_commercial=0 and channel ilike 'Non Paid%' then 1 else 0 end) as MAR961F, -- at_nonpaid_fence_leads,
        sum(case when market = 'GA-AT' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR962F,
        sum(case when market = 'GA-AT' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR963F,
        sum(case when market = 'GA-AT' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR964F,
        sum(case when market = 'GA-AT' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR965F,
        sum(case when market = 'GA-AT' and is_fence = 1 and is_commercial=0 then 1 else 0 end) as MAR953F,
        sum(case when market = 'GA-AT' and is_commercial=0 and channel ilike 'Paid%' then 1 else 0 end) as MAR954F,
        sum(case when market = 'TX-HT' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1000F,
        sum(case when market = 'TX-HT' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1002F,
        sum(case when market = 'TX-HT' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1001F,
        sum(case when market = 'TX-HT' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1003F,
        sum(case when market = 'TX-HT' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1004F,
        sum(case when market = 'TX-HT' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1005F,
        sum(case when market = 'TX-HT' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1006F, -- ht_nonpaid_fence_leads,
        sum(case when market = 'TX-HT' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1007F,
        sum(case when market = 'TX-HT' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1008F,
        sum(case when market = 'TX-HT' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1009F,
        sum(case when market = 'TX-HT' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1010F,
        sum(case when market = 'TX-HT' then 1 else 0 end) as MAR998F,
        sum(case when market = 'TX-HT' and channel ilike 'Paid%' then 1 else 0 end) as MAR999F,
        sum(case when market = 'TX-AU' then 1 else 0 end) as MAR1098F,
        sum(case when market = 'TX-AU' and channel ilike 'Paid%' then 1 else 0 end) as MAR1099F,
        sum(case when market = 'TX-AU' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1100F,
        sum(case when market = 'TX-AU' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1101F,
        sum(case when market = 'TX-AU' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1102F,
        sum(case when market = 'TX-AU' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1070F,
        sum(case when market = 'TX-AU' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1071F,
        sum(case when market = 'TX-AU' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1072F,
        sum(case when market = 'TX-AU' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1073F, -- ht_nonpaid_fence_leads,
        sum(case when market = 'TX-AU' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1074F,
        sum(case when market = 'TX-AU' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1075F,
        sum(case when market = 'TX-AU' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1076F,
        sum(case when market = 'TX-AU' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1077F,
        sum(case when market = 'TX-SA' and channel ilike 'Paid%' then 1 else 0 end) as MAR1028F,
        sum(case when market = 'TX-SA' and channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR1029F,
        sum(case when market = 'TX-SA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR1031F,
        sum(case when market = 'TX-SA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR1030F,
        sum(case when market = 'TX-SA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR1032F,
        sum(case when market = 'TX-SA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR1033F,
        sum(case when market = 'TX-SA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR1034F,
        sum(case when market = 'TX-SA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR1035F, -- ht_nonpaid_fence_leads,
        sum(case when market = 'TX-SA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR1036F,
        sum(case when market = 'TX-SA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR1037F,
        sum(case when market = 'TX-SA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR1038F,
        sum(case when market = 'TX-SA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR1039F,
        sum(case when market_code like 'PA-WA-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2284F,
        sum(case when market = 'WA-SE' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2243F,
        sum(case when market_code like 'WN-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2808F, --Illinois
        sum(case when market = 'WN-CH' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2928F,
        sum(case when market = 'WN-NA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR2987F,
        sum(case when market = 'WN-LA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR3046F
from calc_leads_detailed
where
        is_fence = 1
        and is_commercial = 0
group by 1
order by 1 desc),
commercial_leads as
(select
        date,
        count(*) as MAR101C, --commercial leads
           -- Leads by Market
        sum(case when market like '%CN-%' then 1 else 0 end) as MAR640C,
        sum(case when market = 'CN-EB' then 1 else 0 end) as MAR165C,
        sum(case when market is null then 1 else 0 end) as MAR170C,
        sum(case when market = 'CN-NB' then 1 else 0 end) as MAR168C,
        sum(case when market = 'CN-SA' then 1 else 0 end) as MAR166C,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA') then 1 else 0 end) as MAR167C,
        sum(case when market = 'CN-SF' then 1 else 0 end) as MAR169C,
        sum(case when market = 'CN-FR' then 1 else 0 end) as MAR193C,
        sum(case when market = 'CN-WA' then 1 else 0 end) as MAR804C,
        sum(case when market = 'CN-SJ' then 1 else 0 end) as MAR805C,
        sum(case when market = 'CN-PA' then 1 else 0 end) as MAR806C,
        sum(case when market = 'CN-ST' then 1 else 0 end) as MAR807C,
        sum(case when market like '%MD-%' then 1 else 0 end) as MAR1193C,
        sum(case when market = 'MD-BL' then 1 else 0 end) as MAR1113C,
        sum(case when market = 'MD-DC' then 1 else 0 end) as MAR1154C,
        sum(case when market like '%PA-%' then 1 else 0 end) as MAR1258C,
        sum(case when market = 'PA-PH' then 1 else 0 end) as MAR1220C,
        sum(case when market like '%GA-%' then 1 else 0 end) as MAR1295C,
        sum(case when market like '%VA-%' then 1 else 0 end) as MAR1537C,
        sum(case when market = 'VA-AR' then 1 else 0 end) as MAR1499C,
        sum(case when market like '%FL-%' then 1 else 0 end) as MAR1579C,
        sum(case when market = 'FL-MI' then 1 else 0 end) as MAR1609C,
        sum(case when market = 'FL-OR' then 1 else 0 end) as MAR1657C,
        sum(case when market_code like 'PA-WA-%' then 1 else 0 end) as MAR2273C,
        sum(case when market = 'WA-SE' then 1 else 0 end) as MAR2225C,
        sum(case when market_code like 'WN-%' then 1 else 0 end) as MAR2528C, --Illinois
        sum(case when market = 'WN-CH' then 1 else 0 end) as MAR2893C,
        sum(case when market = 'WN-NA' then 1 else 0 end) as MAR2952C,
        sum(case when market = 'WN-LA' then 1 else 0 end) as MAR3011C,
        -- Leads by Channel
        -- Paid
        sum(case when channel ilike 'Paid%' then 1 else 0 end) as MAR469C,
        sum(case when channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR328C,
        sum(case when channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR135C,
        sum(case when channel = 'Paid/Borg' then 1 else 0 end) as MAR682C,
        sum(case when channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR335C,
        sum(case when channel = 'Paid/Facebook' then 1 else 0 end) as MAR121C,
        sum(case when channel ilike 'Paid/Google%' then 1 else 0 end) as MAR158C,
        sum(case when channel ilike 'Paid/Dodge' then 1 else 0 end) as MAR802C,
        sum(case when channel = 'Paid/Bark' then 1 else 0 end) as MAR1286C,
        sum(case when channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR235C,
        sum(case when channel = 'Paid/Yelp' then 1 else 0 end) as MAR131C,
        --Non Paid
        sum(case when channel ilike 'Non Paid%' then 1 else 0 end) as MAR125C,
        sum(case when channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR470C,
        sum(case when channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR471C,
        sum(case when channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2059C,
        sum(case when channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR472C
from calc_leads_detailed
where is_commercial = 1
group by 1
order by 1 desc),
driveway_leads_by_market_and_channel as
(select
        date,
        -- Paid Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR641D, -- sc_paid_driveway_leads,
        sum(case when market = 'CN-EB' and channel ilike 'Paid%' then 1 else 0 end) as MAR347D, -- eb_paid_driveway_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid%' then 1 else 0 end) as MAR358D, -- sbsf_paid_driveway_leads,
        sum(case when market = 'CN-NB' and channel ilike 'Paid%' then 1 else 0 end) as MAR369D, -- nb_paid_driveway_leads,
        sum(case when market = 'CN-SA' and channel ilike 'Paid%' then 1 else 0 end) as MAR380D, -- sac_paid_driveway_leads,
        sum(case when market = 'CN-FR' and channel ilike 'Paid%' then 1 else 0 end) as MAR391D, -- fr_paid_driveway_leads,
        sum(case when market like '%CS-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR477D, -- sc_paid_driveway_leads,
        sum(case when market = 'CS-SV' and channel ilike 'Paid%' then 1 else 0 end) as MAR541D, -- sv_paid_driveway_leads,
        sum(case when market = 'CS-OC' and channel ilike 'Paid%' then 1 else 0 end) as MAR552D, -- oc_paid_driveway_leads,
        sum(case when market = 'CS-LA' and channel ilike 'Paid%' then 1 else 0 end) as MAR563D, -- la_paid_driveway_leads,
        sum(case when market = 'CS-VC' and channel ilike 'Paid%' then 1 else 0 end) as MAR668D, -- vc_paid_driveway_leads,
        sum(case when market = 'CS-SD' and channel ilike 'Paid%' then 1 else 0 end) as MAR754D, --sd_paid_driveway_leads
        sum(case when market like '%TX-%' and channel ilike 'Paid%' then 1 else 0 end) as MAR766D, -- tx_paid_driveway_leads,
        sum(case when market = 'TX-DL' and channel ilike 'Paid%' then 1 else 0 end) as MAR720D, --dl_paid_driveway_leads
        sum(case when market = 'TX-FW' and channel ilike 'Paid%' then 1 else 0 end) as MAR736D, --fw_paid_driveway_leads
        sum(case when market = 'CN-WA' and channel ilike 'Paid%' then 1 else 0 end) as MAR808D, --wa_paid_driveway_leads
        sum(case when market = 'CN-SJ' and channel ilike 'Paid%' then 1 else 0 end) as MAR809D, --sj_paid_driveway_leads
        sum(case when market = 'CN-PA' and channel ilike 'Paid%' then 1 else 0 end) as MAR810D, --pa_paid_driveway_leads
        sum(case when market = 'CN-ST' and channel ilike 'Paid%' then 1 else 0 end) as MAR811D, --st_paid_driveway_leads
     -- Non Paid Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR642D, -- sc_nonpaid_driveway_leads,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR348D, -- eb_nonpaid_driveway_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%' then 1 else 0 end) as MAR359D, -- sbsf_nonpaid_driveway_leads,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR370D, -- nb_nonpaid_driveway_leads,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR381D, -- sac_nonpaid_driveway_leads,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR392D, -- fr_nonpaid_driveway_leads,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR478D, -- sc_nonpaid_driveway_leads,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR542D, -- sv_nonpaid_driveway_leads,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR553D, -- oc_nonpaid_driveway_leads,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR564D, -- la_nonpaid_driveway_leads,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR669D, -- vc_nonpaid_driveway_leads,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR767D, -- tx_nonpaid_driveway_leads,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR726D, -- dl_nonpaid_driveway_leads,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR737D, -- fw_nonpaid_driveway_leads,
        sum(case when market = 'SD-SD' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR760D, -- sd_nonpaid_driveway_leads,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR812D, --wa_Non Paid_driveway_leads
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR813D, --sj_Non Paid_driveway_leads
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR814D, --pa_Non Paid_driveway_leads
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%' then 1 else 0 end) as MAR815D, --st_Non Paid_driveway_leads
        -- HomeAdvisor Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR643D, -- sc_ha_driveway_leads,
        sum(case when market = 'CN-EB' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR349D, -- eb_ha_driveway_leads,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR360D, -- sbsf_ha_driveway_leads,
        sum(case when market = 'CN-NB' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR371D, -- nb_ha_driveway_leads,
        sum(case when market like '%CS-%' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR479D, -- sc_ha_driveway_leads,
        sum(case when market = 'CS-SV' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR543D, -- sv_ha_driveway_leads,
        sum(case when market = 'CS-OC' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR554D, -- oc_ha_driveway_leads,
        sum(case when market = 'CS-LA' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR565D, -- la_ha_driveway_leads,
        sum(case when market = 'CS-VC' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR670D, -- vc_ha_driveway_leads,
        sum(case when market like '%TX-%' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR768D, -- tx_ha_driveway_leads,
        sum(case when market = 'TX-DL' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR721D, -- dl_ha_driveway_leads,
        sum(case when market = 'TX-FW' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR738D, -- fw_ha_driveway_leads,
        sum(case when market = 'CS-SD' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR755D, -- sd_ha_driveway_leads
        sum(case when market = 'CN-WA' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR816D, --wa_ha_driveway_leads
        sum(case when market = 'CN-SJ' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR817D, --sj_ha_driveway_leads
        sum(case when market = 'CN-PA' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR818D, --pa_ha_driveway_leads
        sum(case when market = 'CN-ST' and channel ilike 'Paid/Home Advisor%' then 1 else 0 end) as MAR819D, --st_ha_driveway_leads
        -- Thumbtack Driveway Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR644D,
        sum(case when market = 'CN-EB' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR350D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR361D,
        sum(case when market = 'CN-NB' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR372D,
        sum(case when market = 'CN-SA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR383D,
        sum(case when market = 'CN-FR' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR394D,
        sum(case when market like '%CS-%'  and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR480D,
        sum(case when market = 'CS-SV' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR544D,
        sum(case when market = 'TX-DL' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR722D,
        sum(case when market = 'CS-OC' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR555D,
        sum(case when market = 'CS-LA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR566D,
        sum(case when market = 'CS-VC' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR671D,
        sum(case when market like '%TX-%' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR769D,
        sum(case when market = 'TX-FW' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR739D,
        sum(case when market = 'CS-SD' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR756D,
        sum(case when market = 'CN-WA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR820D,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR821D,
        sum(case when market = 'CN-PA' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR822D,
        sum(case when market = 'CN-ST' and channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR823D,
               -- Borg Driveway Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Borg' then 1 else 0 end) as MAR689D,
        sum(case when market = 'CN-EB' and channel = 'Paid/Borg' then 1 else 0 end) as MAR702D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Borg' then 1 else 0 end) as MAR690D,
        sum(case when market = 'CN-NB' and channel = 'Paid/Borg' then 1 else 0 end) as MAR703D,
        sum(case when market = 'CN-SA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR704D,
        sum(case when market = 'CN-FR' and channel = 'Paid/Borg' then 1 else 0 end) as MAR705D,
        sum(case when market = 'CN-WA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR824D,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Borg' then 1 else 0 end) as MAR825D,
        sum(case when market = 'CN-PA' and channel = 'Paid/Borg' then 1 else 0 end) as MAR826D,
        sum(case when market = 'CN-ST' and channel = 'Paid/Borg' then 1 else 0 end) as MAR827D,
        -- Paid Google Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR651D,
        sum(case when market = 'CN-EB' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR351D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR362D,
        sum(case when market = 'CN-NB' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR373D,
        sum(case when market = 'CN-SA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR384D,
        sum(case when market = 'CN-FR' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR395D,
        sum(case when market like '%CS-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR481D,
        sum(case when market = 'CS-SV' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR545D,
        sum(case when market = 'CS-OC' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR556D,
        sum(case when market = 'CS-LA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR567D,
        sum(case when market = 'CS-VC' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR672D,
        sum(case when market like '%TX-%' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR770D,
        sum(case when market = 'TX-DL' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR723D,
        sum(case when market = 'TX-FW' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR740D,
        sum(case when market = 'CS-SD' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR757D,
        sum(case when market = 'CN-WA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR828D,
        sum(case when market = 'CN-SJ' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR829D,
        sum(case when market = 'CN-PA' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR830D,
        sum(case when market = 'CN-ST' and channel ilike 'Paid/Google%' then 1 else 0 end) as MAR831D,
        -- Facebook Driveway Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR645D,
        sum(case when market = 'CN-EB' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR352D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Facebook' then 1 else 0 end) as MAR363D,
        sum(case when market = 'CN-NB' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR374D,
        sum(case when market = 'CN-SA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR385D,
        sum(case when market = 'CN-FR' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR396D,
        sum(case when market like '%CS-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR482D,
        sum(case when market = 'CS-SV' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR546D,
        sum(case when market = 'CS-OC' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR557D,
        sum(case when market = 'CS-LA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR568D,
        sum(case when market = 'CS-VC' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR673D,
        sum(case when market like '%TX-%' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR771D,
        sum(case when market = 'TX-DL' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR724D,
        sum(case when market = 'TX-FW' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR741D,
        sum(case when market = 'CS-SD' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR758D,
        sum(case when market = 'CN-WA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR832D,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR833D,
        sum(case when market = 'CN-PA' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR834D,
        sum(case when market = 'CN-ST' and channel = 'Paid/Facebook' then 1 else 0 end) as MAR835D,
        -- Paid Misc Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR646D,
        sum(case when market = 'CN-EB' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR353D,
        sum(case when  market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR364D,
        sum(case when market = 'CN-NB' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR375D,
        sum(case when market = 'CN-SA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR386D,
        sum(case when market = 'CN-FR' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR397D,
        sum(case when market like '%CS-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR483D,
        sum(case when market = 'CS-SV' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR547D,
        sum(case when market = 'CS-OC' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR558D,
        sum(case when market = 'CS-LA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR569D,
        sum(case when market = 'CS-VC' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR674D,
        sum(case when market like '%TX-%' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR772D,
        sum(case when market = 'TX-DL' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR725D,
        sum(case when market = 'TX-FW' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR742D,
        sum(case when market = 'CS-SD' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR759D,
        sum(case when market = 'CN-WA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR836D,
        sum(case when market = 'CN-SJ' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR837D,
        sum(case when market = 'CN-PA' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR838D,
        sum(case when market = 'CN-ST' and channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR839D,
        -- Yelp Driveway Leads
        sum(case when market like '%CN-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR647D,
        sum(case when market = 'CN-EB' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR354D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel = 'Paid/Yelp' then 1 else 0 end) as MAR365D,
        sum(case when market = 'CN-NB' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR376D,
        sum(case when market like '%CS-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR484D,
        sum(case when market = 'CS-SV' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR548D,
        sum(case when market = 'CS-OC' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR559D,
        sum(case when market = 'CS-LA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR570D,
        sum(case when market = 'CS-VC' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR675D,
        sum(case when market like '%TX-%' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR773D,
        sum(case when market = 'TX-DL' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR727D,
        sum(case when market = 'TX-FW' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR743D,
        sum(case when market = 'CS-SD' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR761D,
        sum(case when market = 'CN-WA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR840D,
        sum(case when market = 'CN-SJ' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR841D,
        sum(case when market = 'CN-PA' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR842D,
        sum(case when market = 'CN-ST' and channel = 'Paid/Yelp' then 1 else 0 end) as MAR843D,
        -- Non Paid GMB Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR648D,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR355D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR366D,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR377D,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR388D,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR399D,
        sum(case when market like '%CS-%'  and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR485D,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR549D,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR560D,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR571D,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR676D,
        sum(case when market like '%TX-%'  and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR774D,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR728D,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR744D,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR762D,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR844D,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR845D,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR846D,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR847D,
        -- Non Paid SEO Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR649D,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR356D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR367D,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR378D,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR389D,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR400D,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR486D,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR550D,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR561D,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR572D,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR677D,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR775D,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR729D,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR745D,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR763D,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR848D,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR849D,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR850D,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR851D,
        -- Non Paid Misc Driveway Leads
        sum(case when market like '%CN-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR650D,
        sum(case when market = 'CN-EB' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR357D,
        sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR368D,
        sum(case when market = 'CN-NB' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR379D,
        sum(case when market = 'CN-SA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR390D,
        sum(case when market = 'CN-FR' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR401D,
        sum(case when market like '%CS-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR487D,
        sum(case when market = 'CS-SV' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR551D,
        sum(case when market = 'CS-OC' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR562D,
        sum(case when market = 'CS-LA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR573D,
        sum(case when market = 'CS-VC' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR678D,
        sum(case when market like '%TX-%' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR776D,
        sum(case when market = 'TX-DL' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR730D,
        sum(case when market = 'TX-FW' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR746D,
        sum(case when market = 'CS-SD' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR764D,
        sum(case when market = 'CN-WA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR852D,
        sum(case when market = 'CN-SJ' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR853D,
        sum(case when market = 'CN-PA' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR854D,
        sum(case when market = 'CN-ST' and channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR855D
 from calc_leads_detailed
where
        is_driveway = 1
        and is_commercial = 0
group by 1
order by 1 desc
)
select * from leads l
left join leads_by_market_and_channel using(date)
left join driveway_leads_by_market_and_channel using(date)
left join commercial_leads using (date);
