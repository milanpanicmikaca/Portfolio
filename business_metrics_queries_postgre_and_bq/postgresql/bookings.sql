-- upload to BQ
with calc_returning_customers
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
                is_commercial::integer, --ADDED
                case when crc.rank_order_customer > 1 then 'Non Paid/Returning Customers'
                         when la.channel <> 'Non Paid/Misc/Unknown' then la.channel
                         else la2.channel end as channel_attributed,
                case when crc.rank_order_customer > 1 then cul.id
                         when la.channel <> 'Non Paid/Misc/Unknown' then col.id
                         else cul.id end as final_lead_id,
                case when so.parent_order_id is not null then 1 else 0 end as is_warranty_order
        from store_order so
        left join core_house h on h.id = so.house_id
        left join customers_customer cc on cc.id = h.customer_id
        left join calc_core_lead_id col on col.order_id = so.id
        left join calc_customer_lead_id cul on cul.order_id = so.id
        left join lead_attribution la on la.id = col.id
        left join lead_attribution la2 on la2.id = cul.id
        left join calc_returning_customers crc on crc.order_id = col.order_id and crc.rank_order_customer > 1
        order by order_id desc
),
calc_booking as
(
        select
                ssa.*,
                ga.formatted_address,
                case when o.product_id = 105 then 'Fence' when o.product_id = 34 then 'Driveway'
                when product_id = 132 then 'Turf' else 'Fence' end as product_name,
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
                rank() over (partition by ga.formatted_address, o.product_id order by ssa.created_at desc) as rank
        from schedule_appointment ssa
        left join store_order o on o.id = ssa.order_id
        left join core_house h on h.id = o.house_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_county gcn on gcn.id = ga.county_id
        left join product_countymarket pcm on pcm.county_id = gcn.id
        left join product_market pm on pm.id = pcm.market_id
        left join schedule_appointmenttype ssat on ssat.id = ssa.appointment_type_id
        where
                ssa.cancelled_at is null
                and ssat.code in ('remote_onsite', 'physical_onsite')
                and ssa.created_at <= now()
),
timeseries as
(
        select
        dd date
        from generate_series ('2018-04-16'::timestamp, current_date, '1 day'::interval) dd
),
calc_last_booking as
(
        select
                cb.*,
                date_trunc('{period}', t.date)::date as date_grouping,
                cl.channel_attributed as channel,
                is_commercial,
                is_warranty_order,
                extract(day from age(cb.date + cb.time_start at time zone 'America/Los_Angeles', cb.created_at at time zone 'America/Los_Angeles'))*24
                + extract(hour from age(cb.date + cb.time_start at time zone 'America/Los_Angeles', cb.created_at at time zone 'America/Los_Angeles'))*1
                + extract(minute from age(cb.date + cb.time_start at time zone 'America/Los_Angeles', cb.created_at at time zone 'America/Los_Angeles'))/60 as booked2onsite_hrs,
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
                          else 'OTHER'
                  end as service_category
        from timeseries t
        left join calc_booking cb on cb.created_at::date = t.date
        left join calc_lead cl on cl.order_id = cb.order_id
        left join core_lead l on cl.final_lead_id = l.id
        left join min_lead_service ml on l.id = ml.lead_id
        left join core_lead_services cls on cls.id = ml.first_lead_service
        left join product_service ps on ps.id = cls.service_id
        left join customers_contact co on co.id = l.contact_id
        left join core_user cu on cu.id = co.user_id
        where
                coalesce(l.full_name,'')||coalesce(co.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
                and coalesce(l.email,'')||coalesce(cu.email,'') not ilike '%+test%'
                and cb.rank = 1
),
includes_commercial
as
(
select
        date_grouping as date,
        count(*) as MAR108, --bookings
    avg(booked2onsite_hrs)/24 as SAL125, --avg_booked2onsite
 -- Bookings by Market
    count(market) filter (where market like '%CN-%') as MAR639, --NorCal_bookings
    count(market) filter (where market like '%TX-%') as MAR943, --tx_bookings
    count(market) filter (where market = 'CN-EB') as MAR171, --east_bay_bookings
    count(market) filter (where market = 'CN-SA') as MAR172, --sacramento_bookings
    count(market) filter (where market in ('CN-WA','CN-SJ','CN-PA')) as MAR173, --south_bay_bookings
    count(market) filter (where market = 'CN-SF') as MAR175, --san_francisco_bookings
    count(market) filter (where market = 'CN-NB') as MAR174, --wine_country_bookings
    count(market) filter (where market = 'CN-FR') as MAR192, --fresno_bookings
    count(market) filter (where market = 'TX-DL') as MAR718, --dallas_bookings
    count(market) filter (where market = 'TX-SA') as MAR1059, --sa_bookings
    count(market) filter (where market = 'CS-SD') as MAR752, --sandiego_bookings
    count(market) filter (where market like '%CS-%') as MAR498, --SoCal_bookings
    count(market) filter (where market = 'CS-SV') as MAR607, --sv_bookings
    count(market) filter (where market = 'CS-OC') as MAR608, --oc_bookings
    count(market) filter (where market = 'CS-LA') as MAR609, --la_bookings
    count(market) filter (where market = 'CS-VC') as MAR679, --vc_bookings
    count(market) filter (where market = 'CN-WA') as MAR856, --wa_bookings
    count(market) filter (where market = 'CN-SJ') as MAR857, --sj_bookings
    count(market) filter (where market = 'CN-PA') as MAR858, --pa_bookings
    count(market) filter (where market = 'CN-ST') as MAR859, --st_bookings
    count(market) filter (where market is null) as MAR176, --no_address_out_of_market_bookings
    count(market) filter (where market like '%MD-%') as MAR1192, --Maryland_bookings
    count(market) filter (where market = 'MD-BL') as MAR1114, --baltimore_bookings
    count(market) filter (where market = 'MD-DC') as MAR1155, --maryland_dc_bookings
    count(market) filter (where market like '%PA-%') as MAR1257, --pen_bookings
    count(market) filter (where market = 'PA-PH') as MAR1221, --ph_bookings
    count(market) filter (where market like '%GA-%') as MAR1294, --ga_bookings
    count(market) filter (where market = 'GA-AT') as MAR951, --at_bookings
    count(market) filter (where market like '%VA-%') as MAR1536, --va_bookings
    count(market) filter (where market = 'VA-AR') as MAR1500, --arlington_dc_bookings
    count(market) filter (where market like '%FL-%') as MAR1580, --fl_bookings
    count(market) filter (where market = 'FL-MI') as MAR1610, --miami_dc_bookings
    count(market) filter (where market = 'FL-OR') as MAR1658, --orlando_dc_bookings
    count(market) filter (where market_code like 'PA-WA-%') as MAR2274, --pa_wa_bookings
    count(market) filter (where market = 'WA-SE') as MAR2226, --seatle_bookings
    count(market) filter (where market like 'WN-%') as MAR2527, --wn_il_bookings
    count(market) filter (where market = 'WN-CH') as MAR2894, --wn_il_ch_bookings
    count(market) filter (where market = 'WN-NA') as MAR2953, --wn_il_na_bookings
    count(market) filter (where market = 'WN-LA') as MAR3012, --wn_il_la_bookings
    -- Paid Bookings
        sum(case when channel ilike 'Paid%' then 1 else 0 end) as MAR499, -- Paid Bookings
        sum(case when channel = 'Paid/Home Advisor' then 1 else 0 end) as MAR332, -- Home Advisor Bookings
        sum(case when channel = 'Paid/Home Advisor/Ads' then 1 else 0 end) as MAR2362, -- Home Advisor Ads Bookings
        sum(case when channel = 'Paid/Thumbtack' then 1 else 0 end) as MAR151, -- Thumbtack Bookings
        sum(case when channel = 'Paid/Borg' then 1 else 0 end) as MAR687, -- Borg bookings
        sum(case when channel ilike 'Paid/Misc%' then 1 else 0 end) as MAR340,-- Misc Paid Bookings
        sum(case when channel = 'Paid/Facebook' then 1 else 0 end) as MAR145, -- Facebook Bookings
        sum(case when channel ilike 'Paid/Google%' then 1 else 0 end) as MAR162,-- Google Bookings
        sum(case when channel = 'Paid/Bark' then 1 else 0 end) as MAR1290, -- Bark Bookings
        sum(case when channel = 'Paid/Nextdoor' then 1 else 0 end) as MAR239,-- Nextdoor Bookings
        sum(case when channel = 'Paid/Yelp' then 1 else 0 end) as MAR148, -- Paid/Yelp Leads
        --Non Paid
        sum(case when channel ilike 'Non Paid%' then 1 else 0 end) as MAR154, -- Non Paid Bookings
        sum(case when channel = 'Non Paid/Returning Customers' then 1 else 0 end) as MAR500, -- Non Paid/Returning Customer Bookings
        sum(case when channel ilike 'Non Paid%GMB' then 1 else 0 end) as MAR502, -- Non Paid/GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' then 1 else 0 end) as MAR504,-- Non Paid/SEO
        sum(case when channel ilike 'Non Paid%Direct' then 1 else 0 end) as MAR2061,-- Non Paid/Direct
        sum(case when channel ilike 'Non Paid/Misc%' then 1 else 0 end) as MAR506 --Non Paid/Misc
    from calc_last_booking
    where
    created_at is not null
    and is_warranty_order = 0
    group by 1
),
excludes_commercial as
(
    select
        date_grouping as date,
       -- Bookings by Product
        count(product_name) filter (where product_name = 'Fence' and service_category = 'vinyl') as MAR637F, --vinyl_fence_bookings
        count(product_name) filter (where product_name = 'Fence') as MAR108F, --fence_bookings, moved to excludes_commercial
        count(product_name) filter (where product_name = 'Turf') as MAR108T, --turf_bookings, moved to excludes_commercial
        count(product_name) filter (where product_name = 'Driveway') as MAR108D, --driveway_bookings, moved to excludes_commercial
        -- Fence Paid Bookings
        sum(case when channel ilike 'Paid%' and product_name = 'Fence' then 1 else 0 end) as MAR499F, -- Paid Fence Bookings
        sum(case when channel = 'Paid/Home Advisor' and product_name = 'Fence' then 1 else 0 end) as MAR332F, -- Home Advisor Bookings
        sum(case when channel = 'Paid/Home Advisor/Ads' and product_name = 'Fence' then 1 else 0 end) as MAR2362F, -- Home Advisor Ads Bookings
        sum(case when channel = 'Paid/Thumbtack' and product_name = 'Fence' then 1 else 0 end) as MAR151F, -- Thumbtack Bookings
        sum(case when channel = 'Paid/Borg' and product_name = 'Fence' then 1 else 0 end) as MAR687F, -- Borg Bookings
        sum(case when channel ilike 'Paid/Misc%' and product_name = 'Fence' then 1 else 0 end) as MAR340F,-- Misc Paid Bookings
        sum(case when channel = 'Paid/Facebook' and product_name = 'Fence' then 1 else 0 end) as MAR145F, -- Facebook Bookings
        sum(case when channel ilike 'Paid/Google%' and product_name = 'Fence' then 1 else 0 end) as MAR162F, -- Google Bookings
        sum(case when channel = 'Paid/Bark' and product_name = 'Fence' then 1 else 0 end) as MAR1290F, -- Bark Bookings
        sum(case when channel = 'Paid/Nextdoor' and product_name = 'Fence' then 1 else 0 end) as MAR239F, -- Nextdoor Bookings
        sum(case when channel = 'Paid/Yelp' and product_name = 'Fence' then 1 else 0 end) as MAR148F, -- Paid/Yelp Leads
        -- Fence Non Paid
        sum(case when channel ilike 'Non Paid%' and product_name = 'Fence' then 1 else 0 end) as MAR154F, -- Non Paid Fence Bookings
        sum(case when channel = 'Non Paid/Returning Customers' and product_name = 'Fence' then 1 else 0 end) as MAR500F, -- Non Paid/ Returning Customer Driveway Bookings
        sum(case when channel ilike 'Non Paid%GMB' and product_name = 'Fence' then 1 else 0 end) as MAR502F, -- Non Paid/GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and product_name = 'Fence' then 1 else 0 end) as MAR504F,-- Non Paid/SEO
        sum(case when channel ilike 'Non Paid%Direct' and product_name = 'Fence' then 1 else 0 end) as MAR2061F,-- Non Paid/Direct
        sum(case when channel ilike 'Non Paid/Misc%' and product_name = 'Fence' then 1 else 0 end) as MAR506F, --Non Paid/Misc
        -- Turf Paid Bookings
        sum(case when channel ilike 'Paid%' and product_name = 'Turf' then 1 else 0 end) as MAR499T, -- Paid Turf Bookings
        sum(case when channel = 'Paid/Home Advisor' and product_name = 'Turf' then 1 else 0 end) as MAR332T, -- Home Advisor Bookings
        sum(case when channel = 'Paid/Home Advisor/Ads' and product_name = 'Turf' then 1 else 0 end) as MAR2362T, -- Home Advisor Ads Bookings
        sum(case when channel = 'Paid/Thumbtack' and product_name = 'Turf' then 1 else 0 end) as MAR151T, -- Thumbtack Bookings
        sum(case when channel = 'Paid/Borg' and product_name = 'Turf' then 1 else 0 end) as MAR687T, -- Borg Bookings
        sum(case when channel ilike 'Paid/Misc%' and product_name = 'Turf' then 1 else 0 end) as MAR340T,-- Misc Paid Bookings
        sum(case when channel = 'Paid/Facebook' and product_name = 'Turf' then 1 else 0 end) as MAR145T, -- Facebook Bookings
        sum(case when channel ilike 'Paid/Google%' and product_name = 'Turf' then 1 else 0 end) as MAR162T, -- Google Bookings
        sum(case when channel = 'Paid/Bark' and product_name = 'Turf' then 1 else 0 end) as MAR1290T, -- Bark Bookings
        sum(case when channel = 'Paid/Nextdoor' and product_name = 'Turf' then 1 else 0 end) as MAR239T, -- Nextdoor Bookings
        -- Turf Non Paid
        sum(case when channel ilike 'Non Paid%' and product_name = 'Turf' then 1 else 0 end) as MAR154T, -- Non Paid Turf Bookings
        sum(case when channel = 'Non Paid/Returning Customers' and product_name = 'Turf' then 1 else 0 end) as MAR500T, -- Non Paid/ Returning Customer Driveway Bookings
        sum(case when channel = 'Paid/Yelp' and product_name = 'Turf' then 1 else 0 end) as MAR148T, -- Paid/Yelp Leads
        sum(case when channel ilike 'Non Paid%GMB' and product_name = 'Turf' then 1 else 0 end) as MAR502T, -- Non Paid/GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and product_name = 'Turf' then 1 else 0 end) as MAR504T,-- Non Paid/SEO
        sum(case when channel ilike 'Non Paid%Direct' and product_name = 'Turf' then 1 else 0 end) as MAR2061T,-- Non Paid/Direct
        sum(case when channel ilike 'Non Paid/Misc%' and product_name = 'Turf' then 1 else 0 end) as MAR506T, --Non Paid/Misc
        -- Driveway Paid Bookings
        sum(case when channel ilike 'Paid%' and product_name = 'Driveway' then 1 else 0 end) as MAR499D, -- Paid Driveway Bookings
        sum(case when channel ilike 'Paid/Home Advisor%' and product_name = 'Driveway' then 1 else 0 end) as MAR332D, -- Home Advisor Bookings
        sum(case when channel = 'Paid/Thumbtack' and product_name = 'Driveway' then 1 else 0 end) as MAR151D,-- Thumbtack Bookings
        sum(case when channel ilike 'Paid/Misc%' and product_name = 'Driveway' then 1 else 0 end) as MAR340D,-- Misc Paid Bookings
        sum(case when channel = 'Paid/Facebook' and product_name = 'Driveway' then 1 else 0 end) as MAR145D,-- Facebook Bookings
        sum(case when channel ilike 'Paid/Google%' and product_name = 'Driveway' then 1 else 0 end) as MAR162D,-- Google Bookings
        sum(case when channel = 'Paid/Bark' and product_name = 'Driveway' then 1 else 0 end) as MAR1290D,-- Bark Bookings
        sum(case when channel = 'Paid/Nextdoor' and product_name = 'Driveway' then 1 else 0 end) as MAR239D,-- Nextdoor Bookings
        -- Driveway Non Paid
        sum(case when channel ilike 'Non Paid%' and product_name = 'Driveway' then 1 else 0 end) as MAR154D, -- Non Paid Driveway Bookings
        sum(case when channel = 'Non Paid/Returning Customers' and product_name = 'Driveway' then 1 else 0 end) as MAR500D, -- Non Paid/Returning Customer Fence Bookings
        sum(case when channel = 'Paid/Yelp' and product_name = 'Driveway' then 1 else 0 end) as MAR148D, -- Paid/Yelp Leads
        sum(case when channel ilike 'Non Paid%GMB' and product_name = 'Driveway' then 1 else 0 end) as MAR502D, -- Non Paid/GMB Leads
        sum(case when channel ilike 'Non Paid%SEO' and product_name = 'Driveway' then 1 else 0 end) as MAR504D, -- Non Paid/SEO
        sum(case when channel ilike 'Non Paid/Misc%' and product_name = 'Driveway' then 1 else 0 end) as MAR506D, --Non Paid/Misc
    -- Bookings by Product/Market (Fence)
    count(market) filter (where product_name = 'Fence' and market like '%CN-%') as MAR639F, --NorCal_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-EB') as MAR171F, --east_bay_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-SA') as MAR172F, --sacramento_fence_bookings
    count(market) filter (where product_name = 'Fence' and market in ('CN-WA','CN-SJ','CN-PA')) as MAR173F, --south_bay_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-SF') as MAR175F, --san_francisco_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-NB') as MAR174F, --wine_country_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-FR') as MAR192F, --fresno_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%TX-%') as MAR943F, --tx_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'TX-DL') as MAR718F, --dallas_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'TX-HT') as MAR996F, --hu_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'TX-SA') as MAR1059F, --sa_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'TX-AU') as MAR1096F, --hu_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CS-SD') as MAR752F, --sandiego_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%CS-%') as MAR498F, --SoCal_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CS-SV') as MAR607F, --southern_valley_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CS-OC') as MAR608F, --orange_county_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CS-LA') as MAR609F, --los_angeles_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CS-VC') as MAR679F, --vc_fence_bookings
    count(market) filter (where product_name = 'Fence' and market is null) as MAR176F, --no_address_out_of_market_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-WA') as MAR856F, --wa_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-SJ') as MAR857F, --sj_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-PA') as MAR858F, --pa_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'CN-ST') as MAR859F, --st_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%MD-%') as MAR1192F, --Maryland_fence_bookings
    count(market) filter (where product_name = 'Fence' and (market like '%MD-%' or market like '%PA-%' or market like '%VA-%')) as MAR2024F, --North_East_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'MD-BL') as MAR1114F, --baltimore_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'MD-DC') as MAR1155F, --maryland_dc_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%PA-%') as MAR1257F, --pen_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'PA-PH') as MAR1221F, --ph_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%GA-%') as MAR1294F, --ga_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'GA-AT') as MAR951F, --at_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%VA-%') as MAR1536F, --va_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'VA-AR') as MAR1500F, --ar_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like '%FL-%') as MAR1580F, --fl_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'FL-MI') as MAR1610F, --mi_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'FL-OR') as MAR1658F, --or_fence_bookings
    count(market) filter (where product_name = 'Fence' and market_code like 'PA-WA-%') as MAR2274F, --pa_Wa_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'WA-SE') as MAR2226F, --se_fence_bookings
    count(market) filter (where product_name = 'Fence' and market like 'WN-%') as MAR2527F, --wn_il_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'WN-CH') as MAR2894F, --wn_il_ch_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'WA-NA') as MAR2953F, --wn_il_na_fence_bookings
    count(market) filter (where product_name = 'Fence' and market = 'WA-LA') as MAR3012F, --wn_il_la_fence_bookings
    -- Bookings by Product/Market (Turf)
    count(market) filter (where product_name = 'Turf' and market like '%CN-%') as MAR639T, --NorCal_turf_bookings
     count(market) filter (where product_name = 'Turf' and market like '%CS-%') as MAR498T, --SoCal_turf_bookings
    count(market) filter (where product_name = 'Turf' and market = 'CS-SV') as MAR607T, --southern_valley_turf_bookings
    count(market) filter (where product_name = 'Turf' and market = 'CS-OC') as MAR608T, --orange_county_turf_bookings
    count(market) filter (where product_name = 'Turf' and market = 'CS-LA') as MAR609T, --los_angeles_turf_bookiings
    count(market) filter (where product_name = 'Turf' and market = 'CS-VC') as MAR679T, --vc_turf_bookings
    count(market) filter (where product_name = 'Turf' and market = 'CS-SD') as MAR752T, --sandiego_turf_bookings
    count(market) filter (where product_name = 'Turf' and market is null) as MAR176T, --no_address_out_of_market_turf_bookings
    -- Bookings by Product/Market (Driveway)
    count(market) filter (where product_name = 'Driveway' and market like '%TX-%') as MAR945D, --NorCal_fence_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'TX-FW') as MAR946D, --east_bay_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'TX-DL') as MAR947D, --sacramento_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market like '%CN-%') as MAR639D, --NorCal_fence_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-EB') as MAR171D, --east_bay_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-SA') as MAR172D, --sacramento_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market in ('CN-WA','CN-SJ','CN-PA')) as MAR173D, --south_bay_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-SF') as MAR175D, --san_francisco_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-NB') as MAR174D, --wine_country_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-FR') as MAR192D, --fresno_driveway_bookings
    --count(market) filter (where product_name = 'Driveway' and market = 'DL') as MAR718D, --dallas_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market like '%CS-%') as MAR498D, --SoCal_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CS-SV') as MAR607D, --southern_valley_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CS-OC') as MAR608D, --orange_county_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CS-LA') as MAR609D, --los_angeles_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CS-VC') as MAR679D, --vc_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market is null) as MAR176D, --no_address_out_of_market_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-WA') as MAR856D, --wa_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-SJ') as MAR857D, --sj_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-PA') as MAR858D, --pa_driveway_bookings
    count(market) filter (where product_name = 'Driveway' and market = 'CN-ST') as MAR859D, --st_driveway_bookings
              -- Bookings by Product
    count(service_category) filter (where service_category = 'WF') as MAR254, --wood_fence_bookings
    count(service_category) filter (where service_category = 'CLF') as MAR255, --chainlink_fence_bookings
    count(service_category) filter (where service_category = 'CD') as MAR256, --concrete_driveways_bookings
    count(service_category) filter (where service_category = 'CP') as MAR257, --concrete_patios_bookings
    count(service_category) filter (where service_category = 'BP') as MAR258, --brick_patios_bookings
    count(service_category) filter (where service_category = 'SC') as MAR259, --stamped_concrete_bookings
    count(service_category) filter (where service_category = 'IPP') as MAR261, --pavers_patios_bookings
    count(service_category) filter (where service_category = 'IPD') as MAR260, --pavers_driveways_bookings
    count(service_category) filter (where service_category = 'BD') as MAR262, --brick_driveways_bookings
    count(service_category) filter (where service_category = 'RWF') as MAR263, --repair_fence_bookings
    count(service_category) filter (where service_category = 'AP') as MAR264, --asphalt_bookings
    count(service_category) filter (where service_category = 'OTHER') as MAR265, --other_bookings
    -- Bookings by Product Fence
    count(service_category) filter (where product_name = 'Fence' and service_category = 'OTHER') as MAR265F, --other_fence_bookings
   -- Bookings by Product Turf
    count(service_category) filter (where product_name = 'Turf' and service_category = 'OTHER') as MAR265T, --other_turf_bookings
    -- Bookings by Product Driveway
    count(service_category) filter (where product_name = 'Driveway' and service_category = 'OTHER') as MAR265D --other_driveway_bookings
from calc_last_booking
where
        created_at is not null
        and (is_commercial = 0 or is_commercial is null)
        and is_warranty_order = 0
group by 1
)
select
    *
from includes_commercial ic
left join excludes_commercial ec using(date)
