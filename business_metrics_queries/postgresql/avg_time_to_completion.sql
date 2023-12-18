-- AVG closed2completed overall / by product / by market
-- upload to BQ
with
calc_all_approved_quotes as
(
        select
                rank() over (partition by order_id order by approved_at),
                *
        from quote_quote
        where
                approved_at is not null
                and approved_at >= '2018-04-16'
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
calc_first_approved_quotes as
(
        select
                q.id,
                q.order_id,
                q.created_at,
                q.approved_at,
                o.completed_at,
                extract(day from o.completed_at-q.approved_at) + extract(hour from o.completed_at-q.approved_at)/24 as close_to_complete_days,
                o.product_id,
                case when o.product_id = 105 then 1 else 0 end as is_fence,
                case when o.product_id = 34 then 1 else 0 end as is_driveway,
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
                   else null end as market
        from calc_all_approved_quotes q
        left join store_order o on o.id = q.order_id
        left join core_house h on h.id = o.house_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_city gc on gc.id = ga.city_id
        left join geo_county gcn on gcn.id = ga.county_id
        left join product_countymarket pcm on pcm.county_id = gcn.id
        left join product_market pm on pm.id = pcm.market_id
        left join cancelled_projects cp on cp.order_id = q.order_id
        where
                rank = 1
                and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612,69998)
                and o.completed_at is not null
                and parent_order_id is null
                and cp.order_id is null
)
select
        date_trunc( '{period}', completed_at at time zone 'America/Los_Angeles')::date as date,
        avg(close_to_complete_days) as DEL118, -- avg_cl2cm_in_days
        coalesce(sum(case when is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when is_fence = 1 then 1 else 0 end),0),0) as DEL118F, -- avg_fence_cl2cm_in_days
        coalesce(sum(case when is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when is_driveway = 1 then 1 else 0 end),0),0) as DEL118D, -- avg_driveway_cl2cm_in_days
        -- AVG Closed2Completed by Market
        coalesce(sum(case when market = 'CN-EB' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-EB' then 1 else 0 end),0),0) as DEL119, -- avg_eb_cl2cm_in_days
        coalesce(sum(case when market = 'CN-NB' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-NB' then 1 else 0 end),0),0) as DEL120, -- avg_nb_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SA' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SA' then 1 else 0 end),0),0) as DEL121, -- avg_sac_cl2cm_in_days
        coalesce(sum(case when market in ('CN-WA','CN-SJ','CN-PA') then close_to_complete_days else 0 end)/nullif(sum(case when market in ('CN-WA','CN-SJ','CN-PA') then 1 else 0 end),0),0) as DEL122, -- avg_sb_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SF' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SF' then 1 else 0 end),0),0) as DEL123, -- avg_sf_cl2cm_in_days
        coalesce(sum(case when market = 'CN-WA' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-WA' then 1 else 0 end),0),0) as DEL251, -- avg_wa_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SJ' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SJ' then 1 else 0 end),0),0) as DEL252, -- avg_sj_cl2cm_in_days
        coalesce(sum(case when market = 'CN-PA' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-PA' then 1 else 0 end),0),0) as DEL253, -- avg_pa_cl2cm_in_days
        coalesce(sum(case when market = 'CN-ST' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-ST' then 1 else 0 end),0),0) as DEL254, -- avg_st_cl2cm_in_days
        coalesce(sum(case when market = 'MD-BL' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'MD-BL' then 1 else 0 end),0),0) as DEL212, -- avg_bl_cl2cm_in_days
        coalesce(sum(case when market = 'MD-DC' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'MD-DC' then 1 else 0 end),0),0) as DEL335, -- avg_dc_cl2cm_in_days
        coalesce(sum(case when market = 'PA-PH' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'PA-PH' then 1 else 0 end),0),0) as DEL348, -- avg_ph_cl2cm_in_days
        coalesce(sum(case when market = 'VA-AR' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'VA-AR' then 1 else 0 end),0),0) as DEL365, -- avg_ar_cl2cm_in_days
        coalesce(sum(case when market = 'FL-MI' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'FL-MI' then 1 else 0 end),0),0) as DEL380, -- avg_mi_cl2cm_in_days
        coalesce(sum(case when market = 'FL-OR' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'FL-OR' then 1 else 0 end),0),0) as DEL395, -- avg_mi_cl2cm_in_days
        coalesce(sum(case when market = 'WA-SE' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WA-SE' then 1 else 0 end),0),0) as DEL408, -- avg_se_cl2cm_in_days
        coalesce(sum(case when market = 'WN-CH' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-CH' then 1 else 0 end),0),0) as DEL571, -- avg_wn_ch_cl2cm_in_days
        coalesce(sum(case when market = 'WN-NA' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-NA' then 1 else 0 end),0),0) as DEL582, -- avg_wn_na_cl2cm_in_days
        coalesce(sum(case when market = 'WN-LA' then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-LA' then 1 else 0 end),0),0) as DEL593, -- avg_wn_la_cl2cm_in_days
        -- AVG Closed2Completed by Market (Fence)
        coalesce(sum(case when market = 'CN-EB' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-EB' and is_fence = 1 then 1 else 0 end),0),0) as DEL119F, -- avg_eb_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-NB' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-NB' and is_fence = 1 then 1 else 0 end),0),0) as DEL120F, -- avg_nb_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SA' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SA' and is_fence = 1 then 1 else 0 end),0),0) as DEL121F, -- avg_sac_fence_cl2cm_in_days
        coalesce(sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market in ('CN-WA','CN-SJ','CN-PA')and is_fence = 1 then 1 else 0 end),0),0) as DEL122F, -- avg_sb_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SF' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SF' and is_fence = 1 then 1 else 0 end),0),0) as DEL123F, -- avg_sf_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-WA' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-WA' and is_fence = 1 then 1 else 0 end),0),0) as DEL251F, -- avg_wa_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SJ' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SJ' and is_fence = 1 then 1 else 0 end),0),0) as DEL252F, -- avg_sj_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-PA' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-PA' and is_fence = 1 then 1 else 0 end),0),0) as DEL253F, -- avg_pa_fence_cl2cm_in_days
        coalesce(sum(case when market = 'CN-ST' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-ST' and is_fence = 1 then 1 else 0 end),0),0) as DEL254F, -- avg_st_fence_cl2cm_in_days
        coalesce(sum(case when market = 'MD-BL' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'MD-BL' and is_fence = 1 then 1 else 0 end),0),0) as DEL212F, -- avg_bl_fence_cl2cm_in_days
        coalesce(sum(case when market = 'MD-DC' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'MD-DC' and is_fence = 1 then 1 else 0 end),0),0) as DEL335F, -- avg_st_fence_cl2cm_in_days
        coalesce(sum(case when market = 'PA-PH' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'PA-PH' and is_fence = 1 then 1 else 0 end),0),0) as DEL348F, -- avg_ph_fence_cl2cm_in_days
        coalesce(sum(case when market = 'VA-AR' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'VA-AR' and is_fence = 1 then 1 else 0 end),0),0) as DEL365F, -- avg_ar_fence_cl2cm_in_days
        coalesce(sum(case when market = 'FL-MI' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'FL-MI' and is_fence = 1 then 1 else 0 end),0),0) as DEL380F, -- avg_mi_fence_cl2cm_in_days
        coalesce(sum(case when market = 'FL-OR' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'FL-OR' and is_fence = 1 then 1 else 0 end),0),0) as DEL395F, -- avg_or_fence_cl2cm_in_days
        coalesce(sum(case when market = 'WA-SE' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WA-SE' and is_fence = 1 then 1 else 0 end),0),0) as DEL408F, -- avg_se_fence_cl2cm_in_days
        coalesce(sum(case when market = 'WN-CH' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-CH' and is_fence = 1 then 1 else 0 end),0),0) as DEL571F, -- avg_wn_ch_fence_cl2cm_in_days
        coalesce(sum(case when market = 'WN-NA' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-NA' and is_fence = 1 then 1 else 0 end),0),0) as DEL582F, -- avg_wn_na_fence_cl2cm_in_days
        coalesce(sum(case when market = 'WN-LA' and is_fence = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'WN-LA' and is_fence = 1 then 1 else 0 end),0),0) as DEL593F, -- avg_wn_la_fence_cl2cm_in_days
        -- AVG Closed2Completed by Market (Driveway)
        coalesce(sum(case when market = 'CN-EB' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-EB' and is_driveway = 1 then 1 else 0 end),0),0) as DEL119D, -- avg_eb_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-NB' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-NB' and is_driveway = 1 then 1 else 0 end),0),0) as DEL120D, -- avg_nb_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SA' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SA' and is_driveway = 1 then 1 else 0 end),0),0) as DEL121D, -- avg_sac_driveway_cl2cm_in_days
        coalesce(sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market in ('CN-WA','CN-SJ','CN-PA') and is_driveway = 1 then 1 else 0 end),0),0) as DEL122D, -- avg_sb_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SF' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SF' and is_driveway = 1 then 1 else 0 end),0),0) as DEL123D, -- avg_sf_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-WA' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-WA' and is_driveway = 1 then 1 else 0 end),0),0) as DEL251D, -- avg_wa_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-SJ' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-SJ' and is_driveway = 1 then 1 else 0 end),0),0) as DEL252D, -- avg_sj_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-PA' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-PA' and is_driveway = 1 then 1 else 0 end),0),0) as DEL253D, -- avg_pa_driveway_cl2cm_in_days
        coalesce(sum(case when market = 'CN-ST' and is_driveway = 1 then close_to_complete_days else 0 end)/nullif(sum(case when market = 'CN-ST' and is_driveway = 1 then 1 else 0 end),0),0) as DEL254D -- avg_st_driveway_cl2cm_in_days
from calc_first_approved_quotes
group by 1
order by 1 desc
