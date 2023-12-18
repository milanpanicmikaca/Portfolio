-- upload to BQ
with
calc_approved as
(
        select
            id,
            order_id,
                   approved_at
    from
         (
                   select
                      q.*,
                           rank() over (partition by q.order_id order by q.approved_at) as rank
            from quote_quote q
                 where
                    q.approved_at is not null
         ) as k
         where
        rank = 1
),
calc_lead as
(
        select *
           from
         (
                   select
                      l.id,
                    l.created_at,
                      l.address_id,
                      l.order_id,
                           rank() over (partition by l.order_id order by l.created_at) as rank
                   from core_lead l
                  left join customers_visitoraction cv on cv.id = l.visitor_action_id
        ) as k
        where
                rank = 1
),
calc_booking as
(
        select
                  ssa.*,
                   ga.formatted_address,
            case when o.product_id = 105 then 'Fence' when o.product_id = 34 then 'Driveway' else 'Fence' end as product_name,
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
            case when ssat.code = 'physical_onsite' then 1 else 0 end as is_physical,
             case when ssat.code = 'physical_onsite' and o.product_id = 105 then 1 else 0 end as is_fence_physical,
            case when ssat.code = 'physical_onsite' and o.product_id = 34 then 1 else 0 end as is_driveway_physical,
             rank() over (partition by ga.formatted_address, o.product_id order by ssa.created_at desc) as rank,
             ssat.code,
             is_commercial::integer
          from schedule_appointment ssa
         left join store_order o on o.id = ssa.order_id
          left join core_house h on h.id = o.house_id
          left join customers_customer cc on cc.id = h.customer_id
         left join geo_address ga on ga.id = h.address_id
         left join geo_county gcn on gcn.id = ga.county_id
    left join product_countymarket pcm on pcm.county_id = gcn.id
    left join product_market pm on pm.id = pcm.market_id
    left join schedule_appointmenttype ssat on ssat.id = ssa.appointment_type_id
           where
            (ssa.cancelled_at is null)
            and ssat.code <> 'quote_review'
            and ssa.date <= now()
),
calc_last_booking as
(
        select
            cb.*,
                 case when ca.approved_at - cb.date <= interval '14 days' then 1 else 0 end as a14,
        case when ca.approved_at - cb.date <= interval '28 days' then 1 else 0 end as a28,
        case when ca.approved_at - cb.date <= interval '56 days' then 1 else 0 end as a56,
        case when ca.approved_at is null then 0 else 1 end as appr,
        case when market like '%CN-%' then 1 else 0 end as norcal_appr,
        case when market = 'CN-EB' then 1 else 0 end as eb_appr,
        case when market = 'CN-SF' then 1 else 0 end as sf_appr,
        case when market = 'TX-HT' then 1 else 0 end as ht_appr,
        case when market = 'TX-AU' then 1 else 0 end as au_appr,
        case when market = 'GA-AT' then 1 else 0 end as at_appr,
        case when market = 'TX-SA' then 1 else 0 end as sa_appr,
        case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') then 1 else 0 end as sbsf_appr,
        case when market = 'CN-NB' then 1 else 0 end as nb_appr,
        case when market = 'CN-SA' then 1 else 0 end as sac_appr,
        case when market = 'CN-FR' then 1 else 0 end as fr_appr,
        case when market like '%CS-%' then 1 else 0 end as sc_appr,
        case when market = 'CS-SV' then 1 else 0 end as sv_appr,
        case when market = 'CS-OC' then 1 else 0 end as oc_appr,
        case when market = 'CS-LA' then 1 else 0 end as la_appr,
        case when market = 'CS-VC' then 1 else 0 end as vc_appr,
        case when market like '%TX-%' then 1 else 0 end as tx_appr,
        case when market = 'TX-DL' then 1 else 0 end as dl_appr,
        case when market = 'TX-FW' then 1 else 0 end as fw_appr,
        case when market = 'CS-SD' then 1 else 0 end as sd_appr,
        case when market = 'CN-WA' then 1 else 0 end as wa_appr,
        case when market = 'CN-SJ' then 1 else 0 end as sj_appr,
        case when market = 'CN-PA' then 1 else 0 end as pa_appr,
        case when market = 'CN-ST' then 1 else 0 end as st_appr,
        case when (market like '%MD-%' or market like '%PA-%' or market like '%VA-%') then 1 else 0 end as ne_appr,
        case when market like '%MD-%' then 1 else 0 end as md_appr,
        case when market = 'MD-BL' then 1 else 0 end as bl_appr,
        case when market = 'MD-DC' then 1 else 0 end as dc_appr,
        case when market like '%PA-%' then 1 else 0 end as pen_appr,
        case when market = 'PA-PH' then 1 else 0 end as ph_appr,
        case when market like '%VA-%' then 1 else 0 end as va_appr,
        case when market = 'VA-AR' then 1 else 0 end as ar_appr,
        case when market like '%FL-%' then 1 else 0 end as fl_appr,
        case when market = 'FL-MI' then 1 else 0 end as mi_appr,
        case when market = 'FL-OR' then 1 else 0 end as or_appr,
        case when market like '%GA-%' then 1 else 0 end as ga_appr,
        case when market like '%WA-%' then 1 else 0 end as pa_wa_appr,
        case when market like 'WA-SE' then 1 else 0 end as se_appr,
        case when market like 'WN-%' then 1 else 0 end as wn_il_appr,
        case when market = 'WN-CH' then 1 else 0 end as wn_ch_appr,
        case when market = 'WN-NA' then 1 else 0 end as wn_na_appr,
        case when market = 'WN-LA' then 1 else 0 end as wn_la_appr
        from calc_booking cb
        left join calc_lead cl on cl.order_id = cb.order_id
        left join calc_approved ca on ca.order_id = cb.order_id
        left join core_lead l on l.id = cl.id
        left join customers_contact cco on cco.id = l.contact_id
        left join core_user cu on cu.id = cco.user_id
        where coalesce(l.full_name,'')||coalesce(cco.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
        and coalesce(l.email,'')||coalesce(cu.email,'') not ilike '%+test%'
        and cb.rank = 1
), fence_onsites as
(
select
        date_trunc('{period}', date)::date as date,
        -- Fence Onsites that took place this time period, #
    sum(norcal_appr) as SAL473F, --norcal_onsites
    sum(eb_appr) as SAL311F, -- eb_onsites,
    sum(sf_appr) as SAL1124F, -- sf_onsites,
    sum(at_appr) as SAL695F, -- at_onsites,
    sum(ht_appr) as SAL704F, -- ht_onsites,
    sum(au_appr) as SAL722F, -- au_onsites,
    sum(sa_appr) as SAL713F, --sa_onsites,
    sum(sbsf_appr) as SAL312F, -- sbsf_onsites,
    sum(nb_appr) as SAL313F, -- nb_onsites,
    sum(sac_appr) as SAL314F, -- sac_onsites,
    sum(fr_appr) as SAL315F, -- fr_onsites,
    sum(sc_appr) as SAL366F, -- sc_onsites,
    sum(sv_appr) as SAL402F, -- sv_onsites,
    sum(oc_appr) as SAL407F, -- oc_onsites,
    sum(la_appr) as SAL412F, -- la_onsites,
    sum(vc_appr) as SAL492F, -- vc_onsites,
    sum(tx_appr) as SAL576F, -- tx_onsites,
    sum(dl_appr) as SAL544F, -- dl_onsites,
    sum(fw_appr) as SAL556F, -- fw_onsites,
    sum(sd_appr) as SAL566F, -- sd_onsites,
    sum(wa_appr) as SAL651F, -- wa_onsites,
    sum(sj_appr) as SAL652F, -- sj_onsites,
    sum(pa_appr) as SAL653F, -- pa_onsites,
    sum(st_appr) as SAL654F, -- st_onsites,
    sum(md_appr) as SAL775F, -- maryland_onsites
    sum(ne_appr) as SAL960F, -- north_east_onsites
    sum(bl_appr) as SAL738F, -- bl_onsites,
    sum(dc_appr) as SAL758F, -- dc_onsites,
    sum(pen_appr) as SAL816F, -- pen_onsites,
    sum(ph_appr) as SAL799F, -- ph_onsites,
    sum(ga_appr) as SAL839F, -- ga_onsites,
    sum(va_appr) as SAL879F, -- va_onsites
    sum(ar_appr) as SAL862F, -- ar_onsites,
    sum(fl_appr) as SAL900F, -- va_onsites
    sum(mi_appr) as SAL924F, -- ar_onsites,
    sum(or_appr) as SAL944F, -- ar_onsites,
    sum(se_appr) as SAL1051F, -- se_onsites,
    sum(pa_wa_appr) as SAL1073F, -- pa_wa_onsites
    sum(wn_il_appr) as SAL1145F, -- wn_il_onsites
    sum(wn_ch_appr) as SAL1171F, -- wn_ch_onsites
    sum(wn_na_appr) as SAL1194F, -- wn_na_onsites
    sum(wn_la_appr) as SAL1217F, -- wn_la_onsites
    -- Fence Physical onsites that took place this time period, #
    sum(case when norcal_appr = 1 then is_fence_physical else 0 end) as SAL474F, -- eb_appr_physical,
    sum(case when eb_appr = 1 then is_fence_physical else 0 end) as SAL316F, -- eb_appr_physical,
    sum(case when sf_appr = 1 then is_fence_physical else 0 end) as SAL1125F, -- sf_appr_physical,
    sum(case when at_appr = 1 then is_fence_physical else 0 end) as SAL696F, -- at_appr_physical,
    sum(case when ht_appr = 1 then is_fence_physical else 0 end) as SAL705F, -- ht_appr_physical,
    sum(case when au_appr = 1 then is_fence_physical else 0 end) as SAL723F, -- ht_appr_physical,
    sum(case when sa_appr = 1 then is_fence_physical else 0 end) as SAL714F, -- sa_appr_physical,
    sum(case when sbsf_appr = 1 then is_fence_physical else 0 end) as SAL317F, -- sbsf_appr_physical,
    sum(case when nb_appr = 1 then is_fence_physical else 0 end) as SAL318F, -- nb_appr_physical,
    sum(case when sac_appr = 1 then is_fence_physical else 0 end) as SAL319F, -- sac_appr_physical,
    sum(case when fr_appr = 1 then is_fence_physical else 0 end) as SAL320F, -- fr_appr_physical,
    sum(case when sc_appr = 1 then is_fence_physical else 0 end) as SAL367F, -- sc_appr_physical,
    sum(case when sv_appr = 1 then is_fence_physical else 0 end) as SAL403F, -- sv_appr_physical,
    sum(case when oc_appr = 1 then is_fence_physical else 0 end) as SAL408F, -- oc_appr_physical,
    sum(case when la_appr = 1 then is_fence_physical else 0 end) as SAL413F, -- la_appr_physical,
    sum(case when vc_appr = 1 then is_fence_physical else 0 end) as SAL493F, -- vc_appr_physical,
    sum(case when tx_appr = 1 then is_fence_physical else 0 end) as SAL577F, -- tx_appr_physical,
    sum(case when dl_appr = 1 then is_fence_physical else 0 end) as SAL545F, -- dl_appr_physical,
    sum(case when fw_appr = 1 then is_fence_physical else 0 end) as SAL557F, -- fw_appr_physical,
    sum(case when sd_appr = 1 then is_fence_physical else 0 end) as SAL567F, -- sd_appr_physical,
    sum(case when wa_appr = 1 then is_fence_physical else 0 end) as SAL655F, -- wa_appr_physical,
    sum(case when sj_appr = 1 then is_fence_physical else 0 end) as SAL656F, -- sj_appr_physical,
    sum(case when pa_appr = 1 then is_fence_physical else 0 end) as SAL657F, -- pa_appr_physical,
    sum(case when st_appr = 1 then is_fence_physical else 0 end) as SAL658F, -- st_appr_physical,
    sum(case when md_appr = 1 then is_fence_physical else 0 end) as SAL776F, -- md_appr_physical,
    sum(case when ne_appr = 1 then is_fence_physical else 0 end) as SAL961F, -- ne_appr_physical,
    sum(case when bl_appr = 1 then is_fence_physical else 0 end) as SAL739F, -- bl_appr_physical,
    sum(case when dc_appr = 1 then is_fence_physical else 0 end) as SAL759F, -- dc_appr_physical,
    sum(case when pen_appr = 1 then is_fence_physical else 0 end) as SAL818F, -- pen_appr_physical,
    sum(case when ph_appr = 1 then is_fence_physical else 0 end) as SAL800F, -- ph_appr_physical,
    sum(case when ga_appr = 1 then is_fence_physical else 0 end) as SAL841F, -- ga_appr_physical,
    sum(case when va_appr = 1 then is_fence_physical else 0 end) as SAL880F, -- va_appr_physical,
    sum(case when ar_appr = 1 then is_fence_physical else 0 end) as SAL863F, -- ar_appr_physical,
    sum(case when fl_appr = 1 then is_fence_physical else 0 end) as SAL901, -- fl_appr_physical,
    sum(case when mi_appr = 1 then is_fence_physical else 0 end) as SAL925F, -- mi_appr_physical,
    sum(case when or_appr = 1 then is_fence_physical else 0 end) as SAL945F, -- or_appr_physical,
    sum(case when se_appr = 1 then is_fence_physical else 0 end) as SAL1052F, -- se_appr_physical,
    sum(case when pa_wa_appr = 1 then is_fence_physical else 0 end) as SAL1074, -- pa_wa_appr_physical,
    sum(case when wn_il_appr = 1 then is_fence_physical else 0 end) as SAL1146F, -- wn_il_appr_physical,
    sum(case when wn_ch_appr = 1 then is_fence_physical else 0 end) as SAL1172F, -- wn_ch_appr_physical,
    sum(case when wn_na_appr = 1 then is_fence_physical else 0 end) as SAL1195F, -- wn_na_appr_physical,
    sum(case when wn_la_appr = 1 then is_fence_physical else 0 end) as SAL1218F, -- wn_la_appr_physical,
    -- Fence Onsites to Closes (14 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL475F, -- norcal_onsite2close_14,
    coalesce(cast(sum(case when eb_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL321F, -- eb_onsite2close_14,
    coalesce(cast(sum(case when sf_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sf_appr),0),0) as SAL1126F, -- sf_onsite2close_14,
    coalesce(cast(sum(case when at_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL697F, -- at_onsite2close_14,
    coalesce(cast(sum(case when ht_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL706F, -- ht_onsite2close_14,
    coalesce(cast(sum(case when au_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(au_appr),0),0) as SAL724F, -- au_onsite2close_14,
    coalesce(cast(sum(case when sa_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sa_appr),0),0) as SAL715F, -- sa_onsite2close_14,
    coalesce(cast(sum(case when sbsf_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL322F, -- sbsf_onsite2close_14,
    coalesce(cast(sum(case when nb_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL323F, -- nb_onsite2close_14,
    coalesce(cast(sum(case when sac_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) as SAL324F, -- sac_onsite2close_14,
    coalesce(cast(sum(case when fr_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL325F, -- fr_onsite2close_14,
    coalesce(cast(sum(case when sc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL368F, -- sc_onsite2close_14,
    coalesce(cast(sum(case when sv_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL404F, -- sv_onsite2close_14,
    coalesce(cast(sum(case when oc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL409F, -- oc_onsite2close_14,
    coalesce(cast(sum(case when la_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL414F, -- la_onsite2close_14,
    coalesce(cast(sum(case when vc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL494F, -- vc_onsite2close_14,
    coalesce(cast(sum(case when tx_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL578F, -- tx_onsite2close_14,
    coalesce(cast(sum(case when dl_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL546F, -- dl_onsite2close_14,
    coalesce(cast(sum(case when fw_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL558F, -- fw_onsite2close_14,
    coalesce(cast(sum(case when sd_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL568F, -- sd_onsite2close_14,
    coalesce(cast(sum(case when wa_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(wa_appr),0),0) as SAL659F, -- wa_onsite2close_14,
    coalesce(cast(sum(case when sj_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL660F, -- sj_onsite2close_14,
    coalesce(cast(sum(case when pa_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL661F, -- pa_onsite2close_14,
    coalesce(cast(sum(case when st_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL662F, -- st_onsite2close_14,
    coalesce(cast(sum(case when md_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(md_appr),0),0) as SAL777F, -- md_onsite2close_14,
    coalesce(cast(sum(case when ne_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ne_appr),0),0) as SAL962F, -- ne_onsite2close_14,
    coalesce(cast(sum(case when bl_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(bl_appr),0),0) as SAL740F, -- bl_onsite2close_14,
    coalesce(cast(sum(case when dc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(dc_appr),0),0) as SAL760F, -- dc_onsite2close_14,
    coalesce(cast(sum(case when pen_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(pen_appr),0),0) as SAL817F, -- pen_onsite2close_14,
    coalesce(cast(sum(case when ph_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ph_appr),0),0) as SAL801F, -- ph_onsite2close_14,
    coalesce(cast(sum(case when ga_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ga_appr),0),0) as SAL840F, -- ga_onsite2close_14,
    coalesce(cast(sum(case when va_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(va_appr),0),0) as SAL881F, -- va_onsite2close_14,
    coalesce(cast(sum(case when ar_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ar_appr),0),0) as SAL864F, -- ar_onsite2close_14,
    coalesce(cast(sum(case when fl_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(fl_appr),0),0) as SAL902F, -- va_onsite2close_14,
    coalesce(cast(sum(case when mi_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(mi_appr),0),0) as SAL926F, -- ar_onsite2close_14,
    coalesce(cast(sum(case when or_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(or_appr),0),0) as SAL946F, -- ar_onsite2close_14,
    coalesce(cast(sum(case when se_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(se_appr),0),0) as SAL1053F, -- se_onsite2close_14,
    coalesce(cast(sum(case when pa_wa_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(pa_wa_appr),0),0) as SAL1075F, -- pa_wa_onsite2close_14,
    coalesce(cast(sum(case when wn_il_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(wn_il_appr),0),0) as SAL1147F, -- wn_il_onsite2close_14, CODE NEEDED
    coalesce(cast(sum(case when wn_ch_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(wn_ch_appr),0),0) as SAL1173F, -- wn_ch_onsite2close_14,
    coalesce(cast(sum(case when wn_na_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(wn_na_appr),0),0) as SAL1196F, -- wn_na_onsite2close_14,
    coalesce(cast(sum(case when wn_la_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(wn_la_appr),0),0) as SAL1219F, -- wn_la_onsite2close_14,
    -- Fence Onsites to Closes (28 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL476F, -- norcal_onsite2close_28,
    coalesce(cast(sum(case when eb_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL326F, -- eb_onsite2close_28,
    coalesce(cast(sum(case when sf_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sf_appr),0),0) as SAL1127F, -- sf_onsite2close_28,
    coalesce(cast(sum(case when at_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL698F, -- at_onsite2close_28,
    coalesce(cast(sum(case when ht_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL707F, -- ht_onsite2close_28,
    coalesce(cast(sum(case when au_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(au_appr),0),0) as SAL725F, -- au_onsite2close_28,
    coalesce(cast(sum(case when sa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sa_appr),0),0) as SAL716F, -- sa_onsite2close_28,
    coalesce(cast(sum(case when sbsf_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL327F, -- sbsf_onsite2close_28,
    coalesce(cast(sum(case when nb_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL328F, -- nb_onsite2close_28,
    coalesce(cast(sum(case when sac_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) SAL329F, -- as sac_onsite2close_28,
    coalesce(cast(sum(case when fr_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL330F, -- fr_onsite2close_28,
    coalesce(cast(sum(case when sc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL369F, -- sc_onsite2close_28,
    coalesce(cast(sum(case when sv_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL405F, -- sv_onsite2close_28,
    coalesce(cast(sum(case when oc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL410F, -- oc_onsite2close_28,
    coalesce(cast(sum(case when la_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL415F, -- la_onsite2close_28,
    coalesce(cast(sum(case when vc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL495F, -- vc_onsite2close_28,
    coalesce(cast(sum(case when tx_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL579F, -- tx_onsite2close_28,
    coalesce(cast(sum(case when dl_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL547F, -- dl_onsite2close_28,
    coalesce(cast(sum(case when fw_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL559F, -- fw_onsite2close_28,
    coalesce(cast(sum(case when sd_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL569F, -- sd_onsite2close_28,
    coalesce(cast(sum(case when wa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wa_appr),0),0) as SAL663F, -- wa_onsite2close_28,
    coalesce(cast(sum(case when sj_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL664F, -- sj_onsite2close_28,
    coalesce(cast(sum(case when pa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL665F, -- pa_onsite2close_28,
    coalesce(cast(sum(case when st_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL666F, -- st_onsite2close_28,
    coalesce(cast(sum(case when md_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(md_appr),0),0) as SAL778F, -- md_onsite2close_28,
    coalesce(cast(sum(case when ne_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ne_appr),0),0) as SAL963F, -- ne_onsite2close_28,
    coalesce(cast(sum(case when bl_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(bl_appr),0),0) as SAL741F, -- bl_onsite2close_28,
    coalesce(cast(sum(case when dc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(dc_appr),0),0) as SAL761F, -- dc_onsite2close_28,
    coalesce(cast(sum(case when pen_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(pen_appr),0),0) as SAL819F, -- pen_onsite2close_28,
    coalesce(cast(sum(case when ph_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ph_appr),0),0) as SAL802F, -- ph_onsite2close_28,
    coalesce(cast(sum(case when ga_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ga_appr),0),0) as SAL842F, -- ga_onsite2close_28,
    coalesce(cast(sum(case when va_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(va_appr),0),0) as SAL882F, -- va_onsite2close_28,
    coalesce(cast(sum(case when ar_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ar_appr),0),0) as SAL865F, -- ar_onsite2close_28,
    coalesce(cast(sum(case when fl_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(fl_appr),0),0) as SAL903F, -- fl_onsite2close_28,
    coalesce(cast(sum(case when mi_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(mi_appr),0),0) as SAL927F, -- mi_onsite2close_28,
    coalesce(cast(sum(case when or_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(or_appr),0),0) as SAL947F, -- or_onsite2close_28,
    coalesce(cast(sum(case when pa_wa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(pa_wa_appr),0),0) as SAL1076F, -- pa_wa_onsite2close_28,
    coalesce(cast(sum(case when se_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(se_appr),0),0) as SAL1054F, -- se_onsite2close_28,
    coalesce(cast(sum(case when wn_il_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wn_il_appr),0),0) as SAL1148F, -- wn_il_onsite2close_28, CODE NEEDED
    coalesce(cast(sum(case when wn_ch_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wn_ch_appr),0),0) as SAL1174F, -- wn_ch_onsite2close_28,
    coalesce(cast(sum(case when wn_na_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wn_na_appr),0),0) as SAL1197F, -- wn_na_onsite2close_28,
    coalesce(cast(sum(case when wn_la_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wn_la_appr),0),0) as SAL1220F, -- wn_la_onsite2close_28,
    -- Fence Onsites to Closes (56 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL477F, -- norcal_onsite2close_56,
    coalesce(cast(sum(case when eb_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL331F, -- eb_onsite2close_56,
    coalesce(cast(sum(case when sf_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sf_appr),0),0) as SAL1128F, -- sf_onsite2close_56,
    coalesce(cast(sum(case when at_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL699F, -- at_onsite2close_56,
    coalesce(cast(sum(case when ht_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL708F, -- ht_onsite2close_56,
    coalesce(cast(sum(case when au_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(au_appr),0),0) as SAL726F, -- au_onsite2close_56,
    coalesce(cast(sum(case when sa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sa_appr),0),0) as SAL717F, -- sa_onsite2close_56,
    coalesce(cast(sum(case when sbsf_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL332F, -- sbsf_onsite2close_56,
    coalesce(cast(sum(case when nb_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL333F, -- nb_onsite2close_56,
    coalesce(cast(sum(case when sac_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) as SAL334F, -- sac_onsite2close_56,
    coalesce(cast(sum(case when fr_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL335F, --fr_onsite2close_56,
    coalesce(cast(sum(case when sc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL370F, --fr_onsite2close_56
    coalesce(cast(sum(case when sv_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL406F, --sv_onsite2close_56,
    coalesce(cast(sum(case when oc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL411F, --oc_onsite2close_56,
    coalesce(cast(sum(case when la_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL416F, --la_onsite2close_56,
    coalesce(cast(sum(case when vc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL496F, --vc_onsite2close_56
    coalesce(cast(sum(case when tx_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL580F, --tx_onsite2close_56
    coalesce(cast(sum(case when dl_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL548F, --dl_onsite2close_56
    coalesce(cast(sum(case when fw_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL560F, --fw_onsite2close_56
    coalesce(cast(sum(case when sd_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL570F, --sd_onsite2close_56
    coalesce(cast(sum(case when wa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wa_appr),0),0) as SAL667F, --wa_onsite2close_56
    coalesce(cast(sum(case when sj_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL668F, --sj_onsite2close_56
    coalesce(cast(sum(case when pa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL669F, --pa_onsite2close_56
    coalesce(cast(sum(case when st_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL670F, --st_onsite2close_56
    coalesce(cast(sum(case when md_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(md_appr),0),0) as SAL779F, -- md_onsite2close_56,
    coalesce(cast(sum(case when ne_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ne_appr),0),0) as SAL964F, -- ne_onsite2close_56,
    coalesce(cast(sum(case when bl_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(bl_appr),0),0) as SAL742F, -- bl_onsite2close_56,
    coalesce(cast(sum(case when se_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(se_appr),0),0) as SAL1055F, -- se_onsite2close_56,
    coalesce(cast(sum(case when dc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(dc_appr),0),0) as SAL762F, -- dc_onsite2close_56,
    coalesce(cast(sum(case when ph_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ph_appr),0),0) as SAL803F, -- ph_onsite2close_56,
    coalesce(cast(sum(case when pen_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(pen_appr),0),0) as SAL820F, -- ph_onsite2close_56,
    coalesce(cast(sum(case when ga_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ga_appr),0),0) as SAL843F, -- ga_onsite2close_56
    coalesce(cast(sum(case when va_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(va_appr),0),0) as SAL883F, -- va_onsite2close_56,
    coalesce(cast(sum(case when ar_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ar_appr),0),0) as SAL866F, -- ar_onsite2close_56
    coalesce(cast(sum(case when fl_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(fl_appr),0),0) as SAL904F, -- va_onsite2close_56,
    coalesce(cast(sum(case when mi_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(mi_appr),0),0) as SAL828F, -- ar_onsite2close_56
    coalesce(cast(sum(case when pa_wa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(pa_wa_appr),0),0) as SAL1077F, -- pa_wa_onsite2close_56,
    coalesce(cast(sum(case when or_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(or_appr),0),0) as SAL828F, -- ar_onsite2close_56,
    coalesce(cast(sum(case when wn_il_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wn_il_appr),0),0) as SAL1149F, -- wn_il_onsite2close_56,
    coalesce(cast(sum(case when wn_ch_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wn_ch_appr),0),0) as SAL1175F, -- wn_ch_onsite2close_56,
    coalesce(cast(sum(case when wn_na_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wn_na_appr),0),0) as SAL1198F, -- wn_na_onsite2close_56,
    coalesce(cast(sum(case when wn_la_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wn_la_appr),0),0) as SAL1221F -- wn_la_onsite2close_56,
from calc_last_booking
where
        created_at is not null
        and (is_commercial = 0 or is_commercial is null)  and code in ('remote_onsite', 'physical_onsite')
        and product_name = 'Fence'
group by 1
order by 1 desc
), hardscape_onsites as
(
select
        date_trunc('{period}', date)::date as date,
        -- Hardscape Onsites that took place this time period, #
    sum(norcal_appr) as SAL473D, --norcal_onsites
    sum(eb_appr) as SAL311D, -- eb_onsites,
    sum(at_appr) as SAL695D, -- at_onsites,
    sum(ht_appr) as SAL704D, -- ht_onsites,
    sum(sbsf_appr) as SAL312D, -- sbsf_onsites,
    sum(nb_appr) as SAL313D, -- nb_onsites,
    sum(sac_appr) as SAL314D, -- sac_onsites,
    sum(fr_appr) as SAL315D, -- fr_onsites,
    sum(sc_appr) as SAL366D, -- sc_onsites,
    sum(sv_appr) as SAL402D, -- sv_onsites,
    sum(oc_appr) as SAL407D, -- oc_onsites,
    sum(la_appr) as SAL412D, -- la_onsites,
    sum(vc_appr) as SAL492D, -- vc_onsites,
    sum(tx_appr) as SAL576D, -- tx_onsites,
    sum(dl_appr) as SAL544D, -- dl_onsites,
    sum(fw_appr) as SAL556D, -- fw_onsites,
    sum(sd_appr) as SAL566D, -- sd_onsites,
    sum(wa_appr) as SAL651D, -- wa_onsites,
    sum(sj_appr) as SAL652D, -- sj_onsites,
    sum(pa_appr) as SAL653D, -- pa_onsites,
    sum(st_appr) as SAL654D, -- st_onsites,
    -- Hardscape Physical onsites that took place this time period, #
    sum(case when norcal_appr = 1 then is_driveway_physical else 0 end) as SAL474D, -- eb_appr_physical,
    sum(case when eb_appr = 1 then is_driveway_physical else 0 end) as SAL316D, -- eb_appr_physical,
    sum(case when at_appr = 1 then is_driveway_physical else 0 end) as SAL696D, -- eb_appr_physical,
    sum(case when ht_appr = 1 then is_driveway_physical else 0 end) as SAL705D, -- eb_appr_physical,
    sum(case when sbsf_appr = 1 then is_driveway_physical else 0 end) as SAL317D, -- sbsf_appr_physical,
    sum(case when nb_appr = 1 then is_driveway_physical else 0 end) as SAL318D, -- nb_appr_physical,
    sum(case when sac_appr = 1 then is_driveway_physical else 0 end) as SAL319D, -- sac_appr_physical,
    sum(case when fr_appr = 1 then is_driveway_physical else 0 end) as SAL320D, -- fr_appr_physical,
    sum(case when sc_appr = 1 then is_driveway_physical else 0 end) as SAL367D, -- sc_appr_physical,
    sum(case when sv_appr = 1 then is_driveway_physical else 0 end) as SAL403D, -- sv_appr_physical,
    sum(case when oc_appr = 1 then is_driveway_physical else 0 end) as SAL408D, -- oc_appr_physical,
    sum(case when la_appr = 1 then is_driveway_physical else 0 end) as SAL413D, -- la_appr_physical,
    sum(case when vc_appr = 1 then is_driveway_physical else 0 end) as SAL493D, -- vc_appr_physical,
    sum(case when tx_appr = 1 then is_driveway_physical else 0 end) as SAL577D, -- tx_appr_physical,
    sum(case when dl_appr = 1 then is_driveway_physical else 0 end) as SAL545D, -- dl_appr_physical,
    sum(case when fw_appr = 1 then is_driveway_physical else 0 end) as SAL557D, -- fw_appr_physical,
    sum(case when sd_appr = 1 then is_driveway_physical else 0 end) as SAL567D, -- sd_appr_physical,
    sum(case when wa_appr = 1 then is_driveway_physical else 0 end) as SAL655D, -- wa_appr_physical,
    sum(case when sj_appr = 1 then is_driveway_physical else 0 end) as SAL656D, -- sj_appr_physical,
    sum(case when pa_appr = 1 then is_driveway_physical else 0 end) as SAL657D, -- pa_appr_physical,
    sum(case when st_appr = 1 then is_driveway_physical else 0 end) as SAL658D, -- st_appr_physical,
    -- Hardscape Onsites to Closes (14 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL475D, -- norcal_onsite2close_14,
    coalesce(cast(sum(case when eb_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL321D, -- eb_onsite2close_14,
    coalesce(cast(sum(case when at_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL697D, -- at_onsite2close_14,
    coalesce(cast(sum(case when ht_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL706D, -- ht_onsite2close_14,
    coalesce(cast(sum(case when sbsf_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL322D, -- sbsf_onsite2close_14,
    coalesce(cast(sum(case when nb_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL323D, -- nb_onsite2close_14,
    coalesce(cast(sum(case when sac_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) as SAL324D, -- sac_onsite2close_14,
    coalesce(cast(sum(case when fr_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL325D, -- fr_onsite2close_14,
    coalesce(cast(sum(case when sc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL368D, -- sc_onsite2close_14,
    coalesce(cast(sum(case when sv_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL404D, -- sv_onsite2close_14,
    coalesce(cast(sum(case when oc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL409D, -- oc_onsite2close_14,
    coalesce(cast(sum(case when la_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL414D, -- la_onsite2close_14,
    coalesce(cast(sum(case when vc_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL494D, -- vc_onsite2close_14,
    coalesce(cast(sum(case when tx_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL578D, -- tx_onsite2close_14,
    coalesce(cast(sum(case when dl_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL546D, -- dl_onsite2close_14,
    coalesce(cast(sum(case when fw_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL558D, -- fw_onsite2close_14,
    coalesce(cast(sum(case when sd_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL568D, -- wa_onsite2close_14,
    coalesce(cast(sum(case when sj_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL660D, -- sj_onsite2close_14,
    coalesce(cast(sum(case when pa_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL661D, -- pa_onsite2close_14,
    coalesce(cast(sum(case when st_appr = 1 then a14 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL662D, -- st_onsite2close_14,
    -- Hardscape Onsites to Closes (28 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL476D, -- norcal_onsite2close_28,
    coalesce(cast(sum(case when eb_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL326D, -- eb_onsite2close_28,
    coalesce(cast(sum(case when at_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL698D, -- ht_onsite2close_28,
    coalesce(cast(sum(case when ht_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL707D, -- ht_onsite2close_28,
    coalesce(cast(sum(case when sbsf_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL327D, -- sbsf_onsite2close_28,
    coalesce(cast(sum(case when nb_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL328D, -- nb_onsite2close_28,
    coalesce(cast(sum(case when sac_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) SAL329D, -- as sac_onsite2close_28,
    coalesce(cast(sum(case when fr_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL330D, -- fr_onsite2close_28,
    coalesce(cast(sum(case when sc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL369D, -- sc_onsite2close_28,
    coalesce(cast(sum(case when sv_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL405D, -- sv_onsite2close_28,
    coalesce(cast(sum(case when oc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL410D, -- oc_onsite2close_28,
    coalesce(cast(sum(case when la_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL415D, -- la_onsite2close_28,
    coalesce(cast(sum(case when vc_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL495D, -- vc_onsite2close_28,
    coalesce(cast(sum(case when tx_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL579D, -- tx_onsite2close_28,
    coalesce(cast(sum(case when dl_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL547D, -- dl_onsite2close_28,
    coalesce(cast(sum(case when fw_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL559D, -- fw_onsite2close_28,
    coalesce(cast(sum(case when sd_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL569D, -- sd_onsite2close_28,
    coalesce(cast(sum(case when wa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(wa_appr),0),0) as SAL663D, -- wa_onsite2close_28,
    coalesce(cast(sum(case when sj_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL664D, -- sj_onsite2close_28,
    coalesce(cast(sum(case when pa_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL665D, -- pa_onsite2close_28,
    coalesce(cast(sum(case when st_appr = 1 then a28 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL666D, -- st_onsite2close_28,
    -- Hardscape Onsites to Closes (56 days), %
    coalesce(cast(sum(case when norcal_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(norcal_appr),0),0) as SAL477D, -- norcal_onsite2close_56,
    coalesce(cast(sum(case when eb_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(eb_appr),0),0) as SAL331D, -- eb_onsite2close_56,
    coalesce(cast(sum(case when at_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(at_appr),0),0) as SAL699D, -- at_onsite2close_56,
    coalesce(cast(sum(case when ht_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(ht_appr),0),0) as SAL708D, -- ht_onsite2close_56,
    coalesce(cast(sum(case when sbsf_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sbsf_appr),0),0) as SAL332D, -- sbsf_onsite2close_56,
    coalesce(cast(sum(case when nb_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(nb_appr),0),0) as SAL333D, -- nb_onsite2close_56,
    coalesce(cast(sum(case when sac_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sac_appr),0),0) as SAL334D, -- sac_onsite2close_56,
    coalesce(cast(sum(case when fr_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(fr_appr),0),0) as SAL335D, --fr_onsite2close_56,
    coalesce(cast(sum(case when sc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sc_appr),0),0) as SAL370D, --fr_onsite2close_56
    coalesce(cast(sum(case when sv_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sv_appr),0),0) as SAL406D, --sv_onsite2close_56,
    coalesce(cast(sum(case when oc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(oc_appr),0),0) as SAL411D, --oc_onsite2close_56,
    coalesce(cast(sum(case when la_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(la_appr),0),0) as SAL416D, --la_onsite2close_56,
    coalesce(cast(sum(case when vc_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(vc_appr),0),0) as SAL496D, --vc_onsite2close_56
    coalesce(cast(sum(case when tx_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(tx_appr),0),0) as SAL580D, --tx_onsite2close_56
    coalesce(cast(sum(case when dl_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(dl_appr),0),0) as SAL548D, --dl_onsite2close_56
    coalesce(cast(sum(case when fw_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(fw_appr),0),0) as SAL560D, --fw_onsite2close_56
    coalesce(cast(sum(case when sd_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sd_appr),0),0) as SAL570D, --sd_onsite2close_56
    coalesce(cast(sum(case when wa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(wa_appr),0),0) as SAL667D, --wa_onsite2close_56
    coalesce(cast(sum(case when sj_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(sj_appr),0),0) as SAL668D, --sj_onsite2close_56
    coalesce(cast(sum(case when pa_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(pa_appr),0),0) as SAL669D, --pa_onsite2close_56
    coalesce(cast(sum(case when st_appr = 1 then a56 else 0 end) as decimal) / nullif(sum(st_appr),0),0) as SAL670D --st_onsite2close_56
from calc_last_booking
where
        created_at is not null
        and (is_commercial = 0 or is_commercial is null)  and code in ('remote_onsite', 'physical_onsite')
        and product_name = 'Driveway'
group by 1
order by 1 desc
)
select
    *
from fence_onsites left join hardscape_onsites using(date)
