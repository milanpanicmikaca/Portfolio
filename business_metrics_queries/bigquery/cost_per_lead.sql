-- upload to BQ
with timeseries as 
(
    select 
        date_trunc(date_array,{period}) as date,
        from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
    group by 1
),
cpl_spend_leads as
(
    select 
        date_trunc(created_at, {period}) as date,
        old_region,
        region,
        market_id,
        market,
        case when product like '%Fence Installation%' then 1 else 0 end as is_fence,
        case when product like '%Driveway Installation%' then 1 else 0 end as is_driveway,
        case when type = '/Commercial' then 1 else 0 end as is_commercial,
        sum(mktg_fee) as total_fee,
        sum(ha_fee) as ha_fee,
        sum(ha_ads_fee) as ha_ads_fee,
        sum(tt_fee) as tt_fee,
        sum(bo_fee) as bo_fee,
        sum(gg_fee + gg_gls_fee) as gg_fee,
        sum(fb_fee) as fb_fee, 
        sum(ba_fee) as ba_fee,
        sum(nd_fee) as nd_fee,
        sum(yelp_cpl_budget) as yelp_fee,
        sum(sdr_fee) as misc_fee, --currently only SDR and Lawson (deprecated) attributed as misc 31/03/2023
        sum(case when channel like '%/Paid/%' and lead_id is not null then 1 else 0 end) as total_leads,
        sum(case when channel like '%Home Advisor%' and channel not like '%Home Advisor/Ads' and lead_id is not null then 1 else 0 end) as ha_leads,
        sum(case when channel like '%Home Advisor/Ads' and lead_id is not null then 1 else 0 end) as ha_ads_leads,
        sum(case when channel like '%Thumbtack%' and lead_id is not null then 1 else 0 end) as tt_leads,
        sum(case when channel like '%Borg%' and lead_id is not null then 1 else 0 end) as bo_leads,
        sum(case when channel like '%/Paid/Google%' and lead_id is not null then 1 else 0 end) as gg_leads,
        sum(case when channel like '%/Paid/Facebook%' and lead_id is not null then 1 else 0 end) as fb_leads,
        sum(case when channel like '%Bark%' and lead_id is not null then 1 else 0 end) as ba_leads,
        sum(case when channel like '%Nextdoor%' and lead_id is not null then 1 else 0 end) as nd_leads,
        sum(case when channel like '%Yelp%' and lead_id is not null then 1 else 0 end) as yelp_leads,
        sum(case when channel like '%/Paid/Misc%' and lead_id is not null then 1 else 0 end) as misc_leads
    from int_data.order_ue_materialized
    group by 1,2,3,4,5,6,7,8
),
cpl_sdr as
(
    select 
        date_trunc(created_at,{period}) as date,
        sum(mktg_fee)/count(lead_id) as MAR2209
    from int_data.order_ue_materialized ue
    where channel like "%sdr%"
    group by 1
),
cpls_driveway_commercial as 
( 
    select
        date, 
        ----DRIVEWAY TOTAL
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then total_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then total_leads end),0),0) as MAR652D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then total_leads end),0),0) as MAR402D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then total_leads end),0),0) as MAR404D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then total_leads end),0),0) as MAR405D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then total_leads end),0),0) as MAR867D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then total_leads end),0),0) as MAR406D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then total_leads end),0),0) as MAR864D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then total_leads end),0),0) as MAR865D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then total_leads end),0),0) as MAR866D, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then total_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then total_leads end),0),0) as MAR488D, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then total_leads end),0),0) as MAR583D, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then total_leads end),0),0) as MAR590D, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then total_leads end),0),0) as MAR597D, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then total_leads end),0),0) as MAR701D, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then total_leads end),0),0) as MAR777D, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_driveway = 1 then total_fee end) / nullif(sum(case when market like '%-TX-%' and is_driveway = 1 then total_leads end),0),0) as MAR784D, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 16 and is_driveway = 1 then total_leads end),0),0) as MAR785D, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_driveway = 1 then total_fee end) / nullif(sum(case when market_id = 17 and is_driveway = 1 then total_leads end),0),0) as MAR786D, -- total_fw_cpl,
        ---COMMERCIAL TOTAL
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then total_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then total_leads end),0),0) as MAR652C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then total_leads end),0),0) as MAR402C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then total_leads end),0),0) as MAR404C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then total_leads end),0),0) as MAR405C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then total_leads end),0),0) as MAR867C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then total_leads end),0),0) as MAR406C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then total_leads end),0),0) as MAR864C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then total_leads end),0),0) as MAR865C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then total_leads end),0),0) as MAR866C, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_commercial = 1 then total_fee end) / nullif(sum(case when old_region = 'South California' and is_commercial = 1 then total_leads end),0),0) as MAR488C, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 14 and is_commercial = 1 then total_leads end),0),0) as MAR583C, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 5 and is_commercial = 1 then total_leads end),0),0) as MAR590C, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 6 and is_commercial = 1 then total_leads end),0),0) as MAR597C, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 7 and is_commercial = 1 then total_leads end),0),0) as MAR701C, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 1 and is_commercial = 1 then total_leads end),0),0) as MAR777C, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-TX-%' and is_commercial = 1 then total_leads end),0),0) as MAR784C, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 16 and is_commercial = 1 then total_leads end),0),0) as MAR785C, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 17 and is_commercial = 1 then total_leads end),0),0) as MAR786C, -- total_fw_cpl,
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then total_leads end),0),0) as MAR1204C, -- total_md_cpl,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then total_leads end),0),0) as MAR1134C, -- total_bl_cpl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then total_leads end),0),0) as MAR1175C, -- total_dc_cpl,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then total_leads end),0),0) as MAR1548C, -- total_va_cpl,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 35 and is_commercial = 1 then total_leads end),0),0) as MAR1518C, -- total_ar_cpl,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then total_leads end),0),0) as MAR1592C, -- total_fl_cpl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then total_leads end),0),0) as MAR1628C, -- total_mi_cpl,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then total_leads end),0),0) as MAR1676C, -- total_or_cpl,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then total_leads end),0),0) as MAR1269C, -- total_pen_cpl,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then total_leads end),0),0) as MAR1239C, -- total_ph_cpl,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then total_leads end),0),0) as MAR1306C, -- total_ga_cpl,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then total_leads end),0),0) as MAR2286C, -- total_pa_wa_cpl,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then total_leads end),0),0) as MAR2244C, -- total_se_cpl,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then total_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then total_leads end),0),0) as MAR2870C, -- total_ilinois_cpl,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then total_leads end),0),0) as MAR2939C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then total_leads end),0),0) as MAR2998C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then total_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then total_leads end),0),0) as MAR3057C, -- total_wn_il_la_cpl,
        --coalesce(avg(case when market_id in (4,30,31)and is_driveway = 1 and total_fee > 0 then total_fee  end),0) as MAR403D, -- total_sb_cpl, --deprecated
        --coalesce(avg(case when market_id in (4,30,31)and is_commercial = 1 and total_fee > 0 then total_fee  end),0) as MAR403C, -- total_sb_cpl, --deprecated
        --------- DRIVEWAY HA ---------
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then ha_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then ha_leads end),0),0) as MAR653D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then ha_leads end),0),0) as MAR409D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then ha_leads end),0),0) as MAR411D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then ha_leads end),0),0) as MAR412D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then ha_leads end),0),0) as MAR871D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then ha_leads end),0),0) as MAR413D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then ha_leads end),0),0) as MAR868D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then ha_leads end),0),0) as MAR869D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then ha_leads end),0),0) as MAR870D, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then ha_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then ha_leads end),0),0) as MAR489D, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then ha_leads end),0),0) as MAR584D, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then ha_leads end),0),0) as MAR591D, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then ha_leads end),0),0) as MAR598D, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then ha_leads end),0),0) as MAR696D, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then ha_leads end),0),0) as MAR778D, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_driveway = 1 then ha_fee end) / nullif(sum(case when market like '%-TX-%' and is_driveway = 1 then ha_leads end),0),0) as MAR787D, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 16 and is_driveway = 1 then ha_leads end),0),0) as MAR788D, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_driveway = 1 then ha_fee end) / nullif(sum(case when market_id = 17 and is_driveway = 1 then ha_leads end),0),0) as MAR789D, -- total_fw_cpl,
        ----------COMMERCIAL HA ----------
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then ha_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then ha_leads end),0),0) as MAR653C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then ha_leads end),0),0) as MAR409C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then ha_leads end),0),0) as MAR411C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then ha_leads end),0),0) as MAR412C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then ha_leads end),0),0) as MAR871C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then ha_leads end),0),0) as MAR413C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then ha_leads end),0),0) as MAR868C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then ha_leads end),0),0) as MAR869C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then ha_leads end),0),0) as MAR870C, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_commercial = 1 then ha_fee end) / nullif(sum(case when old_region = 'South California' and is_commercial = 1 then ha_leads end),0),0) as MAR489C, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 14 and is_commercial = 1 then ha_leads end),0),0) as MAR584C, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 5 and is_commercial = 1 then ha_leads end),0),0) as MAR591C, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 6 and is_commercial = 1 then ha_leads end),0),0) as MAR598C, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 7 and is_commercial = 1 then ha_leads end),0),0) as MAR696C, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 1 and is_commercial = 1 then ha_leads end),0),0) as MAR778C, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-TX-%' and is_commercial = 1 then ha_leads end),0),0) as MAR787C, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 16 and is_commercial = 1 then ha_leads end),0),0) as MAR788C, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 17 and is_commercial = 1 then ha_leads end),0),0) as MAR789C, -- total_fw_cpl,
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then ha_leads end),0),0) as MAR1205C, -- total_md_cpl,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then ha_leads end),0),0) as MAR1136C, -- total_bl_cpl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then ha_leads end),0),0) as MAR1177C, -- total_dc_cpl,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then ha_leads end),0),0) as MAR1549C, -- total_va_cpl,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 35 and is_commercial = 1 then ha_leads end),0),0) as MAR1519C, -- total_ar_cpl,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then ha_leads end),0),0) as MAR1593C, -- total_fl_cpl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then ha_leads end),0),0) as MAR1629C, -- total_mi_cpl,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then ha_leads end),0),0) as MAR1677C, -- total_or_cpl,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then ha_leads end),0),0) as MAR1270C, -- total_pen_cpl,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then ha_leads end),0),0) as MAR1240C, -- total_ph_cpl,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then ha_leads end),0),0) as MAR1307C, -- total_ga_cpl,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then ha_leads end),0),0) as MAR2287C, -- total_pa_wa_cpl,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then ha_leads end),0),0) as MAR2254C, -- total_se_cpl,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then ha_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then ha_leads end),0),0) as MAR2871C, -- total_illinois_cpl,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then ha_leads end),0),0) as MAR2930C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then ha_leads end),0),0) as MAR2989C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then ha_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then ha_leads end),0),0) as MAR3048C, -- total_wn_il_la_cpl,
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and ha_fee > 0 then ha_fee  end),0) as MAR410D, -- ha_cpl_sb, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_commercial = 1 and ha_fee > 0 then ha_fee  end),0) as MAR410C, -- ha_cpl_sb, --deprecated
        ---------DRIVEWAY FB ---------
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then fb_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then fb_leads end),0),0) as MAR656D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then fb_leads end),0),0) as MAR424D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then fb_leads end),0),0) as MAR426D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then fb_leads end),0),0) as MAR427D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then fb_leads end),0),0) as MAR875D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then fb_leads end),0),0) as MAR428D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then fb_leads end),0),0) as MAR872D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then fb_leads end),0),0) as MAR873D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then fb_leads end),0),0) as MAR874D, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then fb_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then fb_leads end),0),0) as MAR492D, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then fb_leads end),0),0) as MAR587D, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then fb_leads end),0),0) as MAR594D, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then fb_leads end),0),0) as MAR601D, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then fb_leads end),0),0) as MAR698D, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then fb_leads end),0),0) as MAR781D, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_driveway = 1 then fb_fee end) / nullif(sum(case when market like '%-TX-%' and is_driveway = 1 then fb_leads end),0),0) as MAR790D, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 16 and is_driveway = 1 then fb_leads end),0),0) as MAR791D, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_driveway = 1 then fb_fee end) / nullif(sum(case when market_id = 17 and is_driveway = 1 then fb_leads end),0),0) as MAR792D, -- total_fw_cpl
        ---------COMMERCIAL FB ---------
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then fb_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then fb_leads end),0),0) as MAR656C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then fb_leads end),0),0) as MAR424C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then fb_leads end),0),0) as MAR426C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then fb_leads end),0),0) as MAR427C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then fb_leads end),0),0) as MAR875C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then fb_leads end),0),0) as MAR428C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then fb_leads end),0),0) as MAR872C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then fb_leads end),0),0) as MAR873C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then fb_leads end),0),0) as MAR874C, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_commercial = 1 then fb_fee end) / nullif(sum(case when old_region = 'South California' and is_commercial = 1 then fb_leads end),0),0) as MAR492C, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 14 and is_commercial = 1 then fb_leads end),0),0) as MAR587C, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 5 and is_commercial = 1 then fb_leads end),0),0) as MAR594C, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 6 and is_commercial = 1 then fb_leads end),0),0) as MAR601C, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 7 and is_commercial = 1 then fb_leads end),0),0) as MAR698C, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 1 and is_commercial = 1 then fb_leads end),0),0) as MAR781C, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-TX-%' and is_commercial = 1 then fb_leads end),0),0) as MAR790C, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 16 and is_commercial = 1 then fb_leads end),0),0) as MAR791C, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 17 and is_commercial = 1 then fb_leads end),0),0) as MAR792C, -- total_fw_cpl
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then fb_leads end),0),0) as MAR1208C, -- fb_cpl_md,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then fb_leads end),0),0) as MAR1139C, -- fb_cpl_bl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then fb_leads end),0),0) as MAR1180C, -- fb_cpl_dc,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then fb_leads end),0),0) as MAR1552C, -- fb_cpl_va,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 355 and is_commercial = 1 then fb_leads end),0),0) as MAR1522C, -- fb_cpl_ar,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then fb_leads end),0),0) as MAR1596C, -- fb_cpl_fl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then fb_leads end),0),0) as MAR1632C, -- fb_cpl_mi,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then fb_leads end),0),0) as MAR1680C, -- fb_cpl_or,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then fb_leads end),0),0) as MAR1273C, -- fb_cpl_pen,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then fb_leads end),0),0) as MAR1243C, -- fb_cpl_ph,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then fb_leads end),0),0) as MAR1310C, -- fb_cpl_ga,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then fb_leads end),0),0) as MAR2290C, -- fb_cpl_pa_wa,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then fb_leads end),0),0) as MAR2248C, -- fb_cpl_se,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then fb_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then fb_leads end),0),0) as MAR2874C, -- fb_cpl_illinois,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then fb_leads end),0),0) as MAR2933C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then fb_leads end),0),0) as MAR2992C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then fb_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then fb_leads end),0),0) as MAR3051C, -- total_wn_il_la_cpl,
        ---------DRIVEWAY TT ---------
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then tt_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then tt_leads end),0),0) as MAR654D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then tt_leads end),0),0) as MAR414D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then tt_leads end),0),0) as MAR416D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then tt_leads end),0),0) as MAR417D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then tt_leads end),0),0) as MAR879D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then tt_leads end),0),0) as MAR418D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then tt_leads end),0),0) as MAR876D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then tt_leads end),0),0) as MAR877D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then tt_leads end),0),0) as MAR878D, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then tt_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then tt_leads end),0),0) as MAR490D, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then tt_leads end),0),0) as MAR585D, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then tt_leads end),0),0) as MAR592D, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then tt_leads end),0),0) as MAR599D, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then tt_leads end),0),0) as MAR697D, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then tt_leads end),0),0) as MAR779D, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_driveway = 1 then tt_fee end) / nullif(sum(case when market like '%-TX-%' and is_driveway = 1 then tt_leads end),0),0) as MAR793D, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 16 and is_driveway = 1 then tt_leads end),0),0) as MAR794D, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_driveway = 1 then tt_fee end) / nullif(sum(case when market_id = 17 and is_driveway = 1 then tt_leads end),0),0) as MAR795D, -- total_fw_cpl
        ---------COMMERCIAL TT ---------
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then tt_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then tt_leads end),0),0) as MAR654C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then tt_leads end),0),0) as MAR414C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then tt_leads end),0),0) as MAR416C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then tt_leads end),0),0) as MAR417C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then tt_leads end),0),0) as MAR879C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then tt_leads end),0),0) as MAR418C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then tt_leads end),0),0) as MAR876C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then tt_leads end),0),0) as MAR877C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then tt_leads end),0),0) as MAR878C, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_commercial = 1 then tt_fee end) / nullif(sum(case when old_region = 'South California' and is_commercial = 1 then tt_leads end),0),0) as MAR490C, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 14 and is_commercial = 1 then tt_leads end),0),0) as MAR585C, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 5 and is_commercial = 1 then tt_leads end),0),0) as MAR592C, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 6 and is_commercial = 1 then tt_leads end),0),0) as MAR599C, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 7 and is_commercial = 1 then tt_leads end),0),0) as MAR697C, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 1 and is_commercial = 1 then tt_leads end),0),0) as MAR779C, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-TX-%' and is_commercial = 1 then tt_leads end),0),0) as MAR793C, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 16 and is_commercial = 1 then tt_leads end),0),0) as MAR794C, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 17 and is_commercial = 1 then tt_leads end),0),0) as MAR795C, -- total_fw_cpl
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then tt_leads end),0),0) as MAR1206C, -- fb_cpl_md,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then tt_leads end),0),0) as MAR1137C, -- fb_cpl_bl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then tt_leads end),0),0) as MAR1178C, -- fb_cpl_dc,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then tt_leads end),0),0) as MAR1550C, -- fb_cpl_va,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 355 and is_commercial = 1 then tt_leads end),0),0) as MAR1520C, -- fb_cpl_ar,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then tt_leads end),0),0) as MAR1594C, -- fb_cpl_fl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then tt_leads end),0),0) as MAR1630C, -- fb_cpl_mi,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then tt_leads end),0),0) as MAR1678C, -- fb_cpl_or,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then tt_leads end),0),0) as MAR1271C, -- fb_cpl_pen,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then tt_leads end),0),0) as MAR1241C, -- fb_cpl_ph,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then tt_leads end),0),0) as MAR1308C, -- fb_cpl_ga,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then tt_leads end),0),0) as MAR2288C, -- fb_cpl_pa_wa,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then tt_leads end),0),0) as MAR2246C, -- fb_cpl_se,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then tt_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then tt_leads end),0),0) as MAR2872C, -- total_illinois_cpl,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then tt_leads end),0),0) as MAR2931C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then tt_leads end),0),0) as MAR2990C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then tt_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then tt_leads end),0),0) as MAR3049C, -- total_wn_il_la_cpl,
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and tt_fee > 0 then tt_fee  end) , 0) as MAR415D, -- tt_cpl_sb, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_commercial = 1 and tt_fee > 0 then tt_fee  end) , 0) as MAR415C, -- tt_cpl_sb, --deprecated
        ---------DRIVEWAY Google ---------
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then gg_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then gg_leads end),0),0) as MAR655D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then gg_leads end),0),0) as MAR419D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then gg_leads end),0),0) as MAR421D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then gg_leads end),0),0) as MAR422D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then gg_leads end),0),0) as MAR883D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then gg_leads end),0),0) as MAR423D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then gg_leads end),0),0) as MAR880D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then gg_leads end),0),0) as MAR881D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then gg_leads end),0),0) as MAR882D, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then gg_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then gg_leads end),0),0) as MAR491D, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then gg_leads end),0),0) as MAR586D, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then gg_leads end),0),0) as MAR593D, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then gg_leads end),0),0) as MAR600D, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then gg_leads end),0),0) as MAR693D, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then gg_leads end),0),0) as MAR780D, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_driveway = 1 then gg_fee end) / nullif(sum(case when market like '%-TX-%' and is_driveway = 1 then gg_leads end),0),0) as MAR796D, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 16 and is_driveway = 1 then gg_leads end),0),0) as MAR797D, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_driveway = 1 then gg_fee end) / nullif(sum(case when market_id = 17 and is_driveway = 1 then gg_leads end),0),0) as MAR798D, -- total_fw_cpl
        ---------COMMERCIAL Google ---------
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then gg_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then gg_leads end),0),0) as MAR655C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then gg_leads end),0),0) as MAR419C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then gg_leads end),0),0) as MAR421C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then gg_leads end),0),0) as MAR422C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then gg_leads end),0),0) as MAR883C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then gg_leads end),0),0) as MAR423C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then gg_leads end),0),0) as MAR880C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then gg_leads end),0),0) as MAR881C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then gg_leads end),0),0) as MAR882C, -- total_pa_cpl
        coalesce(sum(case when old_region = 'South California' and is_commercial = 1 then gg_fee end) / nullif(sum(case when old_region = 'South California' and is_commercial = 1 then gg_leads end),0),0) as MAR491C, -- total_sc_cpl,
        coalesce(sum(case when market_id = 14 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 14 and is_commercial = 1 then gg_leads end),0),0) as MAR586C, -- total_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 5 and is_commercial = 1 then gg_leads end),0),0) as MAR593C, -- total_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 6 and is_commercial = 1 then gg_leads end),0),0) as MAR600C, -- total_la_cpl,
        coalesce(sum(case when market_id = 7 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 7 and is_commercial = 1 then gg_leads end),0),0) as MAR693C, -- total_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 1 and is_commercial = 1 then gg_leads end),0),0) as MAR780C, -- total_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-TX-%' and is_commercial = 1 then gg_leads end),0),0) as MAR796C, -- total_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 16 and is_commercial = 1 then gg_leads end),0),0) as MAR797C, -- total_dl_cpl,
        coalesce(sum(case when market_id = 17 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 17 and is_commercial = 1 then gg_leads end),0),0) as MAR798C, -- total_fw_cpl
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then gg_leads end),0),0) as MAR1207C, -- fb_cpl_md,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then gg_leads end),0),0) as MAR1138C, -- fb_cpl_bl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then gg_leads end),0),0) as MAR1179C, -- fb_cpl_dc,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then gg_leads end),0),0) as MAR1551C, -- fb_cpl_va,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 355 and is_commercial = 1 then gg_leads end),0),0) as MAR1521C, -- fb_cpl_ar,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then gg_leads end),0),0) as MAR1595C, -- fb_cpl_fl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then gg_leads end),0),0) as MAR1631C, -- fb_cpl_mi,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then gg_leads end),0),0) as MAR1679C, -- fb_cpl_or,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then gg_leads end),0),0) as MAR1272C, -- fb_cpl_pen,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then gg_leads end),0),0) as MAR1242C, -- fb_cpl_ph,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then gg_leads end),0),0) as MAR1309C, -- fb_cpl_ga,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then gg_leads end),0),0) as MAR2289C, -- fb_cpl_pa_wa,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then gg_leads end),0),0) as MAR2247C, -- fb_cpl_se,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then gg_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then gg_leads end),0),0) as MAR2873C, -- total_illinois_cpl,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then gg_leads end),0),0) as MAR2932C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then gg_leads end),0),0) as MAR2991C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then gg_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then gg_leads end),0),0) as MAR3050C, -- total_wn_il_la_cpl,
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and gg_fee > 0 then gg_fee  end) ,0) as MAR420D, -- gg_cpl_sb, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_commercial = 1 and gg_fee > 0 then gg_fee  end) ,0) as MAR420C, -- gg_cpl_sb, --deprecated
        ---------DRIVEWAY BORG ---------
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then bo_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then bo_leads end),0),0) as MAR691D, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then bo_leads end),0),0) as MAR706D, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then bo_leads end),0),0) as MAR707D, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then bo_leads end),0),0) as MAR708D, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then bo_leads end),0),0) as MAR887D, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then bo_leads end),0),0) as MAR709D, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then bo_leads end),0),0) as MAR884D, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then bo_leads end),0),0) as MAR885D, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then bo_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then bo_leads end),0),0) as MAR886D, -- total_pa_cpl
        ---------COMMERCIAL BORG ---------
        coalesce(sum(case when old_region = 'North California' and is_commercial = 1 then bo_fee end) / nullif(sum(case when old_region = 'North California' and is_commercial = 1 then bo_leads end),0),0) as MAR691C, -- total_nc_cpl,
        coalesce(sum(case when market_id = 2 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 2 and is_commercial = 1 then bo_leads end),0),0) as MAR706C, -- total_eb_cpl,
        coalesce(sum(case when market_id = 9 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 9 and is_commercial = 1 then bo_leads end),0),0) as MAR707C, -- total_nb_cpl,
        coalesce(sum(case when market_id = 3 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 3 and is_commercial = 1 then bo_leads end),0),0) as MAR708C, -- total_sac_cpl,
        coalesce(sum(case when market_id = 29 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 29 and is_commercial = 1 then bo_leads end),0),0) as MAR887C, -- total_st_cpl
        coalesce(sum(case when market_id = 10 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 10 and is_commercial = 1 then bo_leads end),0),0) as MAR709C, -- total_fr_cpl,
        coalesce(sum(case when market_id = 4 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 4 and is_commercial = 1 then bo_leads end),0),0) as MAR884C, -- total_wa_cpl
        coalesce(sum(case when market_id = 30 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 30 and is_commercial = 1 then bo_leads end),0),0) as MAR885C, -- total_sj_cpl
        coalesce(sum(case when market_id = 31 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 31 and is_commercial = 1 then bo_leads end),0),0) as MAR886C, -- total_pa_cpl
        coalesce(sum(case when market like '%-MD-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like '%-MD-%' and is_commercial = 1 then bo_leads end),0),0) as MAR1190C, -- fb_cpl_md,
        coalesce(sum(case when market_id = 22 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 22 and is_commercial = 1 then bo_leads end),0),0) as MAR1145C, -- fb_cpl_bl,
        coalesce(sum(case when market_id = 21 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 21 and is_commercial = 1 then bo_leads end),0),0) as MAR1186C, -- fb_cpl_dc,
        coalesce(sum(case when market like '%-VA-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like '%-VA-%' and is_commercial = 1 then bo_leads end),0),0) as MAR1564C, -- fb_cpl_va,
        coalesce(sum(case when market_id = 35 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 355 and is_commercial = 1 then bo_leads end),0),0) as MAR1534C, -- fb_cpl_ar,
        coalesce(sum(case when market like '%-FL-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like '%-FL-%' and is_commercial = 1 then bo_leads end),0),0) as MAR1608C, -- fb_cpl_fl,
        coalesce(sum(case when market_id = 24 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 24 and is_commercial = 1 then bo_leads end),0),0) as MAR1644C, -- fb_cpl_mi,
        coalesce(sum(case when market_id = 26 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 26 and is_commercial = 1 then bo_leads end),0),0) as MAR1692C, -- fb_cpl_or,
        coalesce(sum(case when market like '%-PA-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like '%-PA-%' and is_commercial = 1 then bo_leads end),0),0) as MAR1285C, -- fb_cpl_pen,
        coalesce(sum(case when market_id = 33 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 33 and is_commercial = 1 then bo_leads end),0),0) as MAR1255C, -- fb_cpl_ph,
        coalesce(sum(case when market like '%-GA-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like '%-GA-%' and is_commercial = 1 then bo_leads end),0),0) as MAR1322C, -- fb_cpl_ga,
        coalesce(sum(case when market like 'PA-WA-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_commercial = 1 then bo_leads end),0),0) as MAR2302C, -- fb_cpl_pa_wa,
        coalesce(sum(case when market_id = 43 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 43 and is_commercial = 1 then bo_leads end),0),0) as MAR2261C, -- fb_cpl_se,
        coalesce(sum(case when market like 'WN-IL-%' and is_commercial = 1 then bo_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_commercial = 1 then bo_leads end),0),0) as MAR2886C, -- total_illinois_cpl,
        coalesce(sum(case when market_id = 42 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 42 and is_commercial = 1 then bo_leads end),0),0) as MAR2945C, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 57 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 57 and is_commercial = 1 then bo_leads end),0),0) as MAR3004C, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 58 and is_commercial = 1 then bo_fee end) / nullif(sum(case when market_id = 58 and is_commercial = 1 then bo_leads end),0),0) as MAR3063C, -- total_wn_il_la_cpl,
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and bo_fee > 0 then bo_fee  end) ,0) as MAR692D, -- bo_cpl_sb, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_commercial = 1 and bo_fee > 0 then bo_fee  end) ,0) as MAR692C, -- bo_cpl_sb, --deprecated
                        --Hardscape Yelp, Misc
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then yelp_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then yelp_leads end),0),0) as MAR657D, -- yelp_nc
        coalesce(sum(case when old_region = 'North California' and is_driveway = 1 then misc_fee end) / nullif(sum(case when old_region = 'North California' and is_driveway = 1 then misc_leads end),0),0) as MAR658D, -- misc_nc
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then yelp_leads end),0),0) as MAR429D, --yelp_eb
        coalesce(sum(case when market_id = 2 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 2 and is_driveway = 1 then misc_leads end),0),0) as MAR434D, --misc_eb
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then yelp_leads end),0),0) as MAR431D, --yelp_nb
        coalesce(sum(case when market_id = 9 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 9 and is_driveway = 1 then misc_leads end),0),0) as MAR436D, --misc_nb
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then yelp_leads end),0),0) as MAR432D, --yelp_sac
        coalesce(sum(case when market_id = 3 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 3 and is_driveway = 1 then misc_leads end),0),0) as MAR437D, --misc_sac
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then yelp_leads end),0),0) as MAR915D, --yelp_st
        coalesce(sum(case when market_id = 29 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 29 and is_driveway = 1 then misc_leads end),0),0) as MAR919D, --misc_st
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then yelp_leads end),0),0) as MAR433D, --yelp_fr
        coalesce(sum(case when market_id = 10 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 10 and is_driveway = 1 then misc_leads end),0),0) as MAR438D, --misc_fr
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then yelp_leads end),0),0) as MAR912D, --yelp_wa
        coalesce(sum(case when market_id = 4 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 4 and is_driveway = 1 then misc_leads end),0),0) as MAR916D, --misc-wa
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then yelp_leads end),0),0) as MAR913D,-- yelp_sj
        coalesce(sum(case when market_id = 30 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 30 and is_driveway = 1 then misc_leads end),0),0) as MAR917D, --misc_sj
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then yelp_leads end),0),0) as MAR914D, -- yelp_pa
        coalesce(sum(case when market_id = 31 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 31 and is_driveway = 1 then misc_leads end),0),0) as MAR918D, --misc_pa
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then yelp_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then yelp_leads end),0),0) as MAR493D, -- yelp_nc
        coalesce(sum(case when old_region = 'South California' and is_driveway = 1 then misc_fee end) / nullif(sum(case when old_region = 'South California' and is_driveway = 1 then misc_leads end),0),0) as MAR494D, -- misc_nc
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then yelp_leads end),0),0) as MAR588D,
        coalesce(sum(case when market_id = 14 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 14 and is_driveway = 1 then misc_leads end),0),0) as MAR589D,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then yelp_leads end),0),0) as MAR595D,
        coalesce(sum(case when market_id = 5 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 5 and is_driveway = 1 then misc_leads end),0),0) as MAR596D,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then yelp_leads end),0),0) as MAR602D,
        coalesce(sum(case when market_id = 6 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 6 and is_driveway = 1 then misc_leads end),0),0) as MAR603D,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then yelp_leads end),0),0) as MAR699D,
        coalesce(sum(case when market_id = 7 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 7 and is_driveway = 1 then misc_leads end),0),0) as MAR700D,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then yelp_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then yelp_leads end),0),0) as MAR782D,
        coalesce(sum(case when market_id = 1 and is_driveway = 1 then misc_fee end) / nullif(sum(case when market_id = 1 and is_driveway = 1 then misc_leads end),0),0) as MAR783D
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and yelp_fee > 0 then yelp_fee  end),0) as MAR430D, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_driveway = 1 and misc_fee > 0 then misc_fee  end),0) as MAR435D, --deprecated
    from cpl_spend_leads r
    group by 1
),
cpls_fence as
(
    select 
        date,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then total_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then total_leads end),0),0) as MAR652F, -- total_nc_cpl,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then ha_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then ha_leads end),0),0) as MAR653F, -- ha_cpl_nc,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then tt_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then tt_leads end),0),0) as MAR654F, -- tt_cpl_nc,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then bo_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then bo_leads end),0),0) as MAR691F, -- bo_cpl_nc,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then gg_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then gg_leads end),0),0) as MAR655F, -- gg_cpl_nc,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then fb_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then fb_leads end),0),0) as MAR656F, -- fb_cpl_nc,
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then ba_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then ba_leads end),0),0) as MAR1325F, -- ba_cpl_nc
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then nd_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then nd_leads end),0),0) as MAR1326F, --nd_cpl_nc
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then yelp_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then yelp_leads end),0),0) as MAR657F, --yelp_cpl_nc
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then misc_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then misc_leads end),0),0) as MAR658F, --misc_cpl_nc
        coalesce(sum(case when market_id = 2 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then total_leads end),0),0) as MAR402F, -- total_eb_cpl,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then ha_leads end),0),0) as MAR409F, -- ha_cpl_eb,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then tt_leads end),0),0) as MAR414F, -- tt_cpl_eb,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then bo_leads end),0),0) as MAR706F, -- bo_cpl_eb,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then gg_leads end),0),0) as MAR419F, -- gg_cpl_eb,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then fb_leads end),0),0) as MAR424F, -- fb_cpl_eb,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then ba_leads end),0),0) as MAR1331F, -- ba_cpl_eb
        coalesce(sum(case when market_id = 2 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then nd_leads end),0),0) as MAR1332F, --nd_cpl_eb
        coalesce(sum(case when market_id = 2 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then yelp_leads end),0),0) as MAR429F, --yelp_cpl_eb
        coalesce(sum(case when market_id = 2 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then misc_leads end),0),0) as MAR434F, --misc_cpl_eb
        coalesce(sum(case when market_id = 8 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then total_leads end),0),0) as MAR2480F, -- total_sf_cpl,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then ha_leads end),0),0) as MAR2481F, -- ha_cpl_sf,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then tt_leads end),0),0) as MAR2482F, -- tt_cpl_sf,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then bo_leads end),0),0) as MAR2494F, -- bo_cpl_sf,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then gg_leads end),0),0) as MAR2483F, -- gg_cpl_sf,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then fb_leads end),0),0) as MAR2484F, -- fb_cpl_sf,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then ba_leads end),0),0) as MAR2496F, -- ba_cpl_sf
        coalesce(sum(case when market_id = 8 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then nd_leads end),0),0) as MAR2497F, --nd_cpl_sf
        coalesce(sum(case when market_id = 8 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then yelp_leads end),0),0) as MAR2485F, --yelp_cpl_sf
        coalesce(sum(case when market_id = 8 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then misc_leads end),0),0) as MAR2486F, --misc_cpl_sf
        coalesce(sum(case when market_id = 9 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then total_leads end),0),0) as MAR404F, -- total_nb_cpl,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then ha_leads end),0),0) as MAR411F, -- ha_cpl_nb,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then tt_leads end),0),0) as MAR416F, -- tt_cpl_nb,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then bo_leads end),0),0) as MAR707F, -- bo_cpl_nb,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then gg_leads end),0),0) as MAR421F, -- gg_cpl_nb,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then fb_leads end),0),0) as MAR426F, -- fb_cpl_nb,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then ba_leads end),0),0) as MAR1343F, -- ba_cpl_nb
        coalesce(sum(case when market_id = 9 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then nd_leads end),0),0) as MAR1344F, --nd_cpl_nb
        coalesce(sum(case when market_id = 9 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then yelp_leads end),0),0) as MAR431F, --yelp_cpl_nb
        coalesce(sum(case when market_id = 9 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then misc_leads end),0),0) as MAR436F, --misc_cpl_nb
        coalesce(sum(case when market_id = 3 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then total_leads end),0),0) as MAR405F, -- total_sac_cpl,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then ha_leads end),0),0) as MAR412F, -- ha_cpl_sac,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then tt_leads end),0),0) as MAR417F, -- tt_cpl_sac,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then bo_leads end),0),0) as MAR708F, -- bo_cpl_sac,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then gg_leads end),0),0) as MAR422F, -- gg_cpl_sac,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then fb_leads end),0),0) as MAR427F, -- fb_cpl_sac,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then ba_leads end),0),0) as MAR1349F, -- ba_cpl_sac
        coalesce(sum(case when market_id = 3 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then nd_leads end),0),0) as MAR1350F, --nd_cpl_sac
        coalesce(sum(case when market_id = 3 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then yelp_leads end),0),0) as MAR432F, --yelp_cpl_sac
        coalesce(sum(case when market_id = 3 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then misc_leads end),0),0) as MAR437F, --misc_cpl_sac
        coalesce(sum(case when market_id = 29 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then total_leads end),0),0) as MAR867F, -- total_st_cpl,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then ha_leads end),0),0) as MAR871F, -- ha_cpl_st,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then tt_leads end),0),0) as MAR879F, -- tt_cpl_st,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then bo_leads end),0),0) as MAR887F, -- bo_cpl_st,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then gg_leads end),0),0) as MAR883F, -- gg_cpl_st,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then fb_leads end),0),0) as MAR875F, -- fb_cpl_st,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then ba_leads end),0),0) as MAR1355F, -- ba_cpl_st
        coalesce(sum(case when market_id = 29 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then nd_leads end),0),0) as MAR1356F, --nd_cpl_st
        coalesce(sum(case when market_id = 29 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then yelp_leads end),0),0) as MAR915F, --yelp_cpl_st
        coalesce(sum(case when market_id = 29 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then misc_leads end),0),0) as MAR919F, --misc_cpl_st
        coalesce(sum(case when market_id = 10 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then total_leads end),0),0) as MAR406F, -- total_fr_cpl,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then ha_leads end),0),0) as MAR413F, -- ha_cpl_fr,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then tt_leads end),0),0) as MAR418F, -- tt_cpl_fr,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then bo_leads end),0),0) as MAR709F, -- bo_cpl_fr,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then gg_leads end),0),0) as MAR423F, -- gg_cpl_fr,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then fb_leads end),0),0) as MAR428F, -- fb_cpl_fr,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then ba_leads end),0),0) as MAR1361F, -- ba_cpl_fr
        coalesce(sum(case when market_id = 10 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then nd_leads end),0),0) as MAR1362F, --nd_cpl_fr
        coalesce(sum(case when market_id = 10 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then yelp_leads end),0),0) as MAR433F, --yelp_cpl_fr
        coalesce(sum(case when market_id = 10 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then misc_leads end),0),0) as MAR438F, --misc_cpl_fr
        coalesce(sum(case when market_id = 4 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then total_leads end),0),0) as MAR864F, -- total_wa_cpl,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then ha_leads end),0),0) as MAR868F, -- ha_cpl_wa,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then tt_leads end),0),0) as MAR876F, -- tt_cpl_wa,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then bo_leads end),0),0) as MAR884F, -- bo_cpl_wa,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then gg_leads end),0),0) as MAR880F, -- gg_cpl_wa,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then fb_leads end),0),0) as MAR872F, -- fb_cpl_wa,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then ba_leads end),0),0) as MAR1367F, -- ba_cpl_wa
        coalesce(sum(case when market_id = 4 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then nd_leads end),0),0) as MAR1368F, --nd_cpl_wa
        coalesce(sum(case when market_id = 4 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then yelp_leads end),0),0) as MAR912F, --yelp_cpl_wa
        coalesce(sum(case when market_id = 4 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then misc_leads end),0),0) as MAR916F, --misc_cpl_wa
        coalesce(sum(case when market_id = 30 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then total_leads end),0),0) as MAR865F, -- total_sj_cpl,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then ha_leads end),0),0) as MAR869F, -- ha_cpl_sj,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then tt_leads end),0),0) as MAR877F, -- tt_cpl_sj,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then bo_leads end),0),0) as MAR885F, -- bo_cpl_sj,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then gg_leads end),0),0) as MAR881F, -- gg_cpl_sj,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then fb_leads end),0),0) as MAR873F, -- fb_cpl_sj,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then ba_leads end),0),0) as MAR1373F, -- ba_cpl_sj
        coalesce(sum(case when market_id = 30 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then nd_leads end),0),0) as MAR1374F, --nd_cpl_sj
        coalesce(sum(case when market_id = 30 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then yelp_leads end),0),0) as MAR913F, --yelp_cpl_sj
        coalesce(sum(case when market_id = 30 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then misc_leads end),0),0) as MAR917F, --misc_cpl_sj
        coalesce(sum(case when market_id = 31 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then total_leads end),0),0) as MAR866F, -- total_pa_cpl,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then ha_leads end),0),0) as MAR870F, -- ha_cpl_pa,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then tt_leads end),0),0) as MAR878F, -- tt_cpl_pa,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then bo_leads end),0),0) as MAR886F, -- bo_cpl_pa,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then gg_leads end),0),0) as MAR882F, -- gg_cpl_pa,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then fb_leads end),0),0) as MAR874F, -- fb_cpl_pa,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then ba_leads end),0),0) as MAR1379F, -- ba_cpl_pa
        coalesce(sum(case when market_id = 31 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then nd_leads end),0),0) as MAR1380F, --nd_cpl_pa
        coalesce(sum(case when market_id = 31 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then yelp_leads end),0),0) as MAR914F, --yelp_cpl_pa
        coalesce(sum(case when market_id = 31 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then misc_leads end),0),0) as MAR918F, --misc_cpl_pa
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then total_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then total_leads end),0),0) as MAR488F, -- total_sc_cpl,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then ha_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then ha_leads end),0),0) as MAR489F, -- ha_cpl_sc,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then tt_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then tt_leads end),0),0) as MAR490F, -- tt_cpl_sc,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then gg_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then gg_leads end),0),0) as MAR491F, -- gg_cpl_sc,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then fb_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then fb_leads end),0),0) as MAR492F, -- fb_cpl_sc,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then ba_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then ba_leads end),0),0) as MAR1385F, -- ba_cpl_sc
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then nd_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then nd_leads end),0),0) as MAR1386F, --nd_cpl_sc
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then yelp_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then yelp_leads end),0),0) as MAR493F, --yelp_cpl_sc
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then misc_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then misc_leads end),0),0) as MAR494F, --misc_cpl_sc
        coalesce(sum(case when market_id = 14 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then total_leads end),0),0) as MAR583F, -- total_sv_cpl,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then ha_leads end),0),0) as MAR584F, -- ha_cpl_sv,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then tt_leads end),0),0) as MAR585F, -- tt_cpl_sv,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then gg_leads end),0),0) as MAR586F, -- gg_cpl_sv,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then fb_leads end),0),0) as MAR587F, -- fb_cpl_sv,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then ba_leads end),0),0) as MAR1393F, -- ba_cpl_sv
        coalesce(sum(case when market_id = 14 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then nd_leads end),0),0) as MAR1394F, --nd_cpl_sv
        coalesce(sum(case when market_id = 14 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then yelp_leads end),0),0) as MAR588F, --yelp_cpl_sv
        coalesce(sum(case when market_id = 14 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then misc_leads end),0),0) as MAR589F, --misc_cpl_sv
        coalesce(sum(case when market_id = 5 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then total_leads end),0),0) as MAR590F, -- total_oc_cpl,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then ha_leads end),0),0) as MAR591F, -- ha_cpl_oc,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then tt_leads end),0),0) as MAR592F, -- tt_cpl_oc,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then gg_leads end),0),0) as MAR593F, -- gg_cpl_oc,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then fb_leads end),0),0) as MAR594F, -- fb_cpl_oc,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then ba_leads end),0),0) as MAR1399F, -- ba_cpl_oc
        coalesce(sum(case when market_id = 5 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then nd_leads end),0),0) as MAR1400F, --nd_cpl_oc
        coalesce(sum(case when market_id = 5 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then yelp_leads end),0),0) as MAR595F, --yelp_cpl_oc
        coalesce(sum(case when market_id = 5 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then misc_leads end),0),0) as MAR596F, --misc_cpl_oc
        coalesce(sum(case when market_id = 6 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then total_leads end),0),0) as MAR597F, -- total_la_cpl,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then ha_leads end),0),0) as MAR598F, -- ha_cpl_la,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then tt_leads end),0),0) as MAR599F, -- tt_cpl_la,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then gg_leads end),0),0) as MAR600F, -- gg_cpl_la,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then fb_leads end),0),0) as MAR694F, -- fb_cpl_la,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then ba_leads end),0),0) as MAR1405F, -- ba_cpl_la
        coalesce(sum(case when market_id = 6 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then nd_leads end),0),0) as MAR1406F, --nd_cpl_la
        coalesce(sum(case when market_id = 6 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then yelp_leads end),0),0) as MAR602F, --yelp_cpl_la
        coalesce(sum(case when market_id = 6 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then misc_leads end),0),0) as MAR603F, --misc_cpl_la
        coalesce(sum(case when market_id = 7 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then total_leads end),0),0) as MAR701F, -- total_vc_cpl,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then ha_leads end),0),0) as MAR696F, -- ha_cpl_vc,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then tt_leads end),0),0) as MAR697F, -- tt_cpl_vc,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then gg_leads end),0),0) as MAR693F, -- gg_cpl_vc,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then fb_leads end),0),0) as MAR698F, -- fb_cpl_vc,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then ba_leads end),0),0) as MAR1411F, -- ba_cpl_vc
        coalesce(sum(case when market_id = 7 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then nd_leads end),0),0) as MAR1412F, --nd_cpl_vc
        coalesce(sum(case when market_id = 7 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then yelp_leads end),0),0) as MAR699F, --yelp_cpl_vc
        coalesce(sum(case when market_id = 7 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then misc_leads end),0),0) as MAR700F, --misc_cpl_vc
        coalesce(sum(case when market_id = 1 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then total_leads end),0),0) as MAR777F, -- total_sd_cpl,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then ha_leads end),0),0) as MAR778F, -- ha_cpl_sd,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then tt_leads end),0),0) as MAR779F, -- tt_cpl_sd,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then gg_leads end),0),0) as MAR780F, -- gg_cpl_sd,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then fb_leads end),0),0) as MAR781F, -- fb_cpl_sd,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then ba_leads end),0),0) as MAR1417F, -- ba_cpl_sd
        coalesce(sum(case when market_id = 1 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then nd_leads end),0),0) as MAR1418F, --nd_cpl_sd
        coalesce(sum(case when market_id = 1 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then yelp_leads end),0),0) as MAR782F, --yelp_cpl_sd
        coalesce(sum(case when market_id = 1 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then misc_leads end),0),0) as MAR783F, --misc_cpl_sd
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then total_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then total_leads end),0),0) as MAR784F, -- total_tx_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then ha_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then ha_leads end),0),0) as MAR787F, -- ha_cpl_tx,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then tt_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then tt_leads end),0),0) as MAR793F, -- tt_cpl_tx,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then gg_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then gg_leads end),0),0) as MAR796F, -- gg_cpl_tx,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then fb_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then fb_leads end),0),0) as MAR790F, -- fb_cpl_tx,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then ba_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then ba_leads end),0),0) as MAR1423F, -- ba_cpl_tx
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then nd_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then nd_leads end),0),0) as MAR1424F, --nd_cpl_tx
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then yelp_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then yelp_leads end),0),0) as MAR982F, --yelp_cpl_tx
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then misc_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then misc_leads end),0),0) as MAR983F, --misc_cpl_tx
        coalesce(sum(case when market_id = 16 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then total_leads end),0),0) as MAR785F, -- total_dl_cpl,
        coalesce(sum(case when market_id = 16 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then ha_leads end),0),0) as MAR788F, -- ha_cpl_dl,
        coalesce(sum(case when market_id = 16 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then tt_leads end),0),0) as MAR794F, -- tt_cpl_dl,
        coalesce(sum(case when market_id = 16 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then gg_leads end),0),0) as MAR797F, -- gg_cpl_dl,
        coalesce(sum(case when market_id = 16 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then fb_leads end),0),0) as MAR791F, -- fb_cpl_dl,
	      coalesce(sum(case when market_id = 16 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then ba_leads end),0),0) as MAR1429F, --ba_dl_cpl
        coalesce(sum(case when market_id = 16 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then nd_leads end),0),0) as MAR1430F, --nd_dl_cpl
        coalesce(sum(case when market_id = 16 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then yelp_leads end),0),0) as MAR984F, --yelp_dl_cpl
        coalesce(sum(case when market_id = 16 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then misc_leads end),0),0) as MAR985F, --misc_dl_cpl
        coalesce(sum(case when market_id = 17 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then total_leads end),0),0) as MAR786F, -- total_fw_cpl,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then ha_leads end),0),0) as MAR789F, -- ha_cpl_fw,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then tt_leads end),0),0) as MAR795F, -- tt_cpl_fw,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then gg_leads end),0),0) as MAR798F, -- gg_cpl_fw,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then fb_leads end),0),0) as MAR792F, -- fb_cpl_fw,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then ba_leads end),0),0) as MAR1435F, --ba_cpl_fw
        coalesce(sum(case when market_id = 17 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then nd_leads end),0),0) as MAR1436F, --nd_cpl_fw
        coalesce(sum(case when market_id = 17 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then yelp_leads end),0),0) as MAR988F, --yelp_cpl_fw
        coalesce(sum(case when market_id = 17 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then misc_leads end),0),0) as MAR989F, --misc_cpl_fw
        coalesce(sum(case when market_id = 18 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then total_leads end),0),0) as MAR1011F, -- total_ht_cpl,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then ha_leads end),0),0) as MAR1012F, -- ha_cpl_ht,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then tt_leads end),0),0) as MAR1013F, -- tt_cpl_ht,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then bo_leads end),0),0) as MAR1014F, -- bo_cpl_ht,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then gg_leads end),0),0) as MAR1015F, -- gg_cpl_ht,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then fb_leads end),0),0) as MAR1016F, -- fb_cpl_ht,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then ba_leads end),0),0) as MAR1441F, --ba_cpl_ht
        coalesce(sum(case when market_id = 18 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then nd_leads end),0),0) as MAR1442F, --nd_cpl_ht
        coalesce(sum(case when market_id = 18 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then yelp_leads end),0),0) as MAR1017F, --yelp_cpl_ht
        coalesce(sum(case when market_id = 18 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then misc_leads end),0),0) as MAR1018F, --misc_cpl_ht
        coalesce(sum(case when market_id = 19 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then total_leads end),0),0) as MAR1040F, -- total_sa_cpl,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then ha_leads end),0),0) as MAR1041F, -- ha_cpl_sa,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then tt_leads end),0),0) as MAR1042F, -- tt_cpl_sa,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then bo_leads end),0),0) as MAR1043F, -- bo_cpl_sa,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then gg_leads end),0),0) as MAR1044F, -- gg_cpl_sa,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then fb_leads end),0),0) as MAR1045F, -- fb_cpl_sa,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then ba_leads end),0),0) as MAR1447F, --ba_cpl_sa
        coalesce(sum(case when market_id = 19 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then nd_leads end),0),0) as MAR1448F, --nd_cpl_sa
        coalesce(sum(case when market_id = 19 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then yelp_leads end),0),0) as MAR1046F, --yelp_cpl_sa
        coalesce(sum(case when market_id = 19 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then misc_leads end),0),0) as MAR1047F, --misc_cpl_sa
        coalesce(sum(case when market_id = 32 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then total_leads end),0),0) as MAR1078F, -- total_au_cpl,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then ha_leads end),0),0) as MAR1079F, -- ha_cpl_au,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then tt_leads end),0),0) as MAR1080F, -- tt_cpl_au,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then bo_leads end),0),0) as MAR1081F, -- bo_cpl_au,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then gg_leads end),0),0) as MAR1082F, -- gg_cpl_au,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then fb_leads end),0),0) as MAR1083F, -- fb_cpl_au,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then ba_leads end),0),0) as MAR1453F, --ba_cpl_au
        coalesce(sum(case when market_id = 32 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then nd_leads end),0),0) as MAR1454F, --nd_cpl_au
        coalesce(sum(case when market_id = 32 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then yelp_leads end),0),0) as MAR1084F, --yelp_cpl_au
        coalesce(sum(case when market_id = 32 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then misc_leads end),0),0) as MAR1085F, --misc_cpl_au
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then total_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then total_leads end),0),0) as MAR1306F, -- total_ga_cpl,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then ha_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then ha_leads end),0),0) as MAR1307F, -- ha_cpl_ga,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then tt_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then tt_leads end),0),0) as MAR1308F, -- tt_cpl_ga,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then bo_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then bo_leads end),0),0) as MAR1322F, -- bo_cpl_ga,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then gg_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then gg_leads end),0),0) as MAR1309F, -- gg_cpl_ga,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then fb_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then fb_leads end),0),0) as MAR1310F, -- fb_cpl_ga,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then ba_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then ba_leads end),0),0) as MAR1459F, -- ba_cpl_ga
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then nd_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then nd_leads end),0),0) as MAR1460F, --nd_cpl_ga
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then yelp_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then yelp_leads end),0),0) as MAR1311F, -- yelp_cpl_ga
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then misc_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then misc_leads end),0),0) as MAR1312F, --misc_cpl_ga
        coalesce(sum(case when market_id = 20 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then total_leads end),0),0) as MAR966F, -- total_at_cpl,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then ha_leads end),0),0) as MAR967F, -- ha_cpl_at,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then tt_leads end),0),0) as MAR968F, -- tt_cpl_at,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then bo_leads end),0),0) as MAR969F, -- bo_cpl_at,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then gg_leads end),0),0) as MAR970F, -- gg_cpl_at,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then fb_leads end),0),0) as MAR971F, -- fb_cpl_at,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then ba_leads end),0),0) as MAR1465F, --ba_cpl_at
        coalesce(sum(case when market_id = 20 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then nd_leads end),0),0) as MAR1466F, --nd_cpl_at
        coalesce(sum(case when market_id = 20 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then yelp_leads end),0),0) as MAR972F, --yelp_cpl_at
        coalesce(sum(case when market_id = 20 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then misc_leads end),0),0) as MAR973F, --misc_cpl_at
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then total_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then total_leads end),0),0) as MAR2036F, -- total_ne_cpl,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ha_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ha_leads end),0),0) as MAR2037F, -- ha_cpl_ne,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then tt_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then tt_leads end),0),0) as MAR2038F, -- tt_cpl_ne,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then bo_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then bo_leads end),0),0) as MAR2052F, -- bo_cpl_ne,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then gg_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then gg_leads end),0),0) as MAR2039F, -- gg_cpl_ne,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then fb_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then fb_leads end),0),0) as MAR2040F, -- fb_cpl_ne,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ba_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ba_leads end),0),0) as MAR2055F, --ba_cpl_ne
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then nd_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then nd_leads end),0),0) as MAR2056F, --nd_cpl_ne
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then yelp_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then yelp_leads end),0),0) as MAR2041F, --yelp_cpl_ne
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then misc_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then misc_leads end),0),0) as MAR2042F, --misc_cpl_ne  
        coalesce(sum(case when market_id = 22 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then total_leads end),0),0) as MAR1134F, -- total_bl_cpl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then ha_leads end),0),0) as MAR1136F, -- ha_cpl_bl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then tt_leads end),0),0) as MAR1137F, -- tt_cpl_bl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then bo_leads end),0),0) as MAR1145F, -- bo_cpl_bl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then gg_leads end),0),0) as MAR1138F, -- gg_cpl_bl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then fb_leads end),0),0) as MAR1139F, -- fb_cpl_bl,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then ba_leads end),0),0) as MAR1477F, --ba_cpl_bl
        coalesce(sum(case when market_id = 22 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then nd_leads end),0),0) as MAR1478F, --nd_cpl_bl
        coalesce(sum(case when market_id = 22 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then yelp_leads end),0),0) as MAR1140F, --yelp_cpl_bl
        coalesce(sum(case when market_id = 22 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then misc_leads end),0),0) as MAR1141F, --misc_cpl_bl
        coalesce(sum(case when market_id = 21 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then total_leads end),0),0) as MAR1175F, -- total_dc_cpl,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then ha_leads end),0),0) as MAR1177F, -- ha_cpl_dc,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then tt_leads end),0),0) as MAR1178F, -- tt_cpl_dc,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then bo_leads end),0),0) as MAR1186F, -- bo_cpl_dc,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then gg_leads end),0),0) as MAR1179F, -- gg_cpl_dc,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then fb_leads end),0),0) as MAR1180F, -- fb_cpl_dc,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then ba_leads end),0),0) as MAR1483F, --ba_cpl_dc
        coalesce(sum(case when market_id = 21 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then nd_leads end),0),0) as MAR1484F, --nd_cpl_dc
        coalesce(sum(case when market_id = 21 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then yelp_leads end),0),0) as MAR1181F, --yelp_cpl_dc
        coalesce(sum(case when market_id = 21 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then misc_leads end),0),0) as MAR1182F, --misc_cpl_dc
        coalesce(sum(case when market_id = 33 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then total_leads end),0),0) as MAR1239F, -- total_ph_cpl,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then ha_leads end),0),0) as MAR1240F, -- ha_cpl_ph,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then tt_leads end),0),0) as MAR1241F, -- tt_cpl_ph,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then bo_leads end),0),0) as MAR1255F, -- bo_cpl_ph,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then gg_leads end),0),0) as MAR1242F, -- gg_cpl_ph,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then fb_leads end),0),0) as MAR1243F, -- fb_cpl_ph,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then ba_leads end),0),0) as MAR1495F, --ba_cpl_ph
        coalesce(sum(case when market_id = 33 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then nd_leads end),0),0) as MAR1496F, --nd_cpl_ph
        coalesce(sum(case when market_id = 33 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then yelp_leads end),0),0) as MAR1244F, --yelp_cpl_ph
        coalesce(sum(case when market_id = 33 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then misc_leads end),0),0) as MAR1245F, --misc_cpl_ph
        coalesce(sum(case when market_id = 35 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then total_leads end),0),0) as MAR1518F, -- total_ar_cpl,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then ha_leads end),0),0) as MAR1519F, -- ha_cpl_ar,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then tt_leads end),0),0) as MAR1520F, -- tt_cpl_ar,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then bo_leads end),0),0) as MAR1534F, -- bo_cpl_ar,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then gg_leads end),0),0) as MAR1521F, -- gg_cpl_ar,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then fb_leads end),0),0) as MAR1522F, -- fb_cpl_ar,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then ba_leads end),0),0) as MAR1574F, --ba_cpl_ar
        coalesce(sum(case when market_id = 35 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then nd_leads end),0),0) as MAR1575F, --nd_cpl_ar
        coalesce(sum(case when market_id = 35 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then yelp_leads end),0),0) as MAR1523F, --yelp_cpl_ar
        coalesce(sum(case when market_id = 35 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then misc_leads end),0),0) as MAR1524F, --misc_cpl_ar
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then total_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then total_leads end),0),0) as MAR1592F, -- total_FL_cpl,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then ha_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then ha_leads end),0),0) as MAR1593F, -- ha_cpl_FL,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then tt_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then tt_leads end),0),0) as MAR1594F, -- tt_cpl_FL,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then bo_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then bo_leads end),0),0) as MAR1608F, -- bo_cpl_FL,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then gg_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then gg_leads end),0),0) as MAR1595F, -- gg_cpl_FL,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then fb_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then fb_leads end),0),0) as MAR1596F, -- fb_cpl_FL,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then ba_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then ba_leads end),0),0) as MAR1646F, -- ba_cpl_FL
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then nd_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then nd_leads end),0),0) as MAR1645F, --nd_cpl_FL
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then yelp_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then yelp_leads end),0),0) as MAR1597F, -- yelp_cpl_FL
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then misc_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then misc_leads end),0),0) as MAR1598F, --misc_cpl_FL
        coalesce(sum(case when market_id = 24 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then total_leads end),0),0) as MAR1628F, -- total_mi_cpl,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then ha_leads end),0),0) as MAR1629F, -- ha_cpl_mi,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then tt_leads end),0),0) as MAR1630F, -- tt_cpl_mi,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then bo_leads end),0),0) as MAR1644F, -- bo_cpl_mi,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then gg_leads end),0),0) as MAR1631F, -- gg_cpl_mi,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then fb_leads end),0),0) as MAR1632F, -- fb_cpl_mi,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then ba_leads end),0),0) as MAR1647F, --ba_cpl_mi
        coalesce(sum(case when market_id = 24 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then nd_leads end),0),0) as MAR1648F, --nd_cpl_mi
        coalesce(sum(case when market_id = 24 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then yelp_leads end),0),0) as MAR1633F, --yelp_cpl_mi
        coalesce(sum(case when market_id = 24 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then misc_leads end),0),0) as MAR1634F, --misc_cpl_mi
        coalesce(sum(case when market_id = 26 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then total_leads end),0),0) as MAR1676F, -- total_or_cpl,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then ha_leads end),0),0) as MAR1677F, -- ha_cpl_or,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then tt_leads end),0),0) as MAR1678F, -- tt_cpl_or,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then gg_leads end),0),0) as MAR1679F, -- gg_cpl_or,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then fb_leads end),0),0) as MAR1680F, -- fb_cpl_or,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then yelp_leads end),0),0) as MAR1681F,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then misc_leads end),0),0) as MAR1682F,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then bo_leads end),0),0) as MAR1692F, -- bo_cpl_or,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then ba_leads end),0),0) as MAR1695F,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then nd_leads end),0),0) as MAR1696F,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then total_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then total_leads end),0),0) as MAR2286F, -- total_pa_wa_cpl,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then ha_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then ha_leads end),0),0) as MAR2287F, -- ha_cpl_pa_wa,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then tt_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then tt_leads end),0),0) as MAR2288F, -- tt_cpl_pa_wa,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then bo_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then bo_leads end),0),0) as MAR2302F, -- bo_cpl_pa_wa,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then gg_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then gg_leads end),0),0) as MAR2289F, -- gg_cpl_pa_wa,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then fb_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then fb_leads end),0),0) as MAR2290F, -- fb_cpl_pa_wa,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then ba_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then ba_leads end),0),0) as MAR2304F,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then nd_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then nd_leads end),0),0) as MAR2303F,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then yelp_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then yelp_leads end),0),0) as MAR2291F,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then misc_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then misc_leads end),0),0) as MAR2292F,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then total_leads end),0),0) as MAR2244F, -- total_se_cpl,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then ha_leads end),0),0) as MAR2245F, -- ha_cpl_se,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then tt_leads end),0),0) as MAR2246F, -- tt_cpl_se,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then bo_leads end),0),0) as MAR2261F, -- bo_cpl_se,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then gg_leads end),0),0) as MAR2247F, -- gg_cpl_se,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then fb_leads end),0),0) as MAR2248F, -- fb_cpl_se,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then ba_leads end),0),0) as MAR2265F,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then nd_leads end),0),0) as MAR2266F,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then yelp_leads end),0),0) as MAR2249F,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then misc_leads end),0),0) as MAR2250F,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then total_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then total_leads end),0),0) as MAR2870F, -- total_wn_il_cpl,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then ha_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then ha_leads end),0),0) as MAR2871F, -- ha_cpl_wn_il,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then tt_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then tt_leads end),0),0) as MAR2872F, -- tt_cpl_wn_il,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then bo_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then bo_leads end),0),0) as MAR2886F, -- bo_cpl_wn_il,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then gg_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then gg_leads end),0),0) as MAR2873F, -- gg_cpl_wn_il,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then fb_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then fb_leads end),0),0) as MAR2874F, -- fb_cpl_wn_il,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then ba_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then ba_leads end),0),0) as MAR2506F,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then nd_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then nd_leads end),0),0) as MAR2507F,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then yelp_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then yelp_leads end),0),0) as MAR2875F,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then misc_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then misc_leads end),0),0) as MAR2876F,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then total_leads end),0),0) as MAR2939F, -- total_wn_il_ch_cpl,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then ha_leads end),0),0) as MAR2930F, -- ha_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then tt_leads end),0),0) as MAR2931F, -- tt_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then bo_leads end),0),0) as MAR2945F, -- bo_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then gg_leads end),0),0) as MAR2932F, -- gg_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then fb_leads end),0),0) as MAR2933F, -- fb_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then ba_leads end),0),0) as MAR2889F, -- ba_cpl_wn_il_ch
        coalesce(sum(case when market_id = 42 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then nd_leads end),0),0) as MAR2890F, --nd_cpl_wn_il_ch
        coalesce(sum(case when market_id = 42 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then yelp_leads end),0),0) as MAR2934F, --yelp_cpl_wn_il_ch
        coalesce(sum(case when market_id = 42 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then misc_leads end),0),0) as MAR2935F, --misc_cpl_wn_il_ch
        coalesce(sum(case when market_id = 57 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then total_leads end),0),0) as MAR2998F, -- total_wn_il_na_cpl,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then ha_leads end),0),0) as MAR2989F, -- ha_cpl_wn_il_na,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then tt_leads end),0),0) as MAR2990F, -- tt_cpl_wn_il_na,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then bo_leads end),0),0) as MAR3004F, -- bo_cpl_wn_il_na,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then gg_leads end),0),0) as MAR2991F, -- gg_cpl_wn_il_na,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then fb_leads end),0),0) as MAR2992F, -- fb_cpl_wn_il_na,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then ba_leads end),0),0) as MAR2948F, -- ba_cpl_wn_il_na
        coalesce(sum(case when market_id = 57 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then nd_leads end),0),0) as MAR2949F, --nd_cpl_wn_il_na
        coalesce(sum(case when market_id = 57 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then yelp_leads end),0),0) as MAR2993F, --yelp_cpl_il
        coalesce(sum(case when market_id = 57 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then misc_leads end),0),0) as MAR2994F, --misc_cpl_wn_il_na
        coalesce(sum(case when market_id = 58 and is_fence = 1 then total_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then total_leads end),0),0) as MAR3057F, -- total_wn_il_la_cpl,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then ha_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then ha_leads end),0),0) as MAR3048F, -- ha_cpl_wn_il_la,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then tt_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then tt_leads end),0),0) as MAR3049F, -- tt_cpl_wn_il_la,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then bo_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then bo_leads end),0),0) as MAR3063F, -- bo_cpl_wn_il_la,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then gg_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then gg_leads end),0),0) as MAR3050F, -- gg_cpl_wn_il_la,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then fb_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then fb_leads end),0),0) as MAR3051F, -- fb_cpl_wn_il_la,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then ba_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then ba_leads end),0),0) as MAR3007F, -- ba_cpl_wn_il_la
        coalesce(sum(case when market_id = 58 and is_fence = 1 then nd_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then nd_leads end),0),0) as MAR3008F, --nd_cpl_wn_il_la
        coalesce(sum(case when market_id = 58 and is_fence = 1 then yelp_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then yelp_leads end),0),0) as MAR3052F, --yelp_cpl_wn_il_la
        coalesce(sum(case when market_id = 58 and is_fence = 1 then misc_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then misc_leads end),0),0) as MAR3053F, --misc_cpl_wn_il_la
        
        coalesce(sum(case when old_region = 'North California' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when old_region = 'North California' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2396F, -- ha_ads_cpl_nc,
        coalesce(sum(case when market_id = 2 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 2 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2397F, -- ha_ads_cpl_eb,
        coalesce(sum(case when market_id = 8 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 8 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2502F, -- ha_ads_cpl_sf,
        coalesce(sum(case when market_id = 9 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 9 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2398F, -- ha_ads_cpl_nb,
        coalesce(sum(case when market_id = 3 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 3 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2399F, -- ha_ads_cpl_sac,
        coalesce(sum(case when market_id = 29 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 29 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2400F, -- ha_ads_cpl_st,
        coalesce(sum(case when market_id = 10 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 10 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2401F, -- ha_ads_cpl_fr,
        coalesce(sum(case when market_id = 4 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 4 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2402F, -- ha_ads_cpl_wa,
        coalesce(sum(case when market_id = 30 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 30 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2403F, -- ha_ads_cpl_sj,
        coalesce(sum(case when market_id = 31 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 31 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2404F, -- ha_ads_cpl_pa,
        coalesce(sum(case when old_region = 'South California' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when old_region = 'South California' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2405F, -- ha_ads_cpl_sc,
        coalesce(sum(case when market_id = 14 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 14 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2406F, -- ha_ads_sv_cpl,
        coalesce(sum(case when market_id = 5 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 5 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2407F, -- ha_ads_oc_cpl,
        coalesce(sum(case when market_id = 6 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 6 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2408F, -- ha_ads_la_cpl,
        coalesce(sum(case when market_id = 7 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 7 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2409F, -- ha_ads_vc_cpl,
        coalesce(sum(case when market_id = 1 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 1 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2410F, -- ha_ads_sd_cpl,
        coalesce(sum(case when market like '%-TX-%' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market like '%-TX-%' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2411F, -- ha_ads_tx_cpl,
        coalesce(sum(case when market_id = 16 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 16 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2412F, -- ha_ads_cpl_dl,
        coalesce(sum(case when market_id = 17 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 17 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2413F, -- ha_ads_cpl_fw,
        coalesce(sum(case when market_id = 18 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 18 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2414F, -- ha_ads_cpl_ht,
        coalesce(sum(case when market_id = 19 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 19 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2415F, -- ha_ads_cpl_sa,
        coalesce(sum(case when market_id = 32 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 32 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2416F, -- ha_ads_cpl_au,
        coalesce(sum(case when market like '%-GA-%' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market like '%-GA-%' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2417F, -- ha_ads_ga_cpl,
        coalesce(sum(case when market_id = 20 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 20 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2418F, -- ha_ads_cpl_at,
        coalesce(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when old_region in ('Maryland','Pennsylvania','Virginia') and is_fence = 1 then ha_ads_leads end),0),0) as MAR2419F, -- ha_ads_cpl_ne,
        coalesce(sum(case when market_id = 22 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 22 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2420F, -- ha_ads_cpl_bl,
        coalesce(sum(case when market_id = 21 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 21 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2421F, -- ha_ads_cpl_dc,
        coalesce(sum(case when market_id = 33 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 33 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2422F, -- ha_ads_cpl_ph,
        coalesce(sum(case when market_id = 35 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 35 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2423F, -- ha_ads_cpl_ar,
        coalesce(sum(case when market like '%-FL-%' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market like '%-FL-%' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2424F, -- ha_ads_fl_cpl,
        coalesce(sum(case when market_id = 24 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 24 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2425F, -- ha_ads_cpl_mi,
        coalesce(sum(case when market_id = 26 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 26 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2426F, -- ha_ads_cpl_or,
        coalesce(sum(case when market like 'PA-WA-%' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market like 'PA-WA-%' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2461F, -- ha_ads_cpl_pa_wa,
        coalesce(sum(case when market_id = 43 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 43 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2462F, -- ha_ads_cpl_se,
        coalesce(sum(case when market_id = 42 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 42 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2909F, -- ha_ads_cpl_wn_il_ch,
        coalesce(sum(case when market_id = 57 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 57 and is_fence = 1 then ha_ads_leads end),0),0) as MAR2968F, -- ha_ads_cpl_wn_il_na,
        coalesce(sum(case when market_id = 58 and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market_id = 58 and is_fence = 1 then ha_ads_leads end),0),0) as MAR3027F, -- ha_ads_cpl_wn_il_la,
        coalesce(sum(case when market like 'WN-IL-%' and is_fence = 1 then ha_ads_fee end) / nullif(sum(case when market like 'WN-IL-%' and is_fence = 1 then ha_ads_leads end),0),0) as MAR2524F -- ha_ads_cpl_illinois,
        ----DEPRECATED
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and yelp_fee > 0 then yelp_fee  end),0) as MAR430F, --deprecated
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and misc_fee > 0 then misc_fee  end),0) as MAR435F, --deprecated
        --coalesce(avg(case when market_id in (4,30,31)and is_fence = 1 and total_fee > 0 then total_fee end),0) as MAR403F, -- total_sb_cpl,
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and ha_fee > 0 then ha_fee end),0) as MAR410F, -- ha_cpl_sb,
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and tt_fee > 0 then tt_fee end) , 0) as MAR415F, -- tt_cpl_sb,
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and bo_fee > 0 then bo_fee end) ,0) as MAR692F, -- bo_cpl_sb,
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and gg_fee > 0 then gg_fee end) ,0) as MAR420F, -- gg_cpl_sb,
        --coalesce(avg(case when market_id in (4,30,31) and is_fence = 1 and fb_fee > 0 then fb_fee end) ,0) as MAR425F, -- fb_cpl_sb,
	    --coalesce(avg(case when market_id in (4,30,31,8) and is_fence = 1 and ba_fee > 0 then ba_fee end),0) as MAR1337F, --ba_cpl_sb_sf
	    --coalesce(avg(case when market_id in (4,30,31,8) and is_fence = 1 and nd_fee > 0 then nd_fee end),0) as MAR1338F, --nd_cpl_sb_sf*/ --deprecated
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and total_fee > 0 then total_fee  end),0) as MAR1204F, -- total_md_cpl,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and tt_fee > 0 then tt_fee  end),0) as MAR1206F, -- tt_cpl_md,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and bo_fee > 0 then bo_fee  end),0) as MAR1190F, -- bo_cpl_md,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and gg_fee > 0 then gg_fee  end),0) as MAR1207F, -- gg_cpl_md,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and fb_fee > 0 then fb_fee  end),0) as MAR1208F, -- fb_cpl_md,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and ba_fee > 0 then ba_fee end),0) as MAR1471F,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and nd_fee > 0 then nd_fee end),0) as MAR1472F,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and tt_fee > 0 then tt_fee  end),0) as MAR1550F, -- tt_cpl_va,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and nd_fee > 0 then nd_fee end),0) as MAR1569F,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and bo_fee > 0 then bo_fee  end),0) as MAR1564F, -- bo_cpl_va,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and fb_fee > 0 then fb_fee  end),0) as MAR1552F, -- fb_cpl_va,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and gg_fee > 0 then gg_fee  end),0) as MAR1551F, -- gg_cpl_va,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and total_fee > 0 then total_fee  end),0) as MAR1548F, -- total_va_cpl,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and ba_fee > 0 then ba_fee end),0) as MAR1568F,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and ha_fee > 0 then ha_fee  end),0) as MAR1270F, -- ha_cpl_pen,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and tt_fee > 0 then tt_fee  end),0) as MAR1271F, -- tt_cpl_pen,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and ba_fee > 0 then ba_fee end),0) as MAR1489F,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and nd_fee > 0 then nd_fee end),0) as MAR1490F,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and total_fee > 0 then total_fee  end),0) as MAR1269F, -- total_pen_cpl,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and bo_fee > 0 then bo_fee  end),0) as MAR1285F, -- bo_cpl_pen,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and fb_fee > 0 then fb_fee  end),0) as MAR1273F, -- fb_cpl_pen,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and gg_fee > 0 then gg_fee  end),0) as MAR1272F, -- gg_cpl_pen,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and yelp_fee > 0 then yelp_fee  end),0) as MAR1209F,
        --coalesce(avg(case when market like '%-MD-%' and is_fence = 1 and misc_fee > 0 then misc_fee  end),0) as MAR1210F,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and misc_fee > 0 then misc_fee  end),0) as MAR1554F,
        --coalesce(avg(case when market like '%-VA-%' and is_fence = 1 and yelp_fee > 0 then yelp_fee  end),0) as MAR1553F,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and misc_fee > 0 then misc_fee  end),0) as MAR1275F,
        --coalesce(avg(case when market like '%-PA-%' and is_fence = 1 and yelp_fee > 0 then yelp_fee  end),0) as MAR1274F,
    from cpl_spend_leads
    where is_commercial = 0
    group by 1)
select 
    t.date,
    cd.*except(date),
    cf.*except(date),
    cs.*except(date)
from timeseries t
left join cpls_driveway_commercial cd using(date)
left join cpls_fence cf using(date)
left join cpl_sdr cs using(date)
