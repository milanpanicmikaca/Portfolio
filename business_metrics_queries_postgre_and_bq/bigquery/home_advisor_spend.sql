-- upload to BQ
with
ha_data as 
(
select 
  created_at as date,
  case 
    when market_id in (2,10,9,3,29,4,31,30,8,13) then 2
    when market_id in (6,5,14,7,1,12,11) then 1
    when region = 'West South Central' then 3
  end as region_id,
  regexp_extract(geo,'/.*/(.*)/') as market,
  market_id,
  case 
    when product like '/Fence%' then 105
    when product like '/Driveway%' then 34
  end as product_id,
  case when type = '/Residential' then 0 else 1 end as is_commercial, 
  product as service_category,
  ha_initial_fee as fee
from int_data.order_ue_materialized
where channel like '%Home Advisor%'
and channel not like '%Home Advisor/Ads'
)
select
  date_trunc(date, {period}) as date,
  -- Homeadvisor Spend (includes Fence, Hardscape and Commercial)
  sum(fee) as MAR330, -- ha_spend
  sum(case when region_id = 2 then fee else 0 end) as MAR665, -- norcal_spend
  sum(case when product_id = 105 and is_commercial = 0 and service_category like '%vinyl%' then fee else 0 end) as MAR630F,
  sum(case when market_id = 9 then fee else 0 end) as MAR267, -- eb_spend
  sum(case when market_id in (4,30,31,8) then fee else 0 end) as MAR269, -- sbsf_spend
  sum(case when market_id = 9 then fee else 0 end) as MAR270, -- nb_spend
  sum(case when market_id = 3 then fee else 0 end) as MAR268, -- sac_spend
  sum(case when market_id = 10 then fee else 0 end) as MAR272, -- fr_spend
  sum(case when region_id = 3 then fee else 0 end) as MAR800, -- tx_spend
  sum(case when market_id = 16 then fee else 0 end) as MAR717, -- dl_spend
  sum(case when market_id = 19 then fee else 0 end) as MAR1058, -- sa_spend
  sum(case when market_id = 17 then fee else 0 end) as MAR747, -- fw_spend
  sum(case when market_id = 1 then fee else 0 end) as MAR751, -- sd_spend
  sum(case when region_id = 1 then fee else 0 end) as MAR495, -- socal_spend
  sum(case when market_id = 14 then fee else 0 end) as MAR509, -- sv_spend
  sum(case when market_id = 5 then fee else 0 end) as MAR510, -- oc_spend
  sum(case when market_id = 6 then fee else 0 end) as MAR511, -- la_spend
  sum(case when market_id = 7 then fee else 0 end) as MAR680, -- vc_spend
  sum(case when market_id = 4 then fee else 0 end) as MAR860, -- wa_spend
  sum(case when market_id = 30 then fee else 0 end) as MAR861, -- sj_spend
  sum(case when market_id = 31 then fee else 0 end) as MAR862, -- pa_spend
  sum(case when market_id = 29 then fee else 0 end) as MAR863, -- st_spend
  sum(case when region_id = 5 then fee else 0 end) as MAR1216, -- maryland_spend
  sum(case when market_id = 22 then fee else 0 end) as MAR1115, -- bl_spend
  sum(case when market_id = 21 then fee else 0 end) as MAR1156, -- dc_spend
  sum(case when region_id = 7 then fee else 0 end) as MAR1281, -- pen_spend
  sum(case when market_id = 33 then fee else 0 end) as MAR1222, -- ph_spend
  sum(case when market_id is null then fee else 0 end) as MAR512, -- nomarket_spend
  sum(case when region_id = 4 then fee else 0 end) as MAR1318, -- ga_spend
  sum(case when region_id = 9 then fee else 0 end) as MAR1560, -- va_spend
  sum(case when market_id = 35 then fee else 0 end) as MAR1501, -- ar_spend
  sum(case when region_id = 6 then fee else 0 end) as MAR1604, -- fl_spend
  sum(case when market_id = 24 then fee else 0 end) as MAR1611, -- mi_spend
  sum(case when market_id = 26 then fee else 0 end) as MAR1659, -- or_spend
  sum(case when market_id = 43 then fee else 0 end) as MAR2227, -- se_spend
  sum(case when region_id = 16 then fee else 0 end) as MAR2298, -- fl_spend
  sum(case when market_id in (42,57,58) then fee else 0 end) as MAR2882, -- illinois_spend
  sum(case when market_id = 42 then fee else 0 end) as MAR2911, -- wn_il_ch_spend
  sum(case when market_id = 57 then fee else 0 end) as MAR2970, -- wn_il_na_spend
  sum(case when market_id = 58 then fee else 0 end) as MAR3029, -- wn_il_la_spend
  -- Homeadvisor Fence Spend (excludes Commercial)
  sum(case when product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR330F,  -- ha_spend_fence
  sum(case when region_id = 2 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR665F, -- norcal_spend_fence
  sum(case when market_id = 2 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR267F, -- eb_spend_fence
  sum(case when market_id = 8 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR271F, -- sf_spend_fence
  sum(case when market_id in (4,30,31,8) and product_id = 105 and is_commercial = 0  then fee else 0 end) as MAR269F, -- sbsf_spend_fence
  sum(case when market_id = 9 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR270F, -- nb_spend_fence
  sum(case when region_id = 3 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR800F, -- TX_spend_fence
  sum(case when market_id = 16 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR717F, -- dl_spend_fence
  sum(case when market_id = 18 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR995F, -- ht_spend_fence
  sum(case when market_id = 19 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1058F, -- sa_spend_fence
  sum(case when market_id = 32 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1095F, -- ht_spend_fence
  sum(case when market_id = 17 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR747F, -- fw_spend_fence
  sum(case when market_id = 1 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR751F, -- sd_spend_fence
  sum(case when market_id = 3 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR268F, -- sac_spend_fence
  sum(case when market_id = 10 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR272F, -- fr_spend_fence
  sum(case when region_id = 1 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR495F, -- socal_spend_fence
  sum(case when market_id = 14 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR509F, -- sv_spend_fence
  sum(case when market_id = 5 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR510F, -- oc_spend_fence
  sum(case when market_id = 6 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR511F, -- la_spend_fence
  sum(case when market_id = 7 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR680F, -- vc_spend_fence
  sum(case when market_id = 4 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR860F, -- wa_spend_fence
  sum(case when market_id = 30 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR861F, -- sj_spend_fence
  sum(case when market_id = 31 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR862F, -- pa_spend_fence
  sum(case when market_id = 29 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR863F, -- st_spend_fence
  sum(case when region_id in (5,7,9) and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2048F, -- north_east_spend_fence
  sum(case when region_id = 5 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1216F, -- maryland_spend_fence
  sum(case when market_id = 22 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1115F, -- bl_spend_fence
  sum(case when market_id = 21 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1156F, -- dc_spend_fence
  sum(case when region_id = 7 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1281F, -- pen_spend_fence
  sum(case when market_id = 33 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1222F, -- ph_spend_fence
  sum(case when region_id = 4 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1318F, -- ga_spend_fence
  sum(case when market_id is null and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR512F, -- nomarket_id_spend_fence
  sum(case when region_id = 9 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1560F, -- va_spend_fence
  sum(case when market_id = 35 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1501F, -- ar_spend_fence
  sum(case when region_id = 6 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1604F, -- fl_spend_fence
  sum(case when market_id = 24 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1611F, -- mi_spend_fence
  sum(case when market_id = 26 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR1659F, -- or_spend_fence
  sum(case when market_id = 43 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2227F, -- se_spend_fence
  sum(case when region_id = 16 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2298F, -- pa_wa_spend_fence
  sum(case when market_id in (42,57,58) and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2882F, -- illinois_spend_fence
  sum(case when market_id = 42 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2911F, -- wn_il_ch_spend_fence
  sum(case when market_id = 57 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR2970F, -- wn_il_na_spend_fence
  sum(case when market_id = 58 and product_id = 105 and is_commercial = 0 then fee else 0 end) as MAR3029F, -- wn_il_la_spend_fence
  -- Homeadvisor Turf Spend (excludes Commercial)
  sum(case when product_id = 132 and is_commercial = 0 then fee else 0 end) as MAR330T,  -- ha_spend_turf
  sum(case when region_id = 2 and product_id = 132 and is_commercial = 0 then fee else 0 end) as MAR665T, -- norcal_spend_turf
  -- Homeadvisor Hardscape Spend (excludes Commercial)
  sum(case when product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR330D, -- ha_spend_hardscape
  sum(case when region_id = 2 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR665D, -- socal_spend_hardscape
  sum(case when market_id = 9 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR267D, -- eb_spend_hardscape
  sum(case when market_id in (4,30,31,8) and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR269D, -- sbsf_spend_hardscape
  sum(case when market_id = 9 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR270D, -- nb_spend_hardscape
  sum(case when market_id = 3 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR268D, -- sac_spend_hardscape
  sum(case when market_id = 10 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR272D, -- fr_spend_hardscape
  sum(case when region_id = 1 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR495D, -- socal_spend_hardscape
  sum(case when market_id = 14 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR509D, -- sv_spend_hardscape
  sum(case when market_id = 5 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR510D, -- oc_spend_hardscape
  sum(case when market_id = 6 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR511D, -- la_spend_hardscape
  sum(case when market_id = 7 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR680D, -- vc_spend_hardscape
  sum(case when market_id is null and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR512D, -- nomarket_spend_hardscape
  sum(case when region_id = 3 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR800D, --tx_spend_hardscape
  sum(case when market_id = 17 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR747D, -- fw_spend_hardscape
  sum(case when market_id= 16 and product_id = 34 and is_commercial = 0  then fee else 0 end) as MAR717D, -- dl_spend_hardscape
  sum(case when market_id = 4 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR860D, -- wa_spend_hardscape
  sum(case when market_id = 30 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR861D, -- sj_spend_hardscape
  sum(case when market_id = 31 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR862D, -- pa_spend_hardscape
  sum(case when market_id = 29 and product_id = 34 and is_commercial = 0 then fee else 0 end) as MAR863D, -- st_spend_hardscape
  -- Homeadvisor Commercial Spend
  sum(case when is_commercial = 1 then fee else 0 end) as MAR330C, -- ha_spend_commercial
  sum(case when market_id = 9 and is_commercial = 1 then fee else 0 end) as MAR267C, -- eb_spend_commercial
  sum(case when market_id in (4,30,31,8) and is_commercial = 1 then fee else 0 end) as MAR269C, -- sbsf_spend_commercial
  sum(case when market_id = 9 and is_commercial = 1 then fee else 0 end) as MAR270C, -- nb_spend_commercial
  sum(case when market_id = 3 and is_commercial = 1 then fee else 0 end) as MAR268C, -- sac_spend_commercial
  sum(case when market_id = 10 and is_commercial = 1 then fee else 0 end) as MAR272C, -- fr_spend_commercial
  sum(case when region_id = 1 and is_commercial = 1 then fee else 0 end) as MAR495C, -- socal_spend_commercial
  sum(case when market_id = 14 and is_commercial = 1 then fee else 0 end) as MAR509C, -- sv_spend_commercial
  sum(case when market_id = 5 and is_commercial = 1 then fee else 0 end) as MAR510C, -- oc_spend_commercial
  sum(case when market_id = 6 and is_commercial = 1 then fee else 0 end) as MAR511C, -- la_spend_commercial
  sum(case when market_id = 7 and is_commercial = 1 then fee else 0 end) as MAR680C, -- vc_spend_commercial
  sum(case when market_id = 4 and is_commercial = 1 then fee else 0 end) as MAR860C, -- wa_spend_commercial
  sum(case when market_id = 30 and is_commercial = 1 then fee else 0 end) as MAR861C, -- sj_spend_commercial
  sum(case when market_id = 31 and is_commercial = 1 then fee else 0 end) as MAR862C, -- pa_spend_commercial
  sum(case when market_id = 29 and is_commercial = 1 then fee else 0 end) as MAR863C, -- st_spend_commercial
  sum(case when market_id = 22 and is_commercial = 1 then fee else 0 end) as MAR1115C, -- bl_spend_commercial
  sum(case when market_id = 21 and is_commercial = 1 then fee else 0 end) as MAR1156C, -- dc_spend_commercial
  sum(case when market_id = 35 and is_commercial = 1 then fee else 0 end) as MAR1501C, -- ar_spend_commercial
  sum(case when market_id = 24 and is_commercial = 1 then fee else 0 end) as MAR1611C, -- mi_spend_commercial
  sum(case when market_id = 26 and is_commercial = 1 then fee else 0 end) as MAR1659C, -- or_spend_commercial
  sum(case when market_id is null and is_commercial = 1 then fee else 0 end) as MAR512C, -- nomarket_spend_commercial
  sum(case when market_id = 43 and is_commercial = 1 then fee else 0 end) as MAR2227C, -- se_spend_commercial
  sum(case when market_id = 42 and is_commercial = 1 then fee else 0 end) as MAR2911C, -- wn_il_ch_spend_commercial
  sum(case when market_id = 57 and is_commercial = 1 then fee else 0 end) as MAR2970C, -- wn_il_na_spend_commercial
  sum(case when market_id = 58 and is_commercial = 1 then fee else 0 end) as MAR3029C -- wn_il_la_spend_commercial
from ha_data
group by 1
order by 1 desc
