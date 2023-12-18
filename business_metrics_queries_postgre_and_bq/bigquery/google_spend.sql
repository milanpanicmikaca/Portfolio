with campaign as
(
  select
    distinct CampaignId,
    CampaignName,
    case when CampaignName like '%Brand%' then 'brand' end as product
  from google.Campaign_6286546613
),
calc_spend as
(
  select
    cs.Date as day,
    sum(case when c.product = 'brand' then cs.Cost/1000000 else 0 end) as google_spend_brand
  from google.CampaignBasicStats_6286546613 cs
  left join campaign c on c.CampaignId = cs.CampaignId
  where
    cs.Cost > 0
  group by 1
),
brand_fees as 
(
select
  date_trunc(day,{period}) as date,
  sum(google_spend_brand) as MAR312
from calc_spend 
group by 1
),
google_fees as 
(
Select 
  date_trunc(created_at,{period}) as date,
  sum(coalesce(gg_fee,0)) as MAR160,
  sum(case when product like '/Fence%' then coalesce(gg_fee,0) end) as MAR160F, 
  sum(case when product like '/Driveway%' then coalesce(gg_fee,0) end) as MAR160D,
  sum(case when product like '/Landscaping%' then coalesce(gg_fee,0) end) as MAR160T
from int_data.order_ue_materialized
group by 1
),
gls_fees as (
  select 
    date_trunc(date, {period}) as date,
    sum(coalesce(value,0)) as gls_spend,
    sum(case when product like '%fence%' then coalesce(value,0) else 0 end) as f_gls_spend,
    sum(case when product like '%landscap%' then coalesce(value,0) else 0 end) as t_gls_spend
  from ext_marketing.gls_spend
  group by 1
)
select 
  date,
  coalesce(gf.MAR160,0) + coalesce(gls.gls_spend,0) as MAR160,
  coalesce(gf.MAR160F,0) + coalesce(gls.f_gls_spend,0) as MAR160F,
  gf.MAR160D as MAR160D,
  coalesce(gf.MAR160T,0) + coalesce(gls.t_gls_spend,0) as MAR160T,
  bf.MAR312
from google_fees gf
left join brand_fees bf using(date)
left join gls_fees gls using(date)
