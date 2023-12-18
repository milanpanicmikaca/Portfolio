-- [todo] we are missing about 5-10% of the spent due to HA call connects that have no visitor action no email in the lead and possibly other un-matched records 
--      (104K sun of ha_fee instead of 113K in BR)
-- [todo] incorporate ha_refunds table from googlesheets tablespace
-- [todo] change google allocation to county or market based monthly CPL from global monthly CPL
with 
ha_unique as ( -- need to use ha_spent_flat records if visitor action missing
  -- join by email/earliest lead whose date is before the order
  select email, min(srOid) as srOid, avg(fee) as fee, min(date) as date, min(lead_description) as lead_description
  from ext_marketing.ha_spend_flat ha group by 1
),
lead_channels as (
  select 
    l.id as lead_id,
    extract( date from l.created_at AT TIME ZONE 'America/Los_Angeles') as lead_arrived_at,
    extract( date from o.created_at AT TIME ZONE 'America/Los_Angeles') as created_at,
    order_id,
case
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%yelp%' then '/Paid/Yelp'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%home%advisor%' then '/Paid/Home Advisor/'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%thumbtack%' then '/Paid/Thumbtack'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%nextdoor%' then '/Paid/Nextdoor'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%lawson%'  then '/Paid/Lawson'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%borg%'  then '/Paid/Borg'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%bark%'  then '/Paid/Bark'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%facebook%' then 
    case 
      when json_extract_scalar(cv.event_object,'$.utm_campaign') is not null and 
             cv.landing_page like '%ergeon.com/blog/%' then '/Non Paid/Facebook/'
      else '/Paid/Facebook/'
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%instagram%' or
       lower(json_extract_scalar(cv.event_object,'$.browser_name')) like '%instagram%' then 
    case
      when json_extract_scalar(cv.event_object,'$.utm_campaign') is not null and 
          cv.landing_page like '%ergeon.com/blog/%' then '/Non Paid/Facebook/Instagram/'
      when cv.landing_page like '%ergeon.com/' or cv.landing_page like '%ergeon.com/?fbclid%' then '/Non Paid/Facebook/Instagram/'
      else '/Paid/Facebook/Instagram/'
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%directmail%'  then '/Non Paid/Direct Mail/'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%email%' or
       lower(json_extract_scalar(cv.event_object,'$.initial_referrer')) like '%android.gm%' then '/Non Paid/Email Marketing/'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%bing%' then '/Paid/Bing'
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%google%' or lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%gmb%' then
    case 
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%gls%' then '/Paid/Google/GLS/'
      when cv.landing_page is null and lower(json_extract_scalar(cv.event_object,'$.utm_medium')) like '%website%' then '/Non Paid/Google/SEO/cities/website_wp_form'
      when cv.landing_page is null then '/Non Paid/Google/Direct/'
      when lower(cv.landing_page) like '%ergeon.com/' then '/Non Paid/Google/Direct/organic/'
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%ls%brand%' then '/Non Paid/Google/Direct/'
      when lower(json_extract_scalar(cv.event_object,'$.utm_campaign')) like '%gmb%' or 
           lower(json_extract_scalar(cv.event_object,'$.utm_source')) like '%gmb%' then '/Non Paid/Google/GMB/'
      when lower(cv.landing_page) like '%gclid%' or
           lower(cv.landing_page) like '%gbraid%' or
           lower(cv.landing_page) like '%wbraid%' then '/Paid/Google/Ads/'
      when lower(cv.landing_page) like '%ergeon.com/blog%' then '/Non Paid/Google/SEO/blog/'
      when lower(cv.landing_page) like '%ergeon.com/cities%' then '/Non Paid/Google/SEO/cities/website_wp' --took place only until July
      when lower(cv.landing_page) like '%ergeon.com/%cities%' then '/Non Paid/Google/SEO/cities/'
      when lower(cv.landing_page) like '%ergeon.com/%gallery%' then '/Non Paid/Google/SEO/gallery/'
      when lower(cv.landing_page) like '%ergeon.com/contacts%' then '/Non Paid/Google/SEO/contacts/'
      when lower(cv.landing_page) like '%ergeon.com/fences%' then '/Non Paid/Google/SEO/fences/'
      when lower(cv.landing_page) like '%ergeon.com/locations%' then '/Non Paid/Google/SEO/locations/'
      when lower(cv.landing_page) like '%ergeon.com/careers%' then '/Non Paid/Google/SEO/careers/'
      when lower(cv.landing_page) like '%ergeon.com/%grass%' then '/Non Paid/Google/SEO/grass/'
      when lower(cv.landing_page) like '%ergeon.com/%' and cv.referrer is not null then '/Non Paid/Google/SEO/'
      else '/Non Paid/Google/Unknown/'
    end
  when lower(json_extract_scalar(cv.event_object,'$.utm_source')) = 'direct' then
    case 
      when lower(json_extract_scalar(cv.event_object,'$.utm_medium')) = 'webchat' then '/Non Paid/Direct/Webchat'
      else '/Non Paid/Direct'
    end 
  else '/Non Paid/Unknown/'
end as channel
  from 
    ergeon.core_lead l left join 
    ergeon.customers_visitoraction cv on cv.id = l.visitor_action_id left join
    ergeon.store_order o on o.id = l.order_id left join 
    ha_unique ha on ha.email = l.email and ha.date <= extract( date from l.created_at AT TIME ZONE 'America/Los_Angeles')
  where o.created_at >= '2018-04-16'
),
monthly_paid_lead_cnts as (
  select 
    date_trunc(l.created_at, month) as date, 
    count(case when lower(channel) like '%thumbtack%' then l.lead_id else null end) as tt_cnt,
    count(case when lower(channel) like '%nextdoor%' then l.lead_id else null end) as nd_cnt,
    count(case when lower(channel) like '%/paid/facebook%' then l.lead_id else null end) as fb_cnt,
    count(case when lower(channel) like '%paid%google%' then l.lead_id else null end) as gg_cnt
  from lead_channels l
  group by 1 order by 1 desc),
monthly_paid_channel_fees as (
  select 
    date_trunc(p.starting_at, month) as date,
    max(case when m.code = 'MAR137' then v.value else 0 end) as tt_fee,
    max(case when m.code = 'MAR160' then v.value else 0 end) as gg_fee,
    max(case when m.code = 'MAR123' then v.value else 0 end) as fb_fee,
    max(case when m.code = 'MAR237' then v.value else 0 end) as nd_fee
  from 
    warehouse.br_metric_value v 
    join warehouse.br_period p on p.id = v.period_id 
    join warehouse.br_metric m on m.id = v.metric_id 
  where 
    p.type = 'month'
  group by 1
  order by 1 desc
),
monthly_paid_lead_cpls as (
  select 
    f.date, 
    round(gg_fee*1.0/greatest(coalesce(gg_cnt,0),1), 2) as gg_cpl,
    round(nd_fee*1.0/greatest(coalesce(nd_cnt,0),1), 2) as nd_cpl,
    round(fb_fee*1.0/greatest(coalesce(fb_cnt,0),1), 2) as fb_cpl,
    round(tt_fee*1.0/greatest(coalesce(tt_cnt,0),1), 2) as tt_cpl,
    100 as bo_cpl
  from 
    monthly_paid_channel_fees f 
    left join monthly_paid_lead_cnts c on c.date = f.date
  order by 1 desc
),
lead_channels_cpls as (
  select 
    lc.*,
    case when lower(channel) like '%paid%google%' then gg_cpl end as gg_fee,
    case when lower(channel) like '%nextdoor%' then nd_cpl end as nd_fee,
    case when lower(channel) like '%/paid/facebook%' then fb_cpl end as fb_fee,
    case when lower(channel) like '%thumbtack%' then tt_cpl end as tt_fee,
    case when lower(channel) like '%borg%' then bo_cpl end as bo_fee,
  from 
    lead_channels lc 
    join monthly_paid_lead_cpls m on m.date = date_trunc(lc.created_at,month)
),
ha_leads1 as (
  select 
    l.id,
    --l.order_id, 
    ha.srOid as sroid,
    coalesce(ha.fee,0) as ha_fee
  from 
    ergeon.core_lead l join 
    ergeon.customers_visitoraction a on a.id = l.visitor_action_id join  
    ext_marketing.ha_spend_flat ha on cast(srOid as string) = json_extract_scalar(a.event_object, '$.job_id') 
),
ha_leads2 as (
  select 
    l.id,
    --l.order_id, 
    ha.srOid as sroid,
    coalesce(ha.fee,0) as ha_fee
  from 
    ergeon.core_lead l  join
    ergeon.customers_visitoraction a on a.id = l.visitor_action_id left join  
    ext_marketing.ha_spend_flat ha1 on cast(srOid as string) = json_extract_scalar(a.event_object, '$.job_id') left join 
    ha_unique ha on ha.email = l.email and  ha.date < extract( date from l.created_at AT TIME ZONE 'America/Los_Angeles')
  where 
    ha1.email is null
),
lead_ha_fee as (
  select 
    l.id as lead_id, 
    sum( coalesce(l1.ha_fee,0)+coalesce(l2.ha_fee,0) ) as ha_fee  
  from 
    ergeon.core_lead l left join 
    ha_leads1 l1 on l1.id = l.id left join
    ha_leads2 l2 on l2.id = l.id
  group by 1
)
select 
   lcc.lead_id,
   lcc.order_id,
   lcc.lead_arrived_at,
   coalesce(ha_fee,0) as ha_fee, 
   coalesce(fb_fee,0) as fb_fee, 
   coalesce(tt_fee,0) as tt_fee, 
   coalesce(nd_fee,0) as nd_fee, 
   coalesce(gg_fee,0) as gg_fee,
   coalesce(bo_fee,0) as bo_fee,
 from 
   lead_channels_cpls lcc left join 
   lead_ha_fee on lcc.lead_id = lead_ha_fee.lead_id

--select date_trunc(o.created_at, year), sum(mktg_fee) from orders join ergeon.store_order o on o.id = orders.order_id group by 1 order by 1