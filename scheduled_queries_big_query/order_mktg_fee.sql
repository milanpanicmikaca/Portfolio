with ha_unique as ( -- need to use ha_spent_flat records if visitor action missing
    -- join by email/earliest lead whose date is before the order
    select
        ha.email,
        min(srOid) as srOid,
        avg(fee) as fee,
        min(date) as date
    from ext_marketing.ha_spend_flat as ha
    group by 1
),

lead_channels as ( --order attributes
    select
        lead_id,
        created_at,
        order_id,
        channel,
        market,
        case
            when market like '%-CA-%' then 'California'
            when market like '%-TX-%' then 'Texas'
            when market like '%-GA-%' then 'Georgia'
            when market like '%-MD-%' then 'Maryland'
            when market like '%-PA-%' then 'Pennsylvania'
            when market like '%-VA-%' then 'Virginia'
            when market like '%-FL-%' then 'Florida'
            when market like '%-WA-%' then 'Washington'
            else 'Other Locations'
        end as region,
        case 
            when product like '/Fence%' then 'fence'
            when product like '/Driveway%' then 'driveway'
            when product like '/Landscaping%' then 'turf'
        end as product
    from
        int_data.order_calculated_fields
    where
        is_warranty_order = FALSE
),

lead_channels_ext as ( --extending lead_channels with additional column for GLS
    select
        l.*,
        -- trim applied to remove spaces
        case when lower(channel) like '%/paid/google/gls%' and created_at >= '2022-09-12' then trim(g.gls_account) else 'no acc' end as gls_account,
        case when l.market in ('WS-TX-DL', 'WS-TX-FW') then 'WS-TX-DL&FW' else l.market end as market_ha_ads
    from lead_channels as l
    left join ext_marketing.gls_account_market as g on g.market = l.market
        and g.product = l.product
        and l.created_at between cast(g.start_date as date)  -- date range when GLS account covered market
        -- null celLs are uploaded as 1970, converting it to current day
        and cast(replace(g.change_date, '1970-01-01', cast(current_date() as string)) as date)
),

weekly_paid_leads_cnt as ( --FB, TT, SDR weekly leads counts
    select
        date_trunc(l.created_at, week(monday)) as date,
        sum(case when lower(channel) like '%thumbtack%' then 1 end) as tt_cnt,
        sum(case when lower(channel) like '%/paid/facebook%' and product = 'fence' then 1 end) as f_fb_cnt,
        sum(case when lower(channel) like '%/paid/facebook%' and product = 'driveway' then 1 end) as d_fb_cnt,
        sum(case when lower(channel) like '%/paid/facebook%' and product = 'turf' then 1 end) as t_fb_cnt,
        sum(case when lower(channel) like '%sdr%' and created_at >= '2022-07-01' then 1 end) as sdr_cnt
    from lead_channels as l
    group by 1
),

monthly_paid_leads_cnt as ( --Nextdoor monthly leads counts
    select
        date_trunc(l.created_at, month) as date,
        sum(case when lower(channel) like '%nextdoor%' and product = 'fence' then 1 end) as f_nd_cnt,
        sum(case when lower(channel) like '%nextdoor%' and product = 'driveway' then 1 end) as d_nd_cnt,
        sum(case when lower(channel) like '%nextdoor%' and product = 'turf' then 1 end) as t_nd_cnt
    from lead_channels as l
    group by 1
),

gls_paid_leads_cnt as ( --GLS  weekly leads counts
    select
        date_trunc(l.created_at, week(monday)) as date,
        l.gls_account,
        count(*) as cnt_leads
    from lead_channels_ext as l
    where channel like '%/Paid/Google/GLS%'
    group by 1, 2
),

google_paid_leads_cnt as ( --Google Ads weekly leads counts
    select
        date_trunc(created_at, week(monday)) as date,
        region,
        count(*) as cnt_leads
    from lead_channels
    where channel like '%/Paid/Google/Ads%'
    group by 1, 2
),

ha_ads_paid_leads_cnt as ( --Angi Ads weekly leads counts
    select
        date_trunc(created_at, week(monday)) as date,
        market_ha_ads as market,
        count(*) as cnt_leads
    from lead_channels_ext
    where lower(channel) like '%home%advisor/ads'
    group by 1, 2
),

weekly_fb_fee as ( --FB weekly fees
    select
        date_trunc(date, week(monday)) as date,
        sum(coalesce(fence, 0)) as f_fb_fee,
        sum(coalesce(driveway, 0)) as d_fb_fee,
        sum(coalesce(turf, 0)) as t_fb_fee
    from googlesheets.facebook_spend
    where date > '2018-04-15'
    group by 1
),

weekly_tt_fee as ( --TT weekly fees
    select
        date_trunc(date, week(monday)) as date,
        sum(coalesce(initial_charge, 0) - coalesce(refund, 0)) as tt_fee
    from googlesheets.thumbtack
    group by 1
),

weekly_sdr_fee as ( --SDR weekly fees
    select
        date_trunc(date_sub(date, interval 1 week), week(monday)) as date,
        abs(0.16 * sum(0.6 * amount)) as sdr_fee
    from int_data.sdr_data
    where
        date_sub(date, interval 1 week) = '2022-06-27'
    group by 1
    union all
    select
        date_trunc(date_sub(date, interval 1 week), week(monday)) as date,
        abs(sum(0.6 * amount)) as sdr_fee
    from int_data.sdr_data
    where
        date_sub(date, interval 1 week) >= '2022-07-01'
    group by 1
),

monthly_nd_fee as (--Nextdoor monthly fees
    select
        date_trunc(date, month) as date,
        sum(case when ns.product = 'Fence' and amount is not null then amount else 0 end) as f_nd_fee,
        sum(case when ns.product = 'Driveway' and amount is not null then amount else 0 end) as d_nd_fee,
        sum(case when lower(ns.product) like '%artificial%grass%' and amount is not null then amount else 0 end) as t_nd_fee
    from googlesheets.nextdoor_spend as ns
    group by 1
),

weekly_gls_fee as ( --GLS weekly fees
    select
        date_trunc(date, week(monday)) as date,
        --no acc to identify period when there were no tracking of GLS account / trim applied to remove spaces
        coalesce(trim(gls_account), 'no acc') as gls_account,
        coalesce(sum(value), 0) as gls_fee
    from ext_marketing.gls_spend
    group by 1, 2
),

google_campaigns as (
    select
        CampaignId,
        CampaignName
    from google.Campaign_6286546613
    where _LATEST_DATE = _DATA_DATE --last updated version of campaign
),

weekly_google_fee as ( --Google Ads weekly fees
    select
        date_trunc(g.Date, week(monday)) as date,
        case
            when array_reverse(split(gt.Canonical_Name)) [safe_offset(1)]
                not in ('California', 'Texas', 'Georgia', 'Pennsylvania', 'Maryland', 'Virginia', 'Florida', 'Washington')
                then 'Other Locations'
            else array_reverse(split(gt.Canonical_Name)) [safe_offset(1)] end as region,
        sum(g.Cost / 1000000) as amount
    from google.p_GeoStats_6286546613 as g
    inner join google_campaigns as c on c.CampaignId = g.CampaignId
    left join int_data_tests.geo_targets as gt on gt.Criteria_ID = g.RegionCriteriaId --ergeon.geo_googletarget (not ready 01/27)
    where
        g.Date >= '2018-04-16'
        and
        IsTargetingLocation = TRUE
        and
        lower(c.CampaignName) not like '%brand%'
    group by 1, 2
),

--local as
--(
--  select
--    date as local_day,
--    sum(value) as local_spend
--  from ext_marketing.google_local_materialized
--  group by 1
--),
bark_fee as ( --Bark fee per order
    select
        order_id,
        sum(cast(json_extract_scalar(cv.event_object, '$.fee') as numeric)) as ba_fee
    from ergeon.core_lead as cl
    left join ergeon.customers_visitoraction as cv on cl.visitor_action_id = cv.id
    where
        lower(json_extract_scalar(cv.event_object, '$.utm_source')) like '%bark%'
    group by 1
),

weekly_paid_leads_cpl as ( --FB, TT, SDR weekly cost per lead per channel
    select
        cnts.date,
        round(f_fb_fee * 1.0 / greatest(coalesce(f_fb_cnt, 0), 1), 2) as f_fb_cpl,
        round(d_fb_fee * 1.0 / greatest(coalesce(d_fb_cnt, 0), 1), 2) as d_fb_cpl,
        round(t_fb_fee * 1.0 / greatest(coalesce(t_fb_cnt, 0), 1), 2) as t_fb_cpl,
        round(tt_fee * 1.0 / greatest(coalesce(tt_cnt, 0), 1), 2) as tt_cpl,
        round(sdr_fee * 1.0 / greatest(coalesce(sdr_cnt, 0), 1), 2) as sdr_cpl
    from weekly_paid_leads_cnt as cnts
    left join weekly_fb_fee as fb on fb.date = cnts.date
    left join weekly_tt_fee as tt on tt.date = cnts.date
    left join weekly_sdr_fee as sdr on sdr.date = cnts.date
),

monthly_paid_leads_cpl as ( --Nextdoor monthly cost per lead per channel
    select
        cnts.date,
        round(f_nd_fee * 1.0 / greatest(coalesce(f_nd_cnt, 0), 1), 2) as f_nd_cpl,
        round(d_nd_fee * 1.0 / greatest(coalesce(d_nd_cnt, 0), 1), 2) as d_nd_cpl,
        round(t_nd_fee * 1.0 / greatest(coalesce(t_nd_cnt, 0), 1), 2) as t_nd_cpl
    from monthly_paid_leads_cnt as cnts
    left join monthly_nd_fee as nd on nd.date = cnts.date
),

gls_timeseries as (
    select
        date_trunc(dates, week(monday)) as date,
        case when dates >= '2022-09-12' then gls_account else 'no acc' end as gls_account
    from unnest(generate_date_array('2020-10-26', current_date('America/Los_Angeles'), interval 1 day)) as dates
    cross join (select distinct gls_account from weekly_gls_fee)
    group by 1, 2
),

gls_intermediate1 as (
    select
        t.date,
        t.gls_account,
        l.cnt_leads,
        f.gls_fee,
        count(l.cnt_leads) over (partition by t.gls_account order by t.date) as lead_exist_cnt --counts days where we have leads
    from gls_timeseries as t
    left join weekly_gls_fee as f on f.gls_account = t.gls_account
        and f.date = t.date
    left join gls_paid_leads_cnt as l on l.gls_account = t.gls_account
        and l.date = t.date
),

gls_intermediate2 as (
    select
        * except(gls_fee),
        sum(gls_fee) over (partition by gls_account, lead_exist_cnt order by date) as gls_fee --sums gls fee per GLS account for weeks w/o leads
    from gls_intermediate1
    where cnt_leads is null
    qualify row_number() over (partition by gls_account, lead_exist_cnt order by date desc) = 1
),

gls_final as (
    select
        gi1.date,
        gi1.gls_account,
        gi1.cnt_leads,
        if(gi1.cnt_leads is not null, coalesce(gi2.gls_fee + coalesce(gi1.gls_fee, 0), gi1.gls_fee), NULL) as gls_fee
    from gls_intermediate1 as gi1
    left join gls_intermediate2 as gi2 on date_add(gi2.date, interval 1 week) = gi1.date
        and gi2.gls_account = gi1.gls_account
),

weekly_gls_leads_cpl as (
    select
        date,
        gls_account,
        sum(gls_fee) / nullif(sum(cnt_leads), 0) as gls_cpl
    from gls_final
    group by 1, 2
),

google_timeseries as (
    select
        date_trunc(dates, week(monday)) as date,
        region
    from unnest(generate_date_array('2018-04-16', current_date('America/Los_Angeles'), interval 1 day)) as dates
    cross join (select distinct region from google_paid_leads_cnt)
    group by 1, 2
),

google_intermediate1 as (
    select
        t.date,
        t.region,
        w.cnt_leads,
        f.amount as google_fee,
        count(w.cnt_leads) over (partition by t.region order by t.date) as lead_exist_cnt
    from google_timeseries as t
    left join google_paid_leads_cnt as w on w.date = t.date
        and w.region = t.region
    left join weekly_google_fee as f on f.date = t.date
        and f.region = t.region
),

google_intermediate2 as (
    select
        * except(google_fee),
        sum(google_fee) over (partition by region, lead_exist_cnt order by date) as google_fee --sums google fee per region for weeks w/o leads
    from google_intermediate1
    where cnt_leads is null
    qualify row_number() over (partition by region, lead_exist_cnt order by date desc) = 1
),

google_final as (
    select
        gi1.date,
        gi1.region,
        gi1.cnt_leads,
        if(gi1.cnt_leads is not null, coalesce(gi2.google_fee + coalesce(gi1.google_fee, 0), gi1.google_fee), NULL) as google_fee
    from google_intermediate1 as gi1
    left join google_intermediate2 as gi2 on date_add(gi2.date, interval 1 week) = gi1.date
        and gi2.region = gi1.region
),

weekly_google_leads_cpl as (
    select
        date,
        region,
        sum(google_fee) / nullif(sum(cnt_leads), 0) as google_cpl
    from google_final
    group by 1, 2
),

ha_ads_timeseries as (
    select
        date_trunc(dates, week(monday)) as date,
        market
    from unnest(generate_date_array('2023-01-02', current_date('America/Los_Angeles'), interval 1 day)) as dates
    cross join (select distinct market from ha_ads_paid_leads_cnt)
    group by 1, 2
),

ha_ads_intermediate1 as (
    select
        t.date,
        t.market,
        l.cnt_leads,
        s.weekly_fee as ha_ads_fee,
        count(l.cnt_leads) over (partition by t.market order by t.date) as lead_exist_cnt
    from ha_ads_timeseries as t
    left join ha_ads_paid_leads_cnt as l on l.date = t.date
        and l.market = t.market
    left join googlesheets.ha_ads_spend as s on s.market = t.market
        and cast(s.date as date) = date_trunc(t.date, month) --fixed weekly spend calculated as fixed monthly spend*12/52
),

ha_ads_intermediate2 as (
    select
        * except(ha_ads_fee),
        sum(ha_ads_fee) over (partition by market, lead_exist_cnt order by date) as ha_ads_fee
    from ha_ads_intermediate1
    where cnt_leads is null
    qualify row_number() over (partition by market, lead_exist_cnt order by date desc) = 1
),

ha_ads_final as (
    select
        gi1.date,
        gi1.market,
        gi1.cnt_leads,
        if(gi1.cnt_leads is not null, coalesce(gi2.ha_ads_fee + coalesce(gi1.ha_ads_fee, 0), gi1.ha_ads_fee), NULL) as ha_ads_fee
    from ha_ads_intermediate1 as gi1
    left join ha_ads_intermediate2 as gi2 on date_add(gi2.date, interval 1 week) = gi1.date
        and gi2.market = gi1.market
),

weekly_ha_ads_leads_cpl as (
    select
        date,
        market,
        sum(ha_ads_fee) / nullif(sum(cnt_leads), 0) as ha_ads_cpl
    from ha_ads_final
    group by 1, 2
),

ha_leads1 as ( --Home Advisor leads through Admin
    select
        l.id,
        l.order_id,
        ha.srOid as sroid,
        case when sales_tax_rate > 0 then ha.fee + sales_tax_rate * ha.fee else ha.fee end as ha_fee
    from
        ergeon.core_lead as l inner join
        ergeon.customers_visitoraction as a on a.id = l.visitor_action_id inner join
        ext_marketing.ha_spend_flat as ha on cast(srOid as string) = json_extract_scalar(a.event_object, '$.job_id') left join
        lead_channels as lc on lc.lead_id = l.id left join
        int_data.ha_taxed_states as hat on hat.name = lc.region
    qualify rank() over(partition by json_extract_scalar(event_object, '$.job_id') order by l.id desc) = 1
),

ha_leads2 as ( --Home Advisor leads through email
    select
        l.id,
        l.order_id,
        ha.srOid as sroid,
        case when sales_tax_rate > 0 then ha.fee + (sales_tax_rate) * ha.fee else ha.fee end as ha_fee
    from
        ergeon.core_lead as l inner join
        ergeon.customers_visitoraction as a on a.id = l.visitor_action_id left join
        ext_marketing.ha_spend_flat as ha1 on cast(srOid as string) = json_extract_scalar(a.event_object, '$.job_id') left join
        ha_unique as ha on ha.email = l.email and ha.date < extract(date from l.created_at at time zone 'America/Los_Angeles') left join
        lead_channels as lc on lc.lead_id = l.id left join
        int_data.ha_taxed_states as hat on hat.name = lc.region
    where
        ha1.email is null
        and lower(lc.channel) like '%home%advisor%' --in order to avoid ha costs to other channels
        and lower(lc.channel) not like '%home%advisor%ads%' --exclude Angi Ads in ha fee calculation 05/01/2023
    qualify rank() over (partition by ha.srOid order by l.id desc) = 1
),

discount_sr_id as (
    select
        srOid,
        case
            when date >= '2023-02-01' and date <= '2023-02-20' and state not in ('TX', 'GA') then 0.1
            when date >= '2023-02-21' then 0.2
        end as discount
    from
        ext_marketing.ha_spend_flat
    where
        (date >= '2023-02-01' and date <= '2023-02-20' and state not in ('TX', 'GA')) or date >= '2023-02-21'
),

ha_refund as ( --Home advisor refunds
    select
        l.id,
        l.order_id,
        sr_id as sroid,
        case
            when sales_tax_rate > 0 then coalesce( --if state has a tax rate 
                (coalesce(discount, 0) * credit_amount + credit_amount) --amount + discounted amount (for leads with discount from Home Advisor)
                + (sales_tax_rate * (coalesce(discount, 0) * credit_amount + credit_amount)), 0) -- plus taxed amount
            else coalesce(coalesce(discount, 0) * credit_amount + credit_amount, 0) --if there is no tax rate then amount + discounted amount
        end as ha_ref,
        extract(date from ha_refund.created_at at time zone 'America/Los_Angeles') as ha_refund_at
    from
        ergeon.core_lead as l inner join
        ergeon.customers_visitoraction as a on a.id = l.visitor_action_id inner join
        googlesheets.ha_refund on cast(sr_id as string) = json_extract_scalar(a.event_object, '$.job_id') left join
        lead_channels as lc on lc.lead_id = l.id left join
        int_data.ha_taxed_states as hat on hat.name = lc.region left join
        discount_sr_id as discount_sr on discount_sr.srOid = ha_refund.sr_id
    qualify rank() over (partition by json_extract_scalar(event_object, '$.job_id') order by l.id desc) = 1
),

order_ha_fee as ( --Home Advisor fee
    select
        o.id as order_id,
        sum(coalesce(l1.ha_fee, 0) + coalesce(l2.ha_fee, 0)) as ha_initial_fee,--Without refunds
        sum(coalesce(l1.ha_fee, 0) + coalesce(l2.ha_fee, 0)) - sum(coalesce(ref.ha_ref, 0)) as ha_fee, --with refunds
        min(ha_refund_at) as ha_refund_at
    from
        ergeon.store_order as o left join
        ha_leads1 as l1 on l1.order_id = o.id left join
        ha_leads2 as l2 on l2.order_id = o.id left join
        ha_refund as ref on ref.order_id = o.id
    group by 1
),

lead_channels_cpls as ( --allocation of marketing fee costs
    select
        lc.*,
        case
            when lower(channel) like '%/paid/facebook%' and product = 'fence' then w.f_fb_cpl
            when lower(channel) like '%/paid/facebook%' and product = 'driveway' then w.d_fb_cpl
            when lower(channel) like '%/paid/facebook%' and product = 'turf' then w.t_fb_cpl
            when lower(channel) like '%thumbtack%' then w.tt_cpl
            when lower(channel) like '%sdr%' and created_at >= '2022-07-01' then w.sdr_cpl
            when lower(channel) like '%nextdoor%' and product = 'fence' then m.f_nd_cpl
            when lower(channel) like '%nextdoor%' and product = 'driveway' then m.d_nd_cpl
            when lower(channel) like '%nextdoor%' and product = 'turf' then m.t_nd_cpl
            when lower(channel) like '%borg%' then 100
            when lower(channel) like '%/paid/google/gls%' then g.gls_cpl
            when lower(channel) like '%/paid/google/ads%' then gg.google_cpl
            when lower(channel) like '%home%advisor/ads' then ha.ha_ads_cpl
            else 0
        end as cpl
    from lead_channels_ext as lc
    left join weekly_paid_leads_cpl as w on w.date = date_trunc(lc.created_at, week(monday))
    left join monthly_paid_leads_cpl as m on m.date = date_trunc(lc.created_at, month)
    left join weekly_gls_leads_cpl as g on g.date = date_trunc(lc.created_at, week(monday))
        and g.gls_account = lc.gls_account
    left join weekly_google_leads_cpl as gg on gg.date = date_trunc(lc.created_at, week(monday))
        and gg.region = lc.region
    left join weekly_ha_ads_leads_cpl as ha on ha.date = date_trunc(lc.created_at, week(monday))
        and ha.market = lc.market_ha_ads
),

orders1 as (
    select
        order_id,
        sum(case when lower(channel) like '%/paid/facebook%' then cpl else 0 end) as fb_fee,
        sum(case when lower(channel) like '%thumbtack%' then cpl else 0 end) as tt_fee,
        sum(case when lower(channel) like '%sdr%' then cpl else 0 end) as sdr_fee,
        sum(case when lower(channel) like '%nextdoor%' then cpl else 0 end) as nd_fee,
        sum(case when lower(channel) like '%borg%' then cpl else 0 end) as bo_fee,
        sum(case when lower(channel) like '%/paid/google/gls%' then cpl else 0 end) as gg_gls_fee,
        sum(case when lower(channel) like '%/paid/google/ads%' then cpl else 0 end) as gg_fee,
        sum(case when lower(channel) like '%home%advisor/ads' then cpl else 0 end) as ha_ads_fee,
        sum(case when lower(channel) like '%bark%' then ba_fee else 0 end) as ba_fee
    from lead_channels_cpls
    left join bark_fee using (order_id)
    group by 1
)

select
    orders1.order_id,
    ha_refund_at,
    coalesce(ha_initial_fee, 0) as ha_initial_fee,
    coalesce(ha_fee, 0) as ha_fee,
    coalesce(fb_fee, 0) as fb_fee, -- 20% discount for TT leads
    coalesce(tt_fee, 0) * 0.8 as tt_fee,
    coalesce(nd_fee, 0) as nd_fee,
    coalesce(gg_fee, 0) as gg_fee,
    coalesce(gg_gls_fee, 0) as gg_gls_fee,
    coalesce(bo_fee, 0) as bo_fee,
    coalesce(ha_ads_fee, 0) as ha_ads_fee,
    coalesce(ba_fee, 0) as ba_fee,
    -- 20% discount for TT leads
    coalesce(sdr_fee, 0) as sdr_fee,
    coalesce(ha_fee + fb_fee + tt_fee * 0.8 + nd_fee + gg_fee + gg_gls_fee + bo_fee + ba_fee + sdr_fee + ha_ads_fee, 0) as mktg_fee
from orders1
left join order_ha_fee on orders1.order_id = order_ha_fee.order_id
