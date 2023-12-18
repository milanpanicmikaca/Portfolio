with
location_map as ( --map yelp_location to the corresponding markets of admin
    (select
        'watsonville' as yelp_location,
        'CN-WA' as location,
        4 as market_id)
    union all
    (select
        'san jose' as yelp_location,
        'CN-SJ' as location,
        30 as market_id)
    union all
    (select
        'palo alto' as yelp_location,
        'CN-PA' as location,
        31 as market_id)
    union all
    (select
        'sacramento' as yelp_location,
        'CN-SA' as location,
        3 as market_id)
    union all
    (select
        'fresno' as yelp_location,
        'CN-FR' as location,
        10 as market_id)
    union all
    (select
        'napa' as yelp_location,
        'CN-NB' as location,
        9 as market_id)
    union all
    (select
        'oakland' as yelp_location,
        'CN-EB' as location,
        2 as market_id)
    union all
    (select
        'stockton' as yelp_location,
        'CN-ST' as location,
        29 as market_id)
    union all
    (select
        'san francisco' as yelp_location,
        'CN-SF' as location,
        8 as market_id)
    union all
    (select
        'dallas' as yelp_location,
        'TX-DL' as location,
        16 as market_id)
    union all
    (select
        'fort worth' as yelp_location,
        'TX-FW' as location,
        17 as market_id)
    union all
    (select
        'los angeles' as yelp_location,
        'CS-LA' as location,
        6 as market_id)
    union all
    (select
        'riverside' as yelp_location,
        'CS-SV' as location,
        14 as market_id)
    union all
    (select
        'thousand oaks' as yelp_location,
        'CS-VC' as location,
        7 as market_id)
    union all
    (select
        'lake forest' as yelp_location,
        'CS-OC' as location,
        5 as market_id)
    union all
    (select
        'san diego' as yelp_location,
        'CS-SD' as location,
        1 as market_id)
    union all
    (select
        'houston' as yelp_location,
        'TX-HT' as location,
        18 as market_id)
    union all
    (select
        'atlanta' as yelp_location,
        'GA-AT' as location,
        20 as market_id)
    union all
    (select
        'san antonio' as yelp_location,
        'TX-SA' as location,
        19 as market_id)
    union all
    (select
        'austin' as yelp_location,
        'TX-AU' as location,
        32 as market_id)
    union all
    (select
        'baltimore' as yelp_location,
        'MD-BL' as location,
        22 as market_id)
    union all
    (select
        'washington dc' as yelp_location,
        'MD-DC' as location,
        21 as market_id)
    union all
    (select
        'philadelphia' as yelp_location,
        'PA-PH' as location,
        33 as market_id)
    union all
    (select
        'arlington' as yelp_location,
        'VA-AR' as location,
        35 as market_id)
    union all
    (select
        'miami' as yelp_location,
        'FL-MI' as location,
        24 as market_id)
    union all
    (select
        'orlado' as yelp_location,
        'FL-OR' as location,
        26 as market_id)
    union all
    (select
        'seattle' as yelp_location,
        'WA-SE' as location,
        43 as market_id)
),

lead_channels --yelp leads/location/product 
as (
    select
        lead_id,
        created_at,
        order_id,
        market_id,
        utm_medium,
        case
            when channel like '%Yelp%' then '/Paid/Yelp'
        end as channel,
        case when product like '/Fence%' then 'fence'
                  when product like '/Driveway%' then 'driveway'
                  when product like '/Landscaping%' then 'landscaping_design'
        end as product
    from
        int_data.order_calculated_fields
    where
        channel like '%Yelp%'
        and is_warranty_order = FALSE
),

--total lead per week/product/location
weekly_paid_lead_cnts as (
    select
        date_trunc(l.created_at, week(monday)) as date,
        product,
        market_id,
        count(*) as lead_cnt,
        sum(case when utm_medium = 'leadgen' then 1 else 0 end) as leadgen_cnt
    from lead_channels as l
    group by 1, 2, 3
),

month_paid_lead_cnts as (
    select
        date_trunc(l.created_at, month) as date,
        product,
        market_id,
        count(*) as lead_cnt,
        sum(case when utm_medium = 'leadgen' then 1 else 0 end) as leadgen_cnt
    from lead_channels as l
    where product = 'landscaping_design'
    group by 1, 2, 3
),

yelp_message --total messages per week/product/location
as (
    select
        date_trunc(date, week(monday)) as date,
        case
            when product = 'driveways' then 'driveway'
            else 'fence'
        end as product,
        case
            when y.location is not null then m.market_id
        end as market_id,
        count(*) as msg_cnt -- messages_tot
    from
        ext_marketing.yelp_message3_int as y left join
        ergeon.marketing_localaccount as l on cast(l.id as string) = cast(y.location as string) left join
        location_map as m on m.yelp_location = lower(split(l.label, ' - ') [offset(1)])
    where
        date_trunc(date, week(monday)) < '2022-06-09'
        and product != 'landscaping_design'
    group by 1, 2, 3
    union all
    select
        date_trunc(extract(date from y.created_at at time zone 'America/Los_Angeles'), week(monday)) as date,
        'fence' as product,
        case
            when y.local_account_id is not null then m.market_id
        end as market_id,
        count(*) as msg_cnt -- messages_tot
    from
        ergeon.marketing_yelpmessage as y left join
        ergeon.marketing_localaccount as l on y.local_account_id = l.id left join
        location_map as m on m.yelp_location = lower(split(l.label, ' - ') [offset(1)])
    where
        date_trunc(extract(date from y.created_at at time zone 'America/Los_Angeles'), week(monday)) >= '2022-06-09'
        and (y.product_id is null or y.product_id in (105, 34))
    group by 1, 2, 3
    union all
    select
        date_trunc(extract(date from y.created_at at time zone 'America/Los_Angeles'), month) as date,
        'landscaping_design' as product,
        case
            when y.local_account_id is not null then m.market_id
        end as market_id,
        count(*) as msg_cnt -- messages_tot
    from
        ergeon.marketing_yelpmessage as y left join
        ergeon.marketing_localaccount as l on y.local_account_id = l.id left join
        location_map as m on m.yelp_location = lower(split(l.label, ' - ') [offset(1)])
    where
        date_trunc(extract(date from y.created_at at time zone 'America/Los_Angeles'), month) >= '2022-07-01'
        and y.product_id = 132
    group by 1, 2, 3
),

yelp_spend as ( --total spend per week/product/location
    select
        date_trunc(date, week(monday)) as date,
        product,
        m.market_id,
        sum(amount) as spend
    from
        googlesheets.yelp_spend as s left join
        location_map as m on m.yelp_location = s.location
    where
        product != 'landscaping_design'
    group by 1, 2, 3
    union all
    select
        date_trunc(date, month) as date,
        product,
        m.market_id,
        sum(amount) as spend
    from
        googlesheets.yelp_spend as s left join
        location_map as m on m.yelp_location = s.location
    where
        product = 'landscaping_design'
    group by 1, 2, 3
),

yelp_budget --total budget per week/product/location
as (           --fence
    select
        date_trunc(date, week(monday)) as date,
        'fence' as product,
        m.market_id,
        sum(fence_budget) as budget
    from
        ext_marketing.yelp_budget as b left join
        location_map as m on m.yelp_location = b.location
    group by 1, 2, 3
    union all
    --driveway
    select
        date_trunc(date, week(monday)) as date,
        'driveway' as product,
        m.market_id,
        sum(driveway_budget) as budget
    from
        ext_marketing.yelp_budget as b left join
        location_map as m on m.yelp_location = b.location
    group by 1, 2, 3
    union all
    --turf
    select
        date_trunc(date, month) as date,
        'landscaping_design' as product,
        m.market_id,
        sum(turf_budget) as budget
    from
        ext_marketing.yelp_budget as b left join
        location_map as m on m.yelp_location = b.location
    group by 1, 2, 3
),

weekly_paid as ( --weekly allocation per week/product/location. 'yelp' in front of the alias was added to support order_ue column source spreadsheet 
    select
        *,
        spend / lead_cnt as yelp_cpl_spend,  --cost per lead (spend)
        spend / msg_cnt as yelp_cpm_spend,  --cost per message (spend)
        budget / lead_cnt as yelp_cpl_budget,  --cost per lead (budget)
        budget / msg_cnt as yelp_cpm_budget,  --cost per message (budget)
        case when leadgen_cnt > 0 then msg_cnt / leadgen_cnt else 0 end as mpl, --message per lead
        case when leadgen_cnt > 0 then 0 else msg_cnt / lead_cnt end as mpl2 --message per lead
    from
        weekly_paid_lead_cnts left join
        yelp_spend using (date, product, market_id) left join
        yelp_budget using (date, product, market_id) left join
        yelp_message using (date, product, market_id)
    where
        product != 'landscaping_design'
),

month_paid as ( --weekly allocation per month/turf/location
    select
        *,
        spend / lead_cnt as yelp_cpl_spend,  --cost per lead (spend)
        spend / msg_cnt as yelp_cpm_spend,  --cost per message (spend)
        budget / lead_cnt as yelp_cpl_budget,  --cost per lead (budget)
        budget / msg_cnt as yelp_cpm_budget,  --cost per message (budget)
        case when leadgen_cnt > 0 then msg_cnt / leadgen_cnt else 0 end as mpl, --message per lead
        case when leadgen_cnt > 0 then 0 else msg_cnt / lead_cnt end as mpl2 --message per lead
    from
        month_paid_lead_cnts left join
        yelp_spend using (date, product, market_id) left join
        yelp_budget using (date, product, market_id) left join
        yelp_message using (date, product, market_id)
    where
        product = 'landscaping_design'
)

select --attribution to all yelp leads
    lc.*,
    yelp_cpl_spend,
    yelp_cpl_budget,
    yelp_cpm_spend,
    yelp_cpm_budget,
    case
        when utm_medium = 'leadgen' then mpl
        when utm_medium != 'leadgen' and mpl2 > 0 then mpl2 else 0
    end as mpl
from
    lead_channels as lc
join weekly_paid as wm on wm.date = date_trunc(lc.created_at, week(monday)) and lc.product = wm.product and lc.market_id = wm.market_id
where
    lc.product != 'landscaping_design'
union all
select --attribution to all yelp leads
    lc.*,
    yelp_cpl_spend,
    yelp_cpl_budget,
    yelp_cpm_spend,
    yelp_cpm_budget,
    case
        when utm_medium = 'leadgen' then mpl
        when utm_medium != 'leadgen' and mpl2 > 0 then mpl2 else 0
    end as mpl
from
    lead_channels as lc
join month_paid as mm on mm.date = date_trunc(lc.created_at, month) and lc.product = mm.product and lc.market_id = mm.market_id
where
    lc.product = 'landscaping_design'
