with
first_lead_arrived_at
as
(
select
    order_id,
    cast(min(created_at) as timestamp) as arrived_at,
    min(lead_id) as lead_id
from int_data.order_ue_materialized
where is_lead is true
group by 1
),
first_quote_approved
as
(
select
    order_id as order_id,
    min(cast(won_at as timestamp)) as first_approved_at
from int_data.order_ue_materialized
where won_at is not null and is_lead is true
group by 1
),
calc_data as
(
select
    ue.order_id,
    fla.arrived_at,
    fqa.first_approved_at,
    coalesce(cast(ue.cancelled_at as timestamp), cast(d.lost_time as timestamp)) as cancelled_at,
    regexp_extract(ue.geo,'/.*/(.*)/') as geo,
    segment,
    case 
        when ue.channel like '%/Paid/Home Advisor%' then 'ha'
        when ue.channel like '%/Paid/Yelp%' then 'yelp'
        when ue.channel not like '%/Paid/Home Advisor%' and ue.channel like '%/Paid%' then 'paid_non_ha'
        when ue.channel like '%/Non Paid%' then 'non_paid_non_yelp' 
        end as lead_channel
from int_data.order_ue_materialized ue
left join first_lead_arrived_at fla on fla.order_id = ue.order_id
left join first_quote_approved fqa on fqa.order_id = ue.order_id
left join pipedrive.deal d on d.admin_lead_number = ue.lead_id
where ue.order_id is not null
and ue.order_id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
    9789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
and ue.order_id not in (select
                    order_id
                from `bigquerydatabase-270315.ergeon.store_orderstatushistory` soh
                where soh.status = 'CAN'
                group by 1
                having count(*) > 1)
and fla.arrived_at >= '2019-01-01' and fla.arrived_at  < '2021-10-31'
order by 1,2
)
select
    --extract(month from arrived_at) as month_arrived,
    segment,
    lead_channel,
    avg(timestamp_diff(first_approved_at, arrived_at, hour)/24) as mean_days_till_win,
from calc_data
where first_approved_at is not null
and first_approved_at < coalesce(cancelled_at,current_timestamp())
group by 1,2
union all
select
    --extract(month from arrived_at) as month_arrived,
    'All' as segment,
    lead_channel as channel,
    avg(timestamp_diff(first_approved_at, arrived_at, hour)/24) as mean_days_till_win,
from calc_data
where first_approved_at is not null
and first_approved_at < coalesce(cancelled_at,current_timestamp())
group by 1,2
order by 1