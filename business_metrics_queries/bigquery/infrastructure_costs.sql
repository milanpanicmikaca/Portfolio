-- upload to BQ
with
timeseries as 
(
select 
    date_trunc(date_array,{period}) as date,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
from unnest(generate_date_array('2021-01-01',current_date(), interval 1 day)) as date_array
),
aws_cost as (
select 
    date_trunc(cast(usage_ending_at as date),{period}) as date,
    sum(line_item_blended_cost) as  aws_cost
from `bigquerydatabase-270315.cloud_costs.aws_billing`
group by 1
),google_cloud_cost as (
select
    date_trunc(cast(usage_end_time as date),{period}) as date,
    sum(cost) as google_cost
from `bigquerydatabase-270315.cloud_costs.gcp_billing_export_v1_012322_769D70_ED55FA`
group by 1
)
select
    date_trunc(t.date,{period}) as date,
    coalesce(awc.aws_cost,0) as ENG109,--aws_cost
    coalesce(gcc.google_cost,0) as ENG108, -- google_cloud_cost
from timeseries t
left join aws_cost awc on awc.date = t.date 
left join google_cloud_cost gcc on gcc.date = t.date
where period_rank = 1
--and t.date >= '2021-10-01' -- data for google cloud wasn't pulled for days earlier than 1st October 2021
and awc.aws_cost is not null and gcc.google_cost is not null
order by 1 desc
