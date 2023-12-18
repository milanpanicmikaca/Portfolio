-- upload to BQ
with
    generate_time_series
as
(
    select 
    date_array as date,
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
    from unnest(generate_date_array('2018-04-16',current_date("America/Los_Angeles"), interval 1 day)) as date_array
),
    requote_requested
as
(
select
    distinct key as key
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
where c.to_string = 'Requote'
order by i.key
),
calc_data
as 
(
select
    i.key,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as requote_requested_at,
    to_string,
    lead(to_string) over (partition by i.key order by datetime(cast(c.created as timestamp), "America/Los_Angeles")) as to_string_lead,
    lead(datetime(cast(c.created as timestamp), "America/Los_Angeles")) over (partition by i.key order by datetime(cast(c.created as timestamp), "America/Los_Angeles")) as completed_at
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join requote_requested rr on rr.key = i.key
where c.to_string in ('Requote', 'Completed') and rr.key is not null
order by i.key
),
canceled
as
(
select
    i.key,
    datetime(cast(c.created as timestamp), "America/Los_Angeles") as canceled_at,
    lead(datetime(cast(c.created as timestamp), "America/Los_Angeles")) over (partition by i.key order by datetime(cast(c.created as timestamp), "America/Los_Angeles")) as move_from_canceled_at
from jira.changelog c
left join jira.issue i on i.id = c.issue_id
left join requote_requested rr on rr.key = i.key
where rr.key is not null
and (c.to_string = 'Canceled' or c.from_string = 'Canceled')
order by i.key, c.created
),
total_canceled_time
as
(
select
    key as canceled_key,
    canceled_at,
    move_from_canceled_at,
    timestamp_diff(move_from_canceled_at,canceled_at,hour) as canceled_time
from canceled
)
select
    date_trunc(gts.date,{period}) as date,
    avg(timestamp_diff(completed_at,requote_requested_at,hour)-coalesce(canceled_time,0)) as CTN022,
    max(timestamp_diff(completed_at,requote_requested_at,hour)-coalesce(canceled_time,0)) as CTN023
from generate_time_series gts
left join calc_data cd on  gts.date = extract(date from completed_at)-- at time zone 'America/Los_Angeles') 
left join total_canceled_time tc on tc.canceled_key = cd.key and tc.canceled_at > cd.requote_requested_at and tc.move_from_canceled_at < cd.completed_at 
where to_string = 'Requote' and to_string_lead = 'Completed' and extract(date from completed_at) is not null-- at time zone 'America/Los_Angeles') is not null
and period_rank = 1
group by 1
order by 1 desc
