select
        {period} as date,
        MAR195,
        MAR196
from
(
select 
        *,
        rank() over (partition by {period} order by date desc) as rank
from 
(
with calc_series as
(
        select
                generate_series('2018-04-15', current_date, '1 day')::date as day
        ),        
google_data as
(
        select
                (posted_at at time zone 'pst')::date as date_id,
                avg(fr.score) as score,
                count(fr.score) as count_score
        from feedback_review fr
        left join marketing_localaccount mnl on mnl.id = fr.account_id
        where mnl.channel_id = 2
        group by 1
        )
select
        cs.day as date,
        date_trunc('{period}',cs.day)::date as {period},
        coalesce(avg(gd.score) over (order by cs.day),0) as MAR196,
        coalesce(sum(gd.count_score) over (order by cs.day),0) as MAR195
from calc_series cs
left join google_data gd on cs.day = gd.date_id
order by 1 desc
) as ordered_data
order by 1
) as full_data
where rank = '1'
order by 1 desc
limit 12
