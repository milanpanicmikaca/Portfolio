with timeseries as 
(
select 
	date_trunc('day', dd)::date as date,
	rank() over (partition by date_trunc('{period}', dd)::date order by dd) as period_rank
from 
	generate_series ('2018-04-16'::timestamp, current_date, '1 day'::interval) dd
),
reviews_log
as
(
(
select
        fr.id,
        fr.posted_at,
        fr.score
from feedback_review fr
left join marketing_localaccount mnl on mnl.id = fr.account_id
where mnl.channel_id = 1
)
union all
(
select
        frl.review_id as id,
        frl.posted_at,
        frl.score
from feedback_review fr 
left join feedback_reviewlog frl on frl.review_id = fr.id
left join marketing_localaccount mnl on mnl.id = fr.account_id
where mnl.channel_id = 1
)
order by 1, 2
),
array_aggregation
as
(
select
        id,
        array_agg(score order by posted_at) as scores,
        min(score) as min_score,
        max(score) as max_score
from reviews_log
where id is not null
group by 1
),
array_positions_calc 
as
(
select
        a.*,
        array_positions(scores, min_score) as position_min_score,
        array_positions(scores, max_score) as position_max_score,
        case when array_positions(scores, min_score) = array_positions(scores, max_score) then 'no_change_in_score' end as test
from array_aggregation a
order by 4 desc
),
positive_revisions
as
(
select
        id,
        scores,
        position_max_score[array_upper(position_max_score,1)] as last_position_max_score,
        case when position_min_score[array_upper(position_min_score, 1)] < position_max_score[array_upper(position_max_score,1)] and min_score < 4 and max_score > 3 then 1 else 0 end as positive_revised
from array_positions_calc
order by test desc
),
ranked_review_revisions
as
(
select
        *,
        rank() over (partition by id order by posted_at) as rank_revision
from reviews_log
),
positive_revised_reviews
as
(
select 
        rrr.id,
        scores,
        posted_at,
        positive_revised
from ranked_review_revisions rrr
left join positive_revisions pr on pr.id = rrr.id and pr.last_position_max_score = rrr.rank_revision
where positive_revised = 1
),
revised_positive as
(
select
        date_trunc('{period}',case when fr.deleted_at is not null then fr.deleted_at 
                                                   when positive_revised = 1 then prr.posted_at 
                                                   else fr.posted_at end)::date as date,
        sum(case when positive_revised = 1 then 1 else 0 end) as MAR613,
        coalesce(sum(case when positive_revised = 1 then 1 else 0 end)::decimal/nullif(sum(case when score < 4 then 1 else 0 end)::decimal,0),0) as MAR614,
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' then 1 else 0 end) as MAR615,
        coalesce(sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' then 1 else 0 end)/nullif(sum(case when score < 4 then 1 else 0 end)::decimal,0),0) as MAR616
from feedback_review fr
left join positive_revised_reviews prr on prr.id = fr.id
left join marketing_localaccount mnl on mnl.id = fr.account_id
where mnl.channel_id = 1
group by 1
order by 1 desc
),
total_reviews as
(
	select		
		date_trunc('{period}', fr.posted_at)::date as date,
		sum(case when score < 4 then 1 else 0 end) as MAR617
	from feedback_review fr
	left join marketing_localaccount mnl on mnl.id = fr.account_id
	where mnl.channel_id = 1 and fr.deleted_at is null
	group by 1
	order by 1 desc
)
select 
	date_trunc('{period}',t.date)::date as date, MAR613, MAR614, MAR615, MAR616, MAR617
from timeseries t
left join revised_positive rp using (date)
left join total_reviews tr using(date)
where period_rank = 1

