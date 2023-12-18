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
        mnl.channel_id,
        fr.score
from feedback_review fr
left join marketing_localaccount mnl on mnl.id = fr.account_id
/*where mnl.channel_id = 1*/
)
union all
(
select
        frl.review_id as id,
        frl.posted_at,
        mnl.channel_id,
        frl.score
from feedback_review fr 
left join feedback_reviewlog frl on frl.review_id = fr.id
left join marketing_localaccount mnl on mnl.id = fr.account_id
/*where mnl.channel_id = 1*/
)
order by 1,2
),
array_aggregation
as
(
select
        id,
        channel_id,
        array_agg(score order by posted_at) as scores,
        min(score) as min_score,
        max(score) as max_score
from reviews_log
where id is not null
group by 1,2
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
        channel_id,
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
        rrr.channel_id,
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
        sum(case when positive_revised = 1 then 1 else 0 end) as MAR1702,
        sum(case when positive_revised = 1 and mnl.channel_id = 1 then 1 else 0 end) as MAR613,
		sum(case when positive_revised = 1 and mnl.channel_id = 2 then 1 else 0 end) as MAR2649,
        sum(case when positive_revised = 1 and mnl.channel_id in (3,10) then 1 else 0 end) as MAR2650,
        sum(case when positive_revised = 1 and mnl.channel_id = 4 then 1 else 0 end) as MAR2651,
        sum(case when positive_revised = 1 and mnl.channel_id = 5 then 1 else 0 end) as MAR2652,
        coalesce(sum(case when positive_revised = 1 then 1 else 0 end)::decimal/nullif(sum(case when score < 4 then 1 else 0 end)::decimal,0),0) as MAR1703,
        coalesce(sum(case when positive_revised = 1 and mnl.channel_id = 1 then 1 else 0 end)::decimal/nullif(sum(case when score < 4 and mnl.channel_id = 1 then 1 else 0 end)::decimal,0),0) as MAR614,
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' then 1 else 0 end) as MAR1704,        
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id = 1 then 1 else 0 end) as MAR615,
		sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id = 2 then 1 else 0 end) as MAR2653,
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id in (3,10) then 1 else 0 end) as MAR2654,
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id = 4 then 1 else 0 end) as MAR2655,
        sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id = 5 then 1 else 0 end) as MAR2656,
		sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and fr.score < 4 then 1 else 0 end) as MAR2222,
        coalesce(sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' then 1 else 0 end)/nullif(sum(case when score < 4 then 1 else 0 end)::decimal,0),0) as MAR1705,
        coalesce(sum(case when fr.deleted_at is not null and fr.deleted_at > '2020-10-01' and mnl.channel_id = 1 then 1 else 0 end)/nullif(sum(case when score < 4 and mnl.channel_id = 1 then 1 else 0 end)::decimal,0),0) as MAR616
from feedback_review fr
left join positive_revised_reviews prr on prr.id = fr.id
left join marketing_localaccount mnl on mnl.id = fr.account_id
/*where mnl.channel_id = 1*/
group by 1
order by 1 desc
),
total_reviews as
(
	select		
		date_trunc('{period}', fr.posted_at)::date as date,
		sum(case when score < 4 then 1 else 0 end) as MAR1701,
		sum(case when score < 4 and pcnm.market_id = 2 then 1 else 0 end) as MAR1712,
		sum(case when score < 4 and pcnm.market_id = 3 then 1 else 0 end) as MAR1719,
		sum(case when score < 4 and pm.code like '%CN-SB%' then 1 else 0 end) as MAR1726,
		sum(case when score < 4 and pcnm.market_id = 30 then 1 else 0 end) as MAR1733,
		sum(case when score < 4 and pcnm.market_id = 31 then 1 else 0 end) as MAR1740,
		sum(case when score < 4 and pcnm.market_id = 9 then 1 else 0 end) as MAR1747,
		sum(case when score < 4 and pcnm.market_id = 8 then 1 else 0 end) as MAR1754,
		sum(case when score < 4 and pcnm.market_id = 10 then 1 else 0 end) as MAR1761,
		sum(case when score < 4 and pcnm.market_id = 29 then 1 else 0 end) as MAR1768,
		sum(case when score < 4 and pcnm.market_id = 4 then 1 else 0 end) as MAR1982,
		sum(case when score < 4 and pcnm.market_id = 14 then 1 else 0 end) as MAR1775,
		sum(case when score < 4 and pcnm.market_id = 5 then 1 else 0 end) as MAR1782,
		sum(case when score < 4 and pcnm.market_id = 6 then 1 else 0 end) as MAR1789,
		sum(case when score < 4 and pcnm.market_id = 7 then 1 else 0 end) as MAR1796,
		sum(case when score < 4 and pcnm.market_id = 1 then 1 else 0 end) as MAR1803,
		sum(case when score < 4 and pcnm.market_id = 16 then 1 else 0 end) as MAR1810,
		sum(case when score < 4 and pcnm.market_id = 19 then 1 else 0 end) as MAR1817,
		sum(case when score < 4 and pcnm.market_id = 17 then 1 else 0 end) as MAR1824,
		sum(case when score < 4 and pcnm.market_id = 18 then 1 else 0 end) as MAR1989,
		sum(case when score < 4 and pcnm.market_id = 32 then 1 else 0 end) as MAR1996,
		sum(case when score < 4 and pcnm.market_id = 20 then 1 else 0 end) as MAR1831,
		sum(case when score < 4 and pcnm.market_id = 22 then 1 else 0 end) as MAR1838,
		sum(case when score < 4 and pcnm.market_id = 21 then 1 else 0 end) as MAR1845,
		sum(case when score < 4 and pcnm.market_id = 33 then 1 else 0 end) as MAR1852,
		sum(case when score < 4 and pcnm.market_id = 35 then 1 else 0 end) as MAR1859,
		sum(case when score < 4 and pcnm.market_id = 24 then 1 else 0 end) as MAR1866,
		sum(case when score < 4 and pcnm.market_id = 26 then 1 else 0 end) as MAR2003,
		sum(case when score < 4 and mnl.channel_id = 1 then 1 else 0 end) as MAR617,
		sum(case when score < 4 and mnl.channel_id = 2 then 1 else 0 end) as MAR2465,
		sum(case when score < 4 and mnl.channel_id in (3,10) then 1 else 0 end) as MAR2466,
		sum(case when score < 4 and mnl.channel_id = 4 then 1 else 0 end) as MAR2467,
		sum(case when score < 4 and mnl.channel_id = 5 then 1 else 0 end) as MAR2468,
		sum(case when score < 4 and pcnm.market_id = 42 then 1 else 0 end) as MAR2897,
		sum(case when score < 4 and pcnm.market_id = 57 then 1 else 0 end) as MAR2956,
		sum(case when score < 4 and pcnm.market_id = 58 then 1 else 0 end) as MAR3015
	from feedback_review fr
	left join marketing_localaccount mnl on mnl.id = fr.account_id
	left join store_order o on o.id = fr.order_id
	left join core_house h on h.id = o.house_id
	left join customers_customer cc on cc.id = h.customer_id
	left join geo_address ga on ga.id = h.address_id
	left join geo_county cn on cn.id = ga.county_id
	left join product_countymarket pcnm on pcnm.county_id = cn.id
	left join product_market pm on pm.id = pcnm.market_id
	where /*mnl.channel_id = 1 and*/ fr.deleted_at is null
	group by 1
	order by 1 desc
)
select 
	date_trunc('{period}',t.date)::date as date, MAR613, MAR614, MAR615, MAR616, MAR617, MAR1701, MAR1702, MAR1703, MAR1704, 
		MAR1705, MAR1712, MAR1719, MAR1726, MAR1733, MAR1740, MAR1747, MAR1754, MAR1761, MAR1768, MAR1775, MAR1782, MAR1789,
		MAR1796, MAR1803, MAR1810, MAR1817, MAR1824, MAR1831, MAR1838, MAR1845, MAR1852, MAR1859, MAR1866, MAR1982, MAR1989,
		MAR1996, MAR2003, MAR2222, MAR2465, MAR2466, MAR2467, MAR2468, MAR2649, MAR2650, MAR2651, MAR2652, MAR2653, MAR2654,
		MAR2655, MAR2656, MAR2897, MAR2956, MAR3015
from timeseries t
left join revised_positive rp using (date)
left join total_reviews tr using(date)
where period_rank = 1
