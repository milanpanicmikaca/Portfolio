-- upload to BQ
with timeseries as (
select 
  date_trunc(date_array,{period}) as date,
from 
  unnest(generate_date_array('2018-05-01',current_date(), interval 1 day)) as date_array
group by 1
),
f_res as 
(
select 
  date_trunc(coalesce(completed_at,cancelled_at),{period}) as date,
  coalesce(round(sum(np),2),0) as DEL323F,
  coalesce(round(sum(case when market_id = 4 then np end),2),0) as DEL285F,
  coalesce(round(sum(case when market_id = 29 then np end),2),0) as DEL286F,
  coalesce(round(sum(case when market_id = 30 then np end),2),0) as DEL287F,
  coalesce(round(sum(case when market_id = 8 then np end),2),0) as DEL288F,
  coalesce(round(sum(case when market_id = 3 then np end),2),0) as DEL289F,
  coalesce(round(sum(case when market_id = 31 then np end),2),0) as DEL290F,
  coalesce(round(sum(case when market_id = 9 then np end),2),0) as DEL291F,
  coalesce(round(sum(case when market_id = 10 then np end),2),0) as DEL292F,
  coalesce(round(sum(case when market_id = 2 then np end),2),0) as DEL333F,
  coalesce(round(sum(case when market_id = 7 then np end),2),0) as DEL293F,
  coalesce(round(sum(case when market_id = 14 then np end),2),0) as DEL294F,
  coalesce(round(sum(case when market_id = 1 then np end),2),0) as DEL295F,
  coalesce(round(sum(case when market_id = 5 then np end),2),0) as DEL296F,
  coalesce(round(sum(case when market_id = 6 then np end),2),0) as DEL297F,
  coalesce(round(sum(case when market_id = 19 then np end),2),0) as DEL298F,
  coalesce(round(sum(case when market_id = 18 then np end),2),0) as DEL299F,
  coalesce(round(sum(case when market_id = 17 then np end),2),0) as DEL300F,
  coalesce(round(sum(case when market_id = 16 then np end),2),0) as DEL301F,
  coalesce(round(sum(case when market_id = 32 then np end),2),0) as DEL302F,
  coalesce(round(sum(case when market_id = 20 then np end),2),0) as DEL303F,
  coalesce(round(sum(case when market_id = 22 then np end),2),0) as DEL218F,
  coalesce(round(sum(case when market_id = 43 then np end),2),0) as DEL414F,
  coalesce(round(sum(case when market_id = 35 then np end),2),0) as DEL370F,
  coalesce(round(sum(case when market_id = 24 then np end),2),0) as DEL385F,
  coalesce(round(sum(case when market_id = 26 then np end),2),0) as DEL400F,
  coalesce(round(sum(case when market_id = 21 then np end),2),0) as DEL341F,
  coalesce(round(sum(case when market_id = 33 then np end),2),0) as DEL353F,
  coalesce(round(sum(case when market_id = 42 then np end),2),0) as DEL576F, -- wn_il_ch
  coalesce(round(sum(case when market_id = 57 then np end),2),0) as DEL587F, -- wn_il_na
  coalesce(round(sum(case when market_id = 58 then np end),2),0) as DEL598F, -- wn_il_la
  coalesce(round(sum(case when market_id in (2,10,9,3,29,4,31,30,8,13) then np end),2),0) as DEL325F,
  coalesce(round(sum(case when market_id in (6,5,14,7,1,12,11) then np end),2),0) as DEL326F,
  coalesce(round(sum(case when region = 'West South Central' then np end),2),0) as DEL327F,
  coalesce(round(sum(case when market like '%-GA-%' then np end),2),0) as DEL328F,
  coalesce(round(sum(case when market_id in (21,22,35,33) then np end),2),0) as DEL407F,
  coalesce(round(sum(case when market like '%-MD-%' then np end),2),0) as DEL347F,
  coalesce(round(sum(case when market_id in (42,57,58) then np end),2),0) as DEL567F, -- Illinois
  coalesce(round(sum(np)/nullif(sum(revenue),0),2),0) as DEL324F,
  coalesce(round(sum(case when market_id = 4 then np end)/nullif(sum(case when market_id = 4 then revenue end),0),2),0) as DEL304F,
  coalesce(round(sum(case when market_id = 29 then np end)/nullif(sum(case when market_id = 29 then revenue end),0),2),0) as DEL305F,
  coalesce(round(sum(case when market_id = 30 then np end)/nullif(sum(case when market_id = 30 then revenue end),0),2),0) as DEL306F,
  coalesce(round(sum(case when market_id = 8 then np end)/nullif(sum(case when market_id = 8 then revenue end),0),2),0) as DEL307F,
  coalesce(round(sum(case when market_id = 3 then np end)/nullif(sum(case when market_id = 3 then revenue end),0),2),0) as DEL308F,
  coalesce(round(sum(case when market_id = 31 then np end)/nullif(sum(case when market_id = 31 then revenue end),0),2),0) as DEL309F,
  coalesce(round(sum(case when market_id = 9 then np end)/nullif(sum(case when market_id = 9 then revenue end),0),2),0) as DEL310F,
  coalesce(round(sum(case when market_id = 10 then np end)/nullif(sum(case when market_id = 10 then revenue end),0),2),0) as DEL311F,
  coalesce(round(sum(case when market_id = 2 then np end)/nullif(sum(case when market_id = 2 then revenue end),0),2),0) as DEL334F,
  coalesce(round(sum(case when market_id = 7 then np end)/nullif(sum(case when market_id = 7 then revenue end),0),2),0) as DEL312F,
  coalesce(round(sum(case when market_id = 14 then np end)/nullif(sum(case when market_id = 14 then revenue end),0),2),0) as DEL313F,
  coalesce(round(sum(case when market_id = 1 then np end)/nullif(sum(case when market_id = 1 then revenue end),0),2),0) as DEL314F,
  coalesce(round(sum(case when market_id = 5 then np end)/nullif(sum(case when market_id = 5 then revenue end),0),2),0) as DEL315F,
  coalesce(round(sum(case when market_id = 6 then np end)/nullif(sum(case when market_id = 6 then revenue end),0),2),0) as DEL316F,
  coalesce(round(sum(case when market_id = 19 then np end)/nullif(sum(case when market_id = 19 then revenue end),0),2),0) as DEL317F,
  coalesce(round(sum(case when market_id = 18 then np end)/nullif(sum(case when market_id = 18 then revenue end),0),2),0) as DEL318F,
  coalesce(round(sum(case when market_id = 17 then np end)/nullif(sum(case when market_id = 17 then revenue end),0),2),0) as DEL319F,
  coalesce(round(sum(case when market_id = 16 then np end)/nullif(sum(case when market_id = 16 then revenue end),0),2),0) as DEL320F,
  coalesce(round(sum(case when market_id = 32 then np end)/nullif(sum(case when market_id = 32 then revenue end),0),2),0) as DEL321F,
  coalesce(round(sum(case when market_id = 20 then np end)/nullif(sum(case when market_id = 20 then revenue end),0),2),0) as DEL322F,
  coalesce(round(sum(case when market_id = 22 then np end)/nullif(sum(case when market_id = 22 then revenue end),0),2),0) as DEL219F,
  coalesce(round(sum(case when market_id = 43 then np end)/nullif(sum(case when market_id = 43 then revenue end),0),2),0) as DEL415F,
  coalesce(round(sum(case when market_id = 21 then np end)/nullif(sum(case when market_id = 21 then revenue end),0),2),0) as DEL342F,
  coalesce(round(sum(case when market_id = 33 then np end)/nullif(sum(case when market_id = 33 then revenue end),0),2),0) as DEL354F,
  coalesce(round(sum(case when market_id = 35 then np end)/nullif(sum(case when market_id = 35 then revenue end),0),2),0) as DEL371F,
  coalesce(round(sum(case when market_id = 24 then np end)/nullif(sum(case when market_id = 24 then revenue end),0),2),0) as DEL386F,
  coalesce(round(sum(case when market_id = 26 then np end)/nullif(sum(case when market_id = 26 then revenue end),0),2),0) as DEL401F,
  coalesce(round(sum(case when market_id = 42 then np end)/nullif(sum(case when market_id = 42 then revenue end),0),2),0) as DEL577F, -- wn_il_ch
  coalesce(round(sum(case when market_id = 57 then np end)/nullif(sum(case when market_id = 57 then revenue end),0),2),0) as DEL588F, -- wn_il_na
  coalesce(round(sum(case when market_id = 58 then np end)/nullif(sum(case when market_id = 58 then revenue end),0),2),0) as DEL599F, -- wn_il_la
  coalesce(round(sum(case when market_id in (2,10,9,3,29,4,31,30,8,13) then np end)/nullif(sum(case when market_id in (2,10,9,3,29,4,31,30,8,13) then revenue end),0),2),0) as DEL329F,
  coalesce(round(sum(case when market_id in (6,5,14,7,1,12,11) then np end)/nullif(sum(case when market_id in (6,5,14,7,1,12,11) then revenue end),0),2),0) as DEL330F,
  coalesce(round(sum(case when region = 'West South Central' then np end)/nullif(sum(case when region = 'West South Central' then revenue end),0),2),0) as DEL331F,
  coalesce(round(sum(case when market like '%-GA-%' then np end)/nullif(sum(case when market like '%-GA-%' then revenue end),0),2),0) as DEL332F,
  coalesce(round(sum(case when market like '%-MD-%' then np end)/nullif(sum(case when market like '%-MD-%' then revenue end),0),2),0) as DEL346F,
  coalesce(round(sum(case when market_id in (21,22,35,33) then np end)/nullif(sum(case when market_id in (21,22,35,33) then revenue end),0),2),0) as DEL406F
from 
  int_data.order_ue_materialized 
where 
  product like '%/Fence Installation%'
  and type = '/Residential'
  and (completed_at is not null or cancelled_at is not null)
group by 1 
),
not_fence as 
(
select 
  date_trunc(coalesce(completed_at,cancelled_at),{period}) as date,
  coalesce(round(sum(np),2),0) as DEL323,
  coalesce(round(sum(case when type = '/Commercial' then np end),2),0) as DEL323C,
  coalesce(round(sum(np)/nullif(sum(revenue),0),2),0) as DEL324,
  coalesce(round(sum(case when type = '/Commercial' then np end)/nullif(sum(case when type = '/Commercial' then revenue end),0),2),0) as DEL324C
from 
  int_data.order_ue_materialized 
where 
  (completed_at is not null or cancelled_at is not null)
group by 1 
)
select * from timeseries left join f_res using(date) left join not_fence using(date)
