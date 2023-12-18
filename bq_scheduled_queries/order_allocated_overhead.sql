with position_levels as ( 
--mapping of fees for each position level  
  (select  2 as level, 4 as fee) UNION ALL 
  (select  3 as level, 5 as fee) UNION ALL 
  (select  4 as level, 6 as fee) UNION ALL 
  (select  5 as level, 7 as fee) UNION ALL 
  (select  6 as level, 8 as fee) UNION ALL 
  (select  7 as level, 9 as fee) UNION ALL 
  (select  8 as level, 10 as fee) UNION ALL 
  (select  9 as level, 11 as fee) UNION ALL 
  (select 10 as level, 12.5 as fee) UNION ALL 
  (select 11 as level, 13.5 as fee) UNION ALL 
  (select 12 as level, 14.5 as fee) 
),

staff_hours_per_day as ( 
--hours per day of week
  select 
    starting_at as day,
    case when extract(dayofweek from starting_at) not in (1,7) then 7.5 else 0 end as hours
  from warehouse.br_period 
  where type = 'day' 
    and starting_at < '2022-02-28'
),

wage_per_position_day as ( 
--find wages per day & position
  select 
    day, 
    internal_title as position,
    ladder,
    hours*fee as pay_per_day
  from staff_hours_per_day 
  cross join useful_sql.hrm hrm 
  join position_levels pl on pl.level = hrm.level
  where day between hrm.started_at and hrm.end_date 
  --include unique stafflogs per day
  qualify rank() over(partition by staff_id,day order by end_date desc,started_at desc) = 1 
),

weekly_wages as ( 
  --allocate weekly fees for cs/quoter/csr/pm according to position and ladder 
  select 
    date_trunc(day, week(monday)) as week, 
    round(sum(case when ladder = 'Sales' then pay_per_day*0.9 end)) as cs, --'Customer Specialist fee'
    round(sum(case when position in ('Estimation Team Specialist','Senior Estimation Team Specialist','Junior Estimator') then pay_per_day*0.96 end)) as quoter,--'Estimation fee'
    round(sum(case when ladder = 'Customer Service Representative' then pay_per_day*0.85 end)) as csr, --'Customer Service Representative fee'
    round(sum(case when ladder = 'Project Management' then pay_per_day*0.96 end)) as pm--'Project Management fee'
  from wage_per_position_day    
  group by 1
),

orders as (
  --count of orders per week
  select 
    date_trunc(created_at, week(Monday)) as date,
    count(if(type = '/Residential',order_id,null)) as order_res_count,
    count(if(type = '/Commercial',order_id,null)) as order_kam_count,
    count(*) as order_count 
  from int_data.order_ue_materialized 
  where geo <> '/Unknown'
  group by 1
),

wins as (
  --count of wins per week
  select 
    date_trunc(won_at, week(Monday)) as date,
    sum(is_win) as win_count 
  from int_data.order_ue_materialized 
  where geo <> '/Unknown'
  group by 1
),

quote_data as (
  --if q.sent_to_customer_at < '2022-07-04' --> sent_to_customer_by_id else preparation_completed_by_id (that's when we added it in admin) 
  select
    q.id,
    extract(date from sent_to_customer_at at time zone 'America/Los_Angeles') as date,
    ue.order_id,
    --rank quotes by sent date
    rank() OVER(partition by order_id order by sent_to_customer_at,q.id) rank
  from ergeon.quote_quote q 
  join int_data.order_ue_materialized ue using(order_id) 
  left join ergeon.core_user cu on cu.id = q.sent_to_customer_by_id 
  left join ergeon.hrm_staff hs on hs.user_id = cu.id 
  left join useful_sql.hrm hrm on hrm.staff_id = if(q.sent_to_customer_at < '2022-07-04',hs.id,q.preparation_completed_by_id) 
                                                  and date(q.sent_to_customer_at) between started_at and end_date
  where geo <> '/Unknown'
    and sent_to_customer_at >= '2018-04-16' 
    and is_cancellation = False
     --use sent_to_customer_at < '2018-07-16' if not, then dates before shows no quotes
    and (department = 'Construction' or sent_to_customer_at < '2018-07-16')
  qualify rank() over(partition by q.id order by hrm.started_at desc,end_date desc) = 1
),

count_quote as (
  --count of quotes/requotes per week
  select 
      date_trunc(date, week(Monday)) as date,
      sum(case when rank = 1 then 1 else 0 end) as quote_count,
      sum(case when rank > 1 then 1 else 0 end) as requote_count
  from quote_data
  group by 1
),

weekly_sdr_fee as (
  select 
    date_trunc(date_sub(date,interval 1 week),week(Monday)) as date,
    abs(0.84*sum(0.6*amount) + sum(0.4*amount)) as sdr_kam_fee,
    abs(sum(amount)) as sdr_fee
  from 
    int_data.sdr_data 
  where date_sub(date,interval 1 week) = '2022-06-27'
  --we started allocating sdr_fees on '2022-06-27'
  group by 1

  union all
  
  select 
    date_trunc(date_sub(date,interval 1 week),week(Monday)) as date,
    abs(sum(0.4*amount)) as sdr_kam_fee, 
    abs(sum(amount)) as sdr_fee
  from 
    int_data.sdr_data 
  where date_sub(date,interval 1 week) >= '2022-07-01'
  --60% goes to leads and 40% to csr fees
  group by 1
),

weekly_kam_fee as (
  --key account manager fees (commercial)
  select 
    date_trunc(date_sub(date,interval 1 week),week(Monday)) as date,
    sum(amount) as cs_kam_fee
  from int_data.title_fees 
  where category = 'cs_fee' 
    and title like '%Key Accounts Manager%'
  group by 1
), 

weekly_staff_fees as (
  select
    uc.date,
    DATE_SUB(DATE_TRUNC(cast(uc.date as date), WEEK(MONDAY)), INTERVAL 1 WEEK) as week,
    sum(if(category = 'CSr Fee',amount,0)) + coalesce(min(sdr_fee),0) as csr_fee,
    coalesce(min(sdr_kam_fee),0) as csr_kam_fee,
    sum(if(category = 'CS Fee',amount,0)) + coalesce(min(cs_kam_fee),0) as cs_fee,
    coalesce(min(cs_kam_fee),0) as cs_kam_fee,
    sum(if(category = 'Quoter Fee',amount,0)) as quoter_fee,
    sum(if(category = 'PM Fee',amount,0)) as pm_fee
  from int_data.upwork_contracts uc 
  left join weekly_sdr_fee sdr on sdr.date = DATE_SUB(DATE_TRUNC(cast(uc.date as date), WEEK(MONDAY)), INTERVAL 1 WEEK) 
  left join weekly_kam_fee kam on kam.date = DATE_SUB(DATE_TRUNC(cast(uc.date as date), WEEK(MONDAY)), INTERVAL 1 WEEK)
  group by 1  
),

benefit_data as (
  select 
    date_trunc(date,week(Monday)) as week,
    sum(if(category = 'csr_fee' and title not like '%Sales Development Representative%',abs(amount),0)) as ben_csr_res_fee,
    sum(if(category = 'csr_fee' and title like '%Sales Development Representative%',abs(amount),0)) as ben_csr_com_fee, 
    sum(if(category = 'cs_fee' and title not like '%Key Accounts Manager%',abs(amount),0)) as ben_cs_res_fee,
    sum(if(category = 'cs_fee' and title like '%Key Accounts Manager%',abs(amount),0)) as ben_cs_com_fee,
    sum(if(category = 'quoter_fee',abs(amount),0)) as ben_quoter_fee,
    sum(if(category = 'pm_fee',abs(amount),0)) as ben_pm_fee
  from int_data.title_mtr_pto_er_fees
  group by 1
),

order_weekly_fee as (
  select 
    o.date,
    csr_fee/order_res_count as order_csr_fee,
    --kam/sdr_fees goes to commercial and rest to residential
    csr_kam_fee/order_kam_count as order_csr_kam_fee,
    cs_fee/order_res_count as order_cs_fee,
    cs_kam_fee/order_kam_count as order_cs_kam_fee,
    pm_fee/win_count as order_pm_fee,
    (quoter_fee)/(coalesce(quote_count,0)+coalesce(0.5*requote_count,0)) as quote_fee,
    (quoter_fee)/(coalesce(2*quote_count,0)+coalesce(requote_count,0)) as requote_fee,
    --csr benefits
    coalesce(ben_csr_res_fee,0)/order_res_count as order_ben_csr_res_fee,
    coalesce(ben_csr_com_fee,0)/order_kam_count as order_ben_csr_com_fee,
    --cs benefits
    coalesce(ben_cs_res_fee,0)/order_res_count as order_ben_cs_res_fee,
    coalesce(ben_cs_com_fee,0)/order_kam_count as order_ben_cs_com_fee,
    --pm benefits
    coalesce(ben_pm_fee,0)/win_count as order_ben_pm_fee,
    --quoter benefits
    coalesce(ben_quoter_fee,0)/(coalesce(quote_count,0)+coalesce(0.5*requote_count,0)) as ben_quote_fee,
    coalesce(ben_quoter_fee,0)/(coalesce(2*quote_count,0)+coalesce(requote_count,0)) as ben_requote_fee
  from orders o 
  left join wins w on w.date = o.date 
  left join count_quote q on q.date = o.date 
  left join weekly_staff_fees sf on o.date = sf.week
  left join benefit_data ben on o.date = ben.week
  where o.date <= DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK)
    and o.date >= '2022-02-28'
),

order_quoted_fee as (
  --allocate fee to quotes/requotes
  select 
    order_id,
    sum(case 
        when rank = 1 then abs(quote_fee) 
        when rank > 1 then abs(requote_fee) 
    end) as quoter_fee,
    sum(case 
        when rank = 1 then abs(ben_quote_fee) 
        when rank > 1 then abs(ben_requote_fee)
    end) as ben_quoter_fee
  from quote_data q 
  left join order_weekly_fee wf on wf.date = date_trunc(q.date,week(Monday))
  where 
    date_trunc(q.date,week(monday)) between '2022-02-28' and DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK)
  group by 1
),

order_arrival_fee as (
  --allocate fee to orders on arrival (cs/csr)
  select 
    order_id,
    if(type = '/Residential',trunc(abs(order_csr_fee),2),trunc(abs(order_csr_kam_fee),2)) as csr_fee,
    if(type = '/Residential',trunc(abs(order_cs_fee),2),trunc(abs(order_cs_kam_fee),2)) as cs_fee,
    if(type = '/Residential',trunc(abs(order_ben_csr_res_fee),2),trunc(abs(order_ben_csr_com_fee),2)) as ben_csr_fee,
    if(type = '/Residential',trunc(abs(order_ben_cs_res_fee),2),trunc(abs(order_ben_cs_com_fee),2)) as ben_cs_fee
  from int_data.order_ue_materialized ue 
  left join order_weekly_fee wf on wf.date = date_trunc(ue.created_at,week(Monday))
  where geo <> '/Unknown'
    and date_trunc(ue.created_at,week(monday)) between '2022-02-28' and DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK)
),

order_win_fee as (
  --allocate fee to orders on approval - pm 
  select 
    order_id,
    abs(order_pm_fee) as pm_fee,
    abs(order_ben_pm_fee) as ben_pm_fee
  from 
    int_data.order_ue_materialized ue left join
    order_weekly_fee wf on wf.date = date_trunc(ue.won_at,week(Monday))
  where geo <> '/Unknown'
    and date_trunc(ue.won_at,week(monday)) between '2022-02-28' and DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK)
),

fees_by_order as (
  select 
    ue.order_id,
    af.cs_fee,
    af.csr_fee,
    qf.quoter_fee,
    wf.pm_fee,
    af.ben_cs_fee,
    af.ben_csr_fee,
    qf.ben_quoter_fee,
    wf.ben_pm_fee
  from int_data.order_ue_materialized ue 
  left join order_arrival_fee af on af.order_id = ue.order_id 
  left join order_quoted_fee qf on qf.order_id = ue.order_id 
  left join order_win_fee wf on wf.order_id = ue.order_id
),

cost_per_order as ( 
  --allocation of fees per order per week according to staff wage/lead count (This is an estimation of fees before < 2022-02-28)
  select 
    wa.week, 
    case when wa.week < '2020-01-01' then 15 else cs/order_count end as cs_fee,
    case when wa.week < '2020-01-01' then 2.6 else csr/order_count end as csr_fee,
    case when wa.week < '2020-01-01' then 11 else quoter/(quote_count + requote_count/4) end as quoter_fee,
    case when wa.week < '2020-01-01' then 54 else pm/win_count end as pm_fee
  from weekly_wages wa
  join orders o on wa.week = o.date
  left join count_quote q on wa.week = q.date
  left join wins w on wa.week = w.date
  where wa.week < '2022-02-28'
),

first_quote_sent as (
  --find first quote sent and date
  select 
    order_id, 
    id as quote_id,
    sent_to_customer_at as first_quoted_at 
  from ergeon.quote_quote 
  where sent_to_customer_at is not null 
  and created_at > '2018-04-15' 
  and is_cancellation = False 
  qualify rank() over(partition by order_id order by sent_to_customer_at,id ) = 1
), 

first_quote_approved as (
  --find first quote approved
  select 
    order_id,
    min(approved_at) as won_at
  from ergeon.quote_quote 
  where approved_at is not null 
    and sent_to_customer_at > '2018-04-15'
    and is_cancellation = False 
 group by 1
),

quote_cost_per_order as
(
  select 
      order_id,
      sum(case 
          when rank = 1 then trunc(abs(quote_fee),2) --if first quote then quote fee
          when rank > 1 then trunc(abs(requote_fee),2) --if requote then requote fee
      end) as quoter_fee
  from quote_data q 
  left join order_weekly_fee wf on wf.date = date_trunc(q.date,week(Monday))
  where date_trunc(q.date,week(monday)) between '2022-02-28' and DATE_SUB(DATE_TRUNC(current_date(), WEEK(MONDAY)), INTERVAL 1 WEEK)
  group by 1
),

order_fees as ( 
  --allocate cs/csr/pm fees per order before 2022-02-28
  select 
    o.id as order_id,
    coalesce(cpoa.cs_fee,0) as cs_fee,
    coalesce(cpoa.csr_fee,0) as csr_fee,
    coalesce(if(fqa.won_at is not null,cpow.pm_fee,0),0) as pm_fee,
  from ergeon.store_order o 
  left join first_quote_sent fqs on fqs.order_id = o.id --join with first quote sent
  left join first_quote_approved fqa on fqa.order_id = o.id --join with orders approved
  left join cost_per_order as cpoa on cpoa.week = cast(date_trunc(o.created_at, week(monday)) as Date)
  left join cost_per_order as cpow on cpow.week = cast (date_trunc(fqa.won_at, week(monday)) as Date)
),

quote_fees as (
  --allocate quoter fees per order before 2022-02-28
  select 
      o.id as order_id,
      cpoq.quoter_fee as cpo1,
      coalesce(sum(
          case when q.sent_to_customer_at = fqs.first_quoted_at then 1 else 0 end)*cpoq.quoter_fee 
                + sum(case when q.sent_to_customer_at > fqs.first_quoted_at then 1 else 0 end)*cpoq.quoter_fee/4,0) 
      as quoter_fee
  from ergeon.store_order o 
  left join first_quote_sent fqs on fqs.order_id = o.id --join with first quote
  left join cost_per_order as cpoq on cpoq.week = cast( date_trunc(fqs.first_quoted_at, week(monday)) as Date)
  left join ergeon.quote_quote q on q.order_id = o.id
  group by 1,2
)

select 
  order_id, 
  coalesce(if(geo <> '/Unknown',orf.csr_fee,0),0) + coalesce(fbo.csr_fee,0) as csr_fee, 
  coalesce(if(geo <> '/Unknown',orf.cs_fee,0),0) + coalesce(fbo.cs_fee,0) as cs_fee, 
  coalesce(if(geo <> '/Unknown',qf.quoter_fee,0),0) + coalesce(fbo.quoter_fee,0) as quoter_fee, 
  coalesce(if(geo <> '/Unknown',orf.pm_fee,0),0) + coalesce(fbo.pm_fee,0) as pm_fee,
  coalesce(fbo.ben_csr_fee,0) as ben_csr_fee, 
  coalesce(fbo.ben_cs_fee,0) as ben_cs_fee, 
  coalesce(fbo.ben_quoter_fee,0) as ben_quoter_fee, 
  coalesce(fbo.ben_pm_fee,0) as ben_pm_fee
from order_fees orf 
left join quote_fees qf using(order_id) 
left join fees_by_order fbo using(order_id) 
left join int_data.order_ue_materialized using(order_id)
