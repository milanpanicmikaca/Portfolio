with month_days_calc as (
  select 
  date_trunc(current_date,month) as start_date,
  date_sub(date_add(date_trunc(current_date,month),interval 1 month), interval 1 day) as end_date,
  extract(dayofweek from date_sub(date_add(date_trunc(current_date,month),interval 1 month), interval 1 day)) as weekday_end,
  extract(dayofweek from date_trunc(current_date,month)) as weekday_start,
), month_days as (
  select 
  start_date,
  end_date,
  date_diff(end_date,start_date,day)+1 as month_days,
  date_diff(end_date,date_add(start_date,interval (weekday_end-weekday_start) day),day)/7*5+(weekday_end-weekday_start)+1+If(weekday_end=7,-1,0)+If(weekday_start=1,-1,0) as network_days,
  date_diff(end_date,date_add(current_date,interval (weekday_end-extract(dayofweek from current_date)) day),day)/7*5+(weekday_end-extract(dayofweek from current_date))+1+If(weekday_end=7,-1,0)+If(extract(dayofweek from current_date)=1,-1,0) as network_days_today,
  (date_diff(end_date,start_date,day)+1)-(date_diff(end_date,date_add(start_date,interval (weekday_end-weekday_start) day),day)/7*5+(weekday_end-weekday_start)+1+If(weekday_end=7,-1,0)+If(weekday_start=1,-1,0)) as month_weekends
  from month_days_calc
), sales_staff as (
  select 
    full_name as full_name,
    case 
      when title like '%House Head%' then 0.5
      when title like '%Team Lead%' then 0.2 
      else 1 end *
    case 
      when hd.tenure > 70 then 1
      when hd.tenure > 63 and hd.tenure <= 70 then 0.95
      when hd.tenure > 56 and hd.tenure <= 63 then 0.9
      when hd.tenure > 49 and hd.tenure <= 56 then 0.85
      when hd.tenure > 42 and hd.tenure <= 49 then 0.8
      when hd.tenure > 35 and hd.tenure <= 42 then 0.7
      when hd.tenure > 28 and hd.tenure <= 35 then 0.6
      when hd.tenure > 21 and hd.tenure <= 28 then 0.5
      when hd.tenure > 14 and hd.tenure <= 21 then 0.4
      when hd.tenure > 7 and hd.tenure <= 14 then 0.3
      when hd.tenure > 1 and hd.tenure <= 7 then 0.1
      else 0 end as FTE, 
  from int_data.hr_dashboard hd
  where ladder_name in ('Sales')
  and (title like "%Customer Specialist%" or title like "%Manager%" or title like "%Account%")
  and hire_date is not null and term_date is null
  --qualify row_number() over (partition by staff_id order by staff_id) = 1
), leads_details as (
  select 
    full_name,
    house,
    segment,
    sum(case when deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as leads_sum,
    sum(case when date_trunc(coalesce(assigned_at,created_at), month) = date_trunc(current_date, month) then 1 else 0 end) as leads_mtd,
    sum(case when date(assigned_at) between date_sub(current_date, interval 90 day) and current_date then 1 else 0 end) as leads_90,
    sum(case when deal_status = 'New lead' then 1 else 0 end) as new_lead, 
    sum(case when deal_status = 'Onsite Scheduled' then 1 else 0 end) as onsite_scheduled, 
    sum(case when deal_status = 'Quote Needed' then 1 else 0 end) as quote_needed, 
    sum(case when deal_status = 'Quote Sent' then 1 else 0 end) as quote_sent, 
    sum(case when deal_status = 'Quote Reviewed' then 1 else 0 end) as quote_reviewed, 
    sum(case when deal_status = 'Requote Needed' then 1 else 0 end) as requote_needed,
    sum(case when deal_status in 
      ('New lead',
      'Onsite Scheduled',
      'Quote Needed',
      'Quote Sent',
      'Quote Reviewed',
      'Requote Needed') then 1 else 0 end) as active_pipeline,
    sum(case when deal_status in 
      ('Quote Sent',
      'Quote Reviewed',
      'Requote Needed') then 1 else 0 end) as closing_pipeline,
    sum(case 
      when date_diff(current_date(), date(cast(assigned_at as timestamp), "America/Los_Angeles"), day) <= 14 
      and deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as t_0_14,
    sum(case 
      when date_diff(current_date(), date(cast(assigned_at as timestamp), "America/Los_Angeles"), day) between 15 and 30
      and deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as t_15_30,
    sum(case 
      when date_diff(current_date(), date(cast(assigned_at as timestamp), "America/Los_Angeles"), day) between 31 and 60 
      and deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as  t_31_60,
    sum(case 
      when date_diff(current_date(), date(cast(assigned_at as timestamp), "America/Los_Angeles"), day) between 61 and 90 
      and deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as  t_61_90,
    sum(case 
      when date_diff(current_date(), date(cast(assigned_at as timestamp), "America/Los_Angeles"), day) > 90
      and deal_status not in ('Deal Won','Lost','On Hold') then 1 else 0 end) as t_91_plus,
    max(date_diff(current_date(), case when deal_status = 'New lead' then date(cast(assigned_at as timestamp), "America/Los_Angeles") end,day) ) as max_time,
    avg(date_diff(current_date(), case when deal_status = 'New lead' then date(cast(assigned_at as timestamp), "America/Los_Angeles") end,day) ) as avg_time,
    sum(case when deal_status = 'Deal Won' and date_diff(current_date, date(created_at), day) <= 14+30 and date_diff(date(won_at), date(created_at), day) <= 14 then 1 else 0 end) as w_0_14,
    sum(case when deal_status = 'Deal Won' and date_diff(current_date, date(created_at), day) <= 30+30 and date_diff(date(won_at), date(created_at), day) between 15 and 30 then 1 else 0 end) as w_15_30,
    sum(case when deal_status = 'Deal Won' and date_diff(current_date, date(created_at), day) <= 60+30 and date_diff(date(won_at), date(created_at), day) between 31 and 60 then 1 else 0 end) as w_31_60,
    sum(case when deal_status = 'Deal Won' and date_diff(current_date, date(created_at), day) <= 90+30 and date_diff(date(won_at), date(created_at), day) between 61 and 90 then 1 else 0 end) as w_61_90,
    sum(case when deal_status = 'Deal Won' and date_diff(date(won_at), date(created_at), day) <= 120+30 then 1 else 0 end) as w_91_plus,

  from int_data.sales_dashboard_od od
  where full_name is not null
    and segment is not null 
    and segment <> 'Other'
    and house is not null
    and date_diff(current_date,date(coalesce(assigned_at,created_at)),day) <= 365
  group by 1, 2, 3
)
select 
  l.full_name,
  s.fte,
  l.house,
  l.segment,
  l.* except (full_name,house,segment),
  m.month_days,
  m.network_days,
  m.network_days_today,
  m.month_weekends,
from leads_details l, month_days m
left join sales_staff s on s.full_name = l.full_name
where fte is not null
order by 1