-- upload to BQ
with
-- generate timeseries
calc_timeseries as
(
 select 
    date_trunc(date_array,{period}) as date,
    date_trunc(date_array, day) as period, 
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
 from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
),lead_backlog as
(
select * from pipedrive.lead_backlog_log b
),
 general_queues as
(  
  select
   * 
  from  
    (select
      date_trunc(extract(date from datetime(cast(b.backlog_datetime as timestamp), "America/Los_Angeles")),{period}) as day_f,
      sum(case when product_id = 105 then leads_backlog else 0 end) as SAL252F,--leads_backlog_fence 
      sum(case when product_id = 105 then longest_lead_wait_time else 0 end) as SAL253F,-- longest_lead_wait_time_fence
      sum(case when product_id = 34 then leads_backlog else 0 end) as SAL252D,--leads_backlog_driveway
      sum(case when product_id = 34 then longest_lead_wait_time else 0 end) as SAL253D,-- longest_lead_wait_time_driveway
      rank() over (partition by date_trunc(extract(date from datetime(cast(b.backlog_datetime as timestamp), "America/Los_Angeles")),{period}) order by backlog_datetime desc) as my_rank_f
   from lead_backlog b
   where product_id in (105,34)  and queue_name = 'general'
   group by b.backlog_datetime
   )
   where my_rank_f = 1
   order by 1 desc
   ),other_queues as (
 select * from
 (select
      date_trunc(extract(date from datetime(cast(b.backlog_datetime as timestamp), "America/Los_Angeles")),{period}) as day_f,  
      sum(case when queue_name = 'Fence - Wood' then leads_backlog else 0 end) as SAL286,
      sum(case when queue_name = 'Fence - Wood' then longest_lead_wait_time else 0 end) as SAL287,
      sum(case when queue_name = 'Fence - Chain Link' then leads_backlog else 0 end) as SAL284,
      sum(case when queue_name = 'Fence - Chain Link' then longest_lead_wait_time else 0 end) as SAL285,
      sum(case when queue_name = 'Hardscape' then leads_backlog else 0 end) as SAL288,
      sum(case when queue_name = 'Hardscape' then longest_lead_wait_time else 0 end) as SAL289,
      sum(case when queue_name = 'Commercial' then leads_backlog else 0 end) as SAL282,
      sum(case when queue_name = 'Commercial' then longest_lead_wait_time else 0 end) as SAL283, 
      sum(case when queue_name = 'Returning Customer' then leads_backlog else 0 end) as SAL456,
      sum(case when  queue_name = 'Returning Customer' then longest_lead_wait_time else 0 end) as SAL457, 
      sum(case when  queue_name = 'Fence - Fresno' then leads_backlog else 0 end) as SAL450,
      sum(case when  queue_name = 'Fence - Fresno' then longest_lead_wait_time else 0 end) as SAL451,
      sum(case when  queue_name = 'Fence - SoCal Ph1' then leads_backlog else 0 end) as SAL452,
      sum(case when  queue_name = 'Fence - SoCal Ph1' then longest_lead_wait_time else 0 end) as SAL453,
      sum(case when  queue_name = 'Fence - SoCal Ph2' then leads_backlog else 0 end) as SAL454,
      sum(case when  queue_name = 'Fence - SoCal Ph2' then longest_lead_wait_time else 0 end) as SAL455,
      sum(case when  queue_name = 'Fence - Dallas' then leads_backlog else 0 end) as SAL552,
      sum (case when  queue_name = 'Fence - Dallas' then longest_lead_wait_time else 0 end) as SAL553,
          sum(case when  queue_name = 'Fence - San Diego (SD)' then leads_backlog else 0 end) as SAL574,
      sum (case when  queue_name = 'Fence - San Diego (SD)' then longest_lead_wait_time else 0 end) as SAL575,
      sum(case when  queue_name = 'Fence - Vinyl' then leads_backlog else 0 end) as SAL554,
      sum(case when  queue_name = 'Fence - Vinyl' then longest_lead_wait_time else 0 end) as SAL555,
      rank() over (partition by date_trunc(extract(date from datetime(cast(b.backlog_datetime as timestamp), "America/Los_Angeles")),{period}) order by backlog_datetime desc) as my_rank_f
   from lead_backlog b
   where product_id = 0
    group by b.backlog_datetime)
   where my_rank_f = 1
)
select 
    date_trunc(cct.date,{period}) as date,
   * except (date,period ,period_rank,day_f,my_rank_f)
from calc_timeseries as cct
left join general_queues lf on cct.date = day_f
left join other_queues oq on oq.day_f = cct.date
where period_rank = 1 and date >= '2020-01-01'
order by 1 desc;
