with timeseries as (
  select
    date_array as date,
    date_trunc(date_array, {period}) as period, 
    rank() over (partition by date_trunc(date_array, {period}) order by date_array desc) as period_rank
  from unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
), date_changes as (
  select 
    staff_id,
    min(case when type = 'bootcamp' then started_at end) as bootcamp_date,
    max(case when type = 'hire' then started_at end) as hire_date,
    max(case when type = 'turnover' or current_change_type = 'left' then started_at end) as term_date,
  from useful_sql.hrm h
  group by 1
), calc_staff_log as (
  select
    h.stafflog_id as id,
    h.full_name,
    h.staff_id,
    h.change_type,
    h.internal_title as title,
    h.department,
    h.ladder as ladder_name,
    sh.hours_per_week,
    h.started_at as start_date,
    h.end_date, 
    h.type,
    h.current_change_type,
    d.bootcamp_date,
    d.hire_date,
    coalesce(d.term_date,current_date()) as term_date
  from useful_sql.hrm h
    left join date_changes d using(staff_id)
    left join ext_quote.staff_house sh on sh.staff_id = h.staff_id
), sum_counts as (
  select
    t.date,
    countif(lower(title) not like '%bootcamp%') as HRS104, --staff
    sum(case when lower(s.title) like '%estimation%'and lower(title) not like '%bootcamp%' then 1 else 0 end) as HRS106,--quoter
    sum(case when lower(title) in ('estimation team specialist bootcamp') then 1 else 0 end) as HRS122, --quote bootcamp
    sum(case when lower(title) in ('junior estimation team specialist') then 1 else 0 end) as HRS123, -- junior estimator
    sum(case when lower(title) in ('estimation team specialist','senior estimation team specialist') then 1 else 0 end) as HRS124, -- Specialist
    sum(case when lower(title) in ('expert estimator') then 1 else 0 end) as HRS125, --expert,
    sum(case when lower(title) in ('construction subject matter expert: estimation') then 1 else 0 end) as HRS126,  --subject matter expert,
    sum(case when lower(title) in('estimation team lead','estimation associate team lead','estimation house head') then 1 else 0 end) as HRS127,--leaders
    sum(case when lower(s.ladder_name) = 'design' then 1 else 0 end) as HRS107, --designer
    sum(case when lower(s.title) = 'onsite estimator' and lower(title) not like '%bootcamp%' then 1 else 0 end) as HRS109, --photographer
    sum(case when lower(s.title) like '%project manager%' and lower(s.title) not like '%senior%' and not lower(s.title) like '%team%' and ladder_name <> 'Engineering' then 1 else 0 end) as HRS110,
    sum(case
        when lower(title) like '%customer specialist%' and lower(title) not like '%bootcamp%' then 1
        when lower(title) like 'senior launch customer specialist' and lower(title) not like '%bootcamp%' then 1
        else 0 end) as HRS105, --cs_count
    sum(case when lower(s.title) in ('key accounts manager', 'account manager') then 1 else 0 end) as HRS118, --KAM
    sum(case when lower(s.ladder_name) = 'field account management' then 1 else 0 end) as HRS119, --FAM
    sum(case when lower(title) like '%customer service%' and lower(title) not like '%bootcamp%' then 1 else 0 end) as HRS108,--csr
    sum(case
        when lower(title) like 'junior quality assurance analyst' then 1 
        when lower(title) like 'quality assurance analyst' then 1
        when lower(title) like 'senior quality assurance analyst' then 1
        else 0 end) as HRS114,--QA
    sum(case when ladder_name = 'Engineering' and lower(title) not like '%bootcamp%' and s.title not in ('VP of Engineering', 'Chief Technology Officer', 'Senior Engineering Manager') then 1 else 0 end) as HRS116, --Engineering count
    sum(case 
      when lower(title) like '%bootcamp%' 
      and bootcamp_date <= date 
      and coalesce(hire_date,term_date,current_date()) >= date then 1 else 0 end ) as HRS103, --bootcamps
    sum(case 
      when lower(title) like '%bootcamp%'
      and lower(title) like '%customer specialist%' 
      and bootcamp_date <= date 
      and coalesce(hire_date,term_date,current_date()) >= date then 1 else 0 end ) as HRS112, --cs bootcamp
    sum(case 
      when lower(title) like '%bootcamp%'
      and (ladder_name = 'Quality Assurance Engineering' or ladder_name = 'Engineering')
      and bootcamp_date <= date 
      and coalesce(hire_date,term_date,current_date()) >= date then 1 else 0 end ) as HRS115, --Engineering bootcamp
    sum(case when lower(s.title) = 'sales development representative' and lower(s.title) not like '%bootcamp%' then 1 else 0 end) as HRS136,
  from timeseries t
  left join calc_staff_log s on s.start_date <= t.date and s.end_date >= t.date
  where change_type <> 'left'
  group by 1
), est_products as (
  select
    date as completed_at,
    full_name as estimator,
    sum(case when product_quoted = '/Fence Installation/Install a Vinyl or PVC Fence' then 1 else 0 end) as vinyl,
    sum(case when product_quoted = '/Fence Installation/Install a Wood Fence' then 1 else 0 end) as wood,
    sum(case when product_quoted = '/Fence Installation/Install a Chain Link Fence' then 1 else 0 end) as chainlink
  from int_data.estimation_dashboard_v3
  group by 1,2
),
quoter_category as ( -- number of estimators by product category quoted(vinyl,chainlink,wood)
  select
    completed_at,
    countif(vinyl > 0) as HRS128,
    countif(wood > 0) as HRS129,
    countif(chainlink > 0) as HRS130
  from est_products
  group by completed_at
), quote_capacity as (
  select
    t.date,
    capacity_est as HRS113,
    capacity_quotes as HRS121
  from timeseries t
    left join int_data.quote_capacity iqc on iqc.date = t.date
  where t.date>= '2022-08-15'
)
select
  date_trunc(t.date, {period}) as date,
  sc.* except (date),
  qc.* except (completed_at),
  eqc.* except (date)
from timeseries t
  left join sum_counts sc on t.date = sc.date
  left join quoter_category qc on qc.completed_at = t.date
  left join quote_capacity eqc on eqc.date = t.date
where period_rank = 1
