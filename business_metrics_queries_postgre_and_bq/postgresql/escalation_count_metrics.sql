-- upload to BQ
with
leads as
(
        select
                so.id as order_id,
                min(cl.id) as lead_id
        from store_order so
        left join core_lead cl on cl.order_id = so.id
        group by 1
),
last_approved_quotes as 
(
  select 
    o.id as order_id,
    completed_at as cancelled_at,
    is_cancellation,
    rank() over(partition by o.id order by approved_at desc,q.id desc) as approved_rank
  from 
    store_order o join 
    quote_quote q on q.order_id = o.id 
  where 
    q.created_at >= '2018-04-16'
    and approved_at is not null
),
cancelled_projects as 
(
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
projects_billed
as
(
select
        date_trunc('{period}', o.completed_at at time zone 'America/Los_Angeles')::date as date,
        count(o.id) as count_projects_completed
from store_order o
left join quote_quote q on q.id = o.approved_quote_id
left join leads l on l.order_id = o.id
left join core_lead cl on cl.id = l.lead_id
left join customers_contact co on co.id = cl.contact_id
left join core_user cu on cu.id = co.user_id
left join cancelled_projects cp on cp.order_id = o.id
where o.completed_at is not null
and cp.order_id is null
and q.approved_at >= '2018-04-15'
and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
and o.parent_order_id is null
and coalesce(cl.full_name,'')||coalesce(co.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
and coalesce(cl.email,'')||coalesce(cu.email,'') not ilike '%+test%'
group by 1
),
scoping_escalations as 
        (
        select			
        se.id as escalation_id
        from store_escalation se
        left join store_escalation_core_issues ei on se.id = ei.escalation_id
        left join store_escalationcoreissue ec on ec.id = ei.escalationcoreissue_id
        where reported_at >= '2021-04-28' and reported_at <= current_date
        and current_status_bucket = 'scoping_process' --scoping tasks
        and ec.name like '%Scoping%' --scoping tasks
        ),
calc_data
as
(
        (
        select			
            se.id as escalation_id,
            reported_at,
            ep.name as team,
            1.0 as value
        from store_escalation se
        left join store_escalation_primary_teams_attributed sep on sep.escalation_id = se.id
        left join store_escalationteamattributed ep on ep.id = sep.escalationteamattributed_id
        where reported_at >= '2021-04-28' and reported_at <= current_date
        and sep.id is not null
        and se.id not in (select * from scoping_escalations)
        )
        union all
        (
        select			
            se.id as escalation_id,
            reported_at,
            es.name as team,
            1.0 as value
        from store_escalation se
        left join store_escalation_secondary_teams_attributed ses on ses.escalation_id = se.id
        left join store_escalationteamattributed es on es.id = ses.escalationteamattributed_id
        where reported_at >= '2021-04-28' and reported_at <= current_date
        and ses.id is not null
        and se.id not in (select * from scoping_escalations)
        )
)
,
count_per_escalation as
(
select
        escalation_id,
        count(*) as count_per_escalation
from calc_data
group by 1
),
counts as 
(
select
        date_trunc('{period}', reported_at at time zone 'America/Los_Angeles')::date as date,
        count(distinct cd.escalation_id),
        sum(value/count_per_escalation) as DEL150,
        sum(case when team = 'Sales Team' then value/count_per_escalation else 0 end) as DEL145,
        sum(case when team = 'Delivery Team' then value/count_per_escalation else 0 end) as DEL146,
        sum(case when team = 'Estimation Team' then value/count_per_escalation else 0 end) as DEL147,
        sum(case when team = 'Customer Driven' then value/count_per_escalation else 0 end) as DEL148,
        sum(case when team = 'Category Team' then value/count_per_escalation else 0 end) as DEL149,
        sum(case when team = 'Engineering Team' then value/count_per_escalation else 0 end) as DEL271,
        sum(case when team = 'Supply Ops Team' then value/count_per_escalation else 0 end) as DEL272,
        sum(case when team = 'Catalog Team' then value/count_per_escalation else 0 end) as DEL273,
        sum(case when team = 'CSR Team' then value/count_per_escalation else 0 end) as DEL274,
        sum(case when team = 'Accounts Receivable Team' then value/count_per_escalation else 0 end) as DEL275
from calc_data cd
left join count_per_escalation cpe on cpe.escalation_id = cd.escalation_id
group by 1
)
select
        c.date,
         del150,
         del150/nullif(count_projects_completed,0) as del124, --Escalations, %
         del145,
         del145/nullif(count_projects_completed,0) as del138, --Escalations due to Sales, %
         del146,
         del146/nullif(count_projects_completed,0) as del136, --Escalations due to Delivery, %
         del147,
         del147/nullif(count_projects_completed,0) as del137, --Escalations due to Estimations, %
         del148,
         del148/nullif(count_projects_completed,0) as del140, --Escalations due to Customer, %
         del149,
         del149/nullif(count_projects_completed,0) as del139, --Escalations due to Category, %
         del271,
         del271/nullif(count_projects_completed,0) as del276, --Escalations due to Engineering, %
         del272,
         del272/nullif(count_projects_completed,0) as del277, --Escalations due to Materials, %
         del273,
         del273/nullif(count_projects_completed,0) as del278, --Escalations due to Catalog, %
         del274,
         del274/nullif(count_projects_completed,0) as del279, --Escalations due to CSR, %
         del275,
         del275/nullif(count_projects_completed,0) as del280 --Escalations due to Accounts Receivable, %
from counts c
left join projects_billed p on p.date = c.date
