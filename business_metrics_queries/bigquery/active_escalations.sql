with -- updating into BQ
-- test
escalations as 
(
    select
            eh.object_id as escalation_id,
            datetime(cast(e.reported_at as timestamp), 'America/Los_Angeles') as start_date,
            datetime(cast(eh.created_at as timestamp), 'America/Los_Angeles') as end_date,
            GENERATE_DATE_ARRAY(cast(e.reported_at as date), current_date(), INTERVAL 1 day) AS date_array,
            st.code,
            u.full_name as pm,
            case 
            when st.code in ('escalation_resolved', 'escalation_concluded', 'escalation_cancelled') then 
                rank() over (partition by eh.object_id order by eh.created_at desc) else null 
            end as rank_end_states,
            case 
            when st2.code in ('escalation_received', 'escalation_fix_agreed','escalation_revisit_scheduled','escalation_QA_scheduled') then 'active' else 'resolved' 
            end as grouped_status,
            case 
            when st2.code in ('escalation_received', 'escalation_fix_agreed','escalation_revisit_scheduled','escalation_QA_scheduled') then 
            rank() over (partition by e.id order by eh.created_at desc) else null end as act_rank
    from ergeon.core_statushistory eh
    left join ergeon.store_escalation e  on e.id = eh.object_id
    left join ergeon.django_content_type d on d.id = eh.content_type_id
    left join ergeon.core_statustype st on st.id = eh.status_id
    left join ergeon.core_statustype st2 on st2.id = e.status_id
    left join ergeon.store_order o on o.id = e.order_id 
    left join ergeon.hrm_staff s on s.id = o.project_manager_id
    left join ergeon.core_user u on s.user_id = u.id
    where reported_at >= '2021-04-28' 
      and d.model = 'escalation'
      and d.app_label = 'store'
      and e.deleted_at is null
),
active_data as
(
    select 
        escalation_id,
        day,
        grouped_status,
        start_date,
        end_date,
        sum(case when grouped_status = 'resolved' and day <= extract(date from end_date) then 1
                 when grouped_status = 'active' and day <= current_date() then 1
            else 0 end) 
        over (partition by escalation_id order by day) as active_days,
        case 
            when grouped_status = 'resolved' and day <= extract(date from end_date) then 1
            when grouped_status = 'active' and day <= current_date() then 1
        else 0 end as active,
        case 
            when grouped_status = 'resolved' and day >= extract(date from end_date) then timestamp_diff(end_date,start_date,day) 
        end as tat 
    from escalations 
    CROSS JOIN UNNEST(date_array) as day
    where (grouped_status = 'active' and act_rank = 1) or rank_end_states = 1
),
aggregate_calc as
(
    select 
        day,
        sum(active) as active_esc,
        sum(case when active_days < 30 then 1 else 0 end) as act_30,
        sum(case when active_days between 30 and 90 then 1 else 0 end) as act_30_90,
        sum(case when active_days > 90 then 1 else 0 end) as act_90,
        avg(tat) as tat 
    from active_data 
    where active = 1 
    group by 1 
),
ranked_date as 
(
    select 
        day,
        active_esc,
        act_30,
        act_30_90,
        act_90,
        tat,
        rank() over (partition by date_trunc(day,{period}) order by day desc) as rank
    from aggregate_calc
),
tat_count as 
(
    select 
        date_trunc(day,{period}) as date,
        avg(tat) as tat
    from ranked_date
    group by 1
),
active_count as 
(
    select 
        date_trunc(day,{period}) as date,
        active_esc,
        act_30,
        act_30_90,
        act_90
    from ranked_date 
    where rank = 1
)
select 
    date,
    active_esc as DEL246,
    act_30/active_esc as DEL247,
    act_30_90/active_esc as DEL248,
    act_90/active_esc as DEL249, 
    tat as DEL250
from active_count a 
left join tat_count t using(date)
order by 1 desc
