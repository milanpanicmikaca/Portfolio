with in_progress_time as (
    select
        iss.id,
        extract(date from max(ch.created_at) at time zone "America/Los_Angeles") as in_progress
    from jira.SD_roadmap_issues iss
        left join jira.SD_roadmap_changelog ch on ch.issue_id = iss.id
    where ch.field = 'status'
        and ch.to_string = 'In Progress'
    group by 1
),rank_changelog as (
    select 
        created_at,
        issue_id,
        key,
        to_string,
        field,
        to_id,
        rank() over(partition by issue_id order by created_at) as rank_asc,
        from jira.SD_roadmap_changelog
        --where key = "SD-2563"
), first_date as (
select 
    issue_id,
    case 
        when lower(field) = 'start date' then cast(to_id as date)
        when lower(field) = 'assignee' then date(cl.created_at,"America/Los_Angeles") 
        when rank_asc = 1 then date(cl.created_at, "America/Los_Angeles") else null end as start_date,
from rank_changelog cl
where rank_asc = 1
), final_status_rank as (
select 
    cl.*,
    date_trunc(cast(cl.created_at as date), quarter) as created_q,
    date_trunc(start_date, quarter) as start_q,
    rank() over (partition by cl.issue_id order by cl.created_at desc) as rank_status
    from rank_changelog cl
    left join first_date fd on fd.issue_id = cl.issue_id
    where date_trunc(cast(cl.created_at as date), quarter) = date_trunc(start_date, quarter) 
    and lower(cl.field) = 'status' 
), final_status as (
select 
    distinct *
from final_status_rank fsr
where  rank_status = 1 
),
calc_data
as
(
select
    iss.id,
    iss.key,
    iss.summary,
    cast(iss.start_date as date) as start_date,
    case when iss.status = 'Done' then extract(date from cast(iss.resolution_date as timestamp) at time zone "America/Los_Angeles") else cast(iss.due_date as date) end as end_date,
    cast(ed.start_date as date) as epic_start,
    iss.assignee,
    u.email,
    iss.parent_id,
    iss.parent_key,
    case when iss.issuetype = 'Sub-task' then iss3.id
        else iss4.id end as epic_id,
    case when iss.issuetype = 'Sub-task' then iss2.epic_key
        else iss.epic_key end as epic_key,
    iss.issuetype,
    case when iss.issuetype = 'Sub-task' then 2
        when iss.issuetype = 'Task' then 1
        when iss.issuetype = 'Epic' then 0 
        end as jira_level,
    iss.status as final_status,
    --fs.to_string as final_status,
from jira.SD_roadmap_issues iss
left join jira.SD_roadmap_issues iss2 on iss.parent_id = iss2.id
left join jira.SD_roadmap_issues iss3 on iss2.epic_key = iss3.key
left join jira.SD_roadmap_issues iss4 on iss.epic_key = iss4.key
left join jira.SD_roadmap_issues ed on ed.key = iss.epic_key
left join jira.user u on u.name = iss.assignee
order by 1 desc
), final as (
select
    cd.id,
    cd.key,
    cd.summary,
    case when cd.start_date is null then cast(iss.start_date as date) else cast(cd.start_date as date) end as start_date,
    cd.end_date,
    cd.assignee,
    cd.email,
    cd.parent_id,
    cd.parent_key,
    cd.epic_id,
    cd.epic_key,
    cd.issuetype,
    cd.jira_level,
    --cd.status,
    cd.final_status,
    concat(coalesce(concat(cd.epic_id, "/"),""), coalesce(concat(cd.parent_id, "/"),""), cd.id) as path,
    case when cd.issuetype = 'Sub-task' then cd.epic_id
         when cd.issuetype = 'Task' then cd.epic_id
         when cd.issuetype = 'Epic' then cd.id end as epic,
    case when cd.issuetype = 'Sub-task' then cd.parent_id
         when cd.issuetype = 'Task' then cd.id else 1111 end as task,
    case when (case when cd.issuetype = 'Epic' then cd.assignee else iss.assignee end) = 'Joana Tching' then 'Estimation'
         when (case when cd.issuetype = 'Epic' then cd.assignee else iss.assignee end) in ('Dilpreet Sethi', 'Kevin Mundarain') then 'Category'
         when (case when cd.issuetype = 'Epic' then cd.assignee else iss.assignee end) in ('Jose Hurtado','Sofia Galindo', 'Dante Bellino') then 'Sales'
         when (case when cd.issuetype = 'Epic' then cd.assignee else iss.assignee end) in ('Carmen Mendez', 'Eliana Olaechea Dongo') then 'Delivery'
         when (case when cd.issuetype = 'Epic' then cd.assignee else iss.assignee end) = 'Dru Jinks' then 'Supply Ops'
         end as team_assigned
from calc_data cd
left join jira.SD_roadmap_issues iss on iss.id = cd.epic_id
where cd.issuetype in ('Sub-task', 'Epic', 'Task') 
and (cd.issuetype = 'Sub-task' and (cd.parent_id is null or cd.epic_id is null)) is not true
and (cd.issuetype = 'Task' and cd.epic_id is  null) is not true
and (case when cd.issuetype = 'Sub-task' then cast(iss.start_date as date)
        when cd.issuetype = 'Task' then cast(iss.start_date as date)
        when cd.issuetype = 'Epic' then cd.start_date end) >= '2021-10-01' 
)
select 
    f.* 
from final f 
left join int_data.hr_dashboard h on h.email = f.email
where coalesce(start_date,end_date) >= '2021-10-01'
and (h.ladder_name = 'Field Account Management' or h.full_name = 'Sheila Duran')
order by start_date desc
