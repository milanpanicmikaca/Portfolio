with house as (
    select
        staff_id,
        name as house
    from ergeon.hrm_stafflog hsg
    left join ergeon.hrm_team ht on ht.id = hsg.team_id
    where
        name in ('Grand Central', 'Playhouse', 'Tjibaou House')
    qualify Rank() over (partition by staff_id order by hsg.created_at desc) = 1
),

qd as (
    select
        q.id,
        sent_to_customer_at,
        preparation_completed_at,
        --us.team as house, --this needs to be debugged in useful_sql.hrm so we can remove one CTE
        h.house,
        q.total_price,
        q.order_id,
        us.full_name,
        Concat('https://admin.ergeon.in/quoting-tool/', q.order_id, '/quote/', q.id) as admin_link,
        case
            when is_scope_change is true then (
                case
                    when Lower(title) like '%scop%' and method = 'measured' then 'Scoping Task'
                    else 'Change Order'
                end)
            else 'Standard Quote'
        end as quote_type,
        case
            --rank will include only estimation team. There are cases when sales did the first quote but they are not included in here
            when Rank() over (partition by q.order_id order by q.id) = 1 then 'new_quote' else 'requote' end as quote_completition,
        case when Lower(description) like '%direct quote%' then 1 else 0 end as direct_quote,
        ue.onsite_type, market, county, city, state, project_manager, ue.sales_rep, ue.type, product_quoted, won_at, is_win, segment
    from ergeon.quote_quote q
    -- there can be only one caveat per quote with description 'direct quote'
    left join ergeon.quote_quoteline l on q.id = l.quote_id and Lower(description) like '%direct quote%'
    left join ergeon.hrm_staff s on s.user_id = q.sent_to_customer_by_id
    --before 4th of July 2022 we were using diferent methodology
    left join
        useful_sql.hrm us on
            us.staff_id = If(
                q.sent_to_customer_at < '2022-07-04', s.id, q.preparation_completed_by_id
            ) and Date(Coalesce(q.sent_to_customer_at, q.preparation_completed_at)) between us.started_at and us.end_date
    left join house h on h.staff_id = us.staff_id
    left join int_data.order_ue_materialized ue on ue.order_id = q.order_id
    where
        --q.sent_to_customer_at >= '2018-04-16' --This code is same as q.sent_to_customer_at is not null (results are the same)
        --and 
        ue.is_warranty_order is false --excluding WWO
        and
        us.department in ('Construction')
        --this filter may exclude some estimators in case their current contract is changed 
        --to the other department and they are still quoting. Alternative is to filter by house.
        and is_cancellation = false
    qualify Rank() over(partition by q.id order by us.started_at desc) = 1
)

select
    --timestamp for scoping task and change order is preparation_completed_at because some of the mentioned is not being sent to the customer
    Date_Trunc(
        Extract(
            date from Datetime(
                Cast(
                    case when quote_type in ('Scoping Task', 'Change Order') then preparation_completed_at else sent_to_customer_at end as timestamp
                ),
                "America/Los_Angeles"
            )
        ),
        day
    ) as date,
    * except (quote_completition, sent_to_customer_at, preparation_completed_at),
    case when quote_type = 'Scoping Task' then 1 else 0 end as is_scoping_task,
    case when quote_type = 'Change Order' then 1 else 0 end as is_change_order,
    case when quote_type = 'Scoping Task' and total_price <> 0 then 1 else 0 end as is_change_order_after_QA,
    case when quote_type = 'Standard Quote' and quote_completition = 'new_quote' then 1 else 0 end as new_quote,
    case when quote_type = 'Standard Quote' and quote_completition = 'requote' then 1 else 0 end as requote,
    case when quote_type = 'Standard Quote' then quote_completition end as quote_class
from qd
--this field should never be null, or it would mean that quote was never finished
where
    Date_Trunc(
        Extract(
            date from Datetime(
                Cast(
                    case when quote_type in ('Scoping Task', 'Change Order') then preparation_completed_at else sent_to_customer_at end as timestamp
                ),
                "America/Los_Angeles"
            )
        ),
        day
    ) >= '2018-04-16'
