WITH sales_title_per_order_at_arrival AS ( --title of sales members across all periods at arrival
    SELECT
        o.id AS order_id,
        internal_title AS sales_title_at_arrival
    FROM ergeon.store_order o
    --join staff member the day order was created
    LEFT JOIN useful_sql.hrm hrm ON hrm.staff_id = o.sales_rep_id AND date(o.created_at) BETWEEN hrm.started_at AND hrm.end_date
    --in case of multiple stafflogs per order, choose stafflog with the last started date
    QUALIFY rank() OVER(PARTITION BY o.id ORDER BY hrm.started_at DESC, end_date DESC) = 1
),

sales_title_per_order_at_win AS ( --title of sales members across all periods at win, same logic but for wins
    SELECT
        order_id,
        internal_title AS sales_title_at_win
    FROM int_data.order_ue_materialized ue
    LEFT JOIN useful_sql.hrm hrm ON hrm.staff_id = ue.sales_staff_id AND date(won_at) BETWEEN hrm.started_at AND hrm.end_date
    QUALIFY rank() OVER(PARTITION BY order_id ORDER BY hrm.started_at DESC, end_date DESC) = 1
),

photographer_per_order AS ( --photgrapher name per order
    SELECT
        order_id,
        cu.full_name AS photographer
    FROM ergeon.store_order so
    LEFT JOIN ergeon.schedule_appointment sa ON so.id = sa.order_id
    LEFT JOIN ergeon.schedule_appointmenttype ssa ON sa.appointment_type_id = ssa.id
    LEFT JOIN ergeon.core_user cu ON cu.id = sa.agent_user_id
    WHERE ssa.code = 'physical_onsite'
        AND sa.cancelled_at IS NOT NULL --only physical onsites which are not cancelled
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY date, sa.id) = 1
),

last_approved_quotes AS (
    SELECT
        o.id AS order_id,
        completed_at AS cancelled_at,
        is_cancellation
    FROM ergeon.store_order o
    JOIN ergeon.quote_quote q ON q.order_id = o.id
    WHERE q.created_at >= '2018-04-16'
        AND approved_at IS NOT NULL
    QUALIFY rank() OVER(PARTITION BY o.id ORDER BY approved_at DESC, q.id DESC) = 1
),

cancelled_projects AS (
    SELECT
        *
    FROM last_approved_quotes
    WHERE is_cancellation = TRUE
),

contractor_per_order AS ( --installer name per order
    SELECT
        so.id AS order_id,
        full_name AS contractor
    FROM ergeon.store_order so
    LEFT JOIN ergeon.contractor_contractororder co ON so.id = co.order_id
    LEFT JOIN ergeon.contractor_contractor hc ON hc.id = co.contractor_id
    LEFT JOIN ergeon.contractor_contractorcontact cc ON cc.id = hc.contact_id
    LEFT JOIN ergeon.core_user u ON u.id = cc.user_id
    LEFT JOIN cancelled_projects cp ON cp.order_id = so.id
    WHERE so.completed_at IS NOT NULL --only for completed projects
        AND cp.order_id IS NULL
        AND co.status_id IN (3, 13, 66) --contractor order in ('agreed', 'completed', 'sent')
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY co.id DESC) = 1 --join with last contractor order
),

contractors_per_order AS ( --installers per order
    SELECT
        so.id AS order_id,
        count(co.contractor_id) AS contractor_count
    FROM ergeon.store_order so
    LEFT JOIN ergeon.contractor_contractororder co ON so.id = co.order_id
    LEFT JOIN ergeon.contractor_contractor hc ON hc.id = co.contractor_id
    LEFT JOIN cancelled_projects cp ON cp.order_id = so.id
    WHERE so.completed_at IS NOT NULL --only for completed projects
        AND cp.order_id IS NULL
        AND co.status_id IN (3, 13, 66) --contractor order in ('agreed', 'completed', 'sent')
    GROUP BY 1
),

sales_member_per_order AS ( --team_leader/CS-CSR name per order
    SELECT
        so.id AS order_id,
        cus.full_name AS sales_rep,
        sales_rep_id AS sales_staff_id,
        cul.full_name AS team_lead,
        team.name AS sales_team
    FROM ergeon.store_order so
    LEFT JOIN ergeon.hrm_staff hs ON hs.id = so.sales_rep_id
    LEFT JOIN ergeon.hrm_stafflog csl ON csl.id = hs.current_stafflog_id
    LEFT JOIN ergeon.hrm_team team ON team.id = csl.team_id -- sales team of sales_rep
    LEFT JOIN ergeon.hrm_staff hsl ON hsl.id = team.lead_id
    LEFT JOIN ergeon.core_user cul ON cul.id = hsl.user_id --team leader of sales_rep
    LEFT JOIN ergeon.core_user cus ON cus.id = hs.user_id --sales_rep
),

project_manager_per_order AS ( --project manager name per order
    SELECT
        so.id AS order_id,
        pm.id AS pm_id,
        user.full_name AS project_manager,
        team.name AS pm_team
    FROM ergeon.store_order so
    LEFT JOIN ergeon.hrm_staff pm ON pm.id = so.project_manager_id
    LEFT JOIN ergeon.core_user user ON user.id = pm.user_id
    LEFT JOIN ergeon.hrm_stafflog sl ON sl.id = pm.current_stafflog_id
    LEFT JOIN ergeon.hrm_team team ON team.id = sl.team_id --team of pm
),

first_quote_sent AS (
    SELECT
        order_id,
        id AS quote_id
    FROM ergeon.quote_quote
    WHERE sent_to_customer_at IS NOT NULL
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY order_id ORDER BY sent_to_customer_at, id) = 1 --rank order by first quote sent
),

last_quote_approved AS (
    SELECT
        order_id,
        id AS quote_id
    FROM ergeon.quote_quote
    WHERE approved_at IS NOT NULL
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY order_id ORDER BY approved_at DESC, id DESC) = 1 --rank order by last quote approved
),

last_quote_sent AS (
    SELECT
        order_id,
        id AS quote_id
    FROM ergeon.quote_quote
    WHERE sent_to_customer_at IS NOT NULL
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY order_id ORDER BY sent_to_customer_at DESC, id DESC) = 1 --rank order by last quote sent
),

first_quote_approved AS (
    SELECT
        order_id,
        id AS quote_id
    FROM ergeon.quote_quote
    WHERE approved_at IS NOT NULL
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY order_id ORDER BY approved_at, id) = 1 --rank order by first quote approved
),

quoter_per_order AS (
    SELECT
        so.id AS order_id,
        coalesce(lqau.full_name, fqu.full_name) AS quoter
    FROM ergeon.store_order so
    LEFT JOIN first_quote_sent fqs ON fqs.order_id = so.id --join by first quote sent
    LEFT JOIN ergeon.quote_quote qfqs ON qfqs.id = fqs.quote_id
    LEFT JOIN ergeon.core_user fqu ON fqu.id = qfqs.sent_to_customer_by_id
    LEFT JOIN ergeon.hrm_staff fqhs ON fqhs.user_id = fqu.id
    LEFT JOIN useful_sql.hrm fqhrm ON fqhrm.staff_id =
        --before 4th July, preparation_completed_by_id has no data
        if(qfqs.sent_to_customer_at < '2022-07-04', fqhs.id,
            --if estimate use created by else preparation_completed_by
            if(qfqs.is_estimate, qfqs.created_by_id, qfqs.preparation_completed_by_id))
        AND
        date(if(qfqs.is_estimate, qfqs.created_at, qfqs.sent_to_customer_at))
        BETWEEN fqhrm.started_at AND fqhrm.end_date --quote sent between start-end date of quoter
    LEFT JOIN last_quote_approved lqa ON lqa.order_id = so.id --join by last approved quote
    LEFT JOIN ergeon.quote_quote qlqa ON qlqa.id = lqa.quote_id
    LEFT JOIN ergeon.core_user lqau ON lqau.id = qlqa.sent_to_customer_by_id
    LEFT JOIN ergeon.hrm_staff lqahs ON lqau.id = lqahs.user_id
    LEFT JOIN useful_sql.hrm lqahrm ON lqahrm.staff_id =
        if(qlqa.sent_to_customer_at < '2022-07-04', lqahs.id, --same logic but uses last quote approved
            if(qlqa.is_estimate, qlqa.created_by_id, qlqa.preparation_completed_by_id))
        AND
        date(if(qlqa.is_estimate, qlqa.created_at, qlqa.sent_to_customer_at))
        BETWEEN lqahrm.started_at AND lqahrm.end_date
    WHERE (lqahrm.department IN ('Construction', 'Sales')
        OR fqhrm.department IN ('Construction', 'Sales')) --quoter should be construction or sales
    --in case of multiple stafflogs per order, choose stafflog with the last started date
    QUALIFY rank() OVER(PARTITION BY so.id ORDER BY lqahrm.started_at DESC, fqhrm.started_at, lqahrm.end_date DESC, fqhrm.end_date) = 1
),

quoted_department AS (
    SELECT
        q.order_id,
        string_agg(
            CASE
                WHEN sqhrm.department = 'Sales' AND q.preparation_completed_by_id IS NOT NULL THEN 'S' --department = Sales 
                WHEN sqhrm.department = 'Construction' THEN
                    CASE
                        WHEN is_scope_change THEN 't' --change order estimation
                        ELSE 'C' END --estimation
                ELSE '?' END, '') --unknown
        AS dept,
        max(CASE WHEN sqhrm.department = 'Sales' AND q.preparation_completed_by_id IS NOT NULL THEN cqcu.full_name ELSE NULL END) AS sales_full_name,
        --delta between first approved and change order, when construction makes the change order
        max(if(sqhrm.department = 'Construction' AND is_scope_change AND q.approved_at IS NOT NULL, total_cost, NULL)) AS delta
    FROM ergeon.quote_quote q
    LEFT JOIN ergeon.hrm_staff s ON s.id = if(q.is_estimate, q.created_by_id, q.preparation_completed_by_id)
    LEFT JOIN ergeon.core_user cqcu ON cqcu.id = s.user_id --join by the creator of quote
    LEFT JOIN ergeon.core_user sqcu ON sqcu.id = q.sent_to_customer_by_id --join by the user who sent quote
    LEFT JOIN ergeon.hrm_staff sqhs ON sqcu.id = sqhs.user_id
    --join staff member the date where quote was sent
    LEFT JOIN useful_sql.hrm sqhrm ON sqhrm.staff_id =
        if(q.sent_to_customer_at < '2022-07-04', sqhs.id,
            if(q.is_estimate, q.created_by_id, q.preparation_completed_by_id))
        AND
        date(if(q.is_estimate, q.created_at, q.sent_to_customer_at)) BETWEEN started_at AND end_date
    WHERE sent_to_customer_at >= '2018-04-16'
        --change order is null or is change order and scoping task
        AND (is_scope_change IS NULL OR (is_scope_change AND lower(title) LIKE '%scop%' AND method = 'measured'))
        AND is_cancellation = FALSE
    GROUP BY 1
),

quoted_department_per_order AS (
    SELECT DISTINCT
        order_id,
        if(dept LIKE '%S%', 'sales', if(dept LIKE '%t%' OR dept LIKE '%C%', 'estimation', 'unknown')) AS quoted_dep,
        if(dept LIKE '%S%', sales_full_name, NULL) AS sales_full_name,
        if(dept LIKE '%t%', delta, NULL) AS delta
    FROM quoted_department
),

first_quoted_department AS ( --same logic with quoted department but for first quote sent
    SELECT
        fqs.order_id,
        CASE
            WHEN fqshrm.department = 'Sales' AND qfqs.preparation_completed_by_id IS NOT NULL THEN 'sales'
            WHEN fqshrm.department = 'Construction' THEN 'estimation'
            ELSE 'unknown'
        END AS first_quoted_dept
    FROM first_quote_sent fqs
    LEFT JOIN ergeon.quote_quote qfqs ON fqs.quote_id = qfqs.id
    LEFT JOIN ergeon.hrm_staff fqshs ON qfqs.sent_to_customer_by_id = fqshs.user_id
    --join staff member the date where quote was sent
    LEFT JOIN useful_sql.hrm fqshrm ON fqshrm.staff_id =
        if(qfqs.sent_to_customer_at < '2022-07-04', fqshs.id,
            if(qfqs.is_estimate, qfqs.created_by_id, qfqs.preparation_completed_by_id))
        AND
        date(if(qfqs.is_estimate, qfqs.created_at, qfqs.sent_to_customer_at)) BETWEEN started_at AND end_date
    WHERE sent_to_customer_at >= '2018-04-16'
        AND (is_scope_change IS NULL OR (is_scope_change AND lower(title) LIKE '%scop%' AND method = 'measured'))
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY fqs.order_id ORDER BY fqshrm.started_at DESC, end_date DESC) = 1
),

last_quoted_department AS ( --same logic with quoted department but for first approved, if not approved we use last quote sent
    SELECT DISTINCT
        lqs . order_id AS order_id,
        CASE
            WHEN sql_hrm.department = 'Sales' AND qfqa.preparation_completed_by_id IS NOT NULL THEN 'sales'
            WHEN sql_hrm.department = 'Construction' THEN 'estimation'
            ELSE 'unknown'
        END AS last_quoted_dept
    FROM last_quote_sent lqs --last quote sent per order
    LEFT JOIN first_quote_approved fqa ON fqa.order_id = lqs.order_id --first approved quote per order
    LEFT JOIN ergeon.quote_quote qfqa ON coalesce(fqa.quote_id, lqs.quote_id) = qfqa.id --join by first approved else last sent quote
    LEFT JOIN ergeon.hrm_staff user ON qfqa.sent_to_customer_by_id = user.user_id
    LEFT JOIN useful_sql.hrm sql_hrm ON sql_hrm.staff_id =
        if(qfqa.sent_to_customer_at < '2022-07-04', user.id,
            if(qfqa.is_estimate, qfqa.created_by_id, qfqa.preparation_completed_by_id))
        AND
        date(if(qfqa.is_estimate, qfqa.created_at, qfqa.sent_to_customer_at)) BETWEEN started_at AND end_date
    WHERE sent_to_customer_at >= '2018-04-16'
        AND (is_scope_change IS NULL OR (is_scope_change AND lower(title) LIKE '%scop%' AND method = 'measured'))
        AND is_cancellation = FALSE
    QUALIFY rank() OVER(PARTITION BY lqs.order_id ORDER BY sql_hrm.started_at DESC, end_date DESC) = 1
)

SELECT
    so.id AS order_id,
    photographer,
    contractor,
    contractor_count,
    CASE
        WHEN team_lead IS NOT NULL THEN '/' || team_lead || '/' || sales_rep
        ELSE sales_rep
    END AS sales_rep,
    sales_team,
    coalesce(sales_title_at_arrival, sales_title_at_win) AS sales_title,
    sales_staff_id,
    pm_id,
    project_manager,
    pm_team,
    if(quoted_dep = 'sales', sales_full_name, quoter) AS quoter,
    quoted_dep, --which department was the quoter of the order
    delta,
    first_quoted_dept, --department of order according to first quote sent
    last_quoted_dept --department of order according to first approved quote else to last quote sent
FROM ergeon.store_order so
LEFT JOIN photographer_per_order p ON p.order_id = so.id
LEFT JOIN contractor_per_order c ON c.order_id = so.id
LEFT JOIN contractors_per_order cs ON cs.order_id = so.id
LEFT JOIN sales_member_per_order s ON s.order_id = so.id
LEFT JOIN project_manager_per_order pm ON pm.order_id = so.id
LEFT JOIN quoter_per_order q ON q.order_id = so.id
LEFT JOIN sales_title_per_order_at_arrival sta ON sta.order_id = so.id
LEFT JOIN sales_title_per_order_at_win stw ON stw.order_id = so.id
LEFT JOIN quoted_department_per_order qd ON qd.order_id = so.id
LEFT JOIN first_quoted_department fqd ON fqd.order_id = so.id
LEFT JOIN last_quoted_department lqd ON lqd.order_id = so.id
