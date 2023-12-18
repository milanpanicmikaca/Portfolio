CREATE TEMP FUNCTION is_trailing_period(input_date DATE, n INT64)
RETURNS STRING
AS (
    (if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) = 0, 'Today',
            if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 1 AND n, 'Period-A',
                if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN n + 1 AND 2 * n, 'Period-B',
                    if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 2 * n + 1 AND 3 * n, 'Period-C',
                        if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 3 * n + 1 AND 4 * n, 'Period-D',
                            if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 4 * n + 1 AND 5 * n, 'Period-E',
                                if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 5 * n + 1 AND 6 * n, 'Period-F',
                                    if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 6 * n + 1 AND 7 * n, 'Period-G',
                                        if(date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 7 * n + 1 AND 8 * n, 'Period-H',
                                            if(
                                                date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 8 * n + 1 AND 9 * n,
                                                'Period-I',
                                                if(
                                                    date_diff(current_date('America/Los_Angeles'), input_date, DAY) BETWEEN 9 * n + 1 AND 10 * n,
                                                    'Period-J',
                                                    if(input_date IS NULL, 'Period-Null', 'Period-Rest')))))))))))))
);
WITH customer_aggregations AS (
    SELECT
        cc.id,
        array_agg(order_id) AS order_list,
        min(ue.created_at) AS customer_arrival_at,
        min(ue.created_ts_at) AS customer_arrival_ts_at,
        min(quoted_at) AS quoted_at,
        min(quoted_ts_at) AS quoted_ts_at,
        min(quote_requested_ts_at) AS quote_requested_ts_at,
        min(won_at) AS won_at,
        min(won_ts_at) AS won_ts_at,
        min(cancelled_at) AS cancelled_at,
        min(cancelled_ts_at) AS cancelled_ts_at,
        min(completed_at) AS completed_at,
        min(completed_ts_at) AS completed_ts_at,
        min(marked_completed_at) AS marked_completed_at,
        min(photographer_visit_at) AS photographer_visit_at,
        min(paid_at) AS paid_at,
        min(onsite_ts_at) AS onsite_ts_at,
        min(estimated_ts_at) AS estimated_ts_at,
        min(booked_ts_at) AS booked_ts_at,
        min(scoping_task_at) AS scoping_task_at,
        count(*) AS orders,
        sum(if(is_lead = TRUE, 1, 0)) AS leads,
        sum(is_win) AS wins,
        sum(is_quoted) AS quotes,
        sum(is_completed) AS completed,
        sum(is_cancelled) AS cancelled,
        sum(is_onsite) AS onsites,
        sum(mktg_fee) AS mktg_fee,
        sum(revenue) AS revenue,
        sum(np) AS np,
        sum(vnp) AS vnp,
        sum(gp) AS gp,
        sum(nr) AS nr,
        sum(last_quoted_price) AS last_quoted_total_price,
        sum(first_quoted_price) AS first_quoted_total_price,
        sum(last_approved_price) AS last_approved_total_price,
        sum(first_approved_price) AS first_approved_total_price,
        sum(last_quoted_cost) AS last_quoted_total_cost,
        sum(first_quoted_cost) AS first_quoted_total_cost,
        sum(last_approved_cost) AS last_approved_total_cost,
        sum(first_approved_cost) AS first_approved_total_cost,
        sum(has_escalation) AS escalations,
        sum(total_length) AS linear_length,
        sum(sales_oh_cost) AS sales_oh_cost,
        sum(sales_cost) AS sales_cost,
        sum(net_costs) AS net_cost,
        sum(cp) AS cp,
        sum(delivery_oh_cost) AS delivery_oh_cost,
        sum(delivery_cost) AS delivery_cost,
        sum(overhead_cost) AS overhead_cost,
        sum(sales_var_cost) AS sales_var_cost,
        sum(delivery_var_cost) AS delivery_var_cost,
        sum(cogs) AS cogs,
        sum(cost_of_sales) AS cost_of_sales,
        sum(contractor_pay) AS contractor_pay,
        sum(last_approved_sales_discount) AS last_approved_sales_discount,
        sum(last_approved_mktg_discount) AS last_approved_mktg_discount,
        sum(materials_pay) AS materials_pay,
        sum(last_approved_pricing_discount) AS last_approved_pricing_discount,
        sum(finance_disc) AS finance_disc,
        sum(if(multi_party_approval = 'yes', 1, 0)) AS multi_party_approval,
        sum(quotes_sent_count) AS quotes_sent_count,
        sum(change_order_count) AS change_order_count,
        sum(is_booked) AS booking_order_count,
        sum(is_onsite) AS onsite_order_count,
        sum(is_estimate) AS estimate_order_count,
        sum(is_draft_editor) AS draft_editor_order_count
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    GROUP BY 1
),

customer_at_arrival AS (
    SELECT
        cc.id,
        order_id,
        lead_id,
        channel1,
        ha_type,
        geo,
        product,
        type,
        old_region,
        region,
        market,
        county,
        city,
        state,
        msa,
        cmsa,
        utm_medium,
        sales_rep,
        sales_team,
        lost_reason,
        CASE
            WHEN channel1 LIKE '%/Home Advisor/Ads%' THEN 'Home Advisor Ads'
            WHEN channel1 LIKE '%Home Advisor%' THEN 'Home Advisor'
            WHEN channel1 LIKE '%/Paid/Facebook%' THEN 'Paid/Facebook'
            WHEN channel1 LIKE '%/Non Paid/Facebook%' THEN 'Non Paid/Facebook'
            WHEN channel1 LIKE '%Thumbtack%' THEN 'Thumbtack'
            WHEN channel1 LIKE '%/Paid/Google%' THEN 'Paid/Google'
            WHEN channel1 LIKE '%/Non Paid/Google%' THEN 'Non Paid/Google'
            WHEN channel1 LIKE '%Yelp%' THEN 'Yelp'
            WHEN channel1 LIKE '%Nextdoor%' THEN 'Nextdoor'
            WHEN channel1 LIKE '%Bark%' THEN 'Bark'
            WHEN channel1 LIKE '%Borg%' THEN 'Borg'
            WHEN channel1 LIKE '%Non Paid/Direct%' THEN 'Non Paid/Direct'
            WHEN channel1 LIKE '%/Paid/%' THEN 'Paid/Misc'
            WHEN channel1 LIKE '%/Non Paid/%' THEN 'Non Paid/Misc'
            ELSE 'Unknown'
        END AS grouped_channel,
        product_quoted,
        segment,
        segment_l1,
        onsite_type,
        project_manager,
        pm_team
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    QUALIFY rank() OVER(PARTITION BY cc.id ORDER BY ue.created_at, order_id) = 1
),

customer_at_quoted AS (
    SELECT
        cc.id,
        product_quoted,
        segment,
        segment_l1,
        tier,
        quoter,
        quoted_dep,
        first_quoted_dept,
        photographer,
        first_quoted_cost,
        first_quoted_price,
        last_quoted_cost,
        last_quoted_price
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    WHERE quoted_at IS NOT NULL
    QUALIFY rank() OVER(PARTITION BY cc.id ORDER BY ue.quoted_at, order_id) = 1
),

customer_at_onsite AS (
    SELECT
        cc.id,
        onsite_type
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    WHERE onsite_ts_at IS NOT NULL
    QUALIFY rank() OVER(PARTITION BY cc.id ORDER BY ue.onsite_ts_at, order_id) = 1
),

customer_at_won AS (
    SELECT
        cc.id,
        project_manager,
        pm_team,
        first_approved_price,
        first_approved_cost,
        last_approved_price,
        last_approved_cost
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    WHERE won_at IS NOT NULL
    QUALIFY rank() OVER(PARTITION BY cc.id ORDER BY ue.won_at, order_id) = 1
),

customer_at_completion AS (
    SELECT
        cc.id,
        contractor,
        merchant_fee_type
    FROM
        ergeon.customers_customer cc JOIN
        int_data.order_ue_materialized ue ON ue.customer_id = cc.id
    WHERE completed_at IS NOT NULL
    QUALIFY rank() OVER(PARTITION BY cc.id ORDER BY merchant_fee_type DESC, ue.completed_at, order_id) = 1
)

SELECT
    agg.*,
    order_id,
    lead_id,
    channel1,
    ha_type,
    geo,
    product,
    type,
    old_region,
    region,
    market,
    county,
    city,
    state,
    msa,
    cmsa,
    utm_medium,
    sales_rep,
    sales_team,
    lost_reason,
    grouped_channel,
    coalesce(q.product_quoted, a.product_quoted) AS product_quoted,
    coalesce(q.segment, a.segment) AS segment,
    coalesce(q.segment_l1, a.segment_l1) AS segment_l1,
    coalesce(o.onsite_type, a.onsite_type) AS onsite_type,
    tier,
    quoter,
    quoted_dep,
    first_quoted_dept,
    photographer,
    first_quoted_cost,
    first_quoted_price,
    last_quoted_cost,
    last_quoted_price,
    coalesce(w.project_manager, a.project_manager) AS project_manager,
    coalesce(w.pm_team, a.pm_team) AS pm_team,
    first_approved_price,
    first_approved_cost,
    last_approved_price,
    last_approved_cost,
    contractor,
    merchant_fee_type,
    if(quoted_at IS NOT NULL, 1, 0) AS quoted_customers,
    if(won_at IS NOT NULL, 1, 0) AS win_customers,
    if(completed_at IS NOT NULL, 1, 0) AS completed_customers,
    if(onsite_ts_at IS NOT NULL, 1, 0) AS onsite_customers,
    if(booked_ts_at IS NOT NULL, 1, 0) AS booked_customers,
    if(estimated_ts_at IS NOT NULL, 1, 0) AS estimated_customers,
    CASE
        WHEN completed_at IS NOT NULL THEN "Completed"
        WHEN won_at IS NOT NULL THEN "Won"
        WHEN quoted_at IS NOT NULL THEN "Quoted"
        ELSE "Lead"
    END AS customer_status,
    coalesce(completed_at, cancelled_at) AS closed_at, coalesce(won_at, cancelled_at) AS closedW_at,
    is_trailing_period(customer_arrival_at, 7) AS arrival_t7days,
    is_trailing_period(customer_arrival_at, 14) AS arrival_t14days,
    is_trailing_period(customer_arrival_at, 28) AS arrival_t28days,
    is_trailing_period(customer_arrival_at, 56) AS arrival_t56days,
    is_trailing_period(customer_arrival_at, 84) AS arrival_t84days,
    is_trailing_period(customer_arrival_at, 112) AS arrival_t112days,
    is_trailing_period(quoted_at, 7) AS quoted_t7days,
    is_trailing_period(quoted_at, 14) AS quoted_t14days,
    is_trailing_period(quoted_at, 28) AS quoted_t28days,
    is_trailing_period(quoted_at, 56) AS quoted_t56days,
    is_trailing_period(quoted_at, 84) AS quoted_t84days,
    is_trailing_period(quoted_at, 112) AS quoted_t112days,
    is_trailing_period(won_at, 7) AS won_t7days,
    is_trailing_period(won_at, 14) AS won_t14days,
    is_trailing_period(won_at, 28) AS won_t28days,
    is_trailing_period(won_at, 56) AS won_t56days,
    is_trailing_period(won_at, 84) AS won_t84days,
    is_trailing_period(won_at, 112) AS won_t112days,
    is_trailing_period(coalesce(won_at, cancelled_at), 7) AS closedW_t7days,
    is_trailing_period(coalesce(won_at, cancelled_at), 14) AS closedW_t14days,
    is_trailing_period(coalesce(won_at, cancelled_at), 28) AS closedW_t28days,
    is_trailing_period(coalesce(won_at, cancelled_at), 56) AS closedW_t56days,
    is_trailing_period(coalesce(won_at, cancelled_at), 84) AS closedW_t84days,
    is_trailing_period(coalesce(won_at, cancelled_at), 112) AS closedW_t112days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 7) AS closed_t7days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 14) AS closed_t14days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 28) AS closed_t28days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 56) AS closed_t56days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 84) AS closed_t84days,
    is_trailing_period(coalesce(completed_at, cancelled_at), 112) AS closed_t112days,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 360, 1, 0) AS is_order360,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 360, 1, 0) AS is_quoted360,
    if(date_diff(current_date(), won_at, DAY) >= 360, 1, 0) AS is_win360,
    if(date_diff(completed_at, won_at, DAY) <= 360, 1, 0) AS is_completed360w,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 360, 1, 0) AS is_won360,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 240, 1, 0) AS is_order240,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 240, 1, 0) AS is_won240,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 240, 1, 0) AS is_quoted240,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 180, 1, 0) AS is_order180,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 180, 1, 0) AS is_won180,
    if(date_diff(current_date(), quoted_at, DAY) >= 180, 1, 0) AS is_quote180,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 180, 1, 0) AS is_quoted180,
    if(date_diff(won_at, quoted_at, DAY) <= 180, 1, 0) AS is_won180q,
    if(date_diff(current_date(), won_at, DAY) >= 180, 1, 0) AS is_win180,
    if(date_diff(completed_at, won_at, DAY) <= 180, 1, 0) AS is_completed180w,
    if(date_diff(completed_at, won_at, DAY) <= 150, 1, 0) AS is_completed150w,
    if(date_diff(current_date(), won_at, DAY) >= 150, 1, 0) AS is_win150,
    if(date_diff(completed_at, won_at, DAY) <= 120, 1, 0) AS is_completed120w,
    if(date_diff(current_date(), won_at, DAY) >= 120, 1, 0) AS is_win120,
    if(date_diff(completed_at, customer_arrival_at, DAY) <= 120, 1, 0) AS is_completed120,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 120, 1, 0) AS is_order120,
    if(date_diff(current_date(), quoted_at, DAY) >= 120, 1, 0) AS is_quote120,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 120, 1, 0) AS is_quoted120,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 120, 1, 0) AS is_won120,
    if(date_diff(won_at, quoted_at, DAY) <= 120, 1, 0) AS is_won120q,
    if(date_diff(completed_at, won_at, DAY) <= 90, 1, 0) AS is_completed90w,
    if(date_diff(current_date(), won_at, DAY) >= 90, 1, 0) AS is_win90,
    if(date_diff(completed_at, customer_arrival_at, DAY) <= 90, 1, 0) AS is_completed90,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 90, 1, 0) AS is_order90,
    if(date_diff(current_date(), quoted_at, DAY) >= 90, 1, 0) AS is_quote90,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 90, 1, 0) AS is_quoted90,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 90, 1, 0) AS is_won90,
    if(date_diff(won_at, quoted_at, DAY) <= 90, 1, 0) AS is_won90q,
    if(date_diff(completed_at, won_at, DAY) <= 60, 1, 0) AS is_completed60w,
    if(date_diff(current_date(), won_at, DAY) >= 60, 1, 0) AS is_win60,
    if(date_diff(completed_at, customer_arrival_at, DAY) <= 60, 1, 0) AS is_completed60,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 60, 1, 0) AS is_order60,
    if(date_diff(current_date(), quoted_at, DAY) >= 60, 1, 0) AS is_quote60,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 60, 1, 0) AS is_quoted60,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 60, 1, 0) AS is_won60,
    if(date_diff(won_at, quoted_at, DAY) <= 60, 1, 0) AS is_won60q,
    if(date_diff(completed_at, won_at, DAY) <= 30, 1, 0) AS is_completed30w,
    if(date_diff(current_date(), won_at, DAY) >= 30, 1, 0) AS is_win30,
    if(date_diff(completed_at, customer_arrival_at, DAY) <= 30, 1, 0) AS is_completed30,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 30, 1, 0) AS is_order30,
    if(date_diff(current_date(), quoted_at, DAY) >= 30, 1, 0) AS is_quote30,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 30, 1, 0) AS is_quoted30,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 30, 1, 0) AS is_won30,
    if(date_diff(won_at, quoted_at, DAY) <= 30, 1, 0) AS is_won30q,
    if(date_diff(completed_at, customer_arrival_at, DAY) <= 14, 1, 0) AS is_completed14,
    if(date_diff(current_date(), customer_arrival_at, DAY) >= 14, 1, 0) AS is_order14,
    if(date_diff(current_date(), quoted_at, DAY) >= 14, 1, 0) AS is_quote14,
    if(date_diff(quoted_at, customer_arrival_at, DAY) <= 14, 1, 0) AS is_quoted14,
    if(date_diff(current_date('America/Los_Angeles'), onsite_ts_at, DAY) >= 14, 1, 0) AS is_onsite14,
    if(date_diff(quoted_at, onsite_ts_at, DAY) <= 14, 1, 0) AS is_quote14ons,
    if(date_diff(won_at, customer_arrival_at, DAY) <= 14, 1, 0) AS is_won14,
    if(date_diff(won_at, quoted_at, DAY) <= 14, 1, 0) AS is_won14q,
    if(date_diff(current_date('America/Los_Angeles'), customer_arrival_at, DAY) >= 7, 1, 0) AS is_order7,
    if(date_diff(onsite_ts_at, customer_arrival_at, DAY) <= 7, 1, 0) AS is_onsited7,
    datetime_diff(completed_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_ad,
    datetime_diff(quoted_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_aq,
    datetime_diff(quote_requested_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_ar,
    CASE WHEN cancelled_ts_at >= quoted_ts_at THEN datetime_diff(cancelled_ts_at, quoted_ts_at, HOUR) / 24 END AS tat_qc,
    datetime_diff(cancelled_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_ac,
    datetime_diff(won_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_aw,
    datetime_diff(won_ts_at, quoted_ts_at, HOUR) / 24 AS tat_qw,
    datetime_diff(quoted_ts_at, quote_requested_ts_at, HOUR) / 24 AS tat_rq,
    datetime_diff(completed_ts_at, won_ts_at, HOUR) / 24 AS tat_wd,
    datetime_diff(onsite_ts_at, booked_ts_at, HOUR) / 24 AS tat_bo,
    datetime_diff(quoted_ts_at, onsite_ts_at, HOUR) / 24 AS tat_oq,
    datetime_diff(onsite_ts_at, customer_arrival_ts_at, HOUR) / 24 AS tat_ao
FROM
    customer_aggregations agg LEFT JOIN
    customer_at_arrival a USING (id) LEFT JOIN
    customer_at_quoted q USING (id) LEFT JOIN
    customer_at_won w USING (id) LEFT JOIN
    customer_at_completion USING (id) LEFT JOIN
    customer_at_onsite o USING (id)
