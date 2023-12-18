-- TODO
-- COGS include the 2% HD kickback from all purchases (acct trx material purchases with order_id) - directly attributed
    -- pallet_tracking history status fulffilled  HD extended) similar to covid pay
    -- 1.5% from CC + 2% from HD
-- COGS distribute material gains into orders as a separate materials_revenue column similar to covid pay
-- NETCOSTS in net costs add all the warranty costs AFTER netting out the 1% already in the COGS
-- CLEANUP customer discounts dating - they should be dated as post completion
WITH trx_attributes AS ( --All transactions and their attributes
    SELECT
        trx.order_id,
        amount,
        type_id,
        pm.name AS pay_method,
        trx.date AS trx_date,
        paymethod_id
    FROM ergeon.accounting_transaction AS trx
    INNER JOIN ergeon.store_order AS o ON o.id = trx.order_id
    LEFT JOIN ergeon.accounting_paymethod AS pm ON pm.id = trx.paymethod_id
    WHERE
        trx.deleted_at IS NULL
        AND order_id NOT IN (
            50815,
            56487,
            59225,
            59348,
            59404,
            59666,
            59670,
            59743,
            59753,
            59789,
            59805,
            59813,
            59878,
            59908,
            59922,
            60273,
            60283,
            60401,
            60547,
            60589,
            60590,
            60595,
            60596,
            60597,
            60612
        )
        AND o.created_at >= '2018-04-16'
),

customer_cc AS ( --Assign Credit Card fees according to type and payment method
    SELECT
        order_id,
        SUM(CASE WHEN type_id = 13 THEN amount ELSE 0 END) AS collected_cc_fees, --Type = 'Merchant Processing Fee'
        SUM(CASE
            WHEN type_id = 9 AND LOWER(pay_method) LIKE '%credit%' THEN amount * 0.029 + 0.30 --Type = 'Customer Paid' & Paymethod = 'Credit Card'
            WHEN type_id = 9 AND paymethod_id = 3 THEN --Type = 'Customer Paid' & Paymethod = 'Bank Transfer'
                 CASE WHEN amount > 1000 THEN 10 --if amount >10 we charge 10 else 1%
                ELSE amount * 0.01 END
            WHEN type_id = 9 AND paymethod_id = 5 THEN 0.039 * amount ELSE 0 --Type = 'Customer Paid' & Paymethod = 'Customer-Financing'
            END) AS paid_cc_fees,
        --only credit card fees
        SUM(CASE WHEN type_id = 9 AND LOWER(pay_method) LIKE '%credit%' THEN amount * 0.029 + 0.30 ELSE 0 END) AS credit_card_fees,
        SUM(CASE WHEN type_id = 9 AND paymethod_id = 3 THEN
            CASE WHEN amount > 1000 THEN 10
                      ELSE amount * 0.01
            END
            END) AS bank_transfer_fees,
        SUM(CASE WHEN type_id = 9 AND paymethod_id = 5 THEN 0.039 * amount ELSE 0 END) AS wisetack_fees
    FROM trx_attributes
    GROUP BY 1
),

last_approved_quotes AS (
    SELECT
        o.id AS order_id,
        completed_at AS cancelled_at,
        is_cancellation
    FROM
        ergeon.store_order AS o INNER JOIN
        ergeon.quote_quote AS q ON q.order_id = o.id
    WHERE
        q.created_at >= '2018-04-16'
        AND approved_at IS NOT NULL
    QUALIFY RANK() OVER(PARTITION BY o.id ORDER BY approved_at DESC, q.id DESC) = 1
),

cancelled_projects AS (
    SELECT *
    FROM last_approved_quotes
    WHERE is_cancellation = TRUE
),

trx_aggr_per_order AS ( --aggregates amounts of transactions per order
    SELECT
        trx_attributes.order_id,
        MIN(CASE WHEN type_id = 5 THEN trx_date END) AS contractor_paid_at, -- First date Contractor Paid
        SUM(CASE WHEN type_id IN (8, 10) THEN amount ELSE 0 END) AS revenue, -- ( 'Customer Signoff' , 'Customer Discounts'
        SUM(CASE WHEN type_id = 4 THEN amount ELSE 0 END) AS contractor_pay, -- ( 'Contractor Signoff')
        -- ('Warranty (labor)', 'Warranty (materials)', 'Warranty (customer discounts)'  )
        SUM(CASE WHEN type_id IN (6, 7, 14) THEN amount ELSE 0 END) AS warranty_pay,
        SUM(CASE WHEN type_id IN (16) THEN amount ELSE 0 END) AS materials_deduction, -- ( 'Materials Deduction')
        -- ( 'Materials Purchased', 'Materials Returned',  'Materials Deduction')
        SUM(CASE WHEN type_id IN (1, 2, 16) THEN amount ELSE 0 END) AS materials_pay,
        SUM(CASE WHEN type_id = 9 AND paymethod_id = 4 THEN amount ELSE 0 END) AS finance_disc -- ( 'Customer Paid','Customer Write-off')
    FROM trx_attributes
    INNER JOIN ergeon.store_order AS o ON o.id = trx_attributes.order_id
    LEFT JOIN cancelled_projects AS cp ON cp.order_id = trx_attributes.order_id
    WHERE completed_at IS NOT NULL
        AND cp.order_id IS NULL
    GROUP BY 1
),

running_balance AS ( --Get the running balance of an order (revenue-paid)
    SELECT
        trx.order_id,
        date,
        pm.name AS pay_method,
        type_id,
        amount,
        trx.id AS trx_id,
        SUM(CASE
            WHEN type_id = 19 THEN amount -- Customer Billed
            WHEN type_id = 9 THEN -amount -- Customer Paid
            WHEN type_id = 10 THEN -ABS(amount) --Customer Discount
            WHEN type_id = 14 THEN -ABS(amount) --Warranty Customer Discount
            END) OVER (PARTITION BY trx.order_id ORDER BY date, trx.id) AS running_sum --rank the sum of these transactions based on trx date
    FROM ergeon.accounting_transaction AS trx
    LEFT JOIN ergeon.store_order AS o ON o.id = trx.order_id
    LEFT JOIN ergeon.accounting_paymethod AS pm ON pm.id = trx.paymethod_id
    LEFT JOIN cancelled_projects AS cp ON cp.order_id = trx.order_id
    WHERE trx.deleted_at IS NULL
        AND trx.order_id NOT IN (
            50815,
            56487,
            59225,
            59348,
            59404,
            59666,
            59670,
            59743,
            59753,
            59789,
            59805,
            59813,
            59878,
            59908,
            59922,
            60273,
            60283,
            60401,
            60547,
            60589,
            60590,
            60595,
            60596,
            60597,
            60612
        )
        AND type_id IN (19, 9, 10, 14) -- ( 'Customer Signoff' ,'Customer Paid' 'Customer Discounts')
        AND o.completed_at IS NOT NULL
        AND amount != 0
        AND cp.order_id IS NULL
),

paid_date AS (--get the paid_at date

    SELECT
        order_id,
        MIN(date) AS paid_at
    FROM running_balance
    WHERE running_sum = 0 --when running balance sums up to 0 then we assign this date AS the pay date
    GROUP BY 1
),

pay_method_order AS (
    SELECT
        order_id,
        pay_method,
        MAX(date) AS max_date,
        MAX(trx_id) AS max_trx_id,
        RANK() OVER(PARTITION BY order_id ORDER BY SUM(amount) DESC) AS order_pay_method_rank --ranking Customer Paid amount per order per pay method
    FROM running_balance
    WHERE
        type_id = 9
        AND pay_method IS NOT NULL
    GROUP BY 1, 2
),

merchant_fee_method AS (
    SELECT
        order_id,
        pay_method AS merchant_fee_type
    FROM pay_method_order
    --Choose they most paid pay method 
    QUALIFY RANK() OVER(PARTITION BY order_id ORDER BY order_pay_method_rank ASC, max_date ASC, max_trx_id ASC) = 1
),

wwo_contractor_pay AS ( ---wwo installer leakage
    SELECT
        so.parent_order_id AS order_id,
        SUM(cc.total_cost) AS wwo_installer_leakage
    FROM ergeon.contractor_contractororder AS cc
    LEFT JOIN ergeon.store_order AS so ON so.id = cc.order_id
    WHERE status_id = 13 --only contractor orders with completed status
        AND so.parent_order_id IS NOT NULL
        AND cc.total_cost < 0 --only negative contractor orders should be counted 
    GROUP BY 1
),

wwo_count_per_order AS ( ---count the warranty work orders per completed order
    SELECT
        parent_order_id AS order_id,
        MIN(created_at) AS warranty_at, --first date a wwwo was created
        MAX(created_at) AS last_warranty_at, --last date a wwo was created, in case there are 2+
        COUNT(*) AS wwo_count,
        SUM(IF(completed_at IS NOT NULL AND cp.order_id IS NULL, 1, 0)) AS wwo_completed_count
    FROM ergeon.store_order AS o
    LEFT JOIN cancelled_projects AS cp ON cp.order_id = o.id
    WHERE parent_order_id IS NOT NULL
    GROUP BY 1
),

handyman_payments AS (
    SELECT 
      order_id,
      SUM(amount) AS handyman_fee
    FROM int_data.handyman_costs
    GROUP BY 1
),

order_trx_attributes AS (
    SELECT
        o.id AS order_id,
        --if order has a cancellation quote then ignore completed_at
        paid_at,
        merchant_fee_type,
        parent_order_id,
        CASE WHEN cp.cancelled_at IS NULL THEN EXTRACT(DATE FROM o.completed_at AT TIME ZONE 'America/Los_Angeles') END AS completed_at,
        CASE WHEN cp.cancelled_at IS NULL THEN DATETIME(o.completed_at, 'America/Los_Angeles') END AS completed_ts_at,
        EXTRACT(DATE FROM warranty_at AT TIME ZONE 'America/Los_Angeles') AS warranty_at,
        DATETIME(warranty_at, 'America/Los_Angeles') AS warranty_ts_at,
        EXTRACT(DATE FROM last_warranty_at AT TIME ZONE 'America/Los_Angeles') AS last_warranty_at,
        COALESCE(revenue, 0) AS revenue,
        COALESCE(revenue, 0) * 0.01 AS warranty_estimate,
        --project should be completed and at least one contractor paid trx
        COALESCE(contractor_pay, 0) AS contractor_pay,
        COALESCE(
            CASE WHEN contractor_paid_at IS NOT NULL AND marked_completed_at IS NOT NULL AND cp.cancelled_at IS NULL THEN materials_pay END, 0
        ) AS materials_pay,
        COALESCE(materials_deduction, 0) AS materials_deduction,
        COALESCE(warranty_pay, 0) AS warranty_pay,
        COALESCE(collected_cc_fees, 0) AS collected_cc_fees,
        COALESCE(paid_cc_fees, 0) AS paid_cc_fees,
        COALESCE(credit_card_fees, 0) AS credit_card_fees,
        COALESCE(bank_transfer_fees, 0) AS bank_transfer_fees,
        COALESCE(wisetack_fees, 0) AS wisetack_fees,
        COALESCE(finance_disc, 0) AS finance_disc,
        COALESCE(wwo_installer_leakage, 0) AS wwo_installer_leakage,
        COALESCE(wwo_count, 0) AS wwo_count,
        COALESCE(wwo_completed_count, 0) AS wwo_completed_count,
        COALESCE(handyman_fee, 0) AS handyman_fee
    FROM ergeon.store_order AS o
    LEFT JOIN trx_aggr_per_order AS agg ON agg.order_id = o.id
    LEFT JOIN customer_cc AS cc ON cc.order_id = o.id
    LEFT JOIN paid_date AS pd ON pd.order_id = o.id
    LEFT JOIN merchant_fee_method AS pm ON pm.order_id = o.id
    LEFT JOIN cancelled_projects AS cp ON cp.order_id = o.id
    LEFT JOIN wwo_contractor_pay AS wwwocp ON wwwocp.order_id = o.id
    LEFT JOIN wwo_count_per_order AS wwo ON wwo.order_id = o.id
    LEFT JOIN handyman_payments AS hp ON hp.order_id = o.id
)

SELECT
    *,
    paid_cc_fees - collected_cc_fees AS cost_of_sales, -- Cost of Sales
    contractor_pay + materials_pay AS cogs, --Cost of goods sold = the sum of all direct costs associated with making a product
    -- Gross Profit = revenue - (cogs + cos + warranty_estimate)
    revenue - ((contractor_pay + materials_pay) + (paid_cc_fees - collected_cc_fees) + warranty_estimate) AS gp
FROM order_trx_attributes
