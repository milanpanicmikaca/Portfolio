SELECT
    wwo.*,
    if(wwo.revenue > 0, wwo.revenue, 0) AS new_revenue,
    if(wwo.revenue < 0, wwo.revenue, 0) AS returned_revenue,
    datetime_diff(wwo.completed_ts_at, o.completed_ts_at, HOUR) / 24 AS tat_pw,
    CASE
        WHEN o.wwo_completed_count = 1 THEN datetime_diff(wwo.completed_ts_at, o.completed_ts_at, HOUR) / 24
        WHEN o.wwo_completed_count > 1 THEN (1 / o.wwo_completed_count) * (datetime_diff(wwo.completed_ts_at, o.completed_ts_at, HOUR) / 24)
    END AS weighted_tat_pw,
    o.revenue AS parent_revenue,
    CASE
        WHEN wwo.completed_at IS NOT NULL THEN "Completed"
        WHEN wwo.won_at IS NOT NULL AND wwo.cancelled_at IS NOT NULL AND wwo.cancelled_at >= wwo.won_at THEN "Cancelled - Won"
        WHEN wwo.won_at IS NOT NULL THEN "Won"
        WHEN wwo.quoted_at IS NOT NULL AND wwo.cancelled_at IS NOT NULL AND wwo.cancelled_at >= wwo.quoted_at THEN "Cancelled - Quoted"
        WHEN wwo.quoted_at IS NOT NULL THEN "Quoted"
        WHEN wwo.cancelled_at IS NOT NULL THEN "Cancelled - Order"
        ELSE "Order"
    END AS wwo_status
FROM int_data.order_calculated_fields wwo
LEFT JOIN int_data.order_calculated_fields o ON wwo.parent_order_id = o.order_id
WHERE wwo.is_warranty_order = TRUE
