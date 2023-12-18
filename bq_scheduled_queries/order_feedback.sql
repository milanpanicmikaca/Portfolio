WITH feedback_attributes AS ( --attributes of last review per order and account
    SELECT
        fr.id,
        fr.score,
        fr.order_id,
        channel_id,
        mnl.label AS location,
        EXTRACT(DATE FROM posted_at AT TIME ZONE 'America/Los_Angeles') AS posted_at,
        is_yelp_recommended,
        ha_customer_service,
        ha_value_for_money
    FROM ( --last review id per order and account
        SELECT
            order_id,
            mnl.label,
            MAX(r.id) AS review_id
        FROM ergeon.feedback_review r
        LEFT JOIN ergeon.marketing_localaccount mnl ON mnl.id = r.account_id
        WHERE
            order_id IS NOT NULL
        GROUP BY 1, 2
    ) AS last_review
    LEFT JOIN ergeon.feedback_review fr ON fr.id = last_review.review_id
    LEFT JOIN ergeon.marketing_localaccount mnl ON mnl.id = fr.account_id
),

yelp_reviews AS ( --yelp reviews per order
    SELECT
        id,
        order_id,
        score AS fb_yelp_score, --fb = feedback
        yelp_location,
        posted_at AS fb_yelp_posted_at,
        CASE WHEN is_yelp_recommended = TRUE THEN 1 ELSE 0 END AS yelp_recommended
    FROM feedback_attributes
    LEFT JOIN ( --location of the first review per order in yelp channel
        SELECT
            id,
            location AS yelp_location
        FROM feedback_attributes
        WHERE
            channel_id = 1
        QUALIFY RANK() OVER(PARTITION BY order_id ORDER BY posted_at, id) = 1
    ) AS yelp_review_location USING (id)
    WHERE
        channel_id = 1 --yelp channel
),

google_reviews AS ( --google reviews per order
    SELECT
        id,
        order_id,
        score AS fb_google_score,
        google_location,
        posted_at AS fb_google_posted_at
    FROM feedback_attributes
    LEFT JOIN
        ( --location of the first review per order in google channel
            SELECT
                id,
                location AS google_location
            FROM feedback_attributes
            WHERE
                channel_id = 2
            QUALIFY RANK() OVER(PARTITION BY order_id ORDER BY posted_at, id) = 1
        ) AS google_review_location USING (id)
    WHERE channel_id = 2
),

thumbtack_reviews AS ( --thumbtack reviews per order
    SELECT
        id,
        order_id,
        score AS fb_thumbtack_score,
        posted_at AS fb_thumbtack_posted_at
    FROM feedback_attributes
    WHERE
        channel_id = 4
),

bbb_reviews AS ( --Better Business Bureau reviews per order
    SELECT
        id,
        order_id,
        score AS fb_bbb_score,
        posted_at AS fb_bbb_posted_at
    FROM feedback_attributes
    WHERE
        channel_id = 5
),

ha_reviews AS ( --Home Advisor reviews per order 
    SELECT
        id,
        order_id,
        score AS fb_ha_score,
        ha_customer_service AS fb_ha_customer_service_score,
        ha_value_for_money AS fb_ha_value_for_money_score,
        ha_location,
        posted_at AS fb_ha_posted_at
    FROM feedback_attributes
    LEFT JOIN
        ( --location of the first review per order in ha channel
            SELECT
                id,
                location AS ha_location
            FROM feedback_attributes
            WHERE
                channel_id = 3
            QUALIFY RANK() OVER(PARTITION BY order_id ORDER BY posted_at, id) = 1
        ) AS ha_review_location USING (id)
    WHERE
        channel_id = 3
),

internal_feedback AS ( --Internal channel (communication/installation/quoting score etc.)
    SELECT
        order_id,
        EXTRACT(DATE FROM submitted_at AT TIME ZONE 'America/Los_Angeles') AS fb_internal_posted_at,
        nps AS fb_internal_score,
        communication AS fb_internal_communication_score,
        installation AS fb_internal_installation_score,
        scheduling AS fb_internal_scheduling_score,
        quoting AS fb_internal_quoting_score
    FROM ergeon.feedback_orderfeedback
    WHERE
        submitted_at IS NOT NULL
        AND order_id IS NOT NULL
),

staff_review_attributed AS ( --first attributed person and department per feedback
    SELECT
        order_id,
        --member of sales team that review got attributed
        CASE WHEN sales_staff_attributed_id IS NOT NULL THEN sales_user.full_name END AS fb_sales_attributed,
        --member of delivery team that review got attributed
        CASE WHEN delivery_staff_attributed_id IS NOT NULL THEN delivery_user.full_name END AS fb_delivery_attributed,
        CASE
            WHEN sales_staff_attributed_id IS NOT NULL AND delivery_staff_attributed_id IS NOT NULL THEN 'Both Sales-Delivery'
            WHEN sales_staff_attributed_id IS NOT NULL THEN 'Sales'
            WHEN delivery_staff_attributed_id IS NOT NULL THEN 'Delivery'
        END AS fb_attributed_dept --department that review was attributed
    FROM ergeon.feedback_review review
    LEFT JOIN ergeon.hrm_staff sales ON sales.id = review.sales_staff_attributed_id --join with sales attributed id
    LEFT JOIN ergeon.hrm_staff delivery ON delivery.id = review.delivery_staff_attributed_id --join with delivery attributed id
    LEFT JOIN ergeon.core_user sales_user ON sales_user.id = sales.user_id
    LEFT JOIN ergeon.core_user delivery_user ON delivery_user.id = delivery.user_id
    WHERE
        order_id IS NOT NULL
        AND (sales_staff_attributed_id IS NOT NULL OR delivery_staff_attributed_id IS NOT NULL)
    QUALIFY RANK() OVER(PARTITION BY order_id ORDER BY posted_at, review.id) = 1
),

all_channel_reviews AS ( --feedback attributes per order
    SELECT
        feedback_attributes.order_id,
        AVG(fb_yelp_score) AS fb_yelp_score,
        MAX(yelp_recommended) AS yelp_recommended,
        MIN(fb_yelp_posted_at) AS fb_yelp_posted_at,
        MAX(yelp_location) AS yelp_location,
        SUM(yelp_recommended) AS count_yelp_recommended,
        COUNT(fb_yelp_posted_at) AS count_yelp_reviews,
        AVG(fb_google_score) AS fb_google_score,
        MIN(fb_google_posted_at) AS fb_google_posted_at,
        MAX(google_location) AS google_location,
        COUNT(fb_google_posted_at) AS count_google_reviews,
        AVG(fb_thumbtack_score) AS fb_thumbtack_score,
        MIN(fb_thumbtack_posted_at) AS fb_thumbtack_posted_at,
        COUNT(fb_thumbtack_posted_at) AS count_thumbtack_reviews,
        AVG(fb_bbb_score) AS fb_bbb_score,
        MIN(fb_bbb_posted_at) AS fb_bbb_posted_at,
        COUNT(fb_bbb_posted_at) AS count_bbb_reviews,
        AVG(fb_ha_score) AS fb_ha_score,
        MIN(fb_ha_posted_at) AS fb_ha_posted_at,
        AVG(fb_ha_customer_service_score) AS fb_ha_customer_service_score,
        AVG(fb_ha_value_for_money_score) AS fb_ha_value_for_money_score,
        MAX(ha_location) AS ha_location,
        COUNT(fb_ha_posted_at) AS count_ha_reviews
    FROM feedback_attributes
    LEFT JOIN yelp_reviews USING (id)
    LEFT JOIN google_reviews USING (id)
    LEFT JOIN thumbtack_reviews USING (id)
    LEFT JOIN bbb_reviews USING (id)
    LEFT JOIN ha_reviews USING (id)
    GROUP BY 1
),

all_channel_reviews_feedback AS (
    SELECT
        *
    FROM all_channel_reviews
    FULL OUTER JOIN internal_feedback USING (order_id)
    LEFT JOIN staff_review_attributed USING (order_id)
),

first_posted_date AS ( --find the first channel and date in case we have multiple channel reviews per order
    SELECT
        order_id,
        MIN(posted_at) AS fb_first_posted_at
    FROM all_channel_reviews_feedback
    --assign the date of the first channel that had a review
    UNPIVOT (posted_at FOR dates IN (fb_yelp_posted_at, fb_google_posted_at, fb_thumbtack_posted_at, fb_bbb_posted_at, fb_ha_posted_at))
    GROUP BY 1
)

SELECT
    *
FROM all_channel_reviews_feedback
LEFT JOIN first_posted_date USING (order_id)
