{{ config(
    materialized='table',
    tags=['gold', 'ecommerce']
) }}

/*
Ecommerce Gold Layer: Customer Lifetime Value
- Calculates CLV metrics per customer
- Segments customers by value and behavior
- Identifies at-risk and churned customers
- Provides customer intelligence for marketing
*/

WITH user_sessions AS (
    SELECT * FROM read_parquet('{{ var("silver_base_path") }}/refined/ecommerce_user_sessions.parquet')
),

-- Calculate user-level aggregates
user_aggregates AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) AS total_sessions,
        SUM(CASE WHEN conversion_flag THEN 1 ELSE 0 END) AS total_orders,
        SUM(COALESCE(total_revenue, 0)) AS total_revenue,
        MIN(CASE WHEN conversion_flag THEN session_start END) AS first_purchase_date,
        MAX(CASE WHEN conversion_flag THEN session_start END) AS last_purchase_date,
        SUM(page_views) AS total_page_views,
        SUM(products_viewed) AS total_products_viewed,
        MAX(device_type) AS preferred_device  -- Simplified, should use mode
    FROM user_sessions
    WHERE user_id IS NOT NULL
    GROUP BY user_id
    HAVING total_orders > 0  -- Only customers who have purchased
),

-- Calculate derived metrics
user_metrics AS (
    SELECT
        user_id,
        CAST(first_purchase_date AS DATE) AS first_purchase_date,
        CAST(last_purchase_date AS DATE) AS last_purchase_date,
        total_orders,
        total_revenue,
        total_revenue / NULLIF(total_orders, 0) AS average_order_value,
        total_sessions,
        total_orders::DECIMAL / NULLIF(total_sessions, 0) AS conversion_rate,
        DATE_DIFF('day', CAST(last_purchase_date AS DATE), CURRENT_DATE) AS days_since_last_purchase,
        preferred_device
    FROM user_aggregates
),

-- Calculate CLV and segment
with_clv AS (
    SELECT
        *,
        -- Simple CLV calculation: total_revenue * (1 + conversion_rate)
        -- More sophisticated models would use cohort analysis and predictive models
        total_revenue * (1 + COALESCE(conversion_rate, 0)) AS customer_lifetime_value,
        -- Customer segmentation logic
        CASE
            WHEN days_since_last_purchase > 180 THEN 'churned'
            WHEN days_since_last_purchase > 90 AND total_revenue > 500 THEN 'at_risk'
            WHEN total_revenue > 1000 AND total_orders > 5 THEN 'high_value'
            WHEN total_revenue > 300 OR total_orders > 3 THEN 'medium_value'
            ELSE 'low_value'
        END AS customer_segment
    FROM user_metrics
)

SELECT
    user_id,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    total_revenue,
    average_order_value,
    total_sessions,
    conversion_rate,
    days_since_last_purchase,
    customer_lifetime_value,
    customer_segment,
    preferred_device
FROM with_clv
WHERE total_orders > 0
    AND total_revenue >= 0
    AND conversion_rate BETWEEN 0 AND 1
    AND first_purchase_date <= last_purchase_date
