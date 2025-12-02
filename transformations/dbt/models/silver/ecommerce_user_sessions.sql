{{ config(
    materialized='table',
    tags=['silver', 'ecommerce']
) }}

/*
Ecommerce Silver Layer: User Sessions
- Cleanses and validates session data
- Calculates session metrics (duration, engagement)
- Links sessions to conversions
- Standardizes device and traffic source data
*/

WITH bronze_sessions AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/ecommerce/sessions.parquet')
),

bronze_events AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/ecommerce/events.parquet')
),

-- Aggregate events per session
session_events AS (
    SELECT
        session_id,
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN product_id END) AS products_viewed,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_adds,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS conversion_flag
    FROM bronze_events
    GROUP BY session_id
),

-- Get purchase amounts
session_purchases AS (
    SELECT
        session_id,
        SUM(CAST(order_total AS DECIMAL(18,2))) AS total_revenue
    FROM bronze_events
    WHERE event_type = 'purchase'
    GROUP BY session_id
),

-- Cleanse session data
cleansed_sessions AS (
    SELECT
        s.session_id,
        s.user_id,
        CAST(s.session_start AS TIMESTAMP) AS session_start,
        CAST(s.session_end AS TIMESTAMP) AS session_end,
        CAST(EPOCH(s.session_end) - EPOCH(s.session_start) AS INTEGER) AS duration_seconds,
        LOWER(TRIM(s.device_type)) AS device_type,
        LOWER(TRIM(s.traffic_source)) AS traffic_source,
        s.landing_page,
        s.exit_page
    FROM bronze_sessions s
    WHERE s.session_id IS NOT NULL
        AND s.session_start IS NOT NULL
        AND s.session_end IS NOT NULL
        AND s.session_end >= s.session_start
),

-- Combine all metrics
final_sessions AS (
    SELECT
        cs.session_id,
        cs.user_id,
        cs.session_start,
        cs.session_end,
        cs.duration_seconds,
        COALESCE(se.page_views, 0) AS page_views,
        COALESCE(se.products_viewed, 0) AS products_viewed,
        COALESCE(se.cart_adds, 0) AS cart_adds,
        COALESCE(se.conversion_flag, 0)::BOOLEAN AS conversion_flag,
        sp.total_revenue,
        cs.device_type,
        cs.traffic_source
    FROM cleansed_sessions cs
    LEFT JOIN session_events se ON cs.session_id = se.session_id
    LEFT JOIN session_purchases sp ON cs.session_id = sp.session_id
)

SELECT
    session_id,
    user_id,
    session_start,
    session_end,
    duration_seconds,
    page_views,
    products_viewed,
    cart_adds,
    conversion_flag,
    total_revenue,
    device_type,
    traffic_source
FROM final_sessions
WHERE duration_seconds >= 0
    AND duration_seconds <= 86400
    AND page_views >= 1
    AND device_type IN ('mobile', 'tablet', 'desktop')
    AND traffic_source IN ('organic', 'paid', 'social', 'direct', 'email', 'referral')
    -- Ensure conversion logic is consistent
    AND NOT (conversion_flag = true AND (total_revenue IS NULL OR total_revenue = 0))
    AND NOT (conversion_flag = false AND total_revenue IS NOT NULL)
