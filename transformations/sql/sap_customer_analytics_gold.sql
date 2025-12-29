-- Generated from contract: sap_customer_analytics_gold
-- Description: 360-degree customer analytics with GDPR compliance
-- Stage: Gold
-- Inputs: Silver OCRD, OINV
-- Output: Comprehensive customer view with financial and engagement metrics
--
-- This transformation:
-- - Joins customer master data (OCRD) with invoice history (OINV)
-- - Calculates lifetime revenue, credit utilization, engagement metrics
-- - Uses pseudonymized customer names (GDPR-compliant)
-- - Provides 360-degree customer view for analytics

WITH customer_revenue AS (
    SELECT
        CardCode,
        SUM(DocTotal) AS lifetime_revenue,
        COUNT(*) AS total_invoices
    FROM silver_oinv
    WHERE DocTotal IS NOT NULL
    GROUP BY CardCode
),

last_invoice AS (
    SELECT
        CardCode,
        MAX(DocDate) AS last_invoice_date
    FROM silver_oinv
    WHERE DocDate IS NOT NULL
    GROUP BY CardCode
)

SELECT
    -- Customer identification (GDPR-compliant)
    c.CardCode AS customer_code,
    c.CardName_Hash AS customer_name_hash,
    c.CardType AS customer_type,

    -- Financial metrics
    COALESCE(r.lifetime_revenue, 0.0) AS lifetime_revenue,
    c.Balance AS current_balance,
    c.CreditLine AS credit_limit,

    -- Credit utilization
    CASE
        WHEN c.CreditLine > 0 THEN (c.Balance / c.CreditLine * 100.0)
        ELSE 0.0
    END AS credit_utilization_pct,

    -- Engagement metrics
    CASE
        WHEN l.last_invoice_date IS NOT NULL THEN
            DATE_DIFF('day', l.last_invoice_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_last_invoice,

    -- Risk indicators
    (c.frozenFor = 'Y') AS is_frozen,

    -- Attribution
    c.SlpCode AS primary_sales_employee

FROM silver_ocrd c
LEFT JOIN customer_revenue r ON c.CardCode = r.CardCode
LEFT JOIN last_invoice l ON c.CardCode = l.CardCode

WHERE
    c.CardType = 'C'  -- Customers only (not suppliers or leads)

ORDER BY
    lifetime_revenue DESC NULLS LAST
