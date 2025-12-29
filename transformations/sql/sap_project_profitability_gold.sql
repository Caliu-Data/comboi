-- Generated from contract: sap_project_profitability_gold
-- Description: Project P&L analysis with revenue and cost tracking
-- Stage: Gold
-- Inputs: Silver OPRJ, INV1 (revenue), PCH1 (costs)
-- Output: Project profitability with gross margin calculations
--
-- This transformation:
-- - Joins project master (OPRJ) with revenue (INV1) and cost (PCH1) line items
-- - Calculates P&L metrics: revenue, cost, gross profit, margin percentage
-- - Uses pseudonymized project names (GDPR-compliant)
-- - Provides project-level profitability analysis for services company

WITH project_revenue AS (
    SELECT
        Project AS project_code,
        SUM(LineTotal) AS total_revenue
    FROM silver_inv1
    WHERE Project IS NOT NULL
        AND Project != ''
        AND LineTotal IS NOT NULL
    GROUP BY Project
),

project_cost AS (
    SELECT
        Project AS project_code,
        SUM(LineTotal) AS total_cost
    FROM silver_pch1
    WHERE Project IS NOT NULL
        AND Project != ''
        AND LineTotal IS NOT NULL
    GROUP BY Project
)

SELECT
    -- Project identification (GDPR-compliant)
    p.PrjCode AS project_code,
    p.PrjName_Hash AS project_name_hash,
    p.CardCode AS customer_code,

    -- Project status
    CASE
        WHEN p.Active = 'Y' THEN 'Active'
        ELSE 'Closed'
    END AS project_status,

    -- P&L metrics
    COALESCE(r.total_revenue, 0.0) AS total_revenue,
    COALESCE(c.total_cost, 0.0) AS total_cost,
    COALESCE(r.total_revenue, 0.0) - COALESCE(c.total_cost, 0.0) AS gross_profit,

    -- Profitability percentage
    CASE
        WHEN COALESCE(r.total_revenue, 0.0) > 0 THEN
            ((COALESCE(r.total_revenue, 0.0) - COALESCE(c.total_cost, 0.0)) / r.total_revenue * 100.0)
        ELSE 0.0
    END AS gross_margin_pct,

    -- Project timeline
    p.ValidFrom AS project_start_date,
    p.ValidTo AS project_end_date,
    CASE
        WHEN p.ValidFrom IS NOT NULL THEN
            DATE_DIFF('day', p.ValidFrom, COALESCE(p.ValidTo, CURRENT_DATE))
        ELSE NULL
    END AS duration_days

FROM silver_oprj p
LEFT JOIN project_revenue r ON p.PrjCode = r.project_code
LEFT JOIN project_cost c ON p.PrjCode = c.project_code

ORDER BY
    gross_profit DESC NULLS LAST
