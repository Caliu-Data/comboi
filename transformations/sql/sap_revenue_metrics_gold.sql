-- Generated from contract: sap_revenue_metrics_gold
-- Description: Revenue KPIs by customer, project, and sales employee
-- Stage: Gold
-- Inputs: Silver OINV, INV1, OCRD, OPRJ
-- Output: Daily revenue metrics with GDPR-compliant customer/project attribution
--
-- This transformation:
-- - Joins invoices (OINV/INV1) with customers (OCRD) and projects (OPRJ)
-- - Calculates revenue metrics by date, customer, project, sales employee
-- - Uses pseudonymized hashes for customer and project names (GDPR)
-- - Aggregates to daily grain with invoice counts and averages

WITH invoice_details AS (
    SELECT
        i.DocDate AS metric_date,
        i.CardCode AS customer_code,
        c.CardName_Hash AS customer_name_hash,
        COALESCE(l.Project, '') AS project_code,
        i.SlpCode AS sales_employee_code,
        i.DocTotal AS invoice_total,
        i.DocEntry AS invoice_id
    FROM silver_oinv i
    LEFT JOIN silver_ocrd c ON i.CardCode = c.CardCode
    LEFT JOIN silver_inv1 l ON i.DocEntry = l.DocEntry
    WHERE i.DocDate IS NOT NULL
),

project_enriched AS (
    SELECT
        id.*,
        p.PrjName_Hash AS project_name_hash
    FROM invoice_details id
    LEFT JOIN silver_oprj p ON id.project_code = p.PrjCode
)

SELECT
    metric_date,
    customer_code,
    customer_name_hash,
    project_code,
    project_name_hash,
    sales_employee_code,

    -- Revenue metrics
    SUM(invoice_total) AS total_revenue,
    COUNT(DISTINCT invoice_id) AS invoice_count,
    AVG(invoice_total) AS avg_invoice_value,

    -- Placeholder for future service breakdown
    NULL AS revenue_by_service

FROM project_enriched

GROUP BY
    metric_date,
    customer_code,
    customer_name_hash,
    project_code,
    project_name_hash,
    sales_employee_code

HAVING
    SUM(invoice_total) >= 0  -- Filter out negative revenue (quality check)

ORDER BY
    metric_date DESC,
    total_revenue DESC
