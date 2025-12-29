-- Pass-through transformation for SAP AP Invoice Lines
-- Description: Simple pass-through from Bronze to Silver
-- Stage: Silver
-- Input: Bronze PCH1 (already GDPR-compliant, no PII in transaction tables)
-- Output: Silver PCH1 for use in Gold project profitability calculations
--
-- Note: This is a simple pass-through because:
-- 1. PCH1 contains no PII (only cost transaction line items)
-- 2. GDPR processing was already applied in Bronze layer
-- 3. No additional cleansing needed for this table

SELECT * FROM bronze_pch1
