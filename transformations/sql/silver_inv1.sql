-- Pass-through transformation for SAP AR Invoice Lines
-- Description: Simple pass-through from Bronze to Silver
-- Stage: Silver
-- Input: Bronze INV1 (already GDPR-compliant, no PII in transaction tables)
-- Output: Silver INV1 for use in Gold KPI calculations
--
-- Note: This is a simple pass-through because:
-- 1. INV1 contains no PII (only transaction line items)
-- 2. GDPR processing was already applied in Bronze layer
-- 3. No additional cleansing needed for this table

SELECT * FROM bronze_inv1
