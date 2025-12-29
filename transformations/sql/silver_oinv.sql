-- Pass-through transformation for SAP AR Invoice Headers
-- Description: Simple pass-through from Bronze to Silver
-- Stage: Silver
-- Input: Bronze OINV (already GDPR-compliant, no PII in transaction tables)
-- Output: Silver OINV for use in Gold KPI calculations
--
-- Note: This is a simple pass-through because:
-- 1. OINV contains no PII (only transaction data)
-- 2. GDPR processing was already applied in Bronze layer
-- 3. No additional cleansing needed for this table

SELECT * FROM bronze_oinv
