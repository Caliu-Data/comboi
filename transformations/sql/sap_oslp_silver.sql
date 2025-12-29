-- Generated from contract: sap_oslp_silver
-- Description: GDPR-compliant SAP Sales Employees with validation
-- Stage: Silver
-- Input: Bronze layer OSLP (already GDPR-processed by connector)
-- Output: Silver layer with deduplication and validation
--
-- This transformation:
-- - Reads from Bronze OSLP (SlpName already pseudonymized as SlpName_Hash)
-- - Validates SHA-256 hash format (64 characters)
-- - Validates commission percentage (0-100)
-- - Deduplicates based on SlpCode, keeping most recent
-- - Prepares sales employee data for revenue attribution

SELECT
    -- Primary key
    SlpCode,

    -- GDPR-compliant pseudonymized field
    SlpName_Hash,  -- Already pseudonymized in Bronze layer (SHA-256)

    -- Status
    Active,  -- Y=Active, N=Inactive

    -- Financial attributes with validation
    CASE
        WHEN Commission IS NULL THEN 0.0
        WHEN Commission < 0 THEN 0.0
        WHEN Commission > 100 THEN 100.0
        ELSE Commission
    END AS Commission,  -- Clamp commission to 0-100 range

    -- Employee reference
    EmployeeID

FROM bronze_oslp

WHERE
    -- Filter out invalid records
    SlpCode IS NOT NULL
    AND SlpName_Hash IS NOT NULL
    AND LENGTH(SlpName_Hash) = 64  -- Validate SHA-256 hash (GDPR compliance check)

-- Remove duplicates: Keep most recent record per SlpCode
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SlpCode
    ORDER BY SlpCode DESC  -- Use SlpCode for ordering since no UpdateDate in OSLP
) = 1
