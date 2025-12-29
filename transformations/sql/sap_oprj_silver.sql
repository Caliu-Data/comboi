-- Generated from contract: sap_oprj_silver
-- Description: GDPR-compliant SAP Projects with validation
-- Stage: Silver
-- Input: Bronze layer OPRJ (already GDPR-processed by connector)
-- Output: Silver layer with deduplication and validation
--
-- This transformation:
-- - Reads from Bronze OPRJ (PrjName already pseudonymized as PrjName_Hash)
-- - Validates date ranges (ValidTo >= ValidFrom)
-- - Validates SHA-256 hash format (64 characters)
-- - Deduplicates based on PrjCode, keeping most recent
-- - Prepares project data for analytics and KPI calculation

SELECT
    -- Primary key
    PrjCode,

    -- GDPR-compliant pseudonymized field
    PrjName_Hash,  -- Already pseudonymized in Bronze layer (SHA-256)

    -- Related entities
    CardCode,  -- Associated business partner

    -- Status
    Active,  -- Y=Active, N=Inactive

    -- Project timeline
    ValidFrom,
    ValidTo,
    DueDate,
    ClosingDate,

    -- Timestamps
    CreateDate,
    UpdateDate,

    -- Financial period
    FinncPriod

FROM bronze_oprj

WHERE
    -- Filter out invalid records
    PrjCode IS NOT NULL
    AND PrjCode != ''
    AND PrjName_Hash IS NOT NULL
    AND LENGTH(PrjName_Hash) = 64  -- Validate SHA-256 hash (GDPR compliance check)

    -- Validate date ranges
    AND (ValidTo IS NULL OR ValidFrom IS NULL OR ValidTo >= ValidFrom)

    -- Validate closing date logic
    AND (ClosingDate IS NULL OR DueDate IS NULL OR ClosingDate >= ValidFrom)

-- Remove duplicates: Keep most recent record per PrjCode
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PrjCode
    ORDER BY
        UpdateDate DESC NULLS LAST,
        CreateDate DESC NULLS LAST
) = 1
