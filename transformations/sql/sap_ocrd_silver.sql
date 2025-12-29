-- Generated from contract: sap_ocrd_silver
-- Description: GDPR-compliant SAP Business Partners with cleansing and validation
-- Stage: Silver
-- Input: Bronze layer OCRD (already GDPR-processed by connector)
-- Output: Silver layer with deduplication, normalization, and contract validation
--
-- This transformation:
-- - Reads from Bronze OCRD (CardName already pseudonymized as CardName_Hash)
-- - Normalizes data types and values (uppercase CardType, Currency)
-- - Applies default values for null fields
-- - Validates SHA-256 hash format (64 characters)
-- - Deduplicates based on CardCode, keeping most recent
-- - Prepares data for Silver layer consumption

SELECT
    -- Primary key
    CardCode,

    -- GDPR-compliant pseudonymized field
    CardName_Hash,  -- Already pseudonymized in Bronze layer (SHA-256)

    -- Normalized partner classification
    UPPER(TRIM(CardType)) AS CardType,  -- Normalize to uppercase: C, S, L

    -- Partner attributes
    GroupCode,
    Territory,
    SlpCode,
    UPPER(TRIM(Currency)) AS Currency,  -- Normalize currency codes (USD, EUR, etc.)

    -- Timestamps
    CreateDate,
    UpdateDate,

    -- Financial attributes with defaults
    COALESCE(Balance, 0.0) AS Balance,  -- Default null balances to 0
    COALESCE(CreditLine, 0.0) AS CreditLine,  -- Default null credit lines to 0

    -- Status attributes with defaults
    COALESCE(frozenFor, 'N') AS frozenFor,  -- Default to not frozen
    FrozenFrom,
    FrozenTo,

    -- Account attributes
    COALESCE(DebPayAcct, '') AS DebPayAcct,
    COALESCE(Discount, 0.0) AS Discount,

    -- Validity dates
    ValidFrom,
    ValidTo

FROM bronze_ocrd

WHERE
    -- Filter out invalid records
    CardCode IS NOT NULL
    AND CardCode != ''
    AND CardName_Hash IS NOT NULL
    AND LENGTH(CardName_Hash) = 64  -- Validate SHA-256 hash (GDPR compliance check)
    AND CardType IN ('C', 'S', 'L')  -- Valid partner types only

-- Remove duplicates: Keep most recent record per CardCode
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CardCode
    ORDER BY
        UpdateDate DESC NULLS LAST,
        CreateDate DESC NULLS LAST
) = 1
