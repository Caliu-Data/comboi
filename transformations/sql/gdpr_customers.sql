-- Generated from contract: gdpr_customers
-- Description: GDPR-compliant customer data with privacy controls and data quality checks
--
-- This SQL transformation is automatically generated from the data contract schema.
-- Customize as needed for your business logic.

SELECT
    customer_id,
    SHA2(email, 256) AS email_hash,  -- Pseudonymize PII
    UPPER(SUBSTR(country, 1, 2)) AS country_code,  -- Normalize code
    consent_marketing,
    consent_analytics,
    consent_timestamp,
    data_subject_rights_exercised,
    retention_category,
    created_at,
    updated_at,
    gdpr_delete_after
FROM bronze_customers
WHERE
    customer_id IS NOT NULL
-- Remove duplicates based on primary key
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
