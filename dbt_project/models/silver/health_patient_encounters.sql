{{ config(
    materialized='table',
    tags=['silver', 'health']
) }}

/*
Health Silver Layer: Patient Encounters
- Cleanses and validates encounter data
- Standardizes ICD-10 codes
- Validates clinical data quality
- Ensures HIPAA compliance patterns
*/

WITH bronze_encounters AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/health/encounters.parquet')
),

cleansed AS (
    SELECT
        encounter_id,
        patient_id,
        CAST(encounter_date AS DATE) AS encounter_date,
        LOWER(TRIM(encounter_type)) AS encounter_type,
        UPPER(TRIM(primary_diagnosis)) AS primary_diagnosis,
        CAST(length_of_stay_days AS INTEGER) AS length_of_stay_days,
        CAST(total_charges AS DECIMAL(18,2)) AS total_charges,
        LOWER(TRIM(discharge_status)) AS discharge_status,
        admitting_physician_id,
        facility_id,
        updated_at
    FROM bronze_encounters
    WHERE encounter_id IS NOT NULL
        AND patient_id IS NOT NULL
        AND encounter_date IS NOT NULL
        AND primary_diagnosis IS NOT NULL
),

validated AS (
    SELECT
        encounter_id,
        patient_id,
        encounter_date,
        encounter_type,
        primary_diagnosis,
        length_of_stay_days,
        total_charges,
        discharge_status
    FROM cleansed
    WHERE encounter_type IN ('inpatient', 'outpatient', 'emergency', 'telehealth', 'surgery')
        AND (discharge_status IS NULL OR discharge_status IN ('home', 'transferred', 'deceased', 'left_ama', 'hospice'))
        AND primary_diagnosis ~ '^[A-Z][0-9]{2}'  -- Basic ICD-10 format validation
        AND total_charges >= 0
        AND (length_of_stay_days IS NULL OR length_of_stay_days >= 0)
        -- Inpatient encounters must have length of stay
        AND NOT (encounter_type = 'inpatient' AND length_of_stay_days IS NULL)
)

SELECT * FROM validated
