{{ config(
    materialized='table',
    tags=['gold', 'health']
) }}

/*
Health Gold Layer: 30-Day Readmission Analysis
- Identifies readmissions within 30 days
- Calculates readmission rates by diagnosis
- Supports quality reporting (CMS metrics)
- Enables readmission risk stratification
*/

WITH encounters AS (
    SELECT * FROM read_parquet('{{ var("silver_base_path") }}/refined/health_patient_encounters.parquet')
),

-- Get only completed inpatient stays
index_admissions AS (
    SELECT
        encounter_id,
        patient_id,
        encounter_date,
        primary_diagnosis,
        length_of_stay_days,
        discharge_status,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY encounter_date) AS admission_sequence
    FROM encounters
    WHERE encounter_type = 'inpatient'
        AND discharge_status IN ('home', 'transferred')  -- Exclude deaths and AMA
),

-- Find readmissions within 30 days
readmissions AS (
    SELECT
        idx.encounter_id AS index_encounter_id,
        idx.patient_id,
        idx.encounter_date AS index_date,
        idx.primary_diagnosis AS index_diagnosis,
        idx.length_of_stay_days AS index_los,
        readm.encounter_id AS readmission_encounter_id,
        readm.encounter_date AS readmission_date,
        readm.primary_diagnosis AS readmission_diagnosis,
        DATEDIFF('day', idx.encounter_date, readm.encounter_date) AS days_to_readmission
    FROM index_admissions idx
    LEFT JOIN index_admissions readm
        ON idx.patient_id = readm.patient_id
        AND readm.admission_sequence = idx.admission_sequence + 1
        AND DATEDIFF('day', idx.encounter_date, readm.encounter_date) BETWEEN 1 AND 30
),

-- Calculate risk scores based on various factors
with_risk_score AS (
    SELECT
        patient_id,
        index_encounter_id,
        index_date,
        readmission_encounter_id,
        readmission_date,
        days_to_readmission,
        CASE
            WHEN readmission_encounter_id IS NOT NULL THEN true
            ELSE false
        END AS is_30day_readmission,
        index_diagnosis,
        readmission_diagnosis,
        -- Simple risk scoring model
        CASE
            WHEN index_los > 10 THEN 0.7
            WHEN index_los > 5 THEN 0.5
            WHEN index_diagnosis LIKE 'I%' THEN 0.6  -- Cardiovascular
            WHEN index_diagnosis LIKE 'J%' THEN 0.55  -- Respiratory
            ELSE 0.3
        END AS risk_score
    FROM readmissions
)

SELECT
    patient_id,
    index_encounter_id,
    index_date,
    readmission_encounter_id,
    readmission_date,
    days_to_readmission,
    is_30day_readmission,
    index_diagnosis,
    readmission_diagnosis,
    risk_score
FROM with_risk_score
