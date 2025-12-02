{{ config(
    materialized='table',
    tags=['silver', 'energy']
) }}

/*
Energy Silver Layer: Smart Meter Readings
- Validates and cleanses meter reading data
- Detects anomalies in consumption patterns
- Normalizes voltage and power measurements
- Flags meter operational issues
*/

WITH bronze_readings AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/energy/meter_readings.parquet')
),

cleansed AS (
    SELECT
        reading_id,
        meter_id,
        CAST(reading_timestamp AS TIMESTAMP) AS reading_timestamp,
        CAST(consumption_kwh AS DECIMAL(10,3)) AS consumption_kwh,
        CAST(voltage AS DECIMAL(6,2)) AS voltage,
        CAST(power_factor AS DECIMAL(4,3)) AS power_factor,
        LOWER(TRIM(meter_status)) AS meter_status,
        temperature,
        location_id
    FROM bronze_readings
    WHERE reading_id IS NOT NULL
        AND meter_id IS NOT NULL
        AND reading_timestamp IS NOT NULL
        AND consumption_kwh IS NOT NULL
),

-- Calculate rolling statistics for anomaly detection
with_statistics AS (
    SELECT
        *,
        AVG(consumption_kwh) OVER (
            PARTITION BY meter_id
            ORDER BY reading_timestamp
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS avg_24h_consumption,
        STDDEV(consumption_kwh) OVER (
            PARTITION BY meter_id
            ORDER BY reading_timestamp
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS stddev_24h_consumption
    FROM cleansed
),

-- Detect anomalies
with_anomaly_detection AS (
    SELECT
        reading_id,
        meter_id,
        reading_timestamp,
        consumption_kwh,
        voltage,
        power_factor,
        meter_status,
        -- Anomaly detection logic
        CASE
            WHEN consumption_kwh > (avg_24h_consumption + 3 * stddev_24h_consumption) THEN true
            WHEN consumption_kwh = 0 AND avg_24h_consumption > 1 THEN true
            WHEN voltage IS NOT NULL AND (voltage < 110 OR voltage > 250) THEN true
            WHEN meter_status IN ('error', 'offline') THEN true
            ELSE false
        END AS is_anomaly
    FROM with_statistics
)

SELECT
    reading_id,
    meter_id,
    reading_timestamp,
    consumption_kwh,
    voltage,
    power_factor,
    meter_status,
    is_anomaly
FROM with_anomaly_detection
WHERE consumption_kwh >= 0
    AND consumption_kwh <= 1000
    AND meter_status IN ('normal', 'warning', 'error', 'offline')
    AND (voltage IS NULL OR voltage BETWEEN 100 AND 300)
    AND (power_factor IS NULL OR power_factor BETWEEN 0 AND 1)
