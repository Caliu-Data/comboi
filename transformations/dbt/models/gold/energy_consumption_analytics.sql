{{ config(
    materialized='table',
    tags=['gold', 'energy']
) }}

/*
Energy Gold Layer: Consumption Analytics
- Daily aggregated consumption metrics per meter
- Peak and off-peak usage analysis
- Efficiency scoring
- Anomaly reporting
*/

WITH meter_readings AS (
    SELECT * FROM read_parquet('{{ var("silver_base_path") }}/refined/energy_meter_readings.parquet')
),

-- Aggregate daily consumption
daily_consumption AS (
    SELECT
        meter_id,
        DATE(reading_timestamp) AS analysis_date,
        COUNT(*) AS reading_count,
        SUM(consumption_kwh) AS total_consumption_kwh,
        AVG(consumption_kwh) AS avg_consumption_kwh,
        MAX(consumption_kwh) AS peak_consumption_kwh,
        MIN(consumption_kwh) AS min_consumption_kwh,
        STDDEV(consumption_kwh) AS consumption_stddev,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
        AVG(CASE WHEN voltage IS NOT NULL THEN voltage END) AS avg_voltage,
        AVG(CASE WHEN power_factor IS NOT NULL THEN power_factor END) AS avg_power_factor
    FROM meter_readings
    GROUP BY meter_id, DATE(reading_timestamp)
),

-- Calculate peak hour
peak_hours AS (
    SELECT
        meter_id,
        DATE(reading_timestamp) AS analysis_date,
        EXTRACT(HOUR FROM reading_timestamp) AS hour_of_day,
        SUM(consumption_kwh) AS hourly_consumption
    FROM meter_readings
    GROUP BY meter_id, DATE(reading_timestamp), EXTRACT(HOUR FROM reading_timestamp)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY meter_id, DATE(reading_timestamp)
        ORDER BY SUM(consumption_kwh) DESC
    ) = 1
),

-- Calculate off-peak consumption (10 PM - 6 AM)
off_peak_consumption AS (
    SELECT
        meter_id,
        DATE(reading_timestamp) AS analysis_date,
        SUM(consumption_kwh) AS off_peak_consumption_kwh
    FROM meter_readings
    WHERE EXTRACT(HOUR FROM reading_timestamp) BETWEEN 22 AND 23
        OR EXTRACT(HOUR FROM reading_timestamp) BETWEEN 0 AND 6
    GROUP BY meter_id, DATE(reading_timestamp)
),

-- Combine all metrics
combined_metrics AS (
    SELECT
        dc.meter_id,
        dc.analysis_date,
        dc.total_consumption_kwh,
        dc.avg_consumption_kwh,
        dc.peak_consumption_kwh,
        ph.hour_of_day AS peak_hour,
        COALESCE(opc.off_peak_consumption_kwh, 0) AS off_peak_consumption_kwh,
        COALESCE(dc.consumption_stddev, 0) AS consumption_variance,
        dc.anomaly_count,
        -- Efficiency score calculation
        CASE
            WHEN dc.total_consumption_kwh = 0 THEN 100
            WHEN dc.anomaly_count > 5 THEN 30
            WHEN dc.consumption_stddev / NULLIF(dc.avg_consumption_kwh, 0) > 0.5 THEN 50
            WHEN COALESCE(opc.off_peak_consumption_kwh, 0) / NULLIF(dc.total_consumption_kwh, 0) > 0.4 THEN 85
            ELSE 70
        END AS efficiency_score
    FROM daily_consumption dc
    LEFT JOIN peak_hours ph
        ON dc.meter_id = ph.meter_id
        AND dc.analysis_date = ph.analysis_date
    LEFT JOIN off_peak_consumption opc
        ON dc.meter_id = opc.meter_id
        AND dc.analysis_date = opc.analysis_date
)

SELECT
    meter_id,
    analysis_date,
    total_consumption_kwh,
    avg_consumption_kwh,
    peak_consumption_kwh,
    peak_hour,
    off_peak_consumption_kwh,
    consumption_variance,
    anomaly_count,
    efficiency_score
FROM combined_metrics
WHERE total_consumption_kwh >= 0
