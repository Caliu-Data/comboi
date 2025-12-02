"""
Energy Bruin Transformation: Load Forecasting

This transformation performs energy load forecasting using:
- Historical consumption patterns
- Time series decomposition
- Seasonal and trend analysis
- Weather correlation (if available)

This analysis requires complex time series logic better suited for Python.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def transform(con, inputs):
    """
    Forecast energy load based on historical patterns.

    Args:
        con: DuckDB connection
        inputs: Dict with 'meter_readings' pointing to parquet file

    Returns:
        DataFrame with load forecasts per meter
    """
    # Load meter readings
    df = con.execute("SELECT * FROM meter_readings").df()

    # Convert timestamp and sort
    df['reading_timestamp'] = pd.to_datetime(df['reading_timestamp'])
    df = df.sort_values(['meter_id', 'reading_timestamp'])

    # Extract time features
    df['hour'] = df['reading_timestamp'].dt.hour
    df['day_of_week'] = df['reading_timestamp'].dt.dayofweek
    df['day_of_month'] = df['reading_timestamp'].dt.day
    df['month'] = df['reading_timestamp'].dt.month
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_business_hours'] = df['hour'].between(8, 18).astype(int)

    # Calculate rolling statistics per meter
    df['rolling_24h_avg'] = df.groupby('meter_id')['consumption_kwh'].transform(
        lambda x: x.rolling(window=24, min_periods=1).mean()
    )
    df['rolling_24h_std'] = df.groupby('meter_id')['consumption_kwh'].transform(
        lambda x: x.rolling(window=24, min_periods=1).std()
    )
    df['rolling_7d_avg'] = df.groupby('meter_id')['consumption_kwh'].transform(
        lambda x: x.rolling(window=168, min_periods=1).mean()  # 7 days * 24 hours
    )

    # Calculate hour-of-day patterns per meter
    hourly_patterns = df.groupby(['meter_id', 'hour'])['consumption_kwh'].mean().reset_index()
    hourly_patterns.columns = ['meter_id', 'hour', 'hourly_avg_consumption']

    df = df.merge(hourly_patterns, on=['meter_id', 'hour'], how='left')

    # Calculate day-of-week patterns
    dow_patterns = df.groupby(['meter_id', 'day_of_week'])['consumption_kwh'].mean().reset_index()
    dow_patterns.columns = ['meter_id', 'day_of_week', 'dow_avg_consumption']

    df = df.merge(dow_patterns, on=['meter_id', 'day_of_week'], how='left')

    # Simple forecasting model
    def calculate_forecast(row):
        """Calculate next hour forecast based on patterns."""
        base = row['hourly_avg_consumption']

        # Adjust for day of week
        dow_factor = row['dow_avg_consumption'] / (row['rolling_7d_avg'] + 0.001)

        # Adjust for recent trend
        trend = row['rolling_24h_avg'] - row['rolling_7d_avg']

        # Weekend adjustment
        if row['is_weekend']:
            base *= 0.85

        # Business hours adjustment
        if row['is_business_hours']:
            base *= 1.1

        forecast = base * dow_factor + (trend * 0.3)
        return max(0, forecast)

    df['forecasted_next_hour'] = df.apply(calculate_forecast, axis=1)

    # Calculate forecast confidence
    df['forecast_confidence'] = np.exp(-df['rolling_24h_std'] / (df['rolling_24h_avg'] + 0.001))
    df['forecast_confidence'] = df['forecast_confidence'].clip(0, 1)

    # Identify load type
    def classify_load_profile(group):
        """Classify meter load profile."""
        peak_hours = group.groupby('hour')['consumption_kwh'].mean()
        peak_hour = peak_hours.idxmax()

        avg_consumption = group['consumption_kwh'].mean()
        weekend_avg = group[group['is_weekend'] == 1]['consumption_kwh'].mean()
        weekday_avg = group[group['is_weekend'] == 0]['consumption_kwh'].mean()

        if peak_hour >= 18 and peak_hour <= 22:
            if weekday_avg > weekend_avg * 1.5:
                return 'residential'
            else:
                return 'residential_standard'
        elif peak_hour >= 9 and peak_hour <= 17:
            return 'commercial'
        elif avg_consumption > 50:
            return 'industrial'
        else:
            return 'mixed'

    load_profiles = df.groupby('meter_id').apply(classify_load_profile).reset_index()
    load_profiles.columns = ['meter_id', 'load_profile']

    df = df.merge(load_profiles, on='meter_id', how='left')

    # Calculate load factor (utilization efficiency)
    meter_load_factors = df.groupby('meter_id').agg({
        'consumption_kwh': ['mean', 'max']
    }).reset_index()
    meter_load_factors.columns = ['meter_id', 'avg_load', 'peak_load']
    meter_load_factors['load_factor'] = (
        meter_load_factors['avg_load'] / (meter_load_factors['peak_load'] + 0.001)
    )

    df = df.merge(meter_load_factors[['meter_id', 'load_factor']], on='meter_id', how='left')

    # Get most recent reading per meter for final output
    latest_readings = df.sort_values('reading_timestamp').groupby('meter_id').tail(1)

    result = latest_readings[[
        'meter_id',
        'reading_timestamp',
        'consumption_kwh',
        'forecasted_next_hour',
        'forecast_confidence',
        'load_profile',
        'load_factor',
        'rolling_24h_avg',
        'rolling_7d_avg',
        'hourly_avg_consumption'
    ]]

    return result
