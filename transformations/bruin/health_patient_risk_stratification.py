"""
Health Bruin Transformation: Patient Risk Stratification

This transformation performs advanced patient risk stratification using:
- Comorbidity indexing (Charlson Comorbidity Index)
- Historical encounter patterns
- Age and clinical factors
- Predictive risk scoring

This type of analysis requires complex logic better suited for Python than SQL.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def transform(con, inputs):
    """
    Stratify patients by readmission and complication risk.

    Args:
        con: DuckDB connection
        inputs: Dict with 'patient_encounters' pointing to parquet file

    Returns:
        DataFrame with patient risk profiles
    """
    # Load encounter data
    df = con.execute("SELECT * FROM patient_encounters").df()

    # Calculate patient-level aggregates
    df['encounter_date'] = pd.to_datetime(df['encounter_date'])

    # Extract ICD-10 chapter (first letter) for comorbidity categories
    df['diagnosis_chapter'] = df['primary_diagnosis'].str[0]

    # Patient-level statistics
    patient_stats = df.groupby('patient_id').agg({
        'encounter_id': 'count',
        'encounter_date': ['min', 'max'],
        'total_charges': ['sum', 'mean', 'max'],
        'length_of_stay_days': ['mean', 'sum', 'max']
    }).reset_index()

    patient_stats.columns = [
        'patient_id', 'total_encounters', 'first_encounter_date',
        'last_encounter_date', 'total_charges_sum', 'avg_charges',
        'max_charges', 'avg_los', 'total_los_days', 'max_los'
    ]

    # Calculate comorbidity indicators based on ICD-10 chapters
    comorbidity_indicators = df.groupby('patient_id')['diagnosis_chapter'].apply(
        lambda x: x.value_counts().to_dict()
    ).reset_index()

    comorbidity_indicators = comorbidity_indicators.rename(
        columns={'diagnosis_chapter': 'diagnosis_distribution'}
    )

    # Merge stats and comorbidities
    patient_risk = patient_stats.merge(
        comorbidity_indicators, on='patient_id', how='left'
    )

    # Calculate days since last encounter
    today = pd.Timestamp.now()
    patient_risk['days_since_last_encounter'] = (
        today - pd.to_datetime(patient_risk['last_encounter_date'])
    ).dt.days

    # Calculate encounter frequency (encounters per month)
    patient_risk['days_active'] = (
        pd.to_datetime(patient_risk['last_encounter_date']) -
        pd.to_datetime(patient_risk['first_encounter_date'])
    ).dt.days + 1

    patient_risk['encounter_frequency'] = (
        patient_risk['total_encounters'] / (patient_risk['days_active'] / 30)
    ).fillna(0)

    # Charlson Comorbidity Index (simplified)
    def calculate_charlson_score(diagnosis_dist):
        if pd.isna(diagnosis_dist) or not diagnosis_dist:
            return 0

        score = 0
        # Cardiovascular: I codes
        if 'I' in diagnosis_dist:
            score += min(diagnosis_dist['I'], 3)
        # Respiratory: J codes
        if 'J' in diagnosis_dist:
            score += min(diagnosis_dist['J'], 2)
        # Kidney disease: N codes
        if 'N' in diagnosis_dist:
            score += min(diagnosis_dist['N'], 2)
        # Diabetes: E codes
        if 'E' in diagnosis_dist:
            score += min(diagnosis_dist['E'], 2)
        # Cancer: C codes
        if 'C' in diagnosis_dist:
            score += min(diagnosis_dist['C'], 4)

        return min(score, 10)  # Cap at 10

    patient_risk['charlson_score'] = patient_risk['diagnosis_distribution'].apply(
        calculate_charlson_score
    )

    # Calculate overall risk score (0-100)
    def calculate_risk_score(row):
        score = 0

        # High frequency encounters
        if row['encounter_frequency'] > 2:
            score += 25

        # High comorbidity burden
        if row['charlson_score'] >= 5:
            score += 30
        elif row['charlson_score'] >= 3:
            score += 20

        # High cost utilization
        if row['total_charges_sum'] > 100000:
            score += 20
        elif row['total_charges_sum'] > 50000:
            score += 10

        # Long length of stay
        if row['avg_los'] and row['avg_los'] > 7:
            score += 15

        # Recent high utilization
        if row['days_since_last_encounter'] < 30 and row['total_encounters'] > 3:
            score += 10

        return min(score, 100)

    patient_risk['risk_score'] = patient_risk.apply(calculate_risk_score, axis=1)

    # Assign risk categories
    patient_risk['risk_category'] = pd.cut(
        patient_risk['risk_score'],
        bins=[0, 30, 60, 100],
        labels=['low', 'medium', 'high'],
        include_lowest=True
    )

    # Select final columns
    result = patient_risk[[
        'patient_id',
        'total_encounters',
        'encounter_frequency',
        'charlson_score',
        'total_charges_sum',
        'avg_charges',
        'avg_los',
        'days_since_last_encounter',
        'risk_score',
        'risk_category'
    ]]

    return result
