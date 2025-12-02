"""
Finance Bruin Transformation: ML-Based Fraud Detection

This transformation demonstrates complex fraud detection using Python that would be
difficult to implement in SQL alone. It:
- Calculates statistical features per account
- Detects anomalies in transaction patterns
- Uses simple ML-style logic for fraud scoring
"""

import pandas as pd
import numpy as np


def transform(con, inputs):
    """
    Advanced fraud detection using statistical analysis.

    Args:
        con: DuckDB connection
        inputs: Dict with 'verified_transactions' pointing to parquet file

    Returns:
        SQL query string with enhanced fraud scores
    """
    # Load transactions
    df = con.execute("SELECT * FROM verified_transactions").df()

    # Calculate account-level statistics
    account_stats = df.groupby('account_id').agg({
        'amount': ['mean', 'std', 'min', 'max', 'count'],
        'transaction_date': ['min', 'max']
    }).reset_index()

    account_stats.columns = [
        'account_id', 'avg_amount', 'std_amount', 'min_amount',
        'max_amount', 'txn_count', 'first_txn_date', 'last_txn_date'
    ]

    # Merge stats back to transactions
    df = df.merge(account_stats, on='account_id', how='left')

    # Enhanced fraud detection features
    df['amount_deviation'] = (df['amount'] - df['avg_amount']) / (df['std_amount'] + 1)
    df['is_outlier'] = np.abs(df['amount_deviation']) > 3
    df['is_high_velocity'] = df['txn_count'] > 100

    # Calculate time-based features
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['hour_of_day'] = df['transaction_date'].dt.hour
    df['is_unusual_hour'] = (df['hour_of_day'] < 6) | (df['hour_of_day'] > 23)

    # Enhanced fraud score calculation
    def calculate_fraud_score(row):
        score = row['fraud_score']  # Start with base score

        # Increase score for outliers
        if row['is_outlier']:
            score = min(1.0, score + 0.3)

        # Increase score for unusual hours
        if row['is_unusual_hour']:
            score = min(1.0, score + 0.1)

        # Increase score for very large amounts
        if abs(row['amount']) > row['max_amount'] * 0.9:
            score = min(1.0, score + 0.2)

        # High velocity accounts
        if row['is_high_velocity'] and abs(row['amount']) > row['avg_amount'] * 2:
            score = min(1.0, score + 0.15)

        return round(score, 3)

    df['enhanced_fraud_score'] = df.apply(calculate_fraud_score, axis=1)
    df['fraud_risk_level'] = pd.cut(
        df['enhanced_fraud_score'],
        bins=[0, 0.3, 0.7, 1.0],
        labels=['low', 'medium', 'high']
    )

    # Select final columns
    result = df[[
        'transaction_id',
        'account_id',
        'transaction_date',
        'amount',
        'currency',
        'transaction_type',
        'status',
        'enhanced_fraud_score',
        'fraud_risk_level',
        'amount_deviation',
        'is_outlier'
    ]]

    return result
