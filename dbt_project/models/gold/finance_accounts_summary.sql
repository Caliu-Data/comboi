{{ config(
    materialized='table',
    tags=['gold', 'finance']
) }}

/*
Finance Gold Layer: Account Summary Metrics
- Aggregates transaction activity by account
- Calculates balances and transaction counts
- Identifies high-risk accounts
- Provides account-level analytics
*/

WITH verified_transactions AS (
    SELECT * FROM read_parquet('{{ var("silver_base_path") }}/refined/finance_transactions_verified.parquet')
),

account_aggregates AS (
    SELECT
        account_id,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_withdrawals,
        SUM(amount) AS current_balance,
        COUNT(*) AS transaction_count,
        AVG(amount) AS avg_transaction_amount,
        COUNT(CASE WHEN fraud_score > 0.7 THEN 1 END) AS high_risk_transaction_count,
        MAX(transaction_date) AS last_transaction_date
    FROM verified_transactions
    WHERE status = 'completed'
    GROUP BY account_id
),

-- Join with account master data (if available in bronze)
with_account_type AS (
    SELECT
        a.*,
        'checking' AS account_type  -- Default, would join from bronze account master
    FROM account_aggregates a
)

SELECT
    account_id,
    account_type,
    current_balance,
    total_deposits,
    total_withdrawals,
    transaction_count,
    avg_transaction_amount,
    high_risk_transaction_count
FROM with_account_type
WHERE transaction_count > 0
