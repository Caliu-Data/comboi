{{ config(
    materialized='table',
    tags=['silver', 'finance']
) }}

/*
Finance Silver Layer: Verified Transactions
- Cleanses raw transaction data
- Validates amounts and currencies
- Flags suspicious transactions
- Standardizes transaction types
*/

WITH bronze_transactions AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/finance/transactions.parquet')
),

cleansed AS (
    SELECT
        transaction_id,
        account_id,
        CAST(transaction_date AS TIMESTAMP) AS transaction_date,
        CAST(amount AS DECIMAL(18,2)) AS amount,
        UPPER(TRIM(currency)) AS currency,
        LOWER(TRIM(transaction_type)) AS transaction_type,
        LOWER(TRIM(status)) AS status,
        merchant_name,
        merchant_category,
        location,
        updated_at
    FROM bronze_transactions
    WHERE transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount IS NOT NULL
        AND transaction_date IS NOT NULL
),

with_fraud_score AS (
    SELECT
        *,
        -- Simple fraud score based on amount and merchant patterns
        CASE
            WHEN ABS(amount) > 10000 THEN 0.8
            WHEN ABS(amount) > 5000 AND merchant_category IN ('gambling', 'cryptocurrency') THEN 0.9
            WHEN merchant_name LIKE '%SUSPICIOUS%' THEN 0.95
            WHEN ABS(amount) > 1000 THEN 0.3
            ELSE 0.1
        END AS fraud_score
    FROM cleansed
)

SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    currency,
    transaction_type,
    status,
    fraud_score
FROM with_fraud_score
WHERE status IN ('pending', 'completed', 'failed', 'cancelled')
    AND transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment', 'refund')
    AND LENGTH(currency) = 3
