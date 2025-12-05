# DuckDB SQL Transformations

This directory contains DuckDB SQL transformations that are automatically generated from data contracts.

## Contract-Driven SQL Transformations

All transformations in Comboi are **declarative** and **contract-driven**. Instead of writing SQL manually, you:

1. **Define a data contract** in `transformations/contracts/` specifying the desired output schema
2. **Generate SQL transformations** automatically using `build.py --generate-sql`
3. **Execute transformations** via the pipeline

This approach ensures:
- ✅ Schema consistency between contracts and transformations
- ✅ Automatic validation logic generation
- ✅ Type-safe transformations
- ✅ Reduced manual coding errors

## Transformation Structure

Each transformation is a DuckDB SQL file that:
- Reads from input sources (Bronze or Silver layers)
- Applies transformations based on the contract schema
- Returns data matching the contract definition

### Example: `gdpr_customers.sql`

```sql
-- Generated from contract: gdpr_customers.yml
-- This transformation is automatically derived from the contract schema

SELECT
    customer_id,
    SHA2(email, 256) AS email_hash,  -- Pseudonymize PII
    UPPER(SUBSTR(country, 1, 2)) AS country_code,
    consent_marketing,
    consent_analytics,
    consent_timestamp,
    data_subject_rights_exercised,
    retention_category,
    created_at,
    updated_at,
    gdpr_delete_after
FROM bronze_customers
WHERE customer_id IS NOT NULL  -- Contract constraint: not_null
```

## Generating SQL from Contracts

Use `build.py` to generate SQL transformations from contracts:

```bash
# Generate SQL transformation from contract
python build.py \
  --generate-sql \
  --contract transformations/contracts/gdpr_customers.yml \
  --output transformations/sql/gdpr_customers.sql
```

The generated SQL will:
- Select columns defined in the contract schema
- Apply type conversions based on contract types
- Implement constraints as WHERE clauses
- Add data quality transformations

## Manual SQL Transformations

While contracts generate basic transformations, you can manually edit the SQL for:
- Complex business logic
- Aggregations
- Joins across multiple sources
- Window functions
- Advanced DuckDB features

**Important**: After manual edits, ensure the output still matches the contract schema.

## Available SQL Features

Since transformations run in DuckDB, you have access to:

### Data Types
- VARCHAR, INTEGER, BIGINT, DOUBLE, DECIMAL
- DATE, TIMESTAMP, TIME
- BOOLEAN, BLOB, UUID
- Arrays, Structs, Maps

### Functions
- String: `UPPER()`, `LOWER()`, `SUBSTR()`, `REGEXP_MATCHES()`
- Cryptographic: `SHA2()`, `MD5()`
- Date/Time: `DATE_TRUNC()`, `DATE_DIFF()`, `CURRENT_TIMESTAMP`
- Aggregation: `SUM()`, `AVG()`, `COUNT()`, `MIN()`, `MAX()`
- Window: `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`

### Advanced Features
- CTEs (Common Table Expressions)
- Window functions
- JSON functions
- Regular expressions
- Statistical functions

## Testing SQL Transformations

Test your SQL transformations locally:

```bash
# Test a transformation in DuckDB
duckdb << EOF
CREATE VIEW bronze_customers AS SELECT * FROM read_parquet('data/bronze/crm/customers.parquet');
.read transformations/sql/gdpr_customers.sql
SELECT * FROM result LIMIT 10;
EOF
```

## Configuration

Reference SQL transformations in `configs/transformations.yml`:

```yaml
silver:
  - name: gdpr_customers
    type: sql
    inputs:
      - alias: bronze_customers
        stage: bronze
        source_path: "crm/customers.parquet"
    quality_checks:
      - contract:gdpr_customers
```

## Best Practices

1. **Always generate from contracts first**: Use `build.py --generate-sql` as a starting point
2. **Keep it simple**: Prefer declarative SQL over complex logic
3. **Document complex logic**: Add comments for non-obvious transformations
4. **Test incrementally**: Test each transformation independently
5. **Maintain contract alignment**: Ensure SQL output matches contract schema

## Example Workflows

### Simple Cleansing
```sql
-- Simple field selection and cleaning
SELECT
    id,
    TRIM(UPPER(name)) AS name,
    valid_email AS email
FROM bronze_table
WHERE id IS NOT NULL
```

### GDPR Pseudonymization
```sql
-- Pseudonymize PII fields
SELECT
    customer_id,
    SHA2(email, 256) AS email_hash,
    SHA2(phone, 256) AS phone_hash,
    -- Keep non-PII fields
    country_code,
    signup_date
FROM bronze_customers
```

### Aggregations (Gold Layer)
```sql
-- Aggregate metrics
SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_spent,
    AVG(amount) AS avg_order_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM silver_orders
GROUP BY customer_id
HAVING COUNT(*) > 0
```

### Temporal Filtering
```sql
-- Apply retention policy
SELECT *
FROM bronze_data
WHERE
    created_at >= CURRENT_DATE - INTERVAL '7 years'
    AND (deleted_at IS NULL OR deleted_at >= CURRENT_DATE - INTERVAL '30 days')
```

## Troubleshooting

**SQL syntax errors:**
```bash
# Validate SQL syntax
duckdb -c ".read transformations/sql/my_transformation.sql"
```

**Schema mismatches:**
```bash
# Compare SQL output with contract
python build.py --validate-sql \
  --contract transformations/contracts/my_contract.yml \
  --sql transformations/sql/my_transformation.sql
```

**Performance issues:**
```bash
# Use EXPLAIN to analyze query plan
duckdb -c "EXPLAIN SELECT * FROM ..."
```

## See Also

- [Data Contracts Documentation](../contracts/README.md)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Pipeline Configuration](../../configs/README.md)
