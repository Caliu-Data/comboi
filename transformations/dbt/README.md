# dbt-duckdb Transformations

This directory contains dbt models for transforming data in the Comboi medallion architecture using [dbt-duckdb](https://github.com/duckdb/dbt-duckdb).

## Structure

```
dbt_project/
├── dbt_project.yml          # dbt project configuration
├── profiles.yml.template    # Connection profile template
├── models/
│   ├── silver/              # Silver layer models (data cleansing)
│   │   ├── schema.yml       # Model definitions and tests
│   │   ├── orders_clean.sql # Example: Clean orders data
│   │   └── customers_clean.sql
│   └── gold/                # Gold layer models (aggregations)
│       ├── schema.yml       # Model definitions and tests
│       ├── daily_sales.sql  # Example: Daily sales metrics
│       └── customer_metrics.sql
```

## Getting Started

### 1. Configure dbt Profile

The dbt runner automatically configures profiles for you, but you can also run dbt manually:

```bash
# Copy the template
cp profiles.yml.template profiles.yml

# Set environment variables if needed
export AZURE_STORAGE_CONNECTION_STRING="your_connection_string"
```

### 2. Create dbt Models

Create SQL models in the appropriate directory:

**Silver Layer** (`models/silver/`):
- Clean and standardize data from bronze layer
- Apply business rules and filters
- Deduplicate records

**Gold Layer** (`models/gold/`):
- Create aggregations and metrics
- Join multiple silver datasets
- Generate business-ready analytics

### 3. Model Example

```sql
-- models/silver/orders_clean.sql
{{ config(
    materialized='table',
    tags=['silver']
) }}

WITH bronze_orders AS (
    SELECT * FROM read_parquet('{{ var("bronze_base_path") }}/sales_azure/orders.parquet')
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_total,
    status,
    updated_at
FROM bronze_orders
WHERE status IS NOT NULL
    AND order_total > 0
```

### 4. Using dbt in Comboi

Configure transformations in `configs/transformations.yml`:

```yaml
silver:
  - name: orders_clean
    type: dbt  # Specify dbt transformation
    model: orders_clean  # dbt model name
    inputs:
      - alias: bronze_orders
        stage: bronze
        source_path: "sales_azure/orders.parquet"
```

## Running dbt Models

### Via Comboi CLI

```bash
# Run all transformations (both bruin and dbt)
comboi run silver --config configs/my-env.yml

# Run specific stage
comboi run gold --config configs/my-env.yml
```

### Standalone dbt Commands

```bash
cd dbt_project

# Run all models
dbt run

# Run specific model
dbt run --select orders_clean

# Run tests
dbt test

# Run silver models only
dbt run --select tag:silver

# Run gold models only
dbt run --select tag:gold
```

## dbt Features

### Tests

Define tests in `schema.yml`:

```yaml
models:
  - name: orders_clean
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

Run tests:
```bash
dbt test
```

### Documentation

Generate and serve documentation:

```bash
dbt docs generate
dbt docs serve
```

### Sources

Define bronze layer sources in `models/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    tables:
      - name: orders
        meta:
          external_location: "{{ var('bronze_base_path') }}/sales_azure/orders.parquet"
```

Reference in models:
```sql
SELECT * FROM {{ source('bronze', 'orders') }}
```

## Best Practices

1. **Materialization**: Use `table` for silver/gold layers, `view` for intermediate transformations
2. **Modularity**: Break complex transformations into CTEs or intermediate models
3. **Testing**: Add tests for critical columns (uniqueness, not_null, relationships)
4. **Documentation**: Document models and columns in `schema.yml`
5. **Naming**: Use clear, descriptive names that indicate the layer (e.g., `stg_`, `fct_`, `dim_`)

## Combining bruin and dbt

You can mix bruin and dbt transformations in the same pipeline:

```yaml
silver:
  # Python-based bruin transformation
  - name: complex_ml_features
    type: bruin
    inputs: [...]

  # SQL-based dbt transformation
  - name: orders_clean
    type: dbt
    model: orders_clean
    inputs: [...]
```

Use bruin for:
- Complex Python logic
- Machine learning features
- Custom deduplication with Splink

Use dbt for:
- SQL-based transformations
- Standard data cleansing
- Aggregations and metrics
- Leveraging dbt's testing framework

## Troubleshooting

### DuckDB Extensions

If you get extension errors, ensure DuckDB can access the internet to download extensions, or pre-install them:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("INSTALL parquet")
```

### Azure Storage Access

For accessing ADLS via dbt, ensure `AZURE_STORAGE_CONNECTION_STRING` is set or configure in profiles.yml.

### Model Not Found

Verify:
- Model file exists in `models/` directory
- Model name in config matches SQL file name (without `.sql`)
- dbt_project.yml is properly configured
