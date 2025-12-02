# Bruin Transformations

This directory contains Python-based transformation scripts using the Bruin framework.

## Creating a New Transformation

### 1. Create a Python File

Create a new file in this directory, e.g., `my_transformation.py`

### 2. Implement the Transform Function

Each transformation script must define a `transform(con, inputs)` function:

```python
"""My transformation: Brief description of what it does."""


def transform(con, inputs):
    """
    Transform data using DuckDB.

    Args:
        con: DuckDB connection object
        inputs: Dictionary mapping input aliases to file paths

    Returns:
        Either:
        - SQL query string (SELECT statement)
        - pandas DataFrame
    """
    # Option 1: Return SQL query
    return """
        SELECT
            column1,
            column2,
            UPPER(column3) AS column3_upper
        FROM input_alias
        WHERE column1 IS NOT NULL
    """

    # Option 2: Return pandas DataFrame
    # df = con.execute("SELECT * FROM input_alias").df()
    # df['new_column'] = df['old_column'] * 2
    # return df
```

### 3. Configure in transformations.yml

Add your transformation to `configs/transformations.yml`:

```yaml
silver:
  - name: my_transformation
    type: bruin  # Optional, defaults to bruin
    inputs:
      - alias: input_alias  # Used in your SQL/Python
        stage: bronze
        source_path: "source_name/table.parquet"
    quality_checks:
      - contract:my_transformation  # Optional: reference data contract
```

### 4. Test Locally

```bash
# Test your transformation
comboi run silver --config configs/your-config.yml

# Or test the full pipeline
comboi run all --config configs/your-config.yml
```

## Examples

### Simple SQL Transformation

```python
def transform(con, inputs):
    """Clean and filter orders data."""
    return """
        SELECT
            order_id,
            customer_id,
            order_date,
            order_total
        FROM bronze_orders
        WHERE order_total > 0
            AND status = 'completed'
    """
```

### Complex Python Transformation

```python
import pandas as pd


def transform(con, inputs):
    """Calculate customer lifetime value with complex logic."""
    # Load data
    df = con.execute("SELECT * FROM bronze_orders").df()

    # Complex transformations
    df['order_month'] = pd.to_datetime(df['order_date']).dt.to_period('M')
    customer_ltv = df.groupby('customer_id').agg({
        'order_total': 'sum',
        'order_id': 'count'
    }).reset_index()

    customer_ltv.columns = ['customer_id', 'lifetime_value', 'order_count']

    return customer_ltv
```

### Using Multiple Inputs

```python
def transform(con, inputs):
    """Join customers and orders."""
    return """
        SELECT
            c.customer_id,
            c.customer_name,
            COUNT(o.order_id) AS total_orders,
            SUM(o.order_total) AS total_spent
        FROM bronze_customers c
        LEFT JOIN bronze_orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name
    """
```

## Best Practices

1. **Keep it Simple**: Prefer SQL over Python when possible
2. **Use Descriptive Names**: Name functions and columns clearly
3. **Add Docstrings**: Document what your transformation does
4. **Filter Early**: Apply WHERE clauses to reduce data processing
5. **Test Incrementally**: Test with small datasets first
6. **Use Data Contracts**: Define and validate data quality expectations

## When to Use Bruin vs dbt

**Use Bruin (Python) for:**
- Complex business logic that's easier in Python
- Machine learning features or predictions
- Custom deduplication with Splink
- Integration with Python libraries (pandas, numpy, sklearn)
- Dynamic transformations based on configuration

**Use dbt (SQL) for:**
- Standard SQL transformations
- Simple data cleansing and filtering
- Aggregations and metrics
- When you want dbt's built-in testing framework
- When your team prefers SQL over Python

## Additional Resources

- [Bruin Documentation](https://github.com/bruin-data/bruin)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Data Contracts Guide](../contracts/README.md)
- [Configuration Guide](../configs/transformations.yml)
