"""Bruin transformation: Clean customers data from bronze layer."""


def transform(con, inputs):
    """
    Transform bronze customers data into cleaned silver dataset.
    
    Args:
        con: DuckDB connection
        inputs: Dictionary mapping input aliases to parquet file paths
        
    Returns:
        SQL query string that selects the transformed data
    """
    return """
        SELECT
            customer_id,
            customer_name,
            email,
            created_at,
            updated_at
        FROM bronze_customers
        WHERE email IS NOT NULL
            AND customer_name IS NOT NULL
    """

