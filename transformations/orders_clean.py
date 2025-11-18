"""Bruin transformation: Clean orders data from bronze layer."""


def transform(con, inputs):
    """
    Transform bronze orders data into cleaned silver dataset.
    
    Args:
        con: DuckDB connection
        inputs: Dictionary mapping input aliases to parquet file paths
        
    Returns:
        SQL query string that selects the transformed data
    """
    return """
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
    """

