"""Bruin transformation: Calculate daily sales metrics."""


def transform(con, inputs):
    """
    Aggregate daily sales metrics from cleaned orders.
    
    Args:
        con: DuckDB connection
        inputs: Dictionary mapping input aliases to parquet file paths
        
    Returns:
        SQL query string that selects the aggregated metrics
    """
    return """
        SELECT
            order_date,
            SUM(order_total) AS total_revenue,
            COUNT(*) AS orders_count,
            AVG(order_total) AS avg_order_value
        FROM orders_clean
        GROUP BY order_date
        ORDER BY order_date DESC
    """

