"""Bruin transformation: Calculate customer-level metrics."""


def transform(con, inputs):
    """
    Calculate customer lifetime value and order metrics.
    
    Args:
        con: DuckDB connection
        inputs: Dictionary mapping input aliases to parquet file paths
        
    Returns:
        SQL query string that selects the customer metrics
    """
    return """
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            COUNT(o.order_id) AS total_orders,
            COALESCE(SUM(o.order_total), 0) AS lifetime_value,
            MAX(o.order_date) AS last_order_date
        FROM customers_clean c
        LEFT JOIN orders_clean o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_name, c.email
    """

