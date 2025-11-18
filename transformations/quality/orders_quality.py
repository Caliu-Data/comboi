"""Bruin quality check: Validate orders data quality."""


def check(con, dataset_name: str) -> tuple[bool, str]:
    """
    Run quality checks on orders data.
    
    Args:
        con: DuckDB connection with the dataset loaded as a view
        dataset_name: Name of the dataset view
        
    Returns:
        Tuple of (passed: bool, message: str)
    """
    checks = []

    # Check 1: Row count > 0
    row_count_result = con.execute(
        f"SELECT COUNT(*) as cnt FROM {dataset_name}"
    ).fetchone()
    row_count = row_count_result[0] if row_count_result else 0
    checks.append(("Row count > 0", row_count > 0, f"Found {row_count} rows"))

    # Check 2: No duplicate order_ids
    duplicate_result = con.execute(
        f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT order_id, COUNT(*) as cnt
            FROM {dataset_name}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    dup_count = duplicate_result[0] if duplicate_result else 0
    checks.append(
        ("No duplicate order_ids", dup_count == 0, f"Found {dup_count} duplicates")
    )

    # Check 3: All required columns present
    columns_result = con.execute(
        f"DESCRIBE {dataset_name}"
    ).fetchall()
    column_names = {row[0] for row in columns_result}
    required_columns = {"order_id", "customer_id", "order_date", "order_total", "status"}
    missing_columns = required_columns - column_names
    checks.append(
        (
            "All required columns present",
            len(missing_columns) == 0,
            f"Missing columns: {missing_columns}" if missing_columns else "All columns present",
        )
    )

    # Check 4: No null order_ids
    null_order_id_result = con.execute(
        f"SELECT COUNT(*) as null_count FROM {dataset_name} WHERE order_id IS NULL"
    ).fetchone()
    null_count = null_order_id_result[0] if null_order_id_result else 0
    checks.append(
        ("No null order_ids", null_count == 0, f"Found {null_count} null order_ids")
    )

    # Check 5: order_total > 0 for all rows
    negative_total_result = con.execute(
        f"SELECT COUNT(*) as neg_count FROM {dataset_name} WHERE order_total <= 0"
    ).fetchone()
    neg_count = negative_total_result[0] if negative_total_result else 0
    checks.append(
        (
            "All order_totals > 0",
            neg_count == 0,
            f"Found {neg_count} rows with order_total <= 0",
        )
    )

    # Aggregate results
    passed_checks = [c for c in checks if c[1]]
    failed_checks = [c for c in checks if not c[1]]

    if failed_checks:
        messages = [f"{name}: {msg}" for name, _, msg in failed_checks]
        return False, "; ".join(messages)

    return True, f"All {len(checks)} checks passed"

