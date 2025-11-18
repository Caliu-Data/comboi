"""Bruin quality check: Validate customers data quality."""


def check(con, dataset_name: str) -> tuple[bool, str]:
    """
    Run quality checks on customers data.
    
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

    # Check 2: No duplicate customer_ids
    duplicate_result = con.execute(
        f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT customer_id, COUNT(*) as cnt
            FROM {dataset_name}
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    dup_count = duplicate_result[0] if duplicate_result else 0
    checks.append(
        ("No duplicate customer_ids", dup_count == 0, f"Found {dup_count} duplicates")
    )

    # Check 3: All required columns present
    columns_result = con.execute(
        f"DESCRIBE {dataset_name}"
    ).fetchall()
    column_names = {row[0] for row in columns_result}
    required_columns = {"customer_id", "customer_name", "email"}
    missing_columns = required_columns - column_names
    checks.append(
        (
            "All required columns present",
            len(missing_columns) == 0,
            f"Missing columns: {missing_columns}" if missing_columns else "All columns present",
        )
    )

    # Check 4: No null customer_ids
    null_customer_id_result = con.execute(
        f"SELECT COUNT(*) as null_count FROM {dataset_name} WHERE customer_id IS NULL"
    ).fetchone()
    null_count = null_customer_id_result[0] if null_customer_id_result else 0
    checks.append(
        ("No null customer_ids", null_count == 0, f"Found {null_count} null customer_ids")
    )

    # Check 5: Valid email format (basic check)
    invalid_email_result = con.execute(
        f"""
        SELECT COUNT(*) as invalid_count
        FROM {dataset_name}
        WHERE email IS NOT NULL
          AND (email NOT LIKE '%@%.%' OR email LIKE '@%' OR email LIKE '%@')
        """
    ).fetchone()
    invalid_count = invalid_email_result[0] if invalid_email_result else 0
    checks.append(
        (
            "Valid email format",
            invalid_count == 0,
            f"Found {invalid_count} rows with invalid email format",
        )
    )

    # Aggregate results
    passed_checks = [c for c in checks if c[1]]
    failed_checks = [c for c in checks if not c[1]]

    if failed_checks:
        messages = [f"{name}: {msg}" for name, _, msg in failed_checks]
        return False, "; ".join(messages)

    return True, f"All {len(checks)} checks passed"

