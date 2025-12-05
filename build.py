#!/usr/bin/env python3
"""
Contract Generator for Comboi

This script generates data contract YAML files from source schemas.
It can introspect Parquet files, CSV files, or database tables using DuckDB.

Usage:
    # From a Parquet file
    python build.py --source data/bronze/transactions.parquet --output transformations/contracts/transactions.yml --dataset transactions --stage bronze

    # From a CSV file
    python build.py --source data/raw/customers.csv --output transformations/contracts/customers.yml --dataset customers --stage bronze

    # From a database table via DuckDB connection
    python build.py --table "SELECT * FROM my_table LIMIT 1" --output transformations/contracts/my_table.yml --dataset my_table --stage silver

    # With additional metadata
    python build.py --source data.parquet --output contract.yml --dataset my_data --stage silver --owner my-team --description "My dataset description"
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional
import duckdb
import yaml


# Type mapping from DuckDB types to contract types
TYPE_MAPPING = {
    'BIGINT': 'BIGINT',
    'INTEGER': 'INTEGER',
    'SMALLINT': 'SMALLINT',
    'TINYINT': 'TINYINT',
    'DOUBLE': 'DOUBLE',
    'REAL': 'REAL',
    'DECIMAL': 'DECIMAL',
    'NUMERIC': 'DECIMAL',
    'VARCHAR': 'VARCHAR',
    'CHAR': 'VARCHAR',
    'TEXT': 'VARCHAR',
    'BOOLEAN': 'BOOLEAN',
    'DATE': 'DATE',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP',
    'TIME': 'TIME',
    'INTERVAL': 'INTERVAL',
    'BLOB': 'BLOB',
    'UUID': 'VARCHAR',
}


def map_duckdb_type(duckdb_type: str) -> str:
    """Map DuckDB type to contract type."""
    # Handle parametrized types like DECIMAL(18,2), VARCHAR(255)
    base_type = duckdb_type.split('(')[0].upper().strip()
    return TYPE_MAPPING.get(base_type, 'VARCHAR')


def introspect_schema(con: duckdb.DuckDBPyConnection, source: Optional[str] = None, query: Optional[str] = None) -> List[Dict]:
    """
    Introspect schema from a source using DuckDB.

    Args:
        con: DuckDB connection
        source: Path to Parquet/CSV file
        query: SQL query to introspect

    Returns:
        List of column definitions
    """
    if source:
        # Register the source as a table
        file_ext = Path(source).suffix.lower()
        if file_ext == '.parquet':
            con.execute(f"CREATE OR REPLACE VIEW temp_source AS SELECT * FROM read_parquet('{source}')")
        elif file_ext in ['.csv', '.tsv']:
            con.execute(f"CREATE OR REPLACE VIEW temp_source AS SELECT * FROM read_csv_auto('{source}')")
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

        # Get schema information
        result = con.execute("DESCRIBE temp_source").fetchall()
    elif query:
        # Create a temporary view from the query
        con.execute(f"CREATE OR REPLACE VIEW temp_source AS {query}")
        result = con.execute("DESCRIBE temp_source").fetchall()
    else:
        raise ValueError("Either source or query must be provided")

    columns = []
    for row in result:
        col_name = row[0]
        col_type = row[1]
        col_nullable = row[2] == 'YES' if len(row) > 2 else True

        # Map the type
        mapped_type = map_duckdb_type(col_type)

        # Build column definition
        col_def = {
            'name': col_name,
            'type': mapped_type,
            'nullable': col_nullable,
            'description': f'{col_name} column'
        }

        # Add constraints for non-nullable columns
        if not col_nullable:
            col_def['constraints'] = [{'not_null': True}]

        columns.append(col_def)

    return columns


def generate_quality_rules(columns: List[Dict]) -> List[Dict]:
    """
    Generate basic quality rules based on column definitions.

    Args:
        columns: List of column definitions

    Returns:
        List of quality rules
    """
    rules = []

    # Add not_null checks for non-nullable columns
    for col in columns:
        if not col['nullable']:
            rules.append({
                'name': f'no_null_{col["name"]}',
                'type': 'not_null',
                'column': col['name'],
                'severity': 'error'
            })

    # Add a minimum volume check
    rules.append({
        'name': 'minimum_rows',
        'type': 'volume',
        'min_rows': 1,
        'severity': 'error'
    })

    return rules


def generate_contract(
    dataset: str,
    stage: str,
    columns: List[Dict],
    owner: str = 'data-team',
    description: str = '',
    version: str = '1.0.0'
) -> Dict:
    """
    Generate a complete data contract.

    Args:
        dataset: Dataset name
        stage: Pipeline stage (bronze, silver, gold)
        columns: List of column definitions
        owner: Dataset owner
        description: Dataset description
        version: Contract version

    Returns:
        Complete contract dictionary
    """
    if not description:
        description = f'{dataset} dataset in {stage} stage'

    contract = {
        'version': version,
        'dataset': dataset,
        'stage': stage,
        'owner': owner,
        'description': description,
        'schema': {
            'columns': columns
        },
        'quality_rules': generate_quality_rules(columns),
        'sla': {
            'freshness': {
                'max_age_hours': 24,
                'schedule': 'daily'
            },
            'completeness': {
                'min_row_count': 1
            }
        },
        'evolution': {
            'backward_compatible': True,
            'breaking_changes_allowed': False,
            'deprecation_notice_days': 30
        }
    }

    return contract


def write_contract(contract: Dict, output_path: str):
    """
    Write contract to a YAML file.

    Args:
        contract: Contract dictionary
        output_path: Output file path
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        yaml.dump(contract, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"✅ Contract generated: {output_path}")


def load_contract(contract_path: str) -> Dict:
    """
    Load a contract from a YAML file.

    Args:
        contract_path: Path to contract YAML file

    Returns:
        Contract dictionary
    """
    with open(contract_path, 'r') as f:
        contract = yaml.safe_load(f)
    return contract


def generate_sql_from_contract(contract: Dict, input_alias: str = 'input_data') -> str:
    """
    Generate DuckDB SQL transformation from a contract.

    Args:
        contract: Contract dictionary
        input_alias: Alias for the input table

    Returns:
        SQL query string
    """
    dataset = contract.get('dataset', 'output')
    description = contract.get('description', '')
    columns = contract.get('schema', {}).get('columns', [])

    # Start building SQL
    sql_lines = [
        f"-- Generated from contract: {dataset}",
        f"-- Description: {description}",
        "--",
        "-- This SQL transformation is automatically generated from the data contract schema.",
        "-- Customize as needed for your business logic.",
        "",
        "SELECT"
    ]

    # Generate column selections with transformations
    column_selects = []
    for col in columns:
        col_name = col['name']
        col_type = col['type']
        col_desc = col.get('description', '')
        constraints = col.get('constraints', [])

        # Check for specific transformation patterns
        if 'email' in col_name.lower() and 'hash' in col_name.lower():
            # Pseudonymize email
            original_col = col_name.replace('_hash', '')
            column_selects.append(f"    SHA2({original_col}, 256) AS {col_name}  -- Pseudonymize PII")
        elif col_name.endswith('_code') and col_type == 'VARCHAR':
            # Normalize codes
            original_col = col_name.replace('_code', '')
            column_selects.append(f"    UPPER(SUBSTR({original_col}, 1, 2)) AS {col_name}  -- Normalize code")
        elif col_name == 'processed_at' and col_type == 'TIMESTAMP':
            # Add processing timestamp
            column_selects.append(f"    CURRENT_TIMESTAMP AS {col_name}")
        else:
            # Standard column selection
            comment = f"  -- {col_desc}" if col_desc and col_desc != f"{col_name} column" else ""
            column_selects.append(f"    {col_name}{comment}")

    sql_lines.append(",\n".join(column_selects))
    sql_lines.append(f"FROM {input_alias}")

    # Generate WHERE clauses from constraints
    where_conditions = []
    for col in columns:
        col_name = col['name']
        constraints = col.get('constraints', [])

        for constraint in constraints:
            if isinstance(constraint, dict):
                if 'not_null' in constraint and constraint['not_null']:
                    where_conditions.append(f"    {col_name} IS NOT NULL")
                if 'min_value' in constraint:
                    where_conditions.append(f"    {col_name} >= {constraint['min_value']}")
                if 'max_value' in constraint:
                    where_conditions.append(f"    {col_name} <= {constraint['max_value']}")

    if where_conditions:
        sql_lines.append("WHERE")
        sql_lines.append("\n    AND ".join(where_conditions))

    # Add deduplication if there's a unique constraint
    unique_cols = []
    for col in columns:
        constraints = col.get('constraints', [])
        for constraint in constraints:
            if isinstance(constraint, dict) and constraint.get('unique'):
                unique_cols.append(col['name'])

    if unique_cols:
        sql_lines.append("-- Remove duplicates based on primary key")
        partition_cols = ', '.join(unique_cols)
        # Find a timestamp column for ordering
        timestamp_cols = [c['name'] for c in columns if c.get('type') in ['TIMESTAMP', 'DATE']]
        order_col = timestamp_cols[0] if timestamp_cols else unique_cols[0]
        sql_lines.append(f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {order_col} DESC) = 1")

    sql_lines.append("")  # Empty line at end
    return "\n".join(sql_lines)


def write_sql(sql: str, output_path: str):
    """
    Write SQL to a file.

    Args:
        sql: SQL query string
        output_path: Output file path
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        f.write(sql)

    print(f"✅ SQL transformation generated: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate data contracts and SQL transformations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate contract from Parquet file
  python build.py --source data/transactions.parquet --output transformations/contracts/transactions.yml --dataset transactions --stage bronze

  # Generate contract from CSV file
  python build.py --source data/customers.csv --output transformations/contracts/customers.yml --dataset customers --stage bronze

  # Generate SQL transformation from contract
  python build.py --generate-sql --contract transformations/contracts/customers.yml --output transformations/sql/customers.sql

  # With custom input alias
  python build.py --generate-sql --contract transformations/contracts/customers.yml --output transformations/sql/customers.sql --input-alias bronze_customers
        """
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--source', help='Path to source file (Parquet, CSV) - generates contract')
    mode_group.add_argument('--query', help='SQL query to introspect (e.g., "SELECT * FROM table LIMIT 1") - generates contract')
    mode_group.add_argument('--generate-sql', action='store_true', help='Generate SQL transformation from contract')

    # Contract generation options
    parser.add_argument('--dataset', help='Dataset name (required for contract generation)')
    parser.add_argument('--stage', choices=['bronze', 'silver', 'gold'], help='Pipeline stage (required for contract generation)')
    parser.add_argument('--owner', default='data-team', help='Dataset owner (default: data-team)')
    parser.add_argument('--description', default='', help='Dataset description')
    parser.add_argument('--version', default='1.0.0', help='Contract version (default: 1.0.0)')

    # SQL generation options
    parser.add_argument('--contract', help='Path to contract YAML file (required for SQL generation)')
    parser.add_argument('--input-alias', default='input_data', help='Input table alias for SQL generation (default: input_data)')

    # Output options
    parser.add_argument('--output', required=True, help='Output file path')

    args = parser.parse_args()

    # Validate arguments based on mode
    if args.generate_sql:
        if not args.contract:
            parser.error('--contract is required when using --generate-sql')
    else:
        if not args.dataset:
            parser.error('--dataset is required when generating contracts')
        if not args.stage:
            parser.error('--stage is required when generating contracts')

    try:
        if args.generate_sql:
            # Generate SQL from contract
            print(f"🔍 Loading contract from {args.contract}...")
            contract = load_contract(args.contract)
            dataset = contract.get('dataset', 'output')

            print(f"📝 Generating SQL transformation for {dataset}...")
            sql = generate_sql_from_contract(contract, input_alias=args.input_alias)

            # Write to file
            write_sql(sql, args.output)

            print(f"\n✨ Success! SQL transformation created at {args.output}")
            print(f"\nNext steps:")
            print(f"1. Review and customize the SQL at {args.output}")
            print(f"2. Test the transformation:")
            print(f"   duckdb -c \".read {args.output}\"")
            print(f"3. Add to configs/transformations.yml:")
            print(f"   - name: {dataset}")
            print(f"     type: sql")

        else:
            # Generate contract from source
            # Create DuckDB connection
            con = duckdb.connect(':memory:')

            # Introspect schema
            print(f"🔍 Introspecting schema from {args.source or 'query'}...")
            columns = introspect_schema(con, source=args.source, query=args.query)
            print(f"   Found {len(columns)} columns")

            # Generate contract
            print("📝 Generating contract...")
            contract = generate_contract(
                dataset=args.dataset,
                stage=args.stage,
                columns=columns,
                owner=args.owner,
                description=args.description,
                version=args.version
            )

            # Write to file
            write_contract(contract, args.output)

            print(f"\n✨ Success! Contract created at {args.output}")
            print(f"\nNext steps:")
            print(f"1. Review and customize the contract at {args.output}")
            print(f"2. Generate SQL transformation:")
            print(f"   python build.py --generate-sql --contract {args.output} --output transformations/sql/{args.dataset}.sql")
            print(f"3. Reference in configs/transformations.yml:")
            print(f"   quality_checks:")
            print(f"     - contract:{args.dataset}")

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'con' in locals():
            con.close()


if __name__ == '__main__':
    main()
