#!/usr/bin/env python3
"""
Comboi Transformation Scaffolding Tool

This script helps you quickly scaffold new transformations, contracts, and configurations.

Usage:
    python tools/scaffold_transformation.py --name my_transform --type dbt --stage silver --industry finance
    python tools/scaffold_transformation.py --name my_transform --type bruin --stage gold --industry health
"""

import argparse
import sys
from pathlib import Path
from textwrap import dedent


def create_dbt_model(name: str, stage: str, industry: str) -> None:
    """Create a dbt SQL model template."""
    model_path = Path(f"dbt_project/models/{stage}/{name}.sql")
    model_path.parent.mkdir(parents=True, exist_ok=True)

    template = dedent(f"""
        {{{{ config(
            materialized='table',
            tags=['{stage}', '{industry}']
        ) }}}}

        /*
        {industry.title()} {stage.title()} Layer: {name.replace('_', ' ').title()}

        TODO: Add transformation description here

        This transformation:
        - TODO: Describe what it does
        - TODO: Describe business logic
        - TODO: Describe quality checks
        */

        WITH bronze_data AS (
            -- Load from bronze layer
            SELECT * FROM read_parquet('{{{{ var("bronze_base_path") }}}}/{industry}/source_table.parquet')
        ),

        cleansed AS (
            -- Cleanse and standardize
            SELECT
                id,
                CAST(timestamp_field AS TIMESTAMP) AS timestamp_field,
                LOWER(TRIM(text_field)) AS text_field,
                CAST(numeric_field AS DECIMAL(18,2)) AS numeric_field
            FROM bronze_data
            WHERE id IS NOT NULL
        ),

        transformed AS (
            -- Apply business logic
            SELECT
                *,
                -- Add calculated fields here
                numeric_field * 2 AS calculated_field
            FROM cleansed
        )

        SELECT * FROM transformed
        WHERE true  -- Add quality filters here
    """).strip()

    with open(model_path, "w") as f:
        f.write(template)

    print(f"✅ Created dbt model: {model_path}")


def create_bruin_transformation(name: str, stage: str, industry: str) -> None:
    """Create a Bruin Python transformation template."""
    transform_path = Path(f"transformations/{name}.py")
    transform_path.parent.mkdir(parents=True, exist_ok=True)

    template = dedent(f'''
        """
        {industry.title()} Bruin Transformation: {name.replace('_', ' ').title()}

        TODO: Add transformation description here

        This transformation performs:
        - TODO: Describe complex logic
        - TODO: Describe ML/statistical analysis
        - TODO: Describe why Python is needed vs SQL
        """

        import pandas as pd
        import numpy as np


        def transform(con, inputs):
            """
            {name.replace('_', ' ').title()} transformation.

            Args:
                con: DuckDB connection
                inputs: Dict mapping input aliases to file paths

            Returns:
                Either:
                - SQL query string (for DuckDB execution)
                - pandas DataFrame (for complex Python logic)
            """
            # Option 1: Return SQL query (simple transformations)
            # return """
            #     SELECT
            #         *,
            #         -- Add transformations here
            #     FROM input_alias
            #     WHERE condition
            # """

            # Option 2: Return DataFrame (complex Python logic)
            df = con.execute("SELECT * FROM input_alias").df()

            # Perform complex transformations
            # TODO: Add your transformation logic here
            df['new_column'] = df['existing_column'] * 2

            # TODO: Add statistical analysis
            # TODO: Add ML features
            # TODO: Add complex business logic

            return df
    ''').strip()

    with open(transform_path, "w") as f:
        f.write(template)

    print(f"✅ Created Bruin transformation: {transform_path}")


def create_data_contract(name: str, stage: str, industry: str) -> None:
    """Create a data contract template."""
    contract_path = Path(f"contracts/{name}.yml")
    contract_path.parent.mkdir(parents=True, exist_ok=True)

    template = dedent(f'''
        version: "1.0.0"
        dataset: "{name}"
        stage: "{stage}"
        owner: "{industry}-data-team"
        description: "TODO: Add dataset description"

        schema:
          columns:
            - name: id
              type: VARCHAR
              nullable: false
              description: "Unique identifier"
              constraints:
                - unique: true
                - not_null: true
            # TODO: Add more columns

        quality_rules:
          - name: "no_duplicate_ids"
            type: "uniqueness"
            column: "id"
            severity: "error"
          - name: "no_null_ids"
            type: "not_null"
            column: "id"
            severity: "error"
          - name: "minimum_records"
            type: "volume"
            min_rows: 1
            severity: "error"
          # TODO: Add more quality rules

        sla:
          freshness:
            max_age_hours: 24
            schedule: "daily"
          completeness:
            min_row_count: 1

        evolution:
          backward_compatible: true
          breaking_changes_allowed: false
    ''').strip()

    with open(contract_path, "w") as f:
        f.write(template)

    print(f"✅ Created data contract: {contract_path}")


def add_to_transformations_config(name: str, transform_type: str, stage: str, industry: str, has_contract: bool) -> None:
    """Add entry to transformations.yml."""
    config_path = Path("configs/transformations.yml")

    with open(config_path, "a") as f:
        f.write(f"\n# TODO: Add this to your transformations config:\n")
        f.write(f"# {industry}_{stage}:\n")
        f.write(f"#   - name: {name}\n")
        f.write(f"#     type: {transform_type}\n")

        if transform_type == "dbt":
            f.write(f"#     model: {name}\n")

        f.write(f"#     inputs:\n")
        f.write(f"#       - alias: input_data\n")
        f.write(f"#         stage: bronze  # or silver for gold transformations\n")
        f.write(f"#         source_path: \"{industry}/source_table.parquet\"\n")

        if has_contract:
            f.write(f"#     quality_checks:\n")
            f.write(f"#       - contract:{name}\n")

        f.write("\n")

    print(f"✅ Added config template to: {config_path}")
    print(f"   (Commented out - uncomment and customize)")


def main():
    parser = argparse.ArgumentParser(
        description="Scaffold new Comboi transformations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent("""
            Examples:
              # Create dbt SQL transformation with contract
              python tools/scaffold_transformation.py --name finance_daily_summary --type dbt --stage gold --industry finance --contract

              # Create Bruin Python transformation
              python tools/scaffold_transformation.py --name health_risk_model --type bruin --stage silver --industry health

              # Create just a data contract
              python tools/scaffold_transformation.py --name energy_alerts --contract-only --stage gold --industry energy
        """)
    )

    parser.add_argument("--name", required=True, help="Name of the transformation")
    parser.add_argument("--type", choices=["dbt", "bruin"], help="Transformation type")
    parser.add_argument("--stage", required=True, choices=["silver", "gold"], help="Pipeline stage")
    parser.add_argument("--industry", required=True, choices=["finance", "health", "energy", "ecommerce"], help="Industry vertical")
    parser.add_argument("--contract", action="store_true", help="Also create a data contract")
    parser.add_argument("--contract-only", action="store_true", help="Only create a data contract (no transformation)")

    args = parser.parse_args()

    if args.contract_only:
        create_data_contract(args.name, args.stage, args.industry)
        print(f"\n✨ Contract created successfully!")
        return

    if not args.type:
        parser.error("--type is required unless using --contract-only")

    print(f"\n🚀 Scaffolding {args.type} transformation: {args.name}")
    print(f"   Stage: {args.stage}, Industry: {args.industry}\n")

    # Create transformation
    if args.type == "dbt":
        create_dbt_model(args.name, args.stage, args.industry)
    else:
        create_bruin_transformation(args.name, args.stage, args.industry)

    # Create contract if requested
    if args.contract:
        create_data_contract(args.name, args.stage, args.industry)

    # Add to config
    add_to_transformations_config(args.name, args.type, args.stage, args.industry, args.contract)

    print(f"\n✨ Scaffolding complete!")
    print(f"\nNext steps:")
    print(f"1. Edit the generated files to implement your logic")
    print(f"2. Update configs/transformations.yml with the config snippet")
    print(f"3. Test with: comboi run {args.stage} --config configs/your-config.yml")


if __name__ == "__main__":
    main()
