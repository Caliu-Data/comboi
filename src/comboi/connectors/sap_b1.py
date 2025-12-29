"""SAP Business One connector for reading extracted Parquet files with GDPR compliance."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import duckdb

from comboi.checkpoint import CheckpointStore
from comboi.gdpr import GDPRProcessor, get_sap_b1_table_config
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SAPB1Connector:
    """
    Connector for SAP Business One extracted data with GDPR compliance.

    This connector reads from already-extracted Parquet files in object storage
    (ADLS/S3) rather than connecting directly to SAP B1. It applies GDPR
    pseudonymization rules during the data transfer to the Bronze layer.

    Architecture:
    SAP B1 → File Export (Parquet) → Object Storage → This Connector → Bronze Layer

    The connector supports:
    - Incremental loading based on UpdateDate or other delta columns
    - GDPR pseudonymization of PII fields
    - Column-level exclusion for sensitive data
    - Pre-configured rules for 16 core SAP B1 tables
    """

    source_storage_path: str  # Base path to SAP B1 extracts (e.g., "abfss://raw/sap_b1")
    checkpoint_store: CheckpointStore
    apply_gdpr: bool = True  # Whether to apply GDPR rules

    @property
    def connector_name(self) -> str:
        """Return the connector name for logging."""
        return "SAP Business One"

    def export_table(
        self,
        table_cfg: Dict[str, str],
        destination: Path,
        checkpoint_key: Optional[str] = None,
    ) -> Path:
        """
        Export a SAP B1 table from extracted Parquet files to Bronze layer.

        Args:
            table_cfg: Table configuration containing:
                - name: SAP B1 table name (e.g., 'OINV', 'OCRD')
                - incremental_column: Optional column for incremental loading (e.g., 'UpdateDate')
                - source_file_pattern: Optional custom file pattern (default: {table_name}.parquet)
                - partition_column: Optional partition column in source files (e.g., 'dt')
            destination: Path where the Bronze Parquet file will be written
            checkpoint_key: Optional key for checkpoint tracking

        Returns:
            Path to the exported Parquet file
        """
        destination.parent.mkdir(parents=True, exist_ok=True)

        table_name = table_cfg["name"]
        incremental_column = table_cfg.get("incremental_column")
        source_file_pattern = table_cfg.get(
            "source_file_pattern", f"{table_name}.parquet"
        )
        partition_column = table_cfg.get("partition_column", "dt")

        # Construct source path
        source_path = self._get_source_path(table_name, source_file_pattern)

        last_value = None
        if checkpoint_key and incremental_column:
            last_value = self.checkpoint_store.get(checkpoint_key)

        logger.info(
            f"Processing {self.connector_name} table",
            table=table_name,
            source=source_path,
            incremental=bool(last_value),
            gdpr_enabled=self.apply_gdpr,
        )

        con = duckdb.connect()
        try:
            # Read from source Parquet files
            # Support both partitioned (dt=2024-01-15/*.parquet) and non-partitioned
            if "**" in source_path or "*" in source_path or source_path.endswith(
                ".parquet"
            ):
                base_query = f"SELECT * FROM read_parquet('{source_path}')"
            else:
                # Assume partitioned structure if not explicit
                base_query = (
                    f"SELECT * FROM read_parquet('{source_path}/**/*.parquet')"
                )

            # Apply incremental filter
            if last_value and incremental_column:
                base_query = f"""
                SELECT * FROM ({base_query}) AS src
                WHERE {incremental_column} > '{last_value}'
                """

            # Apply GDPR rules if enabled
            if self.apply_gdpr:
                gdpr_config = get_sap_b1_table_config(table_name)
                if gdpr_config:
                    logger.debug(
                        f"Applying GDPR rules to {table_name}",
                        excluded_columns=len(gdpr_config.get("exclude_columns", [])),
                        pseudonymized_columns=len(
                            gdpr_config.get("pseudonymize", [])
                        ),
                    )
                    # Create a temporary view for GDPR processing
                    con.execute("CREATE OR REPLACE TEMP VIEW source_data AS " + base_query)
                    final_query = GDPRProcessor.apply_gdpr_rules(
                        con, "source_data", gdpr_config
                    )
                else:
                    logger.warning(
                        f"No GDPR configuration found for table {table_name}, "
                        "data will be copied as-is"
                    )
                    final_query = base_query
            else:
                final_query = base_query

            # Export to Parquet
            con.execute(
                f"COPY ({final_query}) TO '{destination.as_posix()}' (FORMAT PARQUET)"
            )

            # Update checkpoint for incremental loads
            if checkpoint_key and incremental_column:
                self._update_checkpoint(
                    con, base_query, incremental_column, checkpoint_key, last_value
                )

        finally:
            con.close()

        logger.info(
            "Exported to Bronze layer",
            destination=str(destination),
            table=table_name,
            connector=self.connector_name,
        )
        return destination

    def _get_source_path(self, table_name: str, file_pattern: str) -> str:
        """
        Construct the full source path for a table.

        Args:
            table_name: SAP B1 table name
            file_pattern: File pattern (can include wildcards)

        Returns:
            Full path to source Parquet files
        """
        # Handle different path formats
        base = self.source_storage_path.rstrip("/")

        # If file_pattern is already a full path, use it as-is
        if file_pattern.startswith("abfss://") or file_pattern.startswith("s3://"):
            return file_pattern

        # Otherwise, construct relative to base path
        # Support table-specific subdirectories (e.g., /raw/sap_b1/ocrd/dt=2024-01-15/)
        return f"{base}/{table_name.lower()}/{file_pattern}"

    def _update_checkpoint(
        self,
        con: duckdb.DuckDBPyConnection,
        base_query: str,
        incremental_column: str,
        checkpoint_key: str,
        last_value: Optional[str],
    ) -> None:
        """Update the checkpoint with the maximum incremental column value."""
        max_query = f"SELECT MAX({incremental_column}) AS chk FROM ({base_query}) src"
        if last_value:
            max_query += f" WHERE {incremental_column} > '{last_value}'"

        result = con.execute(max_query).fetchone()
        if result and result[0]:
            self.checkpoint_store.update(checkpoint_key, str(result[0]))
            logger.debug(
                "Updated checkpoint",
                checkpoint_key=checkpoint_key,
                new_value=result[0],
            )


# Table extraction strategies for SAP B1 (for reference in configuration)
SAP_B1_TABLE_STRATEGIES = {
    # Tier 1: Core transaction tables (incremental, hourly)
    "OINV": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    "INV1": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    "OPCH": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    "PCH1": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    "ORCT": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    "OVPM": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
    # Tier 1: Journal entries (incremental, daily)
    "JDT1": {"strategy": "incremental", "frequency": "daily", "delta_column": "RefDate"},
    # Tier 1: Master data (incremental, daily)
    "OCRD": {"strategy": "incremental", "frequency": "daily", "delta_column": "UpdateDate"},
    "OPRJ": {"strategy": "incremental", "frequency": "daily", "delta_column": "UpdateDate"},
    # Tier 1: Reference data (full snapshot, weekly)
    "OITM": {"strategy": "full", "frequency": "weekly", "delta_column": None},
    "OACT": {"strategy": "full", "frequency": "weekly", "delta_column": None},
    # Tier 2: Reference data (full snapshot, weekly)
    "OCRG": {"strategy": "full", "frequency": "weekly", "delta_column": None},
    "OITB": {"strategy": "full", "frequency": "weekly", "delta_column": None},
    "OOCR": {"strategy": "full", "frequency": "weekly", "delta_column": None},
    "OSLP": {"strategy": "incremental", "frequency": "daily", "delta_column": "UpdateDate"},
    "RCT2": {"strategy": "incremental", "frequency": "hourly", "delta_column": "UpdateDate"},
}


def get_recommended_strategy(table_name: str) -> Optional[Dict[str, str]]:
    """
    Get the recommended extraction strategy for a SAP B1 table.

    Args:
        table_name: SAP B1 table name

    Returns:
        Strategy configuration or None if not in recommended set
    """
    return SAP_B1_TABLE_STRATEGIES.get(table_name.upper())
