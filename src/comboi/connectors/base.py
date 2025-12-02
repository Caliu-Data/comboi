"""Base connector class for database connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import duckdb

from comboi.checkpoint import CheckpointStore
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BaseConnector(ABC):
    """Base class for database connectors using DuckDB."""

    connection_string: str
    checkpoint_store: CheckpointStore

    @property
    @abstractmethod
    def connector_name(self) -> str:
        """Return the name of the connector for logging."""
        pass

    @property
    @abstractmethod
    def duckdb_extension(self) -> str:
        """Return the DuckDB extension name to install."""
        pass

    @property
    @abstractmethod
    def duckdb_attach_type(self) -> str:
        """Return the DuckDB ATTACH type (e.g., 'ODBC', 'POSTGRES')."""
        pass

    def export_table(
        self,
        table_cfg: Dict[str, str],
        destination: Path,
        checkpoint_key: Optional[str] = None,
    ) -> Path:
        """
        Export a table from the source database to Parquet format.

        Args:
            table_cfg: Table configuration containing 'query', 'name', and optional 'incremental_column'
            destination: Path where the Parquet file will be written
            checkpoint_key: Optional key for checkpoint tracking

        Returns:
            Path to the exported Parquet file
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        query = table_cfg["query"]
        incremental_column = table_cfg.get("incremental_column")
        last_value = None

        # Handle incremental loads
        if checkpoint_key and incremental_column:
            last_value = self.checkpoint_store.get(checkpoint_key)
            if last_value:
                query = f"""
                SELECT * FROM ({query}) AS src
                WHERE {incremental_column} > '{last_value}'
                """

        logger.info(
            f"Executing {self.connector_name} query",
            table=table_cfg["name"],
            incremental=bool(last_value),
        )

        con = duckdb.connect()
        try:
            # Install and load the DuckDB extension
            con.execute(f"INSTALL {self.duckdb_extension};")
            con.execute(f"LOAD {self.duckdb_extension};")

            # Attach the database
            con.execute(
                f"ATTACH '{self.connection_string}' "
                f"(TYPE {self.duckdb_attach_type}, READ_ONLY=TRUE)"
            )

            # Export to Parquet
            con.execute(f"COPY ({query}) TO '{destination.as_posix()}' (FORMAT PARQUET)")

            # Update checkpoint for incremental loads
            if checkpoint_key and incremental_column:
                self._update_checkpoint(con, table_cfg, incremental_column, checkpoint_key, last_value)

        finally:
            con.close()

        logger.info(
            "Exported to destination",
            destination=str(destination),
            table=table_cfg["name"],
            connector=self.connector_name,
        )
        return destination

    def _update_checkpoint(
        self,
        con: duckdb.DuckDBPyConnection,
        table_cfg: Dict[str, str],
        incremental_column: str,
        checkpoint_key: str,
        last_value: Optional[str],
    ) -> None:
        """Update the checkpoint with the maximum incremental column value."""
        max_query = f"SELECT MAX({incremental_column}) AS chk FROM ({table_cfg['query']}) src"
        if last_value:
            max_query += f" WHERE {incremental_column} > '{last_value}'"

        result = con.execute(max_query).fetchone()
        if result and result[0]:
            self.checkpoint_store.update(checkpoint_key, result[0])
            logger.debug(
                "Updated checkpoint",
                checkpoint_key=checkpoint_key,
                new_value=result[0],
            )
