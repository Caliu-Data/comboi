"""Azure SQL Database connector using DuckDB ODBC extension."""

from __future__ import annotations

from dataclasses import dataclass

from comboi.checkpoint import CheckpointStore
from comboi.connectors.base import BaseConnector


@dataclass
class AzureSQLConnector(BaseConnector):
    """Connector for Azure SQL Database using DuckDB's ODBC extension."""

    dsn: str
    checkpoint_store: CheckpointStore

    def __post_init__(self) -> None:
        """Initialize the connection_string from dsn for base class."""
        self.connection_string = self.dsn

    @property
    def connector_name(self) -> str:
        """Return the connector name for logging."""
        return "Azure SQL"

    @property
    def duckdb_extension(self) -> str:
        """Return the DuckDB extension to use."""
        return "odbc"

    @property
    def duckdb_attach_type(self) -> str:
        """Return the DuckDB ATTACH type."""
        return "ODBC"

