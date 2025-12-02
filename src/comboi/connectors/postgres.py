"""PostgreSQL connector using DuckDB postgres_scanner extension."""

from __future__ import annotations

from dataclasses import dataclass

from comboi.checkpoint import CheckpointStore
from comboi.connectors.base import BaseConnector


@dataclass
class PostgresConnector(BaseConnector):
    """Connector for PostgreSQL using DuckDB's postgres_scanner extension."""

    conn_str: str
    checkpoint_store: CheckpointStore

    def __post_init__(self) -> None:
        """Initialize the connection_string from conn_str for base class."""
        self.connection_string = self.conn_str

    @property
    def connector_name(self) -> str:
        """Return the connector name for logging."""
        return "PostgreSQL"

    @property
    def duckdb_extension(self) -> str:
        """Return the DuckDB extension to use."""
        return "postgres_scanner"

    @property
    def duckdb_attach_type(self) -> str:
        """Return the DuckDB ATTACH type."""
        return "POSTGRES"
