"""GDPR compliance utilities for pseudonymization and data protection."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

import duckdb


class GDPRProcessor:
    """Handles GDPR-compliant data processing including pseudonymization."""

    @staticmethod
    def pseudonymize_value(value: Any, algorithm: str = "sha256") -> str:
        """
        Pseudonymize a value using the specified hashing algorithm.

        Args:
            value: The value to pseudonymize
            algorithm: The hashing algorithm to use (sha256, sha512, md5)

        Returns:
            Hexadecimal hash string
        """
        if value is None or value == "":
            return None

        str_value = str(value).encode("utf-8")

        if algorithm == "sha256":
            return hashlib.sha256(str_value).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(str_value).hexdigest()
        elif algorithm == "md5":
            return hashlib.md5(str_value).hexdigest()
        else:
            raise ValueError(f"Unsupported hashing algorithm: {algorithm}")

    @staticmethod
    def apply_gdpr_rules(
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        gdpr_config: Dict[str, Any],
    ) -> str:
        """
        Generate SQL to apply GDPR rules to a table.

        Args:
            con: DuckDB connection
            table_name: Name of the source table
            gdpr_config: GDPR configuration with exclude_columns, pseudonymize, and retain

        Returns:
            SQL query string with GDPR transformations applied
        """
        # Get all columns from the source table
        columns_query = f"DESCRIBE SELECT * FROM {table_name}"
        columns_result = con.execute(columns_query).fetchall()
        all_columns = {row[0]: row[1] for row in columns_result}

        # Handle retain_all case
        if gdpr_config.get("retain_all", False):
            return f"SELECT * FROM {table_name}"

        # Get GDPR configuration
        exclude_columns = set(gdpr_config.get("exclude_columns", []))
        pseudonymize_columns = set(gdpr_config.get("pseudonymize", []))
        retain_columns = set(gdpr_config.get("retain", []))
        algorithm = gdpr_config.get("hash_algorithm", "sha256")

        # Build SELECT clause
        select_parts = []

        for col_name in all_columns.keys():
            # Skip excluded columns (PII to remove)
            if col_name in exclude_columns:
                continue

            # Pseudonymize specified columns
            if col_name in pseudonymize_columns:
                if algorithm == "sha256":
                    select_parts.append(
                        f"SHA256(CAST({col_name} AS VARCHAR)) AS {col_name}_Hash"
                    )
                elif algorithm == "sha512":
                    select_parts.append(
                        f"SHA512(CAST({col_name} AS VARCHAR)) AS {col_name}_Hash"
                    )
                elif algorithm == "md5":
                    select_parts.append(
                        f"MD5(CAST({col_name} AS VARCHAR)) AS {col_name}_Hash"
                    )
                continue

            # Retain columns as-is
            if retain_columns and col_name in retain_columns:
                select_parts.append(col_name)
            elif not retain_columns:  # If no retain list specified, keep all non-excluded
                select_parts.append(col_name)

        if not select_parts:
            raise ValueError(
                f"No columns to select after applying GDPR rules for table {table_name}"
            )

        return f"SELECT {', '.join(select_parts)} FROM {table_name}"


# Pre-defined GDPR rules for SAP B1 tables
SAP_B1_GDPR_RULES = {
    "OCRD": {  # Business Partners
        "exclude_columns": [
            "Phone1",
            "Phone2",
            "Cellular",
            "Fax",
            "E_Mail",
            "Address",
            "Address2",
            "ZipCode",
            "Block",
            "City",
            "County",
            "Country",
            "State1",
            "State2",
            "Building",
            "MailAddrss",
            "MailZipCod",
            "MailCity",
            "MailCounty",
            "MailCountr",
            "MailState",
            "MailBuildi",
            "LicTradNum",
            "VatIdUnCmp",
            "DeferrTax",
            "EqualizTax",
        ],
        "pseudonymize": ["CardName"],
        "retain": [
            "CardCode",
            "CardType",
            "GroupCode",
            "Territory",
            "SlpCode",
            "Currency",
            "CreateDate",
            "UpdateDate",
            "frozenFor",
            "FrozenFrom",
            "FrozenTo",
            "Balance",
            "DebPayAcct",
            "CreditLine",
            "Discount",
            "ValidFrom",
            "ValidTo",
        ],
        "hash_algorithm": "sha256",
    },
    "OSLP": {  # Sales Employees
        "exclude_columns": ["Email", "Mobile", "Telephone"],
        "pseudonymize": ["SlpName"],
        "retain": ["SlpCode", "Active", "Commission", "EmployeeID"],
        "hash_algorithm": "sha256",
    },
    "OPRJ": {  # Projects
        "pseudonymize": ["PrjName"],
        "retain": [
            "PrjCode",
            "CardCode",
            "Active",
            "ValidFrom",
            "ValidTo",
            "CreateDate",
            "UpdateDate",
            "DueDate",
            "ClosingDate",
            "FinncPriod",
        ],
        "hash_algorithm": "sha256",
    },
    # Transaction tables - retain all data (no PII)
    "OINV": {"retain_all": True},  # AR Invoice Headers
    "INV1": {"retain_all": True},  # AR Invoice Lines
    "OPCH": {"retain_all": True},  # AP Invoice Headers
    "PCH1": {"retain_all": True},  # AP Invoice Lines
    "ORCT": {"retain_all": True},  # Incoming Payments
    "OVPM": {"retain_all": True},  # Outgoing Payments
    "JDT1": {"retain_all": True},  # Journal Lines
    "OITM": {"retain_all": True},  # Items/Services
    "OACT": {"retain_all": True},  # Chart of Accounts
    "OCRG": {"retain_all": True},  # BP Groups
    "OITB": {"retain_all": True},  # Item Groups
    "OOCR": {"retain_all": True},  # Cost Centers
    "RCT2": {"retain_all": True},  # Payment Invoices
}


def get_sap_b1_table_config(table_name: str) -> Optional[Dict[str, Any]]:
    """
    Get GDPR configuration for a specific SAP B1 table.

    Args:
        table_name: SAP B1 table name (e.g., 'OCRD', 'OINV')

    Returns:
        GDPR configuration dictionary or None if not configured
    """
    return SAP_B1_GDPR_RULES.get(table_name.upper())
