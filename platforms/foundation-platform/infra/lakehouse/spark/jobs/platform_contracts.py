"""Spark-facing lakehouse contract loader.

The canonical contract is owned by Rust `catalog-domain`. This module reads the
exported artifact that is tested against those Rust constants so Spark jobs do
not maintain their own independent column and partition contracts.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


CONTRACTS_SCHEMA_VERSION = "foundation-platform.lakehouse_contracts.v1"
CONTRACTS_PATH_ENV = "FOUNDATION_PLATFORM_LAKEHOUSE_CONTRACTS_PATH"
DEFAULT_CONTRACTS_PATH = (
    Path(__file__).resolve().parents[2]
    / "contracts"
    / "industrial_complex_lakehouse_contracts.json"
)


def load_lakehouse_contract(table_name: str) -> dict[str, Any]:
    path = Path(os.getenv(CONTRACTS_PATH_ENV, str(DEFAULT_CONTRACTS_PATH)))
    artifact = json.loads(path.read_text(encoding="utf-8"))
    schema_version = artifact.get("schema_version")
    if schema_version != CONTRACTS_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported lakehouse contract schema_version {schema_version!r}; "
            f"expected {CONTRACTS_SCHEMA_VERSION!r}"
        )

    contracts = artifact.get("contracts")
    if not isinstance(contracts, dict):
        raise ValueError("lakehouse contract artifact must contain a contracts object")

    contract = contracts.get(table_name)
    if not isinstance(contract, dict):
        raise ValueError(f"lakehouse contract artifact is missing {table_name}")
    return contract


def column_names(contract: dict[str, Any]) -> tuple[str, ...]:
    return tuple(column["name"] for column in columns(contract))


def required_column_names(contract: dict[str, Any]) -> tuple[str, ...]:
    return tuple(column["name"] for column in columns(contract) if column["required"])


def required_string_column_names(contract: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        column["name"]
        for column in columns(contract)
        if column["required"] and column["logical_type"] == "string"
    )


def create_table_columns_sql(contract: dict[str, Any], indent: int = 12) -> str:
    prefix = " " * indent
    return ",\n".join(
        f"{prefix}{column['name']} {spark_sql_type(column['logical_type'])}"
        for column in columns(contract)
    )


def partition_spec_sql(contract: dict[str, Any]) -> str:
    return ", ".join(contract["partition_spec"])


def columns(contract: dict[str, Any]) -> list[dict[str, Any]]:
    value = contract.get("columns")
    if not isinstance(value, list) or not value:
        raise ValueError(f"lakehouse contract {contract.get('table_name')} has no columns")
    return value


def spark_sql_type(logical_type: str) -> str:
    match logical_type:
        case "string":
            return "STRING"
        case "binary":
            return "BINARY"
        case "int":
            return "INT"
        case "long":
            return "BIGINT"
        case "double":
            return "DOUBLE"
        case "date":
            return "DATE"
        case "timestamp":
            return "TIMESTAMP"
        case value if value.startswith("decimal(") and value.endswith(")"):
            return value.upper()
        case _:
            raise ValueError(f"unsupported lakehouse logical type: {logical_type}")
