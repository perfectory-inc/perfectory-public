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


def load_lakehouse_artifact() -> dict[str, Any]:
    path = Path(os.getenv(CONTRACTS_PATH_ENV, str(DEFAULT_CONTRACTS_PATH)))
    artifact = json.loads(path.read_text(encoding="utf-8"))
    schema_version = artifact.get("schema_version")
    if schema_version != CONTRACTS_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported lakehouse contract schema_version {schema_version!r}; "
            f"expected {CONTRACTS_SCHEMA_VERSION!r}"
        )
    return artifact


def load_lakehouse_contract(table_name: str) -> dict[str, Any]:
    artifact = load_lakehouse_artifact()

    contracts = artifact.get("contracts")
    if not isinstance(contracts, dict):
        raise ValueError("lakehouse contract artifact must contain a contracts object")

    contract = contracts.get(table_name)
    if not isinstance(contract, dict):
        raise ValueError(f"lakehouse contract artifact is missing {table_name}")
    current_row_predicate(contract)
    return contract


def jsonl_transport_columns(dataset_name: str) -> tuple[str, ...]:
    """Return the columns a JSONL producer must supply for ``dataset_name``.

    The list is exported from the Rust lakehouse contract, so a job never keeps its own copy of
    an input column list that can drift away from the table it feeds.
    """

    transports = load_lakehouse_artifact().get("jsonl_transports")
    if not isinstance(transports, dict):
        raise ValueError("lakehouse contract artifact must contain a jsonl_transports object")

    transport = transports.get(dataset_name)
    if not isinstance(transport, dict):
        raise ValueError(f"lakehouse contract artifact is missing jsonl transport {dataset_name}")

    names = transport.get("columns")
    if not isinstance(names, list) or not names:
        raise ValueError(f"jsonl transport {dataset_name} has no columns")
    return tuple(names)


def value_domain(table_name: str, column_name: str) -> tuple[str, ...]:
    """Return the wire values ``column_name`` of ``table_name`` admits."""

    domains = load_lakehouse_artifact().get("value_domains")
    if not isinstance(domains, dict):
        raise ValueError("lakehouse contract artifact must contain a value_domains object")

    table_domains = domains.get(table_name)
    if not isinstance(table_domains, dict):
        raise ValueError(f"lakehouse contract artifact is missing value domains for {table_name}")

    values = table_domains.get(column_name)
    if not isinstance(values, list) or not values:
        raise ValueError(f"value domain {table_name}.{column_name} is empty")
    return tuple(values)


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


def string_column_names(contract: dict[str, Any]) -> tuple[str, ...]:
    """Return every string column, required or not.

    An optional string column may be null, but it may never be the empty string: null says the
    source did not state a value and ``""`` says it stated nothing, which is the same claim with the
    absence hidden. Jobs gate every string column on emptiness and only the required ones on
    nullity (root ADR-0035).
    """

    return tuple(
        column["name"]
        for column in columns(contract)
        if column["logical_type"] == "string"
    )


def current_row_predicate(contract: dict[str, Any]) -> str | None:
    if "current_row_predicate" not in contract:
        raise ValueError(
            f"lakehouse contract {contract.get('table_name')} is missing "
            "the current_row_predicate key"
        )

    value = contract["current_row_predicate"]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"lakehouse contract {contract.get('table_name')} "
            "current_row_predicate must be null or a nonblank string"
        )
    return value


def create_table_columns_sql(contract: dict[str, Any], indent: int = 12) -> str:
    prefix = " " * indent
    return ",\n".join(
        f"{prefix}{column['name']} {spark_sql_type(column['logical_type'])}"
        for column in columns(contract)
    )


def partition_spec_sql(contract: dict[str, Any]) -> str:
    return ", ".join(partition_spec(contract))


def partition_spec(contract: dict[str, Any]) -> tuple[str, ...]:
    value = contract.get("partition_spec")
    if not isinstance(value, list):
        raise ValueError(
            f"lakehouse contract {contract.get('table_name')} has no partition_spec"
        )
    return tuple(value)


def partition_column_names(contract: dict[str, Any]) -> tuple[str, ...]:
    """Return the partition-spec entries that name a column directly.

    Iceberg partitions on transforms (``bucket(32, complex_id)``) as well as on plain columns; a
    local Parquet writer can only do the latter. Reading both off the one contract is what keeps a
    job from carrying its own copy of the partitioning, which is how the writer kept saying
    ``sido_code`` after the contract stopped requiring one.
    """

    names = column_names(contract)
    return tuple(entry for entry in partition_spec(contract) if entry in names)


def sort_order(contract: dict[str, Any]) -> tuple[str, ...]:
    value = contract.get("sort_order")
    if not isinstance(value, list):
        raise ValueError(f"lakehouse contract {contract.get('table_name')} has no sort_order")
    return tuple(value)


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
