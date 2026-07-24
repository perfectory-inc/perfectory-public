#!/usr/bin/env python3
"""Prove Iceberg Write-Audit-Publish for the canonical parcel-boundary contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import struct
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from platform_contracts import (
    column_names,
    create_table_columns_sql,
    current_row_predicate,
    load_lakehouse_contract,
    partition_spec_sql,
)
from spatial_tile_wap_evidence import (
    EVIDENCE_CONTRACT,
    EVIDENCE_PROPERTIES,
    EVIDENCE_SCHEMA_VERSION,
    LOGICAL_CONTRACT,
    PROBE_CATALOG,
    PROBE_CATALOG_BUCKET,
    PROBE_NAMESPACE,
    PROBE_TABLE,
    PROVIDER,
    RETENTION_DAYS,
    WapEvidence,
    branch_name_for_release,
    exact_positive_snapshot,
    expected_evidence_for_command,
    historical_branch_name_for_release,
    live_success_line,
    offline_capability_line,
    parse_positive_snapshot,
    validate_branch_reference,
    validate_cloudflare_catalog_uri,
    validate_evidence,
    validate_evidence_contract,
    validate_evidence_payload,
    validate_historical_branch_invariants,
    write_evidence_create_new_atomic,
)


ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
)
SPARK_REDACTION_REGEX = (
    "(?i)secret|password|token|credential|access[.]?key"
)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SYNTHETIC_GEOMETRY_ORIGIN = (127.1231, 36.1231)
SYNTHETIC_GEOMETRY_STEP = 0.0001
SYNTHETIC_GEOMETRY_SIZE = 0.00005


@dataclass(frozen=True)
class WapExecutionOutcome:
    evidence: WapEvidence
    observed_historical_base_snapshot: int
    observed_base_snapshot: int


@dataclass(frozen=True)
class ParcelRow:
    boundary_id: str
    pnu: str
    sido_code: str
    sigungu_code: str
    bjdong_code: str
    jibun: str | None
    bonbun: str | None
    bubun: str | None
    geometry_wkb: bytes
    geometry_srid: int
    bbox_min_x: float
    bbox_min_y: float
    bbox_max_x: float
    bbox_max_y: float
    geometry_checksum_sha256: str
    source_record_id: str
    source_snapshot_id: str
    valid_from_utc: str
    valid_to_utc: str | None
    ingested_at_utc: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProbeFixture:
    release_id: str
    pnus: tuple[str, str, str]
    baseline_rows: tuple[ParcelRow, ParcelRow, ParcelRow]
    replace_active_row: ParcelRow
    add_row: ParcelRow
    replacement_row: ParcelRow
    delete_active_row: ParcelRow
    main_advance_row: ParcelRow
    historical_boundary_id: str
    transition_utc: str


def validate_identifier(label: str, value: str) -> None:
    if IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a simple identifier: {value}")


def qualified_table(catalog: str, namespace: str, table: str) -> str:
    validate_identifier("catalog", catalog)
    validate_identifier("namespace", namespace)
    validate_identifier("table", table)
    return f"{catalog}.{namespace}.{table}"


def branch_table(catalog: str, namespace: str, table: str, branch: str) -> str:
    validate_identifier("branch", branch)
    return f"{qualified_table(catalog, namespace, table)}.branch_{branch}"


def build_create_branch_sql(
    catalog: str,
    namespace: str,
    table: str,
    branch: str,
    base_snapshot: int,
) -> str:
    validate_identifier("branch", branch)
    snapshot = parse_positive_snapshot(base_snapshot)
    return (
        f"ALTER TABLE {qualified_table(catalog, namespace, table)} "
        f"CREATE BRANCH `{branch}` AS OF VERSION {snapshot} "
        f"RETAIN {RETENTION_DAYS} DAYS"
    )


def build_fast_forward_sql(
    catalog: str,
    namespace: str,
    table: str,
    branch: str,
) -> str:
    validate_identifier("branch", branch)
    qualified_table(catalog, namespace, table)
    return (
        f"CALL {catalog}.system.fast_forward("
        f"'{namespace}.{table}', 'main', '{branch}')"
    )


def validate_probe_target(catalog: str, namespace: str, table: str) -> None:
    validate_identifier("catalog", catalog)
    validate_identifier("namespace", namespace)
    validate_identifier("table", table)
    if (
        catalog != PROBE_CATALOG
        or namespace != PROBE_NAMESPACE
        or table != PROBE_TABLE
    ):
        raise ValueError(
            "probe is restricted to the dedicated "
            f"{PROBE_CATALOG}.{PROBE_NAMESPACE}.{PROBE_TABLE} table"
        )


def current_parcel_predicate() -> str:
    contract = load_lakehouse_contract(LOGICAL_CONTRACT)
    predicate = current_row_predicate(contract)
    if predicate is None:
        raise ValueError(f"{LOGICAL_CONTRACT} must define a current-row predicate")
    return predicate


def build_create_table_sql(catalog: str, namespace: str, table: str) -> str:
    contract = load_lakehouse_contract(LOGICAL_CONTRACT)
    target = qualified_table(catalog, namespace, table)
    return f"""
        CREATE TABLE IF NOT EXISTS {target} (
{create_table_columns_sql(contract)}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec_sql(contract)})
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """


def _polygon_wkb(min_x: float, min_y: float, size: float) -> bytes:
    points = (
        (min_x, min_y),
        (min_x + size, min_y),
        (min_x + size, min_y + size),
        (min_x, min_y + size),
        (min_x, min_y),
    )
    return (
        struct.pack("<BII", 1, 3, 1)
        + struct.pack("<I", len(points))
        + b"".join(struct.pack("<dd", x, y) for x, y in points)
    )


def _parcel_row(
    *,
    release_hex: str,
    role: str,
    pnu: str,
    geometry_origin: tuple[float, float],
    valid_from_utc: str,
    valid_to_utc: str | None,
) -> ParcelRow:
    geometry_wkb = _polygon_wkb(
        *geometry_origin,
        size=SYNTHETIC_GEOMETRY_SIZE,
    )
    bjdong_code = pnu[:10]
    return ParcelRow(
        boundary_id=f"wap:{release_hex}:{role}",
        pnu=pnu,
        sido_code=pnu[:2],
        sigungu_code=pnu[:5],
        bjdong_code=bjdong_code,
        jibun=f"{int(pnu[11:15])}-{int(pnu[15:19])}",
        bonbun=pnu[11:15],
        bubun=pnu[15:19],
        geometry_wkb=geometry_wkb,
        geometry_srid=4326,
        bbox_min_x=geometry_origin[0],
        bbox_min_y=geometry_origin[1],
        bbox_max_x=geometry_origin[0] + SYNTHETIC_GEOMETRY_SIZE,
        bbox_max_y=geometry_origin[1] + SYNTHETIC_GEOMETRY_SIZE,
        geometry_checksum_sha256=hashlib.sha256(geometry_wkb).hexdigest(),
        source_record_id=f"wap-probe:{release_hex}:{role}",
        source_snapshot_id=f"wap-probe:{release_hex}",
        valid_from_utc=valid_from_utc,
        valid_to_utc=valid_to_utc,
        ingested_at_utc="2099-01-03T00:00:00Z",
    )


def build_probe_fixture(release_id: str) -> ProbeFixture:
    release = uuid.UUID(release_id)
    base_number = release.int % 99_999_997
    suffixes = tuple(f"{(base_number + offset) % 100_000_000:08d}" for offset in range(4))
    pnus = tuple(f"99999004011{suffix}" for suffix in suffixes)
    add_pnu, replace_pnu, delete_pnu, main_advance_pnu = pnus
    geometry_origins = tuple(
        (
            SYNTHETIC_GEOMETRY_ORIGIN[0] + SYNTHETIC_GEOMETRY_STEP * index,
            SYNTHETIC_GEOMETRY_ORIGIN[1] + SYNTHETIC_GEOMETRY_STEP * index,
        )
        for index in range(6)
    )
    historical = _parcel_row(
        release_hex=release.hex,
        role="replace-history",
        pnu=replace_pnu,
        geometry_origin=geometry_origins[0],
        valid_from_utc="2098-01-01T00:00:00Z",
        valid_to_utc="2098-12-31T23:59:59Z",
    )
    replace_active = _parcel_row(
        release_hex=release.hex,
        role="replace-base",
        pnu=replace_pnu,
        geometry_origin=geometry_origins[1],
        valid_from_utc="2099-01-01T00:00:00Z",
        valid_to_utc=None,
    )
    delete_active = _parcel_row(
        release_hex=release.hex,
        role="delete-base",
        pnu=delete_pnu,
        geometry_origin=geometry_origins[2],
        valid_from_utc="2099-01-01T00:00:00Z",
        valid_to_utc=None,
    )
    add_row = _parcel_row(
        release_hex=release.hex,
        role="add",
        pnu=add_pnu,
        geometry_origin=geometry_origins[3],
        valid_from_utc="2099-01-02T00:00:00Z",
        valid_to_utc=None,
    )
    replacement = _parcel_row(
        release_hex=release.hex,
        role="replace-new",
        pnu=replace_pnu,
        geometry_origin=geometry_origins[4],
        valid_from_utc="2099-01-02T00:00:00Z",
        valid_to_utc=None,
    )
    main_advance = _parcel_row(
        release_hex=release.hex,
        role="main-advance",
        pnu=main_advance_pnu,
        geometry_origin=geometry_origins[5],
        valid_from_utc="2099-01-01T12:00:00Z",
        valid_to_utc=None,
    )
    return ProbeFixture(
        release_id=str(release),
        pnus=(add_pnu, replace_pnu, delete_pnu),
        baseline_rows=(historical, replace_active, delete_active),
        replace_active_row=replace_active,
        add_row=add_row,
        replacement_row=replacement,
        delete_active_row=delete_active,
        main_advance_row=main_advance,
        historical_boundary_id=historical.boundary_id,
        transition_utc="2099-01-02T00:00:00Z",
    )


def _validate_polygon_wkb(value: bytes) -> None:
    if not isinstance(value, bytes) or len(value) < 93:
        raise ValueError("geometry_wkb must contain a polygon")
    byte_order = value[0]
    if byte_order not in (0, 1):
        raise ValueError("geometry_wkb byte order is invalid")
    prefix = "<" if byte_order == 1 else ">"
    geometry_type, ring_count = struct.unpack_from(f"{prefix}II", value, 1)
    if geometry_type != 3 or ring_count < 1:
        raise ValueError("geometry_wkb must be a polygon")
    offset = 9
    for _ in range(ring_count):
        point_count = struct.unpack_from(f"{prefix}I", value, offset)[0]
        offset += 4
        if point_count < 4:
            raise ValueError("geometry_wkb polygon ring is too short")
        points = [
            struct.unpack_from(f"{prefix}dd", value, offset + index * 16)
            for index in range(point_count)
        ]
        offset += point_count * 16
        if points[0] != points[-1]:
            raise ValueError("geometry_wkb polygon ring is not closed")
    if offset != len(value):
        raise ValueError("geometry_wkb contains trailing bytes")


def validate_geometry_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        geometry = row.get("geometry_wkb")
        _validate_polygon_wkb(geometry)
        checksum = hashlib.sha256(geometry).hexdigest()
        if (
            row.get("geometry_srid") != 4326
            or row.get("geometry_checksum_sha256") != checksum
            or row.get("bbox_min_x") >= row.get("bbox_max_x")
            or row.get("bbox_min_y") >= row.get("bbox_max_y")
        ):
            raise ValueError("geometry evidence is invalid")


def _canonical_utc_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError("SCD2 timestamp is invalid") from exc
    else:
        raise ValueError("SCD2 timestamp is invalid")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def validate_closed_history_rows(
    rows: list[dict[str, Any]],
    fixture: ProbeFixture,
) -> None:
    historical = [
        row
        for row in fixture.baseline_rows
        if row.boundary_id == fixture.historical_boundary_id
    ]
    if len(historical) != 1:
        raise ValueError("fixture must define exactly one historical SCD2 row")
    expected_rows = (
        historical[0],
        fixture.replace_active_row,
        fixture.delete_active_row,
    )
    expected = {
        (
            row.boundary_id,
            _canonical_utc_timestamp(row.valid_from_utc),
            _canonical_utc_timestamp(
                row.valid_to_utc
                if row.boundary_id == fixture.historical_boundary_id
                else fixture.transition_utc
            ),
        )
        for row in expected_rows
    }
    fields = {"boundary_id", "valid_from_utc", "valid_to_utc"}
    if len(rows) != len(expected) or any(set(row) != fields for row in rows):
        raise ValueError("candidate SCD2 history row shape or cardinality mismatch")
    actual = {
        (
            str(row["boundary_id"]),
            _canonical_utc_timestamp(row["valid_from_utc"]),
            _canonical_utc_timestamp(row["valid_to_utc"]),
        )
        for row in rows
    }
    if actual != expected:
        raise ValueError(
            f"candidate SCD2 history mismatch: expected={expected!r} actual={actual!r}"
        )


def _sql_literal(value: Any, logical_type: str) -> str:
    if value is None:
        return "NULL"
    if logical_type == "string":
        return "'" + str(value).replace("'", "''") + "'"
    if logical_type == "binary":
        if not isinstance(value, bytes):
            raise ValueError("binary contract value must be bytes")
        return f"X'{value.hex()}'"
    if logical_type == "timestamp":
        canonical = str(value).replace("T", " ").removesuffix("Z")
        return f"TIMESTAMP '{canonical}'"
    if logical_type in {"int", "long", "double"} or logical_type.startswith("decimal("):
        return str(value)
    raise ValueError(f"unsupported SQL literal type: {logical_type}")


def _insert_row_sql(table: str, row: ParcelRow) -> str:
    contract = load_lakehouse_contract(LOGICAL_CONTRACT)
    values = row.to_dict()
    columns = column_names(contract)
    by_name = {column["name"]: column["logical_type"] for column in contract["columns"]}
    rendered_values = ", ".join(
        _sql_literal(values[name], by_name[name]) for name in columns
    )
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({rendered_values})"
    )


def build_prepare_statements(
    catalog: str,
    namespace: str,
    table: str,
    branch: str,
    fixture: ProbeFixture,
) -> tuple[str, str, str, str]:
    candidate = branch_table(catalog, namespace, table, branch)
    predicate = current_parcel_predicate()
    transition = _sql_literal(fixture.transition_utc, "timestamp")
    replacement_pnu = _sql_literal(fixture.replace_active_row.pnu, "string")
    delete_pnu = _sql_literal(fixture.delete_active_row.pnu, "string")
    return (
        _insert_row_sql(candidate, fixture.add_row),
        (
            f"UPDATE {candidate} SET valid_to_utc = {transition} "
            f"WHERE pnu = {replacement_pnu} AND ({predicate})"
        ),
        _insert_row_sql(candidate, fixture.replacement_row),
        (
            f"UPDATE {candidate} SET valid_to_utc = {transition} "
            f"WHERE pnu = {delete_pnu} AND ({predicate})"
        ),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prove Iceberg WAP for the Rust-owned parcel-boundary contract."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "validate", "fast-forward", "probe"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--catalog", default=PROBE_CATALOG)
        subparser.add_argument("--namespace", required=True)
        subparser.add_argument("--table", required=True)
        subparser.add_argument("--release-id")
        subparser.add_argument("--evidence-output")
        if command != "probe":
            subparser.add_argument(
                "--historical-base-snapshot",
                required=True,
                type=parse_positive_snapshot,
            )
            subparser.add_argument(
                "--base-snapshot",
                required=True,
                type=parse_positive_snapshot,
            )
    args = parser.parse_args(argv)
    validate_identifier("catalog", args.catalog)
    validate_identifier("namespace", args.namespace)
    validate_identifier("table", args.table)
    validate_probe_target(args.catalog, args.namespace, args.table)
    if args.command == "probe":
        if args.release_id is None:
            args.release_id = str(uuid.uuid4())
    elif args.release_id is None:
        parser.error("--release-id is required for prepare, validate, and fast-forward")
    branch_name_for_release(args.release_id)
    return args


def _required_lookup(
    lookup: Any,
    name: str,
) -> str:
    value = lookup(name)
    if value is None or not str(value).strip():
        raise ValueError(f"{name} is required")
    return str(value).strip()


def configure_spark_builder(builder: Any, catalog: str, lookup: Any) -> Any:
    if catalog != PROBE_CATALOG:
        raise ValueError(f"catalog must be the dedicated {PROBE_CATALOG} catalog")
    validate_identifier("catalog", catalog)
    provider = (lookup("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER") or "").strip()
    if provider not in {"r2_data_catalog", "cloudflare-r2-data-catalog"}:
        raise ValueError(
            "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_PROVIDER must select R2 Data Catalog"
        )
    catalog_uri = validate_cloudflare_catalog_uri(
        _required_lookup(
            lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI"
        )
    )
    warehouse = _required_lookup(
        lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_WAREHOUSE"
    )
    token = _required_lookup(
        lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"
    )
    return (
        builder.config("spark.redaction.regex", SPARK_REDACTION_REGEX)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", catalog_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.token", token)
        .config(
            f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config(f"spark.sql.catalog.{catalog}.s3.remote-signing-enabled", "false")
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.sql.session.timeZone", "UTC")
    )


def redact_secret_values(message: str, secrets: list[str]) -> str:
    redacted = message
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[redacted]")
    return redacted


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_string_list(values: tuple[str, ...] | list[str]) -> str:
    return ", ".join(_sql_string(value) for value in values)


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    raise ValueError("Spark query returned an unsupported row type")


class SparkWapRuntime:
    def __init__(
        self,
        spark: Any,
        catalog: str,
        namespace: str,
        table: str,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.table = table
        self.physical_table = qualified_table(catalog, namespace, table)
        self.catalog_bucket = PROBE_CATALOG_BUCKET
        self.predicate = current_parcel_predicate()

    def ensure_probe_table(self) -> None:
        validate_probe_target(self.catalog, self.namespace, self.table)
        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {self.catalog}.{self.namespace}"
        )
        self.spark.sql(
            build_create_table_sql(self.catalog, self.namespace, self.table)
        )

    def _collect(self, statement: str) -> list[dict[str, Any]]:
        return [_row_dict(row) for row in self.spark.sql(statement).collect()]

    def _scalar(self, statement: str, label: str) -> int:
        rows = self._collect(statement)
        if len(rows) != 1 or set(rows[0]) != {"value"}:
            raise ValueError(f"{label} query must return exactly one value")
        return exact_positive_snapshot(rows[0]["value"], label)

    def _nonnegative_count(self, statement: str, label: str) -> int:
        rows = self._collect(statement)
        if len(rows) != 1 or set(rows[0]) != {"value"}:
            raise ValueError(f"{label} query must return exactly one value")
        value = rows[0]["value"]
        if type(value) is not int or value < 0:
            raise ValueError(f"{label} must be a nonnegative integer")
        return value

    def snapshot(self, ref: str) -> int:
        validate_identifier("Iceberg ref", ref)
        return self._scalar(
            (
                f"SELECT snapshot_id AS value FROM {self.physical_table}.refs "
                f"WHERE name = {_sql_string(ref)}"
            ),
            f"{ref} snapshot",
        )

    def branch_snapshot(
        self,
        branch: str,
        expected_snapshot: int | None = None,
    ) -> int:
        validate_identifier("Iceberg branch", branch)
        return validate_branch_reference(
            self._branch_reference_rows(branch),
            expected_snapshot=expected_snapshot,
        )

    def _branch_reference_rows(self, branch: str) -> list[dict[str, Any]]:
        validate_identifier("Iceberg branch", branch)
        return self._collect(
            f"""
                SELECT snapshot_id, max_reference_age_in_ms
                FROM {self.physical_table}.refs
                WHERE name = {_sql_string(branch)}
            """
        )

    def _assert_main_snapshot(self, expected: int) -> None:
        actual = self.snapshot("main")
        if actual != parse_positive_snapshot(expected):
            raise ValueError(
                f"main snapshot changed: expected={expected} actual={actual}"
            )

    def _main_advance_count(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> int:
        checksum = _sql_string(fixture.main_advance_row.geometry_checksum_sha256)
        return self._nonnegative_count(
            f"""
                SELECT count(*) AS value
                FROM {table_ref}
                WHERE ({self.predicate})
                  AND boundary_id = {_sql_string(fixture.main_advance_row.boundary_id)}
                  AND pnu = {_sql_string(fixture.main_advance_row.pnu)}
                  AND geometry_checksum_sha256 = {checksum}
            """,
            "main-only sentinel current row count",
        )

    @staticmethod
    def _role_pnus(fixture: ProbeFixture) -> dict[str, str]:
        return {
            "add": fixture.add_row.pnu,
            "replace": fixture.replace_active_row.pnu,
            "delete": fixture.delete_active_row.pnu,
        }

    def _current_counts(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> dict[str, int]:
        rows = self._collect(
            f"""
                SELECT pnu, count(*) AS row_count
                FROM {table_ref}
                WHERE ({self.predicate})
                  AND pnu IN ({_sql_string_list(fixture.pnus)})
                GROUP BY pnu
            """
        )
        by_pnu = {str(row["pnu"]): int(row["row_count"]) for row in rows}
        return {
            role: by_pnu.get(pnu, 0)
            for role, pnu in self._role_pnus(fixture).items()
        }

    def _current_rows(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> list[dict[str, Any]]:
        rows = self._collect(
            f"""
                SELECT boundary_id, pnu, geometry_wkb, geometry_srid,
                       bbox_min_x, bbox_min_y, bbox_max_x, bbox_max_y,
                       geometry_checksum_sha256
                FROM {table_ref}
                WHERE ({self.predicate})
                  AND pnu IN ({_sql_string_list(fixture.pnus)})
                ORDER BY pnu, boundary_id
            """
        )
        for row in rows:
            if isinstance(row.get("geometry_wkb"), bytearray):
                row["geometry_wkb"] = bytes(row["geometry_wkb"])
        validate_geometry_rows(rows)
        return rows

    @staticmethod
    def _state(rows: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
        return {
            (
                str(row["boundary_id"]),
                str(row["pnu"]),
                str(row["geometry_checksum_sha256"]),
            )
            for row in rows
        }

    @staticmethod
    def _main_state(fixture: ProbeFixture) -> set[tuple[str, str, str]]:
        return {
            (
                fixture.replace_active_row.boundary_id,
                fixture.replace_active_row.pnu,
                fixture.replace_active_row.geometry_checksum_sha256,
            ),
            (
                fixture.delete_active_row.boundary_id,
                fixture.delete_active_row.pnu,
                fixture.delete_active_row.geometry_checksum_sha256,
            ),
        }

    @staticmethod
    def _candidate_state(fixture: ProbeFixture) -> set[tuple[str, str, str]]:
        return {
            (
                fixture.add_row.boundary_id,
                fixture.add_row.pnu,
                fixture.add_row.geometry_checksum_sha256,
            ),
            (
                fixture.replacement_row.boundary_id,
                fixture.replacement_row.pnu,
                fixture.replacement_row.geometry_checksum_sha256,
            ),
        }

    def _assert_state(
        self,
        table_ref: str,
        fixture: ProbeFixture,
        expected: set[tuple[str, str, str]],
        label: str,
    ) -> None:
        actual = self._state(self._current_rows(table_ref, fixture))
        if actual != expected:
            raise ValueError(
                f"{label} current content mismatch: expected={expected!r} actual={actual!r}"
            )

    def _duplicate_current_pnu_count(self, table_ref: str) -> int:
        return self._nonnegative_count(
            f"""
                SELECT count(*) AS value
                FROM (
                    SELECT pnu
                    FROM {table_ref}
                    WHERE ({self.predicate})
                    GROUP BY pnu
                    HAVING count(*) > 1
                ) duplicate_current
            """,
            "duplicate current pnu count",
        )

    def _superseded_current_row_count(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> int:
        superseded_ids = (
            fixture.historical_boundary_id,
            fixture.replace_active_row.boundary_id,
            fixture.delete_active_row.boundary_id,
        )
        return self._nonnegative_count(
            f"""
                SELECT count(*) AS value
                FROM {table_ref}
                WHERE ({self.predicate})
                  AND boundary_id IN ({_sql_string_list(superseded_ids)})
            """,
            "superseded current row count",
        )

    def _closed_history_count(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> int:
        expected_ids = (
            fixture.historical_boundary_id,
            fixture.replace_active_row.boundary_id,
            fixture.delete_active_row.boundary_id,
        )
        return self._nonnegative_count(
            f"""
                SELECT count(*) AS value
                FROM {table_ref}
                WHERE NOT ({self.predicate})
                  AND boundary_id IN ({_sql_string_list(expected_ids)})
            """,
            "closed history count",
        )

    def _closed_history_rows(
        self,
        table_ref: str,
        fixture: ProbeFixture,
    ) -> list[dict[str, Any]]:
        expected_ids = (
            fixture.historical_boundary_id,
            fixture.replace_active_row.boundary_id,
            fixture.delete_active_row.boundary_id,
        )
        return self._collect(
            f"""
                SELECT boundary_id, valid_from_utc, valid_to_utc
                FROM {table_ref}
                WHERE NOT ({self.predicate})
                  AND boundary_id IN ({_sql_string_list(expected_ids)})
            """
        )

    def _assert_main_baseline(
        self,
        fixture: ProbeFixture,
        base_snapshot: int,
        sentinel_current_count: int,
    ) -> dict[str, int]:
        self._assert_main_snapshot(base_snapshot)
        before = self._current_counts(self.physical_table, fixture)
        if before != {"add": 0, "replace": 1, "delete": 1}:
            raise ValueError(f"main precondition cardinality mismatch: {before!r}")
        actual_sentinel_count = self._main_advance_count(
            self.physical_table,
            fixture,
        )
        if actual_sentinel_count != sentinel_current_count:
            raise ValueError(
                "main-only sentinel cardinality mismatch: "
                f"expected={sentinel_current_count} actual={actual_sentinel_count}"
            )
        self._assert_state(
            self.physical_table,
            fixture,
            self._main_state(fixture),
            "main",
        )
        return before

    def _assert_historical_branch(
        self,
        fixture: ProbeFixture,
        historical_branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> int:
        table_ref = branch_table(
            self.catalog,
            self.namespace,
            self.table,
            historical_branch,
        )
        rows = self._branch_reference_rows(historical_branch)
        sentinel_count = self._main_advance_count(table_ref, fixture)
        validate_historical_branch_invariants(
            rows,
            historical_base_snapshot=historical_base_snapshot,
            publication_base_snapshot=base_snapshot,
            sentinel_current_count=sentinel_count,
        )
        self._assert_state(
            table_ref,
            fixture,
            self._main_state(fixture),
            "historical branch",
        )
        return historical_base_snapshot

    def _validate_branch(
        self,
        fixture: ProbeFixture,
        historical_branch: str,
        branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        historical_branch_snapshot = self._assert_historical_branch(
            fixture,
            historical_branch,
            historical_base_snapshot,
            base_snapshot,
        )
        before = self._assert_main_baseline(
            fixture,
            base_snapshot,
            sentinel_current_count=1,
        )
        candidate = branch_table(
            self.catalog, self.namespace, self.table, branch
        )
        branch_snapshot = self.branch_snapshot(branch)
        if branch_snapshot == base_snapshot:
            raise ValueError("branch did not create a snapshot after candidate writes")
        if self._main_advance_count(candidate, fixture) != 1:
            raise ValueError("candidate branch lost the main-only sentinel row")
        after = self._current_counts(candidate, fixture)
        self._assert_state(
            candidate,
            fixture,
            self._candidate_state(fixture),
            "candidate branch",
        )
        validate_closed_history_rows(
            self._closed_history_rows(candidate, fixture),
            fixture,
        )
        validate_candidate_invariants(
            before=before,
            after=after,
            duplicate_current_pnu_count=self._duplicate_current_pnu_count(candidate),
            superseded_current_row_count=self._superseded_current_row_count(
                candidate, fixture
            ),
            invalid_geometry_count=0,
        )
        return historical_branch_snapshot, branch_snapshot

    def seed_baseline(self, fixture: ProbeFixture) -> int:
        all_pnus = (*fixture.pnus, fixture.main_advance_row.pnu)
        existing = self._nonnegative_count(
            (
                f"SELECT count(*) AS value FROM {self.physical_table} "
                f"WHERE pnu IN ({_sql_string_list(all_pnus)})"
            ),
            "existing probe fixture row count",
        )
        if existing != 0:
            raise ValueError(
                "probe release PNUs already exist; rerun with a unique release UUID"
            )
        validate_geometry_rows([row.to_dict() for row in fixture.baseline_rows])
        for row in fixture.baseline_rows:
            self.spark.sql(_insert_row_sql(self.physical_table, row))
        base_snapshot = self.snapshot("main")
        self._assert_main_baseline(
            fixture,
            base_snapshot,
            sentinel_current_count=0,
        )
        historical_count = self._closed_history_count(
            self.physical_table, fixture
        )
        if historical_count != 1:
            raise ValueError("probe baseline must contain one historical row")
        return base_snapshot

    def advance_main(
        self,
        fixture: ProbeFixture,
        historical_base_snapshot: int,
    ) -> int:
        self._assert_main_baseline(
            fixture,
            historical_base_snapshot,
            sentinel_current_count=0,
        )
        validate_geometry_rows([fixture.main_advance_row.to_dict()])
        self.spark.sql(
            _insert_row_sql(self.physical_table, fixture.main_advance_row)
        )
        base_snapshot = self.snapshot("main")
        if base_snapshot == historical_base_snapshot:
            raise ValueError(
                "main-only sentinel write did not advance the publication base"
            )
        self._assert_main_baseline(
            fixture,
            base_snapshot,
            sentinel_current_count=1,
        )
        return base_snapshot

    def prepare_candidate(
        self,
        fixture: ProbeFixture,
        historical_branch: str,
        branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        self._assert_main_baseline(
            fixture,
            base_snapshot,
            sentinel_current_count=1,
        )
        self.spark.sql(
            build_create_branch_sql(
                self.catalog,
                self.namespace,
                self.table,
                historical_branch,
                historical_base_snapshot,
            )
        )
        self._assert_historical_branch(
            fixture,
            historical_branch,
            historical_base_snapshot,
            base_snapshot,
        )
        self.spark.sql(
            build_create_branch_sql(
                self.catalog,
                self.namespace,
                self.table,
                branch,
                base_snapshot,
            )
        )
        self.branch_snapshot(branch, expected_snapshot=base_snapshot)
        candidate = branch_table(
            self.catalog,
            self.namespace,
            self.table,
            branch,
        )
        if self._main_advance_count(candidate, fixture) != 1:
            raise ValueError(
                "publication branch must start from the exact main base sentinel"
            )
        self._assert_state(
            candidate,
            fixture,
            self._main_state(fixture),
            "publication branch base",
        )
        for statement in build_prepare_statements(
            self.catalog,
            self.namespace,
            self.table,
            branch,
            fixture,
        ):
            self.spark.sql(statement)
        return self._validate_branch(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )

    def validate_candidate(
        self,
        fixture: ProbeFixture,
        historical_branch: str,
        branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        return self._validate_branch(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )

    def fast_forward_candidate(
        self,
        fixture: ProbeFixture,
        historical_branch: str,
        branch: str,
        historical_base_snapshot: int,
        base_snapshot: int,
    ) -> tuple[int, int]:
        historical_branch_snapshot, branch_snapshot = self._validate_branch(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )
        self.spark.sql(
            build_fast_forward_sql(
                self.catalog, self.namespace, self.table, branch
            )
        )
        main_snapshot = self.snapshot("main")
        if main_snapshot != branch_snapshot:
            raise ValueError(
                "fast-forwarded main snapshot does not equal the validated branch snapshot"
            )
        self._assert_state(
            self.physical_table,
            fixture,
            self._candidate_state(fixture),
            "fast-forwarded main",
        )
        if self._main_advance_count(self.physical_table, fixture) != 1:
            raise ValueError("fast-forwarded main lost the main-only sentinel row")
        return historical_branch_snapshot, branch_snapshot


def execute_command_with_outcome(
    args: argparse.Namespace,
    runtime: Any,
) -> WapExecutionOutcome:
    fixture = build_probe_fixture(args.release_id)
    branch = branch_name_for_release(args.release_id)
    historical_branch = historical_branch_name_for_release(args.release_id)

    if args.command == "probe":
        runtime.ensure_probe_table()
        historical_base_snapshot = exact_positive_snapshot(
            runtime.seed_baseline(fixture),
            "observed historical base snapshot",
        )
        base_snapshot = exact_positive_snapshot(
            runtime.advance_main(fixture, historical_base_snapshot),
            "observed publication base snapshot",
        )
        prepared_snapshots = runtime.prepare_candidate(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )
        validated_snapshots = runtime.validate_candidate(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )
        published_snapshots = runtime.fast_forward_candidate(
            fixture,
            historical_branch,
            branch,
            historical_base_snapshot,
            base_snapshot,
        )
        observed_snapshots = (
            prepared_snapshots,
            validated_snapshots,
            published_snapshots,
        )
    else:
        historical_base_snapshot = parse_positive_snapshot(
            args.historical_base_snapshot
        )
        base_snapshot = parse_positive_snapshot(args.base_snapshot)
        if args.command == "prepare":
            observed_snapshots = (
                runtime.prepare_candidate(
                    fixture,
                    historical_branch,
                    branch,
                    historical_base_snapshot,
                    base_snapshot,
                ),
            )
        elif args.command == "validate":
            observed_snapshots = (
                runtime.validate_candidate(
                    fixture,
                    historical_branch,
                    branch,
                    historical_base_snapshot,
                    base_snapshot,
                ),
            )
        elif args.command == "fast-forward":
            validated_snapshots = runtime.validate_candidate(
                fixture,
                historical_branch,
                branch,
                historical_base_snapshot,
                base_snapshot,
            )
            published_snapshots = runtime.fast_forward_candidate(
                fixture,
                historical_branch,
                branch,
                historical_base_snapshot,
                base_snapshot,
            )
            observed_snapshots = (validated_snapshots, published_snapshots)
        else:
            raise ValueError(f"unsupported WAP command: {args.command}")
    result, fast_forward = expected_evidence_for_command(args.command)

    if historical_base_snapshot == base_snapshot:
        raise ValueError(
            "historical base snapshot must differ from publication base snapshot"
        )
    historical_branch_snapshot: int | None = None
    branch_snapshot: int | None = None
    for index, snapshots in enumerate(observed_snapshots, start=1):
        if not isinstance(snapshots, tuple) or len(snapshots) != 2:
            raise ValueError(
                "runtime branch validation must return exactly two snapshots"
            )
        observed_historical = exact_positive_snapshot(
            snapshots[0],
            f"observed historical branch snapshot #{index}",
        )
        observed_branch = exact_positive_snapshot(
            snapshots[1],
            f"observed publication branch snapshot #{index}",
        )
        if observed_historical != historical_base_snapshot:
            raise ValueError(
                "historical branch snapshot must equal historical base snapshot"
            )
        if observed_branch in {historical_base_snapshot, base_snapshot}:
            raise ValueError(
                "publication branch snapshot must differ from both base snapshots"
            )
        if historical_branch_snapshot is not None and (
            observed_historical != historical_branch_snapshot
            or observed_branch != branch_snapshot
        ):
            raise ValueError("branch snapshots changed between validation stages")
        historical_branch_snapshot = observed_historical
        branch_snapshot = observed_branch
    if historical_branch_snapshot is None or branch_snapshot is None:
        raise ValueError("runtime did not return branch snapshot evidence")

    evidence = WapEvidence(
        schema_version=EVIDENCE_SCHEMA_VERSION,
        logical_contract=LOGICAL_CONTRACT,
        physical_table=runtime.physical_table,
        catalog_bucket=runtime.catalog_bucket,
        historical_base_snapshot=historical_base_snapshot,
        base_snapshot=base_snapshot,
        historical_branch_snapshot=historical_branch_snapshot,
        branch_snapshot=branch_snapshot,
        historical_branch_name=historical_branch,
        branch_name=branch,
        result=result,
        provider=PROVIDER,
        historical_base_isolation=EVIDENCE_PROPERTIES[
            "historical_base_isolation"
        ]["const"],
        branch_isolation=EVIDENCE_PROPERTIES["branch_isolation"]["const"],
        retention=EVIDENCE_PROPERTIES["retention"]["const"],
        fast_forward=fast_forward,
    )
    return WapExecutionOutcome(
        evidence=evidence,
        observed_historical_base_snapshot=historical_base_snapshot,
        observed_base_snapshot=base_snapshot,
    )


def execute_command(args: argparse.Namespace, runtime: Any) -> WapEvidence:
    return execute_command_with_outcome(args, runtime).evidence


def validate_candidate_invariants(
    *,
    before: dict[str, int],
    after: dict[str, int],
    duplicate_current_pnu_count: int,
    superseded_current_row_count: int,
    invalid_geometry_count: int,
) -> None:
    expected_before = {"add": 0, "replace": 1, "delete": 1}
    expected_after = {"add": 1, "replace": 1, "delete": 0}
    if before != expected_before:
        raise ValueError(
            f"candidate precondition cardinality mismatch: {before!r}"
        )
    if after != expected_after:
        raise ValueError(f"candidate current-row cardinality mismatch: {after!r}")
    if duplicate_current_pnu_count != 0:
        raise ValueError("candidate contains multiple current rows for a pnu")
    if superseded_current_row_count != 0:
        raise ValueError("candidate current-row read contains superseded history")
    if invalid_geometry_count != 0:
        raise ValueError("candidate contains invalid geometry evidence")


def _load_pyspark() -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is required; run this job through the pinned Spark container"
        ) from exc
    return SparkSession


def _build_spark_session(args: argparse.Namespace) -> Any:
    SparkSession = _load_pyspark()
    builder = SparkSession.builder.appName(
        "foundation-platform-spatial-tile-publication-wap"
    )
    configure_spark_builder(builder, args.catalog, os.getenv)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    class_loader = (
        spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    )
    for class_name in (
        "org.apache.iceberg.spark.SparkCatalog",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    ):
        try:
            class_loader.loadClass(class_name)
        except Exception as exc:
            raise RuntimeError(
                "Iceberg Spark runtime is not loaded; run spark-submit with "
                f"--conf spark.jars.ivy=/tmp/.ivy2 --packages {ICEBERG_PACKAGES}"
            ) from exc
    return spark


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    expected_branch = branch_name_for_release(args.release_id)
    expected_result, expected_fast_forward = expected_evidence_for_command(
        args.command
    )
    expected_base_snapshot = (
        None
        if args.command == "probe"
        else parse_positive_snapshot(args.base_snapshot)
    )
    if args.evidence_output is None:
        release_hex = uuid.UUID(args.release_id).hex
        args.evidence_output = (
            "target/spatial-tile-publication/"
            f"spatial-tile-wap-{release_hex}-{args.command}.json"
        )
    spark = _build_spark_session(args)
    try:
        runtime = SparkWapRuntime(
            spark, args.catalog, args.namespace, args.table
        )
        outcome = execute_command_with_outcome(args, runtime)
        evidence = outcome.evidence
        raw = json.dumps(
            evidence.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        validate_evidence(
            raw,
            expected_table=runtime.physical_table,
            expected_base_snapshot=(
                outcome.observed_base_snapshot
                if expected_base_snapshot is None
                else expected_base_snapshot
            ),
            expected_branch=expected_branch,
            expected_result=expected_result,
            expected_fast_forward=expected_fast_forward,
        )
        write_evidence_create_new_atomic(Path(args.evidence_output), evidence)
        print(raw)
        if args.command == "probe":
            print(live_success_line(evidence))
        return 0
    finally:
        spark.stop()


def cli(argv: list[str] | None = None) -> int:
    try:
        return run(argv)
    except Exception as exc:
        token = os.getenv("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN", "")
        safe_message = redact_secret_values(str(exc), [token])
        print(f"spatial-tile-wap-error {safe_message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
