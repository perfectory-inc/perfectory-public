"""Complete, once-read panel input selections over Iceberg's snapshot reader (ADR-0130)."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

MAX_PIN_FILE_BYTES = 64 * 1024
MAX_SNAPSHOT_ID = 2**63 - 1


def snapshot_id(value: Any) -> str:
    if type(value) not in (int, str) or re.fullmatch(r"[1-9][0-9]{0,18}", str(value)) is None:
        raise ValueError("source snapshot IDs must be positive signed-64 integers")
    if int(value) > MAX_SNAPSHOT_ID:
        raise ValueError("source snapshot ID exceeds signed-64 range")
    return str(value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate source snapshot key: {key}")
        result[key] = value
    return result


def read_snapshot_document(path: str) -> Any:
    """Read one bounded selection document, rejecting duplicate keys at every depth."""
    with Path(path).open("rb") as source:
        payload = source.read(MAX_PIN_FILE_BYTES + 1)
    if len(payload) > MAX_PIN_FILE_BYTES:
        raise ValueError("source snapshot file exceeds 64 KiB")
    return json.loads(payload, object_pairs_hook=_unique_object)


def load_source_snapshot_pins(
    input_mode: str, path: str | None, sources: Iterable[str], anchor_source: str,
    anchor_snapshot_id: str,
) -> dict[str, str]:
    """Read once before Spark. The caller owns and reuses this exact mapping thereafter."""
    if input_mode == "parquet":
        if path:
            raise ValueError("--source-snapshots-path cannot pin parquet inputs")
        return {}
    if input_mode != "iceberg":
        raise ValueError(f"unsupported snapshot input mode: {input_mode}")
    if not path:
        raise ValueError("--source-snapshots-path is required for every Iceberg input")
    pins = read_snapshot_document(path)
    if not isinstance(pins, dict) or set(pins) != set(sources):
        raise ValueError("--source-snapshots-path must name every Silver source exactly once")
    selected = {name: snapshot_id(value) for name, value in pins.items()}
    if selected[anchor_source] != snapshot_id(anchor_snapshot_id):
        raise ValueError(f"{anchor_source} snapshot pin disagrees with --iceberg-snapshot-id")
    return selected


def read_pinned_iceberg(spark: Any, table: str, source: str, pins: dict[str, str]) -> Any:
    if source not in pins:
        raise ValueError(f"missing source snapshot pin: {source}")
    selected = snapshot_id(pins[source])
    return spark.read.format("iceberg").option("snapshot-id", selected).load(table)
