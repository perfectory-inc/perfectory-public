#!/usr/bin/env python3
"""Keep public fixtures synthetic and private load-test bindings private.

The reserved namespaces below are the code-fixture contract. JSON and JSONL
fixtures receive stronger, structural validation so formatting cannot bypass
the policy and a finite place-name denylist is never needed.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit


TEXT_SUFFIXES = {".rs", ".py", ".json", ".jsonl", ".sql", ".js", ".ts", ".tsx"}
RESERVED_PNU_PREFIX = "99999"
RESERVED_LONGITUDE = (127.123, 127.124)
RESERVED_LATITUDE = (36.123, 36.124)
SYNTHETIC_TOKEN = "synthetic"
RUNTIME_COORDINATE_LINE_MARKER = "public-repository-safety: reviewed-runtime-coordinate"
RUNTIME_COORDINATE_FILE_MARKER = "public-repository-safety: reviewed-runtime-coordinate-inputs"
SQL_FIXTURE_MARKER = "public-repository-safety: synthetic-fixture"
SQL_FIXTURE_PATHS = {
    "platforms/foundation-platform/infra/lakehouse/dbt/smoke/source-fixtures.sql",
}
LOAD_POLICY_PATHS = {
    "products/gongzzang/tests/load/scenarios.v1.json",
    "products/gongzzang/tests/load/README.md",
    "products/gongzzang/docs/testing/load.md",
}
FIXTURE_PATH_TOKENS = {"fixture", "fixtures", "sample", "samples", "seed", "seeds"}

PNU_PATTERN = re.compile(r"(?<!\d)(\d{19})(?!\d)")
KOREA_LONGITUDE_PATTERN = re.compile(r"(?<![\d.])((?:12[4-9]|13[0-2])\.\d+)(?![\d.])")
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
CONCRETE_RPS_PATTERN = re.compile(r"\b\d[\d,.]*\s*(?:RPS|requests\s+per\s+second)\b", re.IGNORECASE)

HUMAN_LOCATION_KEYS = {
    "address_text",
    "complex_name",
    "complex_name_normalized",
    "developer_name",
    "management_agency_name",
    "name",
    "ltno_addr",
    "plat_plc",
    "platplc",
    "print_addr_raw",
    "road_addr",
}
SOURCE_ID_KEYS = {"source_record_id", "source_snapshot_id"}
ADMIN_CODE_LENGTHS = {
    "sido_code": (2, "99"),
    "sigungu_code": (5, "99999"),
    "primary_bjdong_code": (10, "99999"),
    "bjdong_code": (8, "99999"),
}
LONGITUDE_KEYS = {
    "anchor_lng",
    "anchor_lon",
    "bbox_max_x",
    "bbox_min_x",
    "longitude",
    "lng",
    "lon",
    "x_crd",
}
LATITUDE_KEYS = {
    "anchor_lat",
    "bbox_max_y",
    "bbox_min_y",
    "latitude",
    "lat",
    "y_crd",
}


def tracked_paths(root: Path) -> list[PurePosixPath]:
    try:
        output = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=True,
            stdout=subprocess.PIPE,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError(f"cannot enumerate tracked files: {error}") from error
    return sorted(
        PurePosixPath(raw.decode("utf-8", errors="strict"))
        for raw in output.split(b"\0")
        if raw
    )


def read_text(root: Path, relative: PurePosixPath) -> str:
    return (root / Path(*relative.parts)).read_text(encoding="utf-8")


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def is_reserved_coordinate(value: float, bounds: tuple[float, float]) -> bool:
    return bounds[0] <= value < bounds[1]


def scan_code_namespaces(
    root: Path, paths: Iterable[PurePosixPath], errors: list[str]
) -> None:
    for relative in paths:
        if relative.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            text = read_text(root, relative)
        except (OSError, UnicodeDecodeError):
            continue

        for match in PNU_PATTERN.finditer(text):
            value = match.group(1)
            if not value.startswith(RESERVED_PNU_PREFIX):
                errors.append(
                    f"{relative}:{line_number(text, match.start())}: assignable-looking PNU {value}; "
                    f"use the repository-reserved {RESERVED_PNU_PREFIX} PNU range"
                )

        reviewed_file = RUNTIME_COORDINATE_FILE_MARKER in text
        for match in KOREA_LONGITUDE_PATTERN.finditer(text):
            value = float(match.group(1))
            if is_reserved_coordinate(value, RESERVED_LONGITUDE):
                continue
            line_start = text.rfind("\n", 0, match.start()) + 1
            line_end = text.find("\n", match.end())
            if line_end == -1:
                line_end = len(text)
            if reviewed_file or RUNTIME_COORDINATE_LINE_MARKER in text[line_start:line_end]:
                continue
            errors.append(
                f"{relative}:{line_number(text, match.start())}: Korea-area coordinate {value}; "
                "use the reserved synthetic coordinate namespace or a reviewed runtime-input marker"
            )


def is_json_fixture(relative: PurePosixPath) -> bool:
    if relative.suffix.lower() not in {".json", ".jsonl"}:
        return False
    return any(
        token in FIXTURE_PATH_TOKENS
        for part in relative.parts
        for token in re.split(r"[._-]+", part.casefold())
    )


def load_json_documents(path: Path, relative: PurePosixPath) -> list[tuple[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if relative.suffix.lower() == ".jsonl":
        documents: list[tuple[str, Any]] = []
        for number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                documents.append((f"line {number}", json.loads(line)))
            except json.JSONDecodeError as error:
                raise ValueError(f"line {number}: {error.msg}") from error
        return documents
    try:
        return [("document", json.loads(text))]
    except json.JSONDecodeError as error:
        raise ValueError(f"line {error.lineno}: {error.msg}") from error


def walk_json(value: Any, location: str = "$") -> Iterable[tuple[str, str | None, Any]]:
    yield location, None, value
    if isinstance(value, dict):
        for key, child in value.items():
            child_location = f"{location}.{key}"
            yield child_location, str(key), child
            if isinstance(child, (dict, list)):
                yield from walk_json(child, child_location)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_location = f"{location}[{index}]"
            yield child_location, None, child
            if isinstance(child, (dict, list)):
                yield from walk_json(child, child_location)


def contains_synthetic_marker(value: Any) -> bool:
    for _location, _key, child in walk_json(value):
        if isinstance(child, str):
            lowered = child.casefold()
            if SYNTHETIC_TOKEN in lowered or child.startswith(RESERVED_PNU_PREFIX):
                return True
    return False


def synthetic_identity(value: str) -> bool:
    lowered = value.casefold()
    if SYNTHETIC_TOKEN in lowered or RESERVED_PNU_PREFIX in value:
        return True
    return bool(UUID_PATTERN.fullmatch(value) and value.startswith("00000000-0000-"))


def validate_fixture_document(
    relative: PurePosixPath, document_label: str, document: Any, errors: list[str]
) -> None:
    prefix = f"{relative}:{document_label}"
    if not isinstance(document, (dict, list)):
        errors.append(f"{prefix}: fixture root must be a JSON object or array")
        return
    if not contains_synthetic_marker(document):
        errors.append(f"{prefix}: fixture has no declared synthetic namespace marker")

    for location, key, value in walk_json(document):
        if key is None or value is None:
            continue
        normalized_key = key.casefold()

        if normalized_key in HUMAN_LOCATION_KEYS and isinstance(value, str) and value.strip():
            if SYNTHETIC_TOKEN not in value.casefold():
                errors.append(
                    f"{prefix}:{location}: sensitive field {key} must use the declared synthetic namespace"
                )

        if normalized_key in SOURCE_ID_KEYS:
            if not isinstance(value, str) or not synthetic_identity(value):
                errors.append(
                    f"{prefix}:{location}: {key} must use the reserved synthetic namespace"
                )

        if normalized_key == "pnu":
            if not isinstance(value, str) or not re.fullmatch(r"99999\d{14}", value):
                errors.append(f"{prefix}:{location}: pnu must use the reserved synthetic namespace")

        if normalized_key in ADMIN_CODE_LENGTHS:
            expected_length, expected_prefix = ADMIN_CODE_LENGTHS[normalized_key]
            if (
                not isinstance(value, str)
                or len(value) != expected_length
                or not value.startswith(expected_prefix)
            ):
                errors.append(
                    f"{prefix}:{location}: {key} must use the reserved synthetic namespace"
                )

        if normalized_key == "mgm_bldrgst_pk":
            if not isinstance(value, str) or not value.startswith("99999-"):
                errors.append(
                    f"{prefix}:{location}: {key} must use the reserved synthetic namespace"
                )

        if normalized_key.endswith("_id") and isinstance(value, str) and UUID_PATTERN.fullmatch(value):
            if not synthetic_identity(value):
                errors.append(
                    f"{prefix}:{location}: UUID identity must use the zero synthetic namespace"
                )

        if normalized_key in LONGITUDE_KEYS:
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not is_reserved_coordinate(
                float(value), RESERVED_LONGITUDE
            ):
                errors.append(
                    f"{prefix}:{location}: longitude must use the reserved synthetic coordinate namespace"
                )

        if normalized_key in LATITUDE_KEYS:
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not is_reserved_coordinate(
                float(value), RESERVED_LATITUDE
            ):
                errors.append(
                    f"{prefix}:{location}: latitude must use the reserved synthetic coordinate namespace"
                )

        if (
            isinstance(value, str)
            and value
            and (normalized_key.endswith("_date") or normalized_key.endswith("_utc"))
            and not value.startswith("2099-")
        ):
            errors.append(f"{prefix}:{location}: fixture time values must use the reserved 2099 namespace")


def validate_structured_fixtures(
    root: Path, paths: Iterable[PurePosixPath], errors: list[str]
) -> None:
    for relative in paths:
        if not is_json_fixture(relative):
            continue
        path = root / Path(*relative.parts)
        try:
            documents = load_json_documents(path, relative)
        except (OSError, UnicodeDecodeError, ValueError) as error:
            errors.append(f"{relative}: cannot parse fixture structurally: {error}")
            continue
        if not documents:
            errors.append(f"{relative}: fixture must contain at least one JSON document")
            continue
        for label, document in documents:
            validate_fixture_document(relative, label, document, errors)


def validate_sql_fixture_markers(
    root: Path, paths: Iterable[PurePosixPath], errors: list[str]
) -> None:
    tracked = {path.as_posix(): path for path in paths}
    for fixture_path in sorted(SQL_FIXTURE_PATHS & tracked.keys()):
        relative = tracked[fixture_path]
        try:
            text = read_text(root, relative)
        except (OSError, UnicodeDecodeError) as error:
            errors.append(f"{relative}: cannot read SQL fixture: {error}")
            continue
        if SQL_FIXTURE_MARKER not in text:
            errors.append(f"{relative}: requires the synthetic-fixture provenance marker")


def validate_load_policy(root: Path, paths: Iterable[PurePosixPath], errors: list[str]) -> None:
    tracked = {path.as_posix(): path for path in paths}
    for policy_path in sorted(LOAD_POLICY_PATHS & tracked.keys()):
        relative = tracked[policy_path]
        try:
            text = read_text(root, relative)
        except (OSError, UnicodeDecodeError) as error:
            errors.append(f"{relative}: cannot read load-test policy: {error}")
            continue
        if re.search(r"\b[a-z0-9.-]+\.internal\b", text, re.IGNORECASE):
            errors.append(f"{relative}: contains a private load-test binding (*.internal)")
        if "target/audit/load-tests" in text:
            errors.append(f"{relative}: contains a private load-test evidence path")
        if relative.suffix.lower() == ".md" and CONCRETE_RPS_PATTERN.search(text):
            errors.append(f"{relative}: contains a concrete private capacity claim")

    registry_path = "products/gongzzang/tests/load/scenarios.v1.json"
    relative = tracked.get(registry_path)
    if relative is None:
        return
    try:
        document = json.loads(read_text(root, relative))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        errors.append(f"{relative}: cannot parse load registry structurally: {error}")
        return
    if not isinstance(document, dict):
        errors.append(f"{relative}: load registry root must be an object")
        return
    target = document.get("defaultTargetBaseUrl")
    hostname = urlsplit(target).hostname if isinstance(target, str) else None
    if not hostname or not hostname.endswith(".invalid"):
        errors.append(f"{relative}: default target is a private load-test binding; use .invalid")
    if document.get("capacityBinding") != "synthetic-public-safety-ceiling":
        errors.append(f"{relative}: capacityBinding must declare the synthetic public safety ceiling")
    scenarios = document.get("scenarios")
    if not isinstance(scenarios, list):
        errors.append(f"{relative}: scenarios must be an array")
        return
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict) or scenario.get("maxSafeRps") != 1:
            errors.append(
                f"{relative}:$.scenarios[{index}]: maxSafeRps is a private load-test binding; "
                "the public synthetic ceiling must be 1"
            )


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <repository-root>", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    try:
        paths = tracked_paths(root)
    except RuntimeError as error:
        print(f"FAIL public-fixture-safety: {error}", file=sys.stderr)
        return 1

    errors: list[str] = []
    scan_code_namespaces(root, paths, errors)
    validate_structured_fixtures(root, paths, errors)
    validate_sql_fixture_markers(root, paths, errors)
    validate_load_policy(root, paths, errors)
    if errors:
        for error in errors:
            print(f"FAIL public-fixture-safety: {error}", file=sys.stderr)
        return 1
    print("OK public-fixture-safety")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
