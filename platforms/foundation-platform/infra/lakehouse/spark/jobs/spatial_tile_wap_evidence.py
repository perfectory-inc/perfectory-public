"""Strict provider evidence contract for the spatial tile WAP probe."""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


EVIDENCE_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2]
    / "contracts"
    / "spatial_tile_wap_evidence.schema.json"
)
JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
SUPPORTED_CONTRACT_KEYS = {
    "$schema",
    "$id",
    "title",
    "type",
    "additionalProperties",
    "required",
    "properties",
    "allOf",
    "x-perfectory-cross-field-invariants",
    "x-perfectory-branch-pair",
}
SUPPORTED_PROPERTY_KEYS = {"type", "const", "enum", "minimum"}
SUPPORTED_CROSS_FIELD_OPERATIONS = {"equal", "all_distinct"}


def _strict_object(value: Any, label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValueError(f"{label} must be an object")
    return value


def _strict_string_list(value: Any, label: str) -> list[str]:
    if type(value) is not list or not value:
        raise ValueError(f"{label} must be a non-empty array")
    if any(type(item) is not str or not item for item in value):
        raise ValueError(f"{label} must contain non-empty strings")
    if len(set(value)) != len(value):
        raise ValueError(f"{label} must not contain duplicates")
    return value


def _result_fast_forward_map(contract: dict[str, Any]) -> dict[str, str]:
    clauses = contract.get("allOf")
    if type(clauses) is not list or not clauses:
        raise ValueError("evidence contract allOf must be a non-empty array")
    mapping: dict[str, str] = {}
    for index, clause_value in enumerate(clauses):
        clause = _strict_object(clause_value, f"allOf[{index}]")
        if set(clause) != {"if", "then"}:
            raise ValueError(f"allOf[{index}] must contain only if and then")
        condition = _strict_object(clause["if"], f"allOf[{index}].if")
        consequence = _strict_object(clause["then"], f"allOf[{index}].then")
        if set(condition) != {"properties", "required"}:
            raise ValueError(
                f"allOf[{index}].if must contain properties and required"
            )
        if set(consequence) != {"properties", "required"}:
            raise ValueError(
                f"allOf[{index}].then must contain properties and required"
            )
        if condition.get("required") != ["result"]:
            raise ValueError(f"allOf[{index}].if must require result")
        if consequence.get("required") != ["fast_forward"]:
            raise ValueError(f"allOf[{index}].then must require fast_forward")
        condition_properties = _strict_object(
            condition.get("properties"),
            f"allOf[{index}].if.properties",
        )
        consequence_properties = _strict_object(
            consequence.get("properties"),
            f"allOf[{index}].then.properties",
        )
        if set(condition_properties) != {"result"}:
            raise ValueError(f"allOf[{index}] must condition only on result")
        if set(consequence_properties) != {"fast_forward"}:
            raise ValueError(
                f"allOf[{index}] must constrain only fast_forward"
            )
        result_rule = _strict_object(
            condition_properties["result"],
            f"allOf[{index}].if.properties.result",
        )
        status_rule = _strict_object(
            consequence_properties["fast_forward"],
            f"allOf[{index}].then.properties.fast_forward",
        )
        if set(result_rule) != {"enum"}:
            raise ValueError(f"allOf[{index}] result rule must contain only enum")
        if set(status_rule) != {"const"}:
            raise ValueError(
                f"allOf[{index}] fast_forward rule must contain only const"
            )
        results = _strict_string_list(
            result_rule.get("enum"),
            f"allOf[{index}] result enum",
        )
        status = status_rule.get("const")
        if type(status) is not str or not status:
            raise ValueError(
                f"allOf[{index}] fast_forward const must be a string"
            )
        for result in results:
            if result in mapping:
                raise ValueError(f"result {result!r} has duplicate allOf rules")
            mapping[result] = status
    return mapping


def validate_evidence_contract(contract: dict[str, Any]) -> None:
    contract = _strict_object(contract, "evidence contract")
    if set(contract) != SUPPORTED_CONTRACT_KEYS:
        raise ValueError("evidence contract has unsupported or missing keywords")
    if contract["$schema"] != JSON_SCHEMA_DRAFT:
        raise ValueError("evidence contract must use JSON Schema draft 2020-12")
    if type(contract["$id"]) is not str or not contract["$id"]:
        raise ValueError("evidence contract $id must be a non-empty string")
    if type(contract["title"]) is not str or not contract["title"]:
        raise ValueError("evidence contract title must be a non-empty string")
    if (
        contract["type"] != "object"
        or contract["additionalProperties"] is not False
    ):
        raise ValueError(
            "evidence contract must be an object with additionalProperties=false"
        )

    required = _strict_string_list(contract["required"], "contract required")
    properties = _strict_object(contract["properties"], "contract properties")
    if set(required) != set(properties):
        raise ValueError("contract required fields must equal properties")
    for name, raw_rule in properties.items():
        rule = _strict_object(raw_rule, f"property {name}")
        if not set(rule).issubset(SUPPORTED_PROPERTY_KEYS):
            raise ValueError(f"property {name} has unsupported keywords")
        value_type = rule.get("type")
        if value_type not in {"string", "integer"}:
            raise ValueError(f"property {name} has unsupported type")
        if value_type == "integer":
            minimum = rule.get("minimum")
            if type(minimum) is not int or minimum < 1:
                raise ValueError(
                    f"integer property {name} must have a positive minimum"
                )
        elif "minimum" in rule:
            raise ValueError(f"string property {name} cannot have minimum")
        if "const" in rule and "enum" in rule:
            raise ValueError(f"property {name} cannot define const and enum")
        if "const" in rule:
            const = rule["const"]
            if value_type == "string" and type(const) is not str:
                raise ValueError(f"property {name} const has wrong type")
            if value_type == "integer" and (
                type(const) is not int or const < rule["minimum"]
            ):
                raise ValueError(f"property {name} const has wrong type")
        if "enum" in rule:
            values = _strict_string_list(rule["enum"], f"property {name} enum")
            if value_type != "string" or not values:
                raise ValueError(f"property {name} enum has wrong type")

    result_mapping = _result_fast_forward_map(contract)
    result_values = set(properties["result"].get("enum", []))
    if set(result_mapping) != result_values:
        raise ValueError("allOf result mapping must cover the result enum exactly")
    fast_forward_values = set(properties["fast_forward"].get("enum", []))
    if not set(result_mapping.values()).issubset(fast_forward_values):
        raise ValueError("allOf uses an invalid fast_forward status")

    invariants = contract["x-perfectory-cross-field-invariants"]
    if type(invariants) is not list or not invariants:
        raise ValueError("cross-field invariants must be a non-empty array")
    for index, raw_invariant in enumerate(invariants):
        invariant = _strict_object(raw_invariant, f"invariant[{index}]")
        if set(invariant) != {"op", "fields"}:
            raise ValueError(f"invariant[{index}] has unsupported keys")
        operation = invariant["op"]
        if operation not in SUPPORTED_CROSS_FIELD_OPERATIONS:
            raise ValueError(
                f"unsupported cross-field operation {operation!r}"
            )
        fields = _strict_string_list(
            invariant["fields"],
            f"invariant[{index}].fields",
        )
        if operation == "equal" and len(fields) != 2:
            raise ValueError("equal invariant must contain exactly two fields")
        if operation == "all_distinct" and len(fields) < 2:
            raise ValueError(
                "all_distinct invariant must contain at least two fields"
            )
        if not set(fields).issubset(properties):
            raise ValueError(f"invariant[{index}] references an unknown field")
        if any(properties[field]["type"] != "integer" for field in fields):
            raise ValueError(
                f"invariant[{index}] must reference integer properties"
            )

    branch_pair = _strict_object(
        contract["x-perfectory-branch-pair"],
        "branch pair",
    )
    if set(branch_pair) != {
        "historical",
        "publication",
        "suffix_encoding",
        "suffix_length",
        "same_suffix",
    }:
        raise ValueError("branch pair has unsupported or missing keys")
    if branch_pair["suffix_encoding"] != "lowercase_hex":
        raise ValueError("branch suffix encoding must be lowercase_hex")
    if (
        type(branch_pair["suffix_length"]) is not int
        or branch_pair["suffix_length"] < 1
    ):
        raise ValueError("branch suffix length must be a positive integer")
    if branch_pair["same_suffix"] is not True:
        raise ValueError("branch pair must require the same suffix")
    branch_fields: list[str] = []
    for role in ("historical", "publication"):
        specification = _strict_object(branch_pair[role], f"{role} branch")
        if set(specification) != {"field", "prefix"}:
            raise ValueError(f"{role} branch has unsupported keys")
        field = specification["field"]
        prefix = specification["prefix"]
        if (
            field not in properties
            or properties[field]["type"] != "string"
            or type(prefix) is not str
            or not prefix
        ):
            raise ValueError(f"{role} branch specification is invalid")
        branch_fields.append(field)
    if len(set(branch_fields)) != 2:
        raise ValueError("branch pair fields must be distinct")


def _load_evidence_contract() -> dict[str, Any]:
    try:
        contract = json.loads(EVIDENCE_SCHEMA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"failed to load evidence contract {EVIDENCE_SCHEMA_PATH}"
        ) from exc
    validate_evidence_contract(contract)
    return contract


EVIDENCE_CONTRACT = _load_evidence_contract()
EVIDENCE_PROPERTIES = EVIDENCE_CONTRACT["properties"]
EVIDENCE_SCHEMA_VERSION = EVIDENCE_PROPERTIES["schema_version"]["const"]
LOGICAL_CONTRACT = EVIDENCE_PROPERTIES["logical_contract"]["const"]
PHYSICAL_TABLE = EVIDENCE_PROPERTIES["physical_table"]["const"]
PROBE_CATALOG, PROBE_NAMESPACE, PROBE_TABLE = PHYSICAL_TABLE.split(".")
PROBE_CATALOG_BUCKET = EVIDENCE_PROPERTIES["catalog_bucket"]["const"]
PROVIDER = EVIDENCE_PROPERTIES["provider"]["const"]
RESULT_FAST_FORWARD_STATUS = _result_fast_forward_map(EVIDENCE_CONTRACT)
BRANCH_PAIR = EVIDENCE_CONTRACT["x-perfectory-branch-pair"]
HISTORICAL_BRANCH_PREFIX = BRANCH_PAIR["historical"]["prefix"]
PUBLICATION_BRANCH_PREFIX = BRANCH_PAIR["publication"]["prefix"]
PROVIDER_CATALOG_HOST = "catalog.cloudflarestorage.com"
RETENTION_DAYS = 7
BRANCH_MAX_REFERENCE_AGE_MS = RETENTION_DAYS * 24 * 60 * 60 * 1000
PROVIDER_CATALOG_PATH_PATTERN = re.compile(
    rf"^/[0-9a-f]{{32}}/{re.escape(PROBE_CATALOG_BUCKET)}$"
)
PROVIDER_CATALOG_URI_ERROR = (
    "catalog URI must target the official Cloudflare R2 Data Catalog "
    f"for the dedicated {PROBE_CATALOG_BUCKET} bucket"
)
COMMAND_RESULTS = {
    "prepare": "prepared",
    "validate": "validated",
    "fast-forward": "fast_forwarded",
    "probe": "probe_ok",
}


@dataclass(frozen=True)
class WapEvidence:
    schema_version: str
    logical_contract: str
    physical_table: str
    catalog_bucket: str
    historical_base_snapshot: int
    base_snapshot: int
    historical_branch_snapshot: int
    branch_snapshot: int
    historical_branch_name: str
    branch_name: str
    result: str
    provider: str
    historical_base_isolation: str
    branch_isolation: str
    retention: str
    fast_forward: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_evidence_create_new_atomic(path: Path, evidence: WapEvidence) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            evidence.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary_path, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)


def parse_positive_snapshot(value: str | int) -> int:
    if type(value) is int:
        snapshot = value
    elif type(value) is str and re.fullmatch(r"[0-9]+", value.strip()):
        snapshot = int(value.strip())
    else:
        raise ValueError("snapshot must be a positive integer")
    if snapshot <= 0:
        raise ValueError("snapshot must be a positive integer")
    return snapshot


def exact_positive_snapshot(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} must be a positive JSON integer")
    return value


def validate_branch_reference(
    rows: list[dict[str, Any]],
    *,
    expected_snapshot: int | None = None,
) -> int:
    fields = {"snapshot_id", "max_reference_age_in_ms"}
    if len(rows) != 1 or set(rows[0]) != fields:
        raise ValueError(
            "branch reference metadata must return exactly one strict row"
        )
    snapshot = exact_positive_snapshot(
        rows[0]["snapshot_id"],
        "branch reference snapshot",
    )
    if expected_snapshot is not None:
        expected = exact_positive_snapshot(
            expected_snapshot,
            "expected branch snapshot",
        )
        if snapshot != expected:
            raise ValueError(
                f"branch AS OF snapshot mismatch: expected={expected} actual={snapshot}"
            )
    retention = rows[0]["max_reference_age_in_ms"]
    if type(retention) is not int or retention != BRANCH_MAX_REFERENCE_AGE_MS:
        raise ValueError(
            "branch retention mismatch: expected "
            f"{BRANCH_MAX_REFERENCE_AGE_MS}ms, got {retention!r}"
        )
    return snapshot


def validate_historical_branch_invariants(
    rows: list[dict[str, Any]],
    *,
    historical_base_snapshot: int,
    publication_base_snapshot: int,
    sentinel_current_count: int,
) -> None:
    historical_base = exact_positive_snapshot(
        historical_base_snapshot,
        "historical base snapshot",
    )
    publication_base = exact_positive_snapshot(
        publication_base_snapshot,
        "publication base snapshot",
    )
    if historical_base == publication_base:
        raise ValueError(
            "historical base must be an older distinct snapshot than publication base"
        )
    validate_branch_reference(rows, expected_snapshot=historical_base)
    if type(sentinel_current_count) is not int or sentinel_current_count != 0:
        raise ValueError(
            "historical branch must exclude the main-only sentinel current row"
        )


def branch_name_for_release(release_id: str) -> str:
    try:
        release = uuid.UUID(release_id)
    except (AttributeError, ValueError) as exc:
        raise ValueError("release-id must be a UUID") from exc
    if release.int == 0:
        raise ValueError("release-id must not be a nil UUID")
    return f"{PUBLICATION_BRANCH_PREFIX}{release.hex}"


def historical_branch_name_for_release(release_id: str) -> str:
    branch_name_for_release(release_id)
    return f"{HISTORICAL_BRANCH_PREFIX}{uuid.UUID(release_id).hex}"


def historical_branch_name_for_publication_branch(branch: str) -> str:
    suffix_length = BRANCH_PAIR["suffix_length"]
    if not branch.startswith(PUBLICATION_BRANCH_PREFIX):
        raise ValueError("publication branch has the wrong prefix")
    suffix = branch[len(PUBLICATION_BRANCH_PREFIX) :]
    if len(suffix) != suffix_length or any(
        character not in "0123456789abcdef" for character in suffix
    ):
        raise ValueError(
            "publication branch must have a lowercase hexadecimal suffix"
        )
    return f"{HISTORICAL_BRANCH_PREFIX}{suffix}"


def validate_cloudflare_catalog_uri(catalog_uri: str) -> str:
    try:
        parsed = urlsplit(catalog_uri)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(PROVIDER_CATALOG_URI_ERROR) from exc

    if (
        parsed.scheme != "https"
        or parsed.hostname != PROVIDER_CATALOG_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or PROVIDER_CATALOG_PATH_PATTERN.fullmatch(parsed.path) is None
    ):
        raise ValueError(PROVIDER_CATALOG_URI_ERROR)
    return catalog_uri


def expected_evidence_for_command(command: str) -> tuple[str, str]:
    try:
        result = COMMAND_RESULTS[command]
    except KeyError as exc:
        raise ValueError(f"unsupported WAP command: {command}") from exc
    return result, RESULT_FAST_FORWARD_STATUS[result]


def validate_evidence_payload(
    payload: dict[str, Any],
    contract: dict[str, Any] = EVIDENCE_CONTRACT,
) -> None:
    validate_evidence_contract(contract)
    payload = _strict_object(payload, "WAP evidence")
    required = set(contract["required"])
    if set(payload) != required:
        raise ValueError("WAP evidence fields do not match the strict schema")

    for name, rule in contract["properties"].items():
        value = payload[name]
        value_type = rule["type"]
        if value_type == "string":
            if type(value) is not str:
                raise ValueError(f"WAP evidence {name} must be a JSON string")
        elif value_type == "integer":
            if type(value) is not int or value < rule["minimum"]:
                raise ValueError(
                    f"WAP evidence {name} must be a positive JSON integer"
                )
        else:
            raise ValueError(f"unsupported property type for {name}")
        if "const" in rule and value != rule["const"]:
            raise ValueError(f"WAP evidence {name} mismatch")
        if "enum" in rule and value not in rule["enum"]:
            raise ValueError(f"WAP evidence {name} is not allowed")

    result_mapping = _result_fast_forward_map(contract)
    result = payload["result"]
    if payload["fast_forward"] != result_mapping[result]:
        raise ValueError("WAP evidence result and fast_forward are inconsistent")

    for invariant in contract["x-perfectory-cross-field-invariants"]:
        fields = invariant["fields"]
        values = [payload[field] for field in fields]
        if invariant["op"] == "equal":
            if values[0] != values[1]:
                raise ValueError(
                    f"WAP evidence {fields[0]} must equal {fields[1]}"
                )
        elif invariant["op"] == "all_distinct":
            if any(
                values[left] == values[right]
                for left in range(len(values))
                for right in range(left + 1, len(values))
            ):
                raise ValueError(
                    "WAP evidence "
                    + ", ".join(fields)
                    + " must be distinct"
                )
        else:
            raise ValueError(
                "unsupported cross-field operation "
                f"{invariant['op']!r}"
            )

    branch_pair = contract["x-perfectory-branch-pair"]
    suffixes: list[str] = []
    for role in ("historical", "publication"):
        specification = branch_pair[role]
        value = payload[specification["field"]]
        prefix = specification["prefix"]
        if not value.startswith(prefix):
            raise ValueError(f"WAP evidence {role} branch prefix mismatch")
        suffix = value[len(prefix) :]
        if len(suffix) != branch_pair["suffix_length"] or any(
            character not in "0123456789abcdef" for character in suffix
        ):
            raise ValueError(
                f"WAP evidence {role} branch suffix must be lowercase hex"
            )
        suffixes.append(suffix)
    if branch_pair["same_suffix"] and suffixes[0] != suffixes[1]:
        raise ValueError("WAP evidence branch suffixes must match")


def validate_evidence(
    raw: str,
    *,
    expected_table: str,
    expected_base_snapshot: int,
    expected_branch: str,
    expected_result: str,
    expected_fast_forward: str,
) -> WapEvidence:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("WAP evidence must be valid JSON") from exc
    validate_evidence_payload(payload)

    evidence = WapEvidence(**payload)
    if evidence.physical_table != expected_table:
        raise ValueError("WAP evidence physical_table mismatch")
    base_snapshot = evidence.base_snapshot
    if base_snapshot != exact_positive_snapshot(
        expected_base_snapshot,
        "expected base_snapshot",
    ):
        raise ValueError("WAP evidence base_snapshot mismatch")
    expected_historical_branch = historical_branch_name_for_publication_branch(
        expected_branch
    )
    if evidence.historical_branch_name != expected_historical_branch:
        raise ValueError("WAP evidence historical_branch_name mismatch")
    if evidence.branch_name != expected_branch:
        raise ValueError("WAP evidence branch_name mismatch")
    required_fast_forward = RESULT_FAST_FORWARD_STATUS[evidence.result]
    expected_required_fast_forward = RESULT_FAST_FORWARD_STATUS.get(
        expected_result
    )
    if (
        expected_required_fast_forward is None
        or expected_fast_forward != expected_required_fast_forward
    ):
        raise ValueError("expected WAP result and fast_forward are inconsistent")
    if evidence.result != expected_result:
        raise ValueError("WAP evidence result mismatch")
    if evidence.fast_forward != expected_fast_forward:
        raise ValueError("WAP evidence fast_forward mismatch")
    return evidence


def offline_capability_line() -> str:
    return "provider_capability=not_proven_offline"


def live_success_line(evidence: WapEvidence) -> str:
    try:
        validate_evidence(
            json.dumps(evidence.to_dict()),
            expected_table=f"{PROBE_CATALOG}.{PROBE_NAMESPACE}.{PROBE_TABLE}",
            expected_base_snapshot=evidence.base_snapshot,
            expected_branch=evidence.branch_name,
            expected_result="probe_ok",
            expected_fast_forward="ok",
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "live success requires complete probe_ok evidence"
        ) from exc
    return (
        f"provider={evidence.provider} "
        f"historical_base_isolation={evidence.historical_base_isolation} "
        f"branch_isolation={evidence.branch_isolation} "
        f"retention={evidence.retention} "
        f"fast_forward={evidence.fast_forward}"
    )
