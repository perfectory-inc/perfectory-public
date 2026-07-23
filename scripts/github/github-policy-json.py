#!/usr/bin/env python3
"""Canonical JSON projections for the public GitHub repository policy."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path, PurePosixPath
from typing import Any


THIRD_PARTY_ARTIFACT_PATHS = {
    ".gitattributes",
    "LICENSES/OFL-1.1.txt",
    "THIRD_PARTY_NOTICES.md",
    "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt",
    "products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256",
    "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css",
}


def reject_duplicate_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key: {key}")
        value[key] = item
    return value


def load_json(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin, object_pairs_hook=reject_duplicate_object_keys)
    with Path(path).open(encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=reject_duplicate_object_keys)


def canonical_dump(value: Any) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def sort_json_values(values: list[Any]) -> list[Any]:
    return sorted(values, key=lambda value: json.dumps(value, sort_keys=True))


def normalize_ruleset(value: dict[str, Any], *, without_status: bool) -> dict[str, Any]:
    normalized: dict[str, Any] = {
        "name": value["name"],
        "target": value["target"],
        "enforcement": value["enforcement"],
        "bypass_actors": sort_json_values(copy.deepcopy(value.get("bypass_actors", []))),
        "conditions": copy.deepcopy(value["conditions"]),
        "rules": [],
    }

    ref_name = normalized["conditions"].get("ref_name")
    if isinstance(ref_name, dict):
        for key in ("include", "exclude"):
            if isinstance(ref_name.get(key), list):
                ref_name[key] = sorted(ref_name[key])

    for source_rule in value["rules"]:
        if without_status and source_rule["type"] == "required_status_checks":
            continue
        rule: dict[str, Any] = {"type": source_rule["type"]}
        if "parameters" in source_rule:
            parameters = copy.deepcopy(source_rule["parameters"])
            if isinstance(parameters.get("allowed_merge_methods"), list):
                parameters["allowed_merge_methods"] = sorted(
                    parameters["allowed_merge_methods"]
                )
            dismissal = parameters.get("dismissal_restriction")
            if isinstance(dismissal, dict) and isinstance(
                dismissal.get("allowed_actors"), list
            ):
                dismissal["allowed_actors"] = sort_json_values(
                    dismissal["allowed_actors"]
                )
            reviewers = parameters.get("required_reviewers")
            if isinstance(reviewers, list):
                for reviewer in reviewers:
                    if isinstance(reviewer, dict) and isinstance(
                        reviewer.get("file_patterns"), list
                    ):
                        reviewer["file_patterns"] = sorted(reviewer["file_patterns"])
                parameters["required_reviewers"] = sort_json_values(reviewers)
            checks = parameters.get("required_status_checks")
            if isinstance(checks, list):
                parameters["required_status_checks"] = sorted(
                    checks,
                    key=lambda check: (
                        check.get("context", ""),
                        check.get("integration_id", -1),
                    ),
                )
            rule["parameters"] = parameters
        normalized["rules"].append(rule)

    normalized["rules"].sort(key=lambda rule: rule["type"])
    return normalized


def required_contexts(value: dict[str, Any]) -> list[str]:
    contexts: list[str] = []
    for rule in value.get("rules", []):
        if rule.get("type") != "required_status_checks":
            continue
        for check in rule.get("parameters", {}).get("required_status_checks", []):
            context = check.get("context")
            if not isinstance(context, str) or not context.startswith("required/"):
                raise ValueError("required check context must start with required/")
            contexts.append(context)
    if not contexts or len(contexts) != len(set(contexts)):
        raise ValueError("required check contexts must be non-empty and unique")
    return sorted(contexts)


def ruleset_summaries(values: list[dict[str, Any]], *, expected: bool) -> list[dict[str, Any]]:
    summaries = []
    for value in values:
        summaries.append(
            {
                "name": value["name"],
                "target": value["target"],
                "enforcement": value["enforcement"],
                "source_type": "Repository" if expected else value["source_type"],
            }
        )
    return sorted(summaries, key=lambda value: value["name"])


def validate_repository_identity(value: Any, *, allow_unset: bool) -> None:
    expected_owner = {
        "login": "perfectory-inc",
        "id": 306911903,
        "node_id": "O_kgDOEksanw",
    }
    if not isinstance(value, dict) or set(value) != {
        "hostname", "full_name", "repository_id", "repository_node_id", "owner"
    }:
        raise ValueError("repository identity has missing or unexpected fields")
    if value["hostname"] != "github.com" \
            or value["full_name"] != "perfectory-inc/perfectory-public" \
            or value["owner"] != expected_owner:
        raise ValueError("repository host/name/owner identity drift")

    repository_id = value["repository_id"]
    repository_node_id = value["repository_node_id"]
    unset = repository_id == 0 \
        and repository_node_id == "UNSET_AFTER_REPOSITORY_CREATION"
    if unset:
        if allow_unset:
            return
        raise ValueError(
            "repository immutable identity is unset; capture it after empty-repository creation"
        )
    if isinstance(repository_id, bool) \
            or not isinstance(repository_id, int) \
            or repository_id <= 0 \
            or not isinstance(repository_node_id, str) \
            or re.fullmatch(r"R_[A-Za-z0-9_-]+", repository_node_id) is None:
        raise ValueError("repository immutable id/node_id is malformed")


def validate_repository_runtime_identity(
    value: Any, *, repository_id: str, owner_id: str
) -> None:
    validate_repository_identity(value, allow_unset=False)
    if re.fullmatch(r"[1-9][0-9]*", repository_id) is None \
            or re.fullmatch(r"[1-9][0-9]*", owner_id) is None:
        raise ValueError("runtime immutable repository and owner IDs must be positive decimals")
    if value["repository_id"] != int(repository_id) \
            or value["owner"]["id"] != int(owner_id):
        raise ValueError("runtime immutable identity drift")


def validate_third_party_artifact_policy(artifact_root: Path) -> None:
    if not artifact_root.is_absolute():
        raise ValueError("third-party artifact root must be absolute")
    artifact_root = artifact_root.resolve(strict=True)
    if not artifact_root.is_dir():
        raise ValueError("third-party artifact root must be a directory")

    policy_file = artifact_root / "tools/github/third-party-artifact-policy.json"
    policy = load_json(str(policy_file))
    if not isinstance(policy, dict) or set(policy) != {"version", "artifacts"}:
        raise ValueError("third-party artifact policy has missing or unexpected fields")
    if type(policy["version"]) is not int or policy["version"] != 1:
        raise ValueError("third-party artifact policy version must be integer 1")
    artifacts = policy["artifacts"]
    if not isinstance(artifacts, dict) or set(artifacts) != THIRD_PARTY_ARTIFACT_PATHS:
        raise ValueError("third-party artifact policy path allowlist drift")

    resolved_artifacts: dict[str, Path] = {}
    for relative_path, expected_hash in artifacts.items():
        normalized = PurePosixPath(relative_path)
        if (
            re.fullmatch(r"[A-Za-z0-9._/-]+", relative_path) is None
            or normalized.is_absolute()
            or normalized.as_posix() != relative_path
            or any(part in {"", ".", ".."} for part in normalized.parts)
        ):
            raise ValueError(
                f"third-party artifact policy path is unsafe: {relative_path!r}"
            )
        if not isinstance(expected_hash, str) \
                or re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None:
            raise ValueError(
                f"third-party artifact policy hash is malformed: {relative_path}"
            )
        artifact = (artifact_root / Path(*normalized.parts)).resolve(strict=True)
        try:
            artifact.relative_to(artifact_root)
        except ValueError as error:
            raise ValueError(
                f"third-party artifact escapes the repository root: {relative_path}"
            ) from error
        if not artifact.is_file():
            raise ValueError(f"third-party artifact is not a file: {relative_path}")
        actual_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(f"third-party artifact hash drift: {relative_path}")
        resolved_artifacts[relative_path] = artifact

    root_ofl = resolved_artifacts["LICENSES/OFL-1.1.txt"]
    bundled_ofl = resolved_artifacts[
        "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt"
    ]
    if root_ofl.read_bytes() != bundled_ofl.read_bytes():
        raise ValueError("canonical and bundled OFL-1.1 license copies differ")


def validate_legal_identity(
    value: Any,
    *,
    allow_unconfirmed: bool,
    root_license_path: str,
    proprietary_license_path: str,
    reuse_path: str,
) -> None:
    expected_fields = {
        "copyright_holder",
        "first_party_ownership_or_assignment_confirmed",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise ValueError("legal identity has missing or unexpected fields")

    holder = value["copyright_holder"]
    if (
        not isinstance(holder, str)
        or not holder
        or holder.strip() != holder
        or not holder.isprintable()
        or len(holder.splitlines()) != 1
    ):
        raise ValueError(
            "copyright_holder must be a safe non-empty single-line string"
        )

    confirmed = value["first_party_ownership_or_assignment_confirmed"]
    if not isinstance(confirmed, bool):
        raise ValueError(
            "first_party_ownership_or_assignment_confirmed must be a boolean"
        )

    root_license_file = Path(root_license_path)
    proprietary_license_file = Path(proprietary_license_path)
    reuse_file = Path(reuse_path)
    if (
        not root_license_file.is_absolute()
        or not proprietary_license_file.is_absolute()
        or not reuse_file.is_absolute()
    ):
        raise ValueError("root LICENSE, proprietary license, and REUSE paths must be absolute")

    canonical_root_license = """This repository is source-available proprietary software.

The authoritative license for first-party material is:

  LICENSES/LicenseRef-Proprietary.txt

Separately identified third-party material remains under its own license.
See THIRD_PARTY_NOTICES.md and REUSE.toml.
"""
    if root_license_file.read_text(encoding="utf-8") != canonical_root_license:
        raise ValueError(
            "root LICENSE must exactly delegate to the canonical proprietary license"
        )

    proprietary_license = proprietary_license_file.read_text(encoding="utf-8")
    license_lines = proprietary_license.splitlines()
    copyright_lines = [
        line for line in license_lines if line.startswith("Copyright (c) ")
    ]
    if len(copyright_lines) != 1:
        raise ValueError(
            "proprietary license must contain exactly one copyright line"
        )
    copyright_match = re.fullmatch(
        r"Copyright \(c\) (?P<years>\d{4}(?:-\d{4})?) "
        r"(?P<holder>.+)\. All rights reserved\.",
        copyright_lines[0],
    )
    if copyright_match is None or copyright_match.group("holder") != holder:
        raise ValueError("proprietary license copyright holder drift")

    canonical_proprietary_license = f"""Copyright (c) {copyright_match.group("years")} {holder}. All rights reserved.

This source code and its accompanying first-party materials are proprietary.
Except under a separate written agreement with the copyright holder, no
permission is granted to use, reproduce, modify, translate, publish,
distribute, sublicense, sell, deploy, publicly perform, publicly display, or
create derivative works from them.

Public availability on GitHub does not create an open-source license. Nothing
in this notice limits the rights necessarily granted to GitHub or exercised by
GitHub users solely through GitHub's service features under the GitHub Terms of
Service, including viewing and forking within the service. Those service-level
rights do not grant permission to use or distribute the software outside the
scope of those terms.

Separately identified third-party materials are excluded from this proprietary
license and remain governed by their respective license notices.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, TITLE, AND NON-INFRINGEMENT. IN NO EVENT
SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
LIABILITY ARISING FROM OR RELATED TO THE SOFTWARE OR ITS USE.
"""
    if proprietary_license != canonical_proprietary_license:
        raise ValueError("proprietary license body drift")

    with reuse_file.open("rb") as handle:
        reuse = tomllib.load(handle)
    expected_root_annotation = {
        "path": ["**"],
        "precedence": "override",
        "SPDX-FileCopyrightText": f'{copyright_match.group("years")} {holder}',
        "SPDX-License-Identifier": "LicenseRef-Proprietary",
    }
    expected_pretendard_annotation = {
        "path": [
            "products/gongzzang/apps/web/public/fonts/**/*.woff2",
            "products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css",
            "products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt",
        ],
        "precedence": "override",
        "SPDX-FileCopyrightText": "2021 Kil Hyung-jin",
        "SPDX-License-Identifier": "OFL-1.1",
    }
    if set(reuse) != {"version", "annotations"} \
            or type(reuse.get("version")) is not int \
            or reuse["version"] != 1:
        raise ValueError("REUSE.toml must contain only the canonical version and annotations")
    if reuse["annotations"] != [
        expected_root_annotation,
        expected_pretendard_annotation,
    ]:
        raise ValueError("REUSE.toml legal annotation allowlist drift")

    validate_third_party_artifact_policy(root_license_file.parent)

    if not confirmed and not allow_unconfirmed:
        raise ValueError(
            "first-party ownership or assignment is not confirmed; publication denied"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    canonical = subparsers.add_parser("canonical")
    canonical.add_argument("path")

    ruleset = subparsers.add_parser("normalize-ruleset")
    ruleset.add_argument("--without-status", action="store_true")
    ruleset.add_argument("path")

    contexts = subparsers.add_parser("required-contexts")
    contexts.add_argument("path")

    summaries = subparsers.add_parser("ruleset-summaries")
    summaries.add_argument("--expected", action="store_true")
    summaries.add_argument("paths", nargs="+")

    identity = subparsers.add_parser("validate-repository-identity")
    identity.add_argument("--allow-unset", action="store_true")
    identity.add_argument("path")

    runtime_identity = subparsers.add_parser(
        "validate-repository-runtime-identity"
    )
    runtime_identity.add_argument("path")
    runtime_identity.add_argument("repository_id")
    runtime_identity.add_argument("owner_id")

    legal_identity = subparsers.add_parser("validate-legal-identity")
    legal_identity.add_argument("--allow-unconfirmed", action="store_true")
    legal_identity.add_argument("path")
    legal_identity.add_argument("root_license_path")
    legal_identity.add_argument("proprietary_license_path")
    legal_identity.add_argument("reuse_path")
    return parser.parse_args()


def main() -> int:
    # Git Bash invokes the Windows Python build on developer hosts. Force LF so
    # canonical policy output is byte-identical to Linux CI output.
    sys.stdout.reconfigure(newline="\n")
    args = parse_args()
    try:
        if args.command == "ruleset-summaries":
            loaded = [load_json(path) for path in args.paths]
            values = loaded if args.expected else loaded[0]
            if not isinstance(values, list):
                raise ValueError("actual ruleset summary input must be a JSON array")
            canonical_dump(ruleset_summaries(values, expected=args.expected))
            return 0

        if args.command == "validate-repository-identity":
            validate_repository_identity(
                load_json(args.path), allow_unset=args.allow_unset
            )
            return 0

        if args.command == "validate-repository-runtime-identity":
            validate_repository_runtime_identity(
                load_json(args.path),
                repository_id=args.repository_id,
                owner_id=args.owner_id,
            )
            return 0

        if args.command == "validate-legal-identity":
            validate_legal_identity(
                load_json(args.path),
                allow_unconfirmed=args.allow_unconfirmed,
                root_license_path=args.root_license_path,
                proprietary_license_path=args.proprietary_license_path,
                reuse_path=args.reuse_path,
            )
            return 0

        value = load_json(args.path)
        if args.command == "canonical":
            canonical_dump(value)
        elif args.command == "normalize-ruleset":
            canonical_dump(
                normalize_ruleset(value, without_status=args.without_status)
            )
        elif args.command == "required-contexts":
            for context in required_contexts(value):
                print(context)
    except (KeyError, OSError, TypeError, UnicodeError, ValueError) as error:
        print(f"FAIL github-policy-json: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
