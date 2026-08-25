#!/usr/bin/env python3
"""Select Foundation CI's expensive integration gates from changed repository paths.

The workflow always starts. This module only decides whether a heavy job should do
its expensive work after checkout; an unselected job still finishes successfully so
the existing required/foundation result contract remains unchanged.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable, Mapping


GATES = (
    "boundary-slice",
    "kafka-integration",
    "compose-smoke",
    "static-release-toolchain-windows",
)

# Changes to dependency locks or the path-selection mechanism itself have an
# unbounded blast radius. Running every gate is the safe default.
RUN_ALL_RULE = {
    "exact": [
        ".cargo/config.toml",
        ".github/workflows/foundation-ci.yml",
        "platforms/foundation-platform/Cargo.toml",
        "rust-toolchain.toml",
        "scripts/ci/foundation_ci_scope.py",
    ],
    "basenames": ["Cargo.lock", "pnpm-lock.yaml"],
}

# This is the classification SSOT. Prefixes deliberately describe owned
# components rather than individual files: an added source file then inherits the
# correct gate without requiring a filter edit.
RULES = {
    "boundary-slice": {
        "exact": [
            "platforms/foundation-platform/config/static-release-toolchain.contract.json",
            "tools/container-images.env",
            "tools/verify-image/Dockerfile",
        ],
        "prefixes": [
            "platforms/foundation-platform/crates/catalog/",
            "platforms/foundation-platform/crates/foundation-contracts/",
            "platforms/foundation-platform/crates/foundation-outbox/",
            "platforms/foundation-platform/crates/foundation-shared-kernel/",
            "platforms/foundation-platform/crates/lakehouse/",
            "platforms/foundation-platform/crates/normalization/",
            "platforms/foundation-platform/migrations/",
            "platforms/foundation-platform/services/foundation-api/",
            "platforms/foundation-platform/services/foundation-outbox-publisher/",
            "scripts/tiles/",
        ],
    },
    "kafka-integration": {
        "exact": [
            "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml",
            "scripts/verify/foundation-kafka-live.sh",
        ],
        "prefixes": [
            "platforms/foundation-platform/crates/catalog/",
            "platforms/foundation-platform/crates/foundation-contracts/",
            "platforms/foundation-platform/crates/foundation-outbox/",
            "platforms/foundation-platform/crates/foundation-shared-kernel/",
            "platforms/foundation-platform/crates/lakehouse/",
            "platforms/foundation-platform/migrations/",
            "platforms/foundation-platform/schemas/",
            "platforms/foundation-platform/services/foundation-api/",
            "tools/xtask/",
        ],
    },
    "compose-smoke": {
        "exact": [
            "platforms/foundation-platform/compose.lakehouse.yml",
            "platforms/foundation-platform/compose.observability.yml",
            "platforms/foundation-platform/.dockerignore",
            "platforms/foundation-platform/docker-compose.yml",
            "platforms/foundation-platform/scripts/compose-smoke.sh",
        ],
        "prefixes": [
            "platforms/foundation-platform/crates/catalog/",
            "platforms/foundation-platform/crates/foundation-contracts/",
            "platforms/foundation-platform/crates/foundation-shared-kernel/",
            "platforms/foundation-platform/crates/lakehouse/",
            "platforms/foundation-platform/crates/normalization/",
            "platforms/foundation-platform/infra/compose/",
            "platforms/foundation-platform/infra/db/init/",
            "platforms/foundation-platform/migrations/",
            "platforms/foundation-platform/services/foundation-api/",
        ],
    },
    "static-release-toolchain-windows": {
        "exact": [
            "platforms/foundation-platform/config/static-release-toolchain.contract.json",
            "platforms/foundation-platform/services/foundation-outbox-publisher/Cargo.toml",
            "platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs",
            "platforms/foundation-platform/services/foundation-outbox-publisher/src/static_release_toolchain.rs",
            "scripts/tiles/static_release_toolchain_contract.py",
        ],
        "prefixes": [
            "platforms/foundation-platform/tests/static_release_toolchain/",
        ],
    },
}

# Independent witnesses are regression guards, not a second matcher. Every
# category must retain at least one concrete path whose removal from RULES makes
# validation fail before any CI decision is emitted.
REQUIRED_WITNESSES = {
    "boundary-slice": {
        "tile harness": "scripts/tiles/boundary-slice-proof.sh",
        "serving schema": "platforms/foundation-platform/migrations/20260819030646_project_industrial_complex_boundaries_into_postgis.sql",
        "publisher": "platforms/foundation-platform/services/foundation-outbox-publisher/src/industrial_complex_boundary_static_release_publish.rs",
        "object storage": "platforms/foundation-platform/crates/foundation-outbox/src/object_storage/file.rs",
        "tile domain": "platforms/foundation-platform/crates/catalog/catalog-domain/src/vector_tile.rs",
    },
    "kafka-integration": {
        "live harness": "scripts/verify/foundation-kafka-live.sh",
        "broker compose": "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml",
        "Kafka adapter": "platforms/foundation-platform/crates/foundation-outbox/src/kafka_broadcaster.rs",
        "wire schema": "platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc",
    },
    "compose-smoke": {
        "compose definition": "platforms/foundation-platform/docker-compose.yml",
        "build context": "platforms/foundation-platform/.dockerignore",
        "bootstrap contract": "platforms/foundation-platform/infra/compose/bootstrap-foundation.sql",
        "migration": "platforms/foundation-platform/migrations/20260719000001_foundation_platform_schema.sql",
        "service image": "platforms/foundation-platform/services/foundation-api/Dockerfile",
    },
    "static-release-toolchain-windows": {
        "tool contract": "platforms/foundation-platform/config/static-release-toolchain.contract.json",
        "installer": "scripts/tiles/static_release_toolchain_contract.py",
        "embedded verifier": "platforms/foundation-platform/services/foundation-outbox-publisher/src/static_release_toolchain.rs",
        "command entrypoint": "platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs",
    },
}


def normalize_path(raw_path: str) -> str:
    normalized = raw_path.replace("\\", "/").removeprefix("./")
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"changed path must be repository-relative: {raw_path!r}")
    return path.as_posix()


def _matches_rule(path: str, rule: Mapping[str, list[str]]) -> bool:
    return path in rule.get("exact", ()) or any(
        path.startswith(prefix) for prefix in rule.get("prefixes", ())
    )


def _runs_all(path: str) -> bool:
    return path in RUN_ALL_RULE["exact"] or PurePosixPath(path).name in RUN_ALL_RULE["basenames"]


def validate_rules(rules: Mapping[str, Mapping[str, list[str]]] = RULES) -> None:
    if set(rules) != set(GATES):
        raise ValueError(f"gate set drift: expected {sorted(GATES)}, got {sorted(rules)}")
    for gate, rule in rules.items():
        for kind in ("exact", "prefixes"):
            routes = rule.get(kind)
            if not routes or len(routes) != len(set(routes)):
                raise ValueError(f"{gate}.{kind} must be non-empty and duplicate-free")
            for route in routes:
                route_without_separator = route.removesuffix("/")
                normalized = normalize_path(route_without_separator)
                canonical = f"{normalized}/" if kind == "prefixes" else normalized
                if canonical != route:
                    raise ValueError(f"noncanonical route in {gate}.{kind}: {route!r}")
        for label, witness in REQUIRED_WITNESSES[gate].items():
            if not _matches_rule(witness, rule):
                raise ValueError(
                    f"required witness is not covered: gate={gate} category={label} path={witness}"
                )


def classify_paths(
    paths: Iterable[str], rules: Mapping[str, Mapping[str, list[str]]] = RULES
) -> set[str]:
    validate_rules(rules)
    normalized_paths = {normalize_path(path) for path in paths}
    if any(_runs_all(path) for path in normalized_paths):
        return set(GATES)
    return {
        gate
        for gate, rule in rules.items()
        if any(_matches_rule(path, rule) for path in normalized_paths)
    }


def git_changed_paths(repository: Path, base: str, head: str) -> set[str]:
    command = [
        "git",
        "-C",
        str(repository),
        "diff",
        "--name-only",
        "-z",
        "--no-renames",
        base,
        head,
    ]
    completed = subprocess.run(command, check=True, capture_output=True)
    return {
        os.fsdecode(raw_path)
        for raw_path in completed.stdout.split(b"\0")
        if raw_path
    }


def selected_gates(
    *,
    event_name: str,
    base: str,
    head: str,
    repository: Path | None = None,
    paths: Iterable[str] | None = None,
) -> set[str]:
    validate_rules()
    if event_name == "workflow_dispatch" or not base or not head or set(base) == {"0"}:
        return set(GATES)
    changed = set(paths) if paths is not None else git_changed_paths(repository or Path.cwd(), base, head)
    return classify_paths(changed)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", required=True, choices=GATES)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--github-env", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    fallback_reason = None
    try:
        selected = selected_gates(
            event_name=args.event_name,
            base=args.base,
            head=args.head,
            repository=args.repository,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        # A cost optimization must never turn an uncomparable change into a
        # silent pass. Run all gates when the comparison cannot be established.
        selected = set(GATES)
        fallback_reason = f"git comparison unavailable: {error}"

    gate_selected = args.gate in selected
    with args.github_env.open("a", encoding="utf-8", newline="\n") as github_env:
        github_env.write(f"FOUNDATION_CI_GATE_SELECTED={str(gate_selected).lower()}\n")

    print(
        json.dumps(
            {
                "gate": args.gate,
                "selected": gate_selected,
                "selected_gates": sorted(selected),
                "fallback_reason": fallback_reason,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
