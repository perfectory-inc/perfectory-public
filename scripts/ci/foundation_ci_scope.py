#!/usr/bin/env python3
"""Select Foundation CI's expensive integration gates from changed repository paths.

The workflow always starts. Its `scope` job asks `--skipped-jobs` which jobs have no work, and
those never take a runner (root ADR-0324); required/foundation admits exactly those as skipped.
A job that runs still asks `--gate` for itself and guards every step, as before.
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
    "spark-runtime",
    "static-release-toolchain-windows",
)

# Gates a pull request leaves to the merge queue (root ADR-0199). The boundary slice compiles the
# publisher cold inside a container and took 15 of a pull request's 21 minutes on 2026-10-12; the
# merge group, a push to main and a manual run still select it by path exactly as before, so
# nothing reaches main without it. A pull request only waits less for its first verdict.
MERGE_QUEUE_ONLY = frozenset({"boundary-slice"})

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
            # The publisher's lane library crates (root ADR-0200) build into the same executable.
            "platforms/foundation-platform/crates/publisher/",
            "platforms/foundation-platform/migrations/",
            "platforms/foundation-platform/services/foundation-api/",
            "platforms/foundation-platform/services/foundation-outbox-publisher/",
            "scripts/tiles/",
        ],
    },
    "kafka-integration": {
        "exact": [
            "platforms/intelligence-platform/docker/c2-event-backbone.compose.yml",
            "tools/xtask/src/live.rs",
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
    "spark-runtime": {
        "exact": [
            "platforms/foundation-platform/compose.lakehouse.yml",
            "platforms/foundation-platform/infra/lakehouse/contracts/lakehouse-engine.contract.json",
            # Which files each shard runs (root ADR-0365): a change here runs them all.
            "scripts/ci/foundation-spark-runtime-durations.json",
            "scripts/ci/foundation_spark_shards.py",
            "scripts/ci/test_foundation_spark_runtime_files.py",
            "scripts/ci/test_foundation_spark_shards.py",
            "scripts/verify/foundation-spark-runtime-files.sh",
            "scripts/verify/foundation-spark-runtime.sh",
        ],
        "prefixes": [
            # A by-PNU document record holds the keys Spark writes into the documents' JSON.
            "platforms/foundation-platform/contracts/by-pnu-documents/",
            "platforms/foundation-platform/infra/lakehouse/spark/",
        ],
    },
    "static-release-toolchain-windows": {
        "exact": [
            "platforms/foundation-platform/config/static-release-toolchain.contract.json",
            # The embedded verifier is built from the tile lane crate (root ADR-0200).
            "platforms/foundation-platform/crates/publisher/foundation-publisher-tiles/Cargo.toml",
            "platforms/foundation-platform/services/foundation-outbox-publisher/Cargo.toml",
            "platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs",
            "platforms/foundation-platform/crates/publisher/foundation-publisher-tiles/src/static_release_toolchain.rs",
            "scripts/tiles/static_release_toolchain_contract.py",
        ],
        "prefixes": [
            "platforms/foundation-platform/tests/static_release_toolchain/",
        ],
    },
}

# The jobs `--skipped-jobs` may name besides the gates (root ADR-0324). Each has no work when:
# rust-quality and postgres-integration: the change reaches no crate (`change_scope.py crates`);
# suites: it selects no Python or Node suite (`change_scope.py suites`); supply-chain: it changes
# no manifest, lock, cargo-deny policy or pinned tool (SUPPLY_CHAIN_RULE).
CRATE_JOBS = ("rust-quality", "postgres-integration")
SUITE_JOB = "suites"
SUPPLY_CHAIN_JOB = "supply-chain"
# Root ADR-0345: the release build as a dry run (no attestation, no push). It has work exactly when
# merging would give main a release build: the release workflow's own scope (root ADR-0309,
# tools/ci/change-scope.json), asked of the same change. A pull request and the merge queue run it;
# a push to main and the nightly run leave it to the release workflow, which builds main for real.
# 2026-10-29: #939 compiled in a file the publisher image lacked; every pull request passed and
# every release build on main failed for 11 hours.
RELEASE_JOB = "release-image"
RELEASE_WORKFLOW = "foundation-release-build.yml"
RELEASE_EVENTS = frozenset({"pull_request", "merge_group", "workflow_dispatch"})
WORKSPACE = "platforms/foundation-platform"
# What cargo-deny and the SBOM read, besides every Cargo.toml of the workspace and any lock (run
# all). Advisories change without a commit: the nightly full run and dependency-advisories.yml.
SUPPLY_CHAIN_RULE = {
    "exact": [
        "platforms/foundation-platform/deny.toml",
        "tools/cargo-deny.env",
        "tools/syft.env",
    ],
    "prefixes": ["tools/xtask/"],
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
        "R2 key layout": "platforms/foundation-platform/crates/publisher/foundation-publisher-core/src/r2_layout.rs",
    },
    "kafka-integration": {
        "live harness": "tools/xtask/src/live.rs",
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
    "spark-runtime": {
        "runner": "scripts/verify/foundation-spark-runtime.sh",
        "file runner": "scripts/verify/foundation-spark-runtime-files.sh",
        "image": "platforms/foundation-platform/compose.lakehouse.yml",
        "engine versions": "platforms/foundation-platform/infra/lakehouse/contracts/lakehouse-engine.contract.json",
        "Iceberg commit test": "platforms/foundation-platform/infra/lakehouse/spark/tests/test_gold_rebuild_iceberg.py",
        "job": "platforms/foundation-platform/infra/lakehouse/spark/jobs/gold_rebuild.py",
    },
    "static-release-toolchain-windows": {
        "tool contract": "platforms/foundation-platform/config/static-release-toolchain.contract.json",
        "installer": "scripts/tiles/static_release_toolchain_contract.py",
        "embedded verifier": "platforms/foundation-platform/crates/publisher/foundation-publisher-tiles/src/static_release_toolchain.rs",
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
        # Three dots: what the head added since it left the base. Two dots compared the trees, so
        # every file main changed after the branch point counted as this change (#700 ran all
        # gates, Spark included, for a publisher crate, because main had moved a workflow).
        f"{base}...{head}",
    ]
    completed = subprocess.run(command, check=True, capture_output=True)
    return {
        os.fsdecode(raw_path)
        for raw_path in completed.stdout.split(b"\0")
        if raw_path
    }


def path_selected_gates(
    *,
    event_name: str,
    base: str,
    head: str,
    repository: Path | None = None,
    paths: Iterable[str] | None = None,
) -> set[str]:
    """The gates the changed paths call for, before any is left to the merge queue."""
    validate_rules()
    if event_name == "workflow_dispatch" or not base or not head or set(base) == {"0"}:
        return set(GATES)
    changed = set(paths) if paths is not None else git_changed_paths(repository or Path.cwd(), base, head)
    return classify_paths(changed)


def deferred_gates(event_name: str, path_selected: set[str]) -> set[str]:
    """The path-selected gates a pull request leaves to the merge queue (root ADR-0199)."""
    return path_selected & MERGE_QUEUE_ONLY if event_name == "pull_request" else set()


def selected_gates(
    *,
    event_name: str,
    base: str,
    head: str,
    repository: Path | None = None,
    paths: Iterable[str] | None = None,
) -> set[str]:
    by_path = path_selected_gates(
        event_name=event_name, base=base, head=head, repository=repository, paths=paths
    )
    return by_path - deferred_gates(event_name, by_path)


def deferral_warning(gate: str) -> str:
    """A workflow annotation, so the pull request page says the gate did not run here."""
    return (
        f"::warning title={gate} deferred to the merge queue::This pull request changes inputs of "
        f"{gate}, which pull requests leave to the merge queue (root ADR-0199). It has not run on "
        "this pull request; the merge group runs it before anything reaches main."
    )


def supply_chain_selected(paths: Iterable[str] | None, crates_all: bool) -> bool:
    if paths is None or crates_all:
        return True
    for raw in paths:
        path = normalize_path(raw)
        if _runs_all(path) or _matches_rule(path, SUPPLY_CHAIN_RULE):
            return True
        if path.startswith(f"{WORKSPACE}/") and PurePosixPath(path).name == "Cargo.toml":
            return True
    return False


def release_image_selected(event_name: str, paths: Iterable[str] | None) -> bool:
    """Whether the change would get a release build on main (RELEASE_JOB); unknown paths: yes."""
    if event_name not in RELEASE_EVENTS:
        return False
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import change_scope

    run, _ = change_scope.decide(
        change_scope.load_contract(), RELEASE_WORKFLOW, event_name, None if paths is None else set(paths)
    )
    return run


def skipped_jobs(
    *, selected: set[str], paths: Iterable[str] | None, crates_answer: str, suites_answer: str,
    event_name: str = "pull_request",
) -> list[str]:
    """The Foundation CI jobs with no work (root ADR-0324), from the gate selection and the crate
    and suite selectors' answers. An empty (unreadable) answer skips none of its jobs."""
    skipped = {gate for gate in GATES if gate not in selected}
    if not release_image_selected(event_name, paths):
        skipped.add(RELEASE_JOB)
    crate_lines = crates_answer.splitlines()
    crates_kind = crate_lines[0].split("\t", 1)[0] if crate_lines else ""
    if crates_kind == "some" and not any(line.startswith("affected\t") for line in crate_lines):
        skipped.update(CRATE_JOBS)
    suite_lines = suites_answer.splitlines()
    if len(suite_lines) == 1 and suite_lines[0].split("\t", 1)[0] == "some":
        skipped.add(SUITE_JOB)
    if not supply_chain_selected(paths, crates_kind != "some"):
        skipped.add(SUPPLY_CHAIN_JOB)
    return sorted(skipped)


def _selector(command: str) -> str:
    """One `change_scope.py` answer for the workspace; empty when unreadable."""
    try:
        return subprocess.run(
            [sys.executable, str(Path(__file__).with_name("change_scope.py")), command, "--workspace", WORKSPACE],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"foundation-ci-scope: {command} selection unreadable, its jobs run: {error}", file=sys.stderr)
        return ""


def skipped_jobs_main(args: argparse.Namespace) -> int:
    skipped: list[str] = []
    if args.affected == "true":
        comparable = args.event_name != "workflow_dispatch" and args.base and args.head and set(args.base) != {"0"}
        try:
            paths = git_changed_paths(args.repository, args.base, args.head) if comparable else None
            selected = selected_gates(event_name=args.event_name, base=args.base, head=args.head, paths=paths)
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"foundation-ci-scope: git comparison unavailable, every gate runs: {error}", file=sys.stderr)
            paths, selected = None, set(GATES)
        skipped = skipped_jobs(
            selected=selected, paths=paths, crates_answer=_selector("crates"), suites_answer=_selector("suites"),
            event_name=args.event_name,
        )
    print(json.dumps({"affected": args.affected, "skipped_jobs": skipped}, sort_keys=True))
    with args.github_output.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"skipped={json.dumps(skipped)}\n")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--gate", choices=GATES)
    mode.add_argument("--skipped-jobs", action="store_true", help="write skipped=<JSON list> to --github-output")
    parser.add_argument("--affected", choices=("true", "false"), help="the scope job's answer, for --skipped-jobs")
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--github-env", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    if args.gate and not args.github_env:
        parser.error("--gate needs --github-env")
    if args.skipped_jobs and (not args.github_output or not args.affected):
        parser.error("--skipped-jobs needs --affected and --github-output")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.skipped_jobs:
        return skipped_jobs_main(args)
    fallback_reason = None
    deferred: set[str] = set()
    try:
        by_path = path_selected_gates(
            event_name=args.event_name,
            base=args.base,
            head=args.head,
            repository=args.repository,
        )
        deferred = deferred_gates(args.event_name, by_path)
        selected = by_path - deferred
    except (OSError, subprocess.CalledProcessError) as error:
        # A cost optimization must never turn an uncomparable change into a
        # silent pass. Run all gates when the comparison cannot be established.
        selected = set(GATES)
        fallback_reason = f"git comparison unavailable: {error}"

    gate_selected = args.gate in selected
    if args.gate in deferred:
        print(deferral_warning(args.gate))
    with args.github_env.open("a", encoding="utf-8", newline="\n") as github_env:
        github_env.write(f"FOUNDATION_CI_GATE_SELECTED={str(gate_selected).lower()}\n")

    print(
        json.dumps(
            {
                "gate": args.gate,
                "selected": gate_selected,
                "selected_gates": sorted(selected),
                "deferred_to_merge_queue": sorted(deferred),
                "fallback_reason": fallback_reason,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
