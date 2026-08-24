#!/usr/bin/env python3
"""Reject static-release toolchain facts copied outside their JSON contract."""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping
from typing import Any


CONTRACT_RELATIVE = pathlib.PurePosixPath(
    "platforms/foundation-platform/config/static-release-toolchain.contract.json"
)
TOOL_VERSION = re.compile(
    r"(?ix)\b(?:martin(?:-cp)?|mbtiles|pmtiles)(?=$|[\s`'\"/:=@\\_-])"
    r"(?:[\s`'\"/:=@\\_-]+|version)*"
    r"v?\d+\.\d+(?:\.\d+)?(?![\d.])"
)


def _load_support() -> Any:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "tiles"))
    try:
        import static_release_toolchain_contract as support
    except ImportError as error:
        raise RuntimeError(f"contract support module is unavailable: {error}") from error
    return support


def _tracked_files(root: pathlib.Path) -> Iterable[pathlib.Path]:
    completed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    for raw in completed.stdout.split(b"\0"):
        if not raw:
            continue
        relative = pathlib.PurePosixPath(raw.decode("utf-8", errors="strict"))
        if relative == CONTRACT_RELATIVE:
            continue
        yield root.joinpath(*relative.parts)


def _contract_facts(contract: Mapping[str, Any], support: Any) -> set[str]:
    facts: set[str] = set()
    for distribution_name, distribution in contract["distributions"].items():
        oci = distribution["oci"]
        facts.add(oci["digest"])
        facts.add(support.image_ref(contract, distribution_name))
        for artifact in distribution["platforms"].values():
            facts.add(artifact["url"])
            facts.add(artifact["sha256"])
            for executable in artifact["executables"].values():
                facts.add(executable["sha256"])
    return facts


def _violations(path: pathlib.Path, facts: set[str]) -> Iterable[tuple[int, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return
    violations: set[tuple[int, str]] = set()
    for fact in facts:
        start = 0
        while (position := text.find(fact, start)) >= 0:
            violations.add((text.count("\n", 0, position) + 1, "copied contract fact"))
            start = position + len(fact)
    for match in TOOL_VERSION.finditer(text):
        violations.add(
            (text.count("\n", 0, match.start()) + 1, "tool version outside contract")
        )
    yield from sorted(violations)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    try:
        support = _load_support()
        contract = support.load_contract(root.joinpath(*CONTRACT_RELATIVE.parts))
        facts = _contract_facts(contract, support)
        failures: list[str] = []
        for path in _tracked_files(root):
            relative = path.relative_to(root).as_posix()
            for line_number, category in _violations(path, facts):
                failures.append(f"{relative}:{line_number}: {category}")
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"FAIL static-release-toolchain-ssot: {error}", file=sys.stderr)
        return 1

    if failures:
        print("FAIL static-release-toolchain-ssot", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print("OK static-release-toolchain-ssot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
