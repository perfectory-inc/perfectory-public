#!/usr/bin/env python3
"""Check strict repository-identity gate ordering in public-root preparation."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def fail(message: str) -> None:
    print(f"FAIL repository-identity-prepare-policy: {message}", file=sys.stderr)
    raise SystemExit(1)


def unique_line(source: str, line: str, label: str) -> int:
    matches = list(re.finditer(rf"^{re.escape(line)}$", source, re.MULTILINE))
    if len(matches) != 1:
        fail(f"expected exactly one {label}")
    return matches[0].start()


def position(source: str, needle: str, label: str) -> int:
    offset = source.find(needle)
    if offset < 0:
        fail(f"missing {label}")
    return offset


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <prepare-public-root.sh>", file=sys.stderr)
        return 2

    try:
        source = Path(sys.argv[1]).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        fail(str(error))

    if "--allow-unset" in source:
        fail("prepare must never relax repository identity")

    control_strict = unique_line(
        source,
        'bash "$control_repository_identity_validator"',
        "control-root strict gate",
    )
    candidate_strict = unique_line(
        source,
        'bash "$candidate_repository_identity_validator"',
        "candidate strict gate",
    )
    clone_strict = unique_line(
        source,
        "  bash scripts/github/validate-public-repository-identity.sh",
        "history-free clone strict gate",
    )
    control_legal = position(
        source, 'bash "$control_legal_validator"', "control-root legal gate"
    )
    index_flags = position(source, "ls-files -v", "source index flag inventory")
    status = position(
        source,
        "status --porcelain=v1 --untracked-files=all",
        "source status gate",
    )
    candidate_legal = position(
        source, 'bash "$candidate_legal_validator"', "candidate legal gate"
    )
    builder = position(
        source, '"$root/scripts/github/build-public-root.sh"', "root builder"
    )
    clone = position(source, "clone --quiet --no-local", "history-free clone")
    clone_legal = position(
        source,
        "  bash scripts/github/validate-legal-publication.sh",
        "history-free clone legal gate",
    )
    monorepo = position(
        source, "  bash scripts/guard/monorepo-guard.sh", "monorepo guard"
    )

    if not (
        control_strict
        < control_legal
        < index_flags
        < status
        < candidate_strict
        < candidate_legal
        < builder
        < clone
        < clone_strict
        < clone_legal
        < monorepo
    ):
        fail("strict control/candidate/clone gates or publication checks are reordered")

    print("OK repository-identity-prepare-policy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
