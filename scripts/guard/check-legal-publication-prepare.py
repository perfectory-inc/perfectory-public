#!/usr/bin/env python3
"""Check strict legal-gate ordering in history-free root preparation."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def fail(message: str) -> None:
    print(f"FAIL legal-publication-prepare-policy: {message}", file=sys.stderr)
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

    if "--allow-unconfirmed" in source:
        fail("prepare must never relax legal confirmation")

    control_strict = unique_line(
        source, 'bash "$control_legal_validator"', "control-root strict gate"
    )
    candidate_strict = unique_line(
        source, 'bash "$candidate_legal_validator"', "candidate strict gate"
    )
    clone_strict = unique_line(
        source,
        "  bash scripts/github/validate-legal-publication.sh",
        "history-free clone strict gate",
    )
    index_flags = position(source, "ls-files -v", "source index flag inventory")
    hidden_flag_rejection = position(
        source,
        "source index contains skip-worktree or assume-unchanged entries",
        "hidden index flag rejection",
    )
    status = position(
        source,
        "status --porcelain=v1 --untracked-files=all",
        "source status gate",
    )
    builder = position(
        source, '"$root/scripts/github/build-public-root.sh"', "root builder"
    )
    clone = position(source, "clone --quiet --no-local", "history-free clone")
    monorepo = position(
        source, "bash scripts/guard/monorepo-guard.sh", "monorepo guard"
    )
    reuse = position(source, "CI=true bash scripts/ci/reuse-lint.sh", "REUSE lint")
    final_scan = position(
        source, "bash scripts/ci/gitleaks-scan.sh all .", "history secret scan"
    )

    if not (
        control_strict
        < index_flags
        <= hidden_flag_rejection
        < status
        < candidate_strict
        < builder
        < clone
        < clone_strict
        < monorepo
        < reuse
        < final_scan
    ):
        fail("strict control/candidate/clone gates or publication checks are reordered")

    print("OK legal-publication-prepare-policy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
