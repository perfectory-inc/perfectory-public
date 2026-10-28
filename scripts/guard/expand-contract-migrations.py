#!/usr/bin/env python3
"""A changed migration never does an expand-contract-unsafe thing unguarded (ADR-0299 계열 E, ADR-0300 P1).

Schema change is expand -> backfill -> contract. Two shapes break a rolling deploy's `migrate` step and
each must carry proof the author sequenced it safely:

  * ADD COLUMN ... NOT NULL with no DEFAULT. Postgres refuses to add a NOT NULL column with no default
    to a table that already holds rows, so the migrate step aborts. Acceptable only with a DEFAULT (the
    one safe one-step form), a two-step backfill (a later `SET NOT NULL`), an emptiness/approval guard
    (`RAISE EXCEPTION` or `current_setting(...)` in a DO block, the way the catalog does it), or an
    explicit `expand-contract-ok:` marker comment acknowledging the table is empty at first deploy.
  * DROP TABLE / DROP COLUMN / ALTER ... RENAME. Dropping or renaming an object that running code still
    reads breaks it mid-deploy; the contract step must come after the readers are gone. Acceptable only
    behind the approval gate migration-approval-gates.py already enforces (`current_setting(...)`, which
    that guard then requires to be declared and set in production) or an explicit `expand-contract-ok:`
    marker. We do NOT re-check the gate's declaration here — that stays this repo's one home for it.

Scope: only the migrations a change adds or edits, chosen by the wrapper from the pull request's diff.
A migration that already merged has already run against production and cannot be un-run; migrations are
append-only, so re-litigating the whole tree would only flag history (see the guard's self-test and the
pull request body for the pre-existing shapes this rule would otherwise name). The rule stays
conservative — it prefers a false negative to blocking a legitimate migration.

What real incident does failing this prevent? On 2026-10-13 #469 made a serving column required with no
backfill; the deploy's migrate step refused it and the API stayed down until it was reverted. The same
class in Postgres is an `ADD COLUMN ... NOT NULL` with no default and no backfill.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

GUARD = "expand-contract-migrations"
LINE_COMMENT = re.compile(r"--[^\n]*")
# One ADD COLUMN clause: its name, then its definition up to the next clause or statement end.
ADD_COLUMN = re.compile(
    r"\bADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\"?[A-Za-z_]\w*\"?)\s+(.*?)(?=,\s*(?:ADD|DROP|ALTER)\b|;|\Z)",
    re.IGNORECASE | re.DOTALL,
)
NOT_NULL = re.compile(r"\bNOT\s+NULL\b", re.IGNORECASE)
DEFAULT = re.compile(r"\bDEFAULT\b", re.IGNORECASE)
DESTRUCTIVE = re.compile(r"\b(DROP\s+TABLE|DROP\s+COLUMN|RENAME\s+TO|RENAME\s+COLUMN)\b", re.IGNORECASE)
# The acknowledgments that make an otherwise-risky shape safe.
EMPTINESS_GUARD = re.compile(r"\bRAISE\s+EXCEPTION\b", re.IGNORECASE)  # aborts on existing rows
APPROVAL_GATE = re.compile(r"\bcurrent_setting\s*\(", re.IGNORECASE)   # the operator-approval mechanism
TWO_STEP_BACKFILL = re.compile(r"\bSET\s+NOT\s+NULL\b", re.IGNORECASE)  # added nullable, filled, then set
MARKER = re.compile(r"expand-contract-ok\b", re.IGNORECASE)            # explicit author acknowledgment


def problems(root: Path, files: list[str]) -> list[str]:
    out: list[str] = []
    for raw in files:
        path = Path(raw)
        if not path.is_absolute():
            path = root / raw
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            name = path.relative_to(root).as_posix()
        except ValueError:
            name = path.name
        # Scan SQL with line comments stripped; keep the full text for the marker (it lives in a comment).
        code = LINE_COMMENT.sub("", text)
        guarded_add = bool(
            EMPTINESS_GUARD.search(code)
            or APPROVAL_GATE.search(code)
            or TWO_STEP_BACKFILL.search(code)
            or MARKER.search(text)
        )
        for col, definition in ADD_COLUMN.findall(code):
            clause = f"{col} {definition}"
            if NOT_NULL.search(clause) and not DEFAULT.search(clause) and not guarded_add:
                out.append(
                    f"{name}: ADD COLUMN {col.strip(chr(34))} is NOT NULL with no DEFAULT and no backfill, "
                    "emptiness guard, approval gate, or `expand-contract-ok:` marker (the #469 class)"
                )
        destructive = DESTRUCTIVE.search(code)
        if destructive and not (APPROVAL_GATE.search(code) or MARKER.search(text)):
            op = re.sub(r"\s+", " ", destructive.group(1)).upper()
            out.append(
                f"{name}: {op} is destructive with no approval gate or `expand-contract-ok:` marker "
                "(expand-contract: drop or rename only after the readers are gone)"
            )
    return out


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else Path.cwd()
    found = problems(root, argv[2:])
    for problem in found:
        print(f"FAIL {GUARD}: {problem}", file=sys.stderr)
    if found:
        return 1
    print(f"OK {GUARD} (migrations checked={len(argv[2:])})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
