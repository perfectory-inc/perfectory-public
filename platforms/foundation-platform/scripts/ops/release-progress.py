#!/usr/bin/env python3
"""Appends one line to an approved cut-over's progress (root ADR-0433 §6).

    release-progress.py <approval id> <state> [key=value ...]

The run-now dispatcher (scheduled, refused, cancelled), the operator script (started, refused) and
the cut-over runner (step, finished) each say what happened to the approval, in one file per
approval: <dir>/<approval id>.jsonl, one JSON object per line, never rewritten. foundation-api reads
the directory (mounted read-only) and the console shows it. `at` is when the line was written, UTC.

The directory is FOUNDATION_RELEASE_PROGRESS_DIR, else /var/lib/foundation-jobs/releases. A line
that cannot be written is said on standard error and exits 70: the caller decides whether the run
goes on (the runner's evidence file still holds every step).
"""

from __future__ import annotations

import datetime
import json
import os
import re
import sys

APPROVAL = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
STATES = frozenset({"scheduled", "refused", "cancelled", "started", "step", "finished"})
KEY = re.compile(r"[a-z][a-z_]*")
# Integers stay integers in the line: the console sorts and sums them.
NUMBERS = frozenset({"seconds", "generation", "budget_minutes"})


def line(approval: str, state: str, pairs: list[str], now: datetime.datetime) -> str:
    if not APPROVAL.fullmatch(approval):
        raise ValueError(f"{approval[:40]!r} is not an approval id")
    if state not in STATES:
        raise ValueError(f"{state!r} is not one of {', '.join(sorted(STATES))}")
    record: dict[str, object] = {"at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "approval": approval, "state": state}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not KEY.fullmatch(key) or key in record:
            raise ValueError(f"{pair[:40]!r} is not a new key=value")
        record[key] = int(value) if key in NUMBERS else value
    return json.dumps(record, ensure_ascii=False, sort_keys=True)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 64
    try:
        text = line(argv[0], argv[1], argv[2:], datetime.datetime.now(datetime.timezone.utc))
    except ValueError as error:
        print(f"release-progress: {error}", file=sys.stderr)
        return 64
    directory = os.environ.get("FOUNDATION_RELEASE_PROGRESS_DIR", "/var/lib/foundation-jobs/releases")
    try:
        os.makedirs(directory, mode=0o755, exist_ok=True)
        with open(os.path.join(directory, f"{argv[0]}.jsonl"), "a", encoding="utf-8") as handle:
            handle.write(text + "\n")
    except OSError as error:
        print(f"release-progress: cannot append to {directory}/{argv[0]}.jsonl: {error}", file=sys.stderr)
        return 70
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
