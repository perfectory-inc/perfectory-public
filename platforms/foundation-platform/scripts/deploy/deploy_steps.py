#!/usr/bin/env python3
"""Whether a release deploy step has work, by config/deploy-steps.contract.json (root ADR-0308).

    deploy_steps.py should-run <release> <state-dir> <step>   exit 0: run it (says why); 3: skip it (says why)
    deploy_steps.py ran <release> <state-dir> <step>          after a successful run: records its inputs
    deploy_steps.py budget <release> <step> <seconds>         a line on stderr when an `always` step took
                                                             longer than its budget_seconds

<release> is a Foundation Platform tree (/opt/foundation-platform/current); the contract is its own.
A step with `inputs` may skip while the fingerprint of those paths (worker_autodeploy.py's, one
definition) is the one its last successful run recorded in <state-dir>; a step never run here, an
input that is missing, or a record that cannot be read runs it. Any other exit status than 3 means
"run it": a caller never skips on an error.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from worker_autodeploy import platform_fingerprint  # noqa: E402

CONTRACT = "config/deploy-steps.contract.json"
SKIP = 3


def step_of(release: Path, step: str) -> dict:
    steps = json.loads((release / CONTRACT).read_text(encoding="utf-8"))["steps"]
    if step not in steps:
        raise KeyError(f"{step} is not a step in {CONTRACT}")
    return steps[step]


def record(state: Path, step: str) -> Path:
    return state / f"{step}.inputs"


def should_run(release: Path, state: Path, step: str) -> tuple[bool, str]:
    entry = step_of(release, step)
    if "inputs" not in entry:
        return True, f"{step} runs on every deploy: {entry['always']}"
    now = platform_fingerprint(release, entry["inputs"])
    try:
        before = record(state, step).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return True, f"{step}: no successful run recorded on this host"
    if before != now:
        return True, f"{step}: its inputs changed since its last successful run"
    return False, f"{step}: skipped, its inputs are the bytes its last successful run saw ({', '.join(entry['inputs'])})"


def ran(release: Path, state: Path, step: str) -> None:
    now = platform_fingerprint(release, step_of(release, step)["inputs"])
    state.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=state, delete=False, encoding="utf-8") as handle:
        handle.write(now + "\n")
    os.replace(handle.name, record(state, step))


def over_budget(release: Path, step: str, seconds: int) -> str | None:
    budget = int(step_of(release, step)["budget_seconds"])
    if seconds <= budget:
        return None
    return (f"foundation-deploy: {step} took {seconds} s, over its budget of {budget} s "
            f"({CONTRACT}, root ADR-0308); measure it again and fix it or the budget")


def main(argv: list[str]) -> int:
    try:
        if len(argv) == 4 and argv[0] == "should-run":
            run, reason = should_run(Path(argv[1]), Path(argv[2]), argv[3])
            print(reason)
            return 0 if run else SKIP
        if len(argv) == 4 and argv[0] == "ran":
            ran(Path(argv[1]), Path(argv[2]), argv[3])
            return 0
        if len(argv) == 4 and argv[0] == "budget":
            line = over_budget(Path(argv[1]), argv[2], int(argv[3]))
            if line:
                print(line, file=sys.stderr)
            return 0
    except Exception as error:  # noqa: BLE001 -- any failure means "run it", never "skip it"
        print(f"deploy-steps: {error}", file=sys.stderr)
        return 1
    print(__doc__, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
