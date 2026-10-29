#!/usr/bin/env python3
"""Whether a release deploy step has work, by config/deploy-steps.contract.json (root ADR-0308).

    deploy_steps.py should-run <release> <step>   exit 0: run it (says why); 3: skip it (says why)
    deploy_steps.py ran <release> <step>          after a successful run: records what it ran against
    deploy_steps.py forget <release> <step>       removes that record: the next deploy runs the step
                                                  (after a catalog restore, docs/runbooks/iceberg-snapshot-rollback.md)
    deploy_steps.py budget <release> <step> <seconds>
                                                  a line on stderr when an `always` step took longer
                                                  than its budget_seconds

<release> is a Foundation Platform tree (/opt/foundation-platform/current); the contract is its own,
and so is where the records live (its `state_directory`). A step with `inputs` may skip while what
it runs against is what its last successful run here recorded:

- the fingerprint of its input paths (worker_autodeploy.py's, one definition);
- `environment`: the values of the named, non-secret settings in the environment files its unit
  loads (only those names are read; a name that looks like a credential is refused), hashed;
- `release_jars`: the frozen jars the release's build.json names (SPARK_RELEASE_JARS), by digest;
- `reverify`: production's shape as shape_ledger_check last captured it. A contract table whose
  captured columns lack one of its contract's columns runs the step whatever the record says.

A step never run here, an input that is missing, or anything that cannot be read runs it. Any other
exit status than 3 means "run it": a caller never skips on an error. The records are root's: the
state directory is created 0700 and its parent must belong to the caller or root and be writable by
no one else (a record another account could write could make the deploy skip a migration).

FOUNDATION_DEPLOY_STEPS_HOST_ROOT moves the host's absolute paths (state directory, environment
files, shape capture) under another root; tests only.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from worker_autodeploy import platform_fingerprint  # noqa: E402

CONTRACT = "config/deploy-steps.contract.json"
SKIP = 3
# A setting named like a credential is never read, whatever the contract says.
SECRET_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSPHRASE|CREDENTIAL|_KEY(_|$)|ACCESS_KEY")


def contract_of(release: Path) -> dict:
    return json.loads((release / CONTRACT).read_text(encoding="utf-8"))


def step_of(release: Path, step: str) -> dict:
    steps = contract_of(release)["steps"]
    if step not in steps:
        raise KeyError(f"{step} is not a step in {CONTRACT}")
    return steps[step]


def on_host(host: Path, path: str) -> Path:
    return host / path.lstrip("/")


def state_dir(release: Path, host: Path, create: bool) -> Path:
    """The records' directory, made 0700 when asked; refused when its parent or itself could be
    written by another account."""
    state = on_host(host, contract_of(release)["state_directory"])
    me = os.geteuid()
    parent = os.stat(state.parent)
    if parent.st_uid not in (0, me) or parent.st_mode & 0o022:
        raise PermissionError(f"{state.parent} must belong to root and be writable by no one else")
    if create:
        state.mkdir(mode=0o700, exist_ok=True)
    info = os.lstat(state)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != me or info.st_mode & 0o077:
        raise PermissionError(f"{state} must be a directory of uid {me}, mode 0700")
    return state


def record(state: Path, step: str) -> Path:
    return state / f"{step}.inputs"


def environment_digest(release: Path, entry: dict, host: Path) -> str:
    """The named settings of the files the step's unit loads, later files winning as systemd does.
    Only lines that set one of the names are parsed; no value is kept or printed."""
    names = entry["names"]
    refused = [name for name in names if SECRET_NAME.search(name)]
    if refused:
        raise ValueError(f"{CONTRACT}: {', '.join(refused)} look like credentials and are never read")
    values: dict[str, str] = {}
    unit = (release / entry["unit"]).read_text(encoding="utf-8")
    for line in unit.splitlines():
        if not line.startswith("EnvironmentFile="):
            continue
        path = on_host(host, line.removeprefix("EnvironmentFile=").lstrip("-"))
        if not path.is_file():
            continue  # an optional file (-) that is absent; a required one fails the unit itself
        with path.open(encoding="utf-8") as handle:
            for setting in handle:
                name, sep, value = setting.rstrip("\n").partition("=")
                if sep and name.strip() in names:
                    value = value.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                        value = value[1:-1]
                    values[name.strip()] = value
    digest = hashlib.sha256()
    for name in sorted(names):
        digest.update(f"{name}={values[name]}\0".encode() if name in values else f"{name} unset\0".encode())
    return digest.hexdigest()


def jars_digest(release: Path) -> str:
    """The frozen jars of the release's build (admitted-writer-runtime.sh's SPARK_RELEASE_JARS)."""
    physical = release.resolve()
    build = physical.parent.parent / "artifacts" / physical.name / "build.json"
    if not build.is_file():
        return "no build.json"
    files = json.loads(build.read_text(encoding="utf-8")).get("files") or {}
    jars = sorted((name, digest) for name, digest in files.items() if name.startswith("jars/"))
    return hashlib.sha256(json.dumps(jars).encode()).hexdigest()


def fingerprint(release: Path, entry: dict, host: Path) -> str:
    parts = [f"inputs {platform_fingerprint(release, entry['inputs'])}"]
    if "environment" in entry:
        parts.append(f"environment {environment_digest(release, entry['environment'], host)}")
    if entry.get("release_jars"):
        parts.append(f"jars {jars_digest(release)}")
    if len(parts) == 1:
        return platform_fingerprint(release, entry["inputs"])
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def drifted_tables(release: Path, entry: dict, host: Path) -> list[str]:
    """The contract tables whose columns, as production last showed them (shape_ledger_check's
    capture), lack one their contract declares. No capture yet: none."""
    reverify = entry.get("reverify")
    if not reverify:
        return []
    capture = on_host(host, reverify["production_shape"])
    if not capture.is_file():
        return []
    shapes = json.loads(capture.read_text(encoding="utf-8"))["tables"]
    contracts = json.loads((release / reverify["contracts"]).read_text(encoding="utf-8"))["contracts"]
    drifted = []
    for table, contract in sorted(contracts.items()):
        if table in shapes:
            present = {field[1] for field in shapes[table]["fields"]}
            if any(column["name"] not in present for column in contract["columns"]):
                drifted.append(table)
    return drifted


def should_run(release: Path, step: str, host: Path = Path("/")) -> tuple[bool, str]:
    entry = step_of(release, step)
    if "inputs" not in entry:
        return True, f"{step} runs on every deploy: {entry['always']}"
    drifted = drifted_tables(release, entry, host)
    if drifted:
        return True, (f"{step}: production lacks contract columns of {len(drifted)} tables as last captured "
                      f"({', '.join(drifted[:5])}{', ...' if len(drifted) > 5 else ''})")
    now = fingerprint(release, entry, host)
    try:
        before = record(state_dir(release, host, create=False), step).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return True, f"{step}: no successful run recorded on this host"
    if before != now:
        return True, f"{step}: its inputs, settings or jars changed since its last successful run"
    return False, f"{step}: skipped, it runs against what its last successful run here saw ({', '.join(entry['inputs'])})"


def ran(release: Path, step: str, host: Path = Path("/")) -> None:
    now = fingerprint(release, step_of(release, step), host)
    state = state_dir(release, host, create=True)
    with tempfile.NamedTemporaryFile("w", dir=state, delete=False, encoding="utf-8") as handle:
        handle.write(now + "\n")
    os.replace(handle.name, record(state, step))


def forget(release: Path, step: str, host: Path = Path("/")) -> None:
    step_of(release, step)
    try:
        state = state_dir(release, host, create=False)
    except FileNotFoundError:
        return  # nothing was ever recorded here
    record(state, step).unlink(missing_ok=True)


def over_budget(release: Path, step: str, seconds: int) -> str | None:
    budget = int(step_of(release, step)["budget_seconds"])
    if seconds <= budget:
        return None
    return (f"foundation-deploy: {step} took {seconds} s, over its budget of {budget} s "
            f"({CONTRACT}, root ADR-0308); measure it again and fix it or the budget")


def main(argv: list[str]) -> int:
    host = Path(os.environ.get("FOUNDATION_DEPLOY_STEPS_HOST_ROOT") or "/")
    try:
        if len(argv) == 3 and argv[0] == "should-run":
            run, reason = should_run(Path(argv[1]), argv[2], host)
            print(reason)
            return 0 if run else SKIP
        if len(argv) == 3 and argv[0] == "ran":
            ran(Path(argv[1]), argv[2], host)
            return 0
        if len(argv) == 3 and argv[0] == "forget":
            forget(Path(argv[1]), argv[2], host)
            print(f"{argv[2]}: record removed; the next deploy runs it")
            return 0
        if len(argv) == 4 and argv[0] == "budget":
            line = over_budget(Path(argv[1]), argv[2], int(argv[3]))
            if line:
                print(line, file=sys.stderr)
            return 0
    except Exception as error:  # noqa: BLE001 -- any failure means "run it", never "skip it"
        print(f"deploy-steps: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    print(__doc__, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
