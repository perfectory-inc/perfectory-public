#!/usr/bin/env python3
"""Production's table shapes, captured and published where CI reads them (root ADR-0259 §5, ADR-0476).

CI gates every pull request on two ledgers: the lakehouse's (lakehouse-production-shape.json) and
Postgres's (postgres-production-shape.json). This job captures both, read-only, right after each
deploy (started_once_after_deploy) and daily after the loads, and publishes them to the shape ledger
bucket (foundation-platform-shape-ledger-prod), which CI reads anonymously through its public read
domain: no copy into a pull request, no GitHub token, no App (root ADR-0476 supersedes ADR-0279).

The bucket is append-only: each distinct shape is one object under shape-ledger/objects/, named by
the SHA-256 of the shape (the capture without its time), written only when new (If-None-Match) and
held by the bucket's lock rule; shape-ledger/latest.json is the one mutable head, naming the current
pair and when production last confirmed it, and shape-ledger/history/ keeps every head whose pair
changed. The writer key is minted per run by the unit's root ExecStartPre (shape-ledger-writer-key.py):
read and write under shape-ledger/ of that bucket only, for 20 minutes.

A run that cannot publish waits (root ADR-0260); with nothing published for 48 hours
(first-unpublished.json) it fails and alerts, since CI is then judging against an older production.
How production moved since the last capture is named, never failed: it is what CI reads next. The
fresh captures also stay in /var/lib/foundation-shape-ledger/ (the unit's StateDirectory), which the
deploy's lakehouse-migrate step reads (config/deploy-steps.contract.json reverify).

It also reads, from the same metadata tables and no data file, how each table its readers prune by
key is laid out and was last written (root ADR-0314): a one-region read that opens most of the
table's files, or a commit that added far more of its bytes than the share of keys it says it
changed, fails the run at once. A rewrite that does not say what it changed is named.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RELEASE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_RELEASE", "/opt/foundation-platform/current"))
STATE = Path(os.environ.get("FOUNDATION_SHAPE_LEDGER_STATE", "/var/lib/foundation-shape-ledger"))
LAKEHOUSE = "infra/lakehouse/contracts/lakehouse-production-shape.json"
POSTGRES = "config/postgres-production-shape.json"


def differences(committed: dict, live: dict, key: str) -> list[str]:
    """Each table whose shape differs between the two ledgers, by name. `key` is what holds the
    shapes: a mapping of table to shape, or a list of table names."""
    old, new = committed[key], live[key]
    if isinstance(old, list):
        return ([f"{table}: now holds rows" for table in sorted(set(new) - set(old))]
                + [f"{table}: no longer holds rows" for table in sorted(set(old) - set(new))])
    return ([f"{table}: new in production" for table in sorted(set(new) - set(old))]
            + [f"{table}: gone from production" for table in sorted(set(old) - set(new))]
            + [f"{table}: shape changed" for table in sorted(set(old) & set(new)) if old[table] != new[table]])


def release_module(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, RELEASE / "scripts/ops" / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_capture():
    return release_module("lakehouse_production_shape", "lakehouse-production-shape.py")


# The ledger bucket (root ADR-0476): each shape, the head and the heads' history.
OBJECTS = "shape-ledger/objects/"
HEAD = "shape-ledger/latest.json"
HISTORY = "shape-ledger/history/"
HEAD_SCHEMA = "foundation-platform.shape_ledger_head.v1"
DECISION = "docs/adr/0476-production-shape-ledgers-are-published-by-the-host-and-ci-reads-them-with-no-copy.md"
WRITER_KEY = "writer.json"  # in the unit's RuntimeDirectory, minted by shape-ledger-writer-key.py
PUBLISHED = "published.json"
# A capture's own time is not part of its shape: the same shape captured again is the same object.
CAPTURE_TIME = ("captured_at",)


def shape_digest(text: str) -> str:
    """The SHA-256 of a capture without its time, as canonical JSON: the name of its object."""
    document = {key: value for key, value in json.loads(text).items() if key not in CAPTURE_TIME}
    return hashlib.sha256(json.dumps(document, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def object_key(name: str, digest: str) -> str:
    return f"{OBJECTS}{Path(name).stem}/{digest}.json"


def writer_key() -> dict | None:
    directory = os.environ.get("RUNTIME_DIRECTORY", "").split(":")[0]
    if not directory:
        return None
    try:
        return json.loads((Path(directory) / WRITER_KEY).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def publish(files: dict[str, str], now: datetime, previous: dict | None) -> tuple[str, dict | None, bool]:
    """(what happened, the head now published or None, whether a new shape object was written).

    Each capture whose shape the bucket lacks is written once; the head is written every run, so CI
    knows when production last confirmed it, and goes to the history too when its pair differs from
    `previous`, the head this host last published. A failed or impossible upload never fails the run
    by itself: the 48 hours of first-unpublished.json do."""
    key = writer_key()
    if key is None:
        return "not published (no writer key was minted for this run; the unit's ExecStartPre says why)", None, False
    r2 = release_module("r2_object", "r2_object.py")
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    head = {"schema_version": HEAD_SCHEMA, "decision": DECISION, "checked_at": stamp,
            "release": RELEASE.resolve().name, "ledgers": {}}

    def put(name: str, body: bytes, only_new: bool) -> bool:
        return r2.put(key["endpoint"], key["bucket"], name, key["access_key_id"], key["secret_access_key"], body,
                      session_token=key["session_token"], only_new=only_new)

    written = False
    try:
        for name, text in sorted(files.items()):
            digest = shape_digest(text)
            head["ledgers"][name] = {"key": object_key(name, digest), "shape_sha256": digest}
            written = put(object_key(name, digest), text.encode("utf-8"), True) or written
        body = (json.dumps(head, indent=1, sort_keys=True) + "\n").encode("utf-8")
        if previous is None or previous.get("ledgers") != head["ledgers"]:
            put(f"{HISTORY}{stamp}.json", body, True)
        put(HEAD, body, False)
    except OSError as error:
        return f"not published ({error})", None, False
    return (f"published to {key['bucket']}/{HEAD} ({'a new shape' if written else 'shapes it already held'})",
            head, written)


# Layout and write amplification (root ADR-0314 §3, §4). A table its readers prune by key is one
# whose contract writes it range-distributed by its sort order: the leading sort column is its
# cluster key. The layout measure is the one lakehouse_maintenance re-sorts a table on (root
# ADR-0344 §5), loaded from beside this script in the release tree.
CONTRACTS = "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"
_LAYOUT = importlib.util.spec_from_file_location(
    "table_layout", Path(__file__).resolve().parents[2] / "infra/lakehouse/spark/jobs/table_layout.py")
table_layout = importlib.util.module_from_spec(_LAYOUT)
_LAYOUT.loader.exec_module(table_layout)
REGION_DIGITS = table_layout.REGION_DIGITS
REGION_READ_SHARE_LIMIT = table_layout.REGION_READ_SHARE_LIMIT
region_read_share = table_layout.region_read_share
# A commit that changed a share of the keys may add at most this many times that share of the
# table's bytes; the share is floored, so a small table or change is not judged on noise. On
# 2026-10-26 a 0.12% change added 100% of gold.parcel_panel's bytes.
AMPLIFICATION_LIMIT = 10
KEY_SHARE_FLOOR = 0.01
# The commit's own count of the keys it changed, a snapshot summary property its writer sets
# (root ADR-0314 §4); a commit that lacks it cannot be judged and is named instead.
KEYS_CHANGED = "foundation.keys-changed"
# A derived change log beside its table (root ADR-0191 §1): exempt from the layout judgment.
CHANGE_LOG_SUFFIX = "__changes"


def clustered_tables(contracts: dict) -> dict[str, str]:
    """Each table written range-distributed by a cluster key the check knows a region of, with
    that key. Only contract tables: a derived change log (`gold.<table>__changes`, root ADR-0191
    §1) has no contract and is not judged — it is appended one commit per change set and read by
    snapshot, never pruned by key range, so neither its layout nor its byte share says anything."""
    return {name: spec["sort_order"][0] for name, spec in sorted(contracts.items())
            if not name.endswith(CHANGE_LOG_SUFFIX)
            if spec.get("write_distribution") == "range" and spec.get("sort_order")
            and spec["sort_order"][0] in REGION_DIGITS}


def amplification(summary: dict) -> tuple[float, float | None]:
    """(share of the table's bytes the commit added, share of its keys it says it changed)."""
    added, total = int(summary.get("added-files-size", 0)), int(summary.get("total-files-size", 0))
    byte_share = added / total if total else 0.0
    keys, records = summary.get(KEYS_CHANGED), int(summary.get("total-records", 0))
    return byte_share, (int(keys) / records if keys is not None and records else None)


def layout_findings(table: str, key: str, bounds: list[tuple[str, str]], summary: dict | None,
                    operation: str | None = None) -> tuple[list[str], list[str]]:
    """(failures, notes) for one clustered table: its files' key ranges, and its last data commit
    (`summary` and Iceberg `operation`, None for a table with no commit after its first).

    A whole `overwrite` is the full rebuild, a declared exception (root ADR-0314): it says how many
    keys it changed since root ADR-0347, and the check names that share instead of failing it."""
    failures, notes = [], []
    share, limit = region_read_share(bounds, REGION_DIGITS[key]), table_layout.read_share_limit(len(bounds))
    if share > limit:
        failures.append(f"{table}: a one-region read of {key} opens {share:.0%} of its {len(bounds)} data files "
                        f"(limit {limit:.0%}); its files are not ordered by {key}")
    if summary is not None:
        byte_share, key_share = amplification(summary)
        if key_share is None:
            if byte_share > REGION_READ_SHARE_LIMIT:
                notes.append(f"{table}: its last commit added {byte_share:.0%} of its bytes and does not say "
                             f"how many keys it changed ({KEYS_CHANGED})")
        elif operation == "overwrite":
            notes.append(f"{table}: its last commit was a full rebuild that changed {key_share:.2%} of its keys "
                         f"and wrote {byte_share:.0%} of its bytes")
        elif byte_share > AMPLIFICATION_LIMIT * max(key_share, KEY_SHARE_FLOOR):
            failures.append(f"{table}: its last commit changed {key_share:.2%} of its keys and added "
                            f"{byte_share:.0%} of its bytes (limit {AMPLIFICATION_LIMIT}x the key share, "
                            f"floored at {KEY_SHARE_FLOOR:.0%})")
    return failures, notes


def layout(trino_rows, trino_url: str, contracts: dict, lakehouse: dict) -> tuple[list[str], list[str]]:
    """(failures, notes) for every clustered table production holds rows in, read from Trino's
    `$files` and `$snapshots` metadata tables; no data file is read."""
    failures, notes = [], []
    for table, key in clustered_tables(contracts).items():
        shape = lakehouse["tables"].get(table)
        if not shape or not shape["has_rows"]:
            continue
        field = next(field[0] for field in shape["fields"] if field[1] == key)
        namespace, name = table.split(".", 1)
        bounds = [(low, high) for low, high in trino_rows(trino_url, (
            f"SELECT element_at(lower_bounds, {field}), element_at(upper_bounds, {field}) "
            f'FROM r2.{namespace}."{name}$files" WHERE content = 0')) if low is not None and high is not None]
        last = trino_rows(trino_url, (
            f'SELECT summary, operation FROM r2.{namespace}."{name}$snapshots" '
            "WHERE operation <> 'replace' AND parent_id IS NOT NULL ORDER BY committed_at DESC LIMIT 1"))
        found, noted = layout_findings(table, key, bounds, dict(last[0][0]) if last else None,
                                       last[0][1] if last else None)
        failures += found
        notes += noted
    return failures, notes


UNPUBLISHED_GRACE_HOURS = 48
FIRST_UNPUBLISHED = "first-unpublished.json"


def verdict(now: datetime, first_unpublished: datetime, why: str) -> tuple[str, int]:
    """The outcome line and exit code of a run that could not publish, with none published since
    `first_unpublished` (root ADR-0260): waiting, then failed once CI has judged pull requests
    against an older production for 48 hours."""
    hours = (now - first_unpublished).total_seconds() / 3600
    since = first_unpublished.strftime("%Y-%m-%dT%H:%MZ")
    if hours < UNPUBLISHED_GRACE_HOURS:
        return f"foundation-job-outcome waiting shape ledgers unpublished since {since}; {why}", 0
    return (f"foundation-job-outcome failed shape ledgers unpublished since {since}, "
            f"over {UNPUBLISHED_GRACE_HOURS}h; {why}", 1)


def first_unpublished_at(now: datetime, published: bool) -> datetime | None:
    """When runs first stopped publishing, recorded in the state directory; a run that publishes
    clears it."""
    path = STATE / FIRST_UNPUBLISHED
    if published:
        path.unlink(missing_ok=True)
        return None
    if path.exists():
        return datetime.fromisoformat(json.loads(path.read_text(encoding="utf-8"))["first_unpublished"])
    path.write_text(json.dumps({"first_unpublished": now.isoformat()}) + "\n", encoding="utf-8")
    return now


def read_state(name: str) -> dict | None:
    try:
        return json.loads((STATE / name).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def outcome(now: datetime, published: bool, written: bool, said: str, misplaced: list[str]) -> tuple[str, int]:
    """The run's last line and exit code (root ADR-0260)."""
    first_unpublished = first_unpublished_at(now, published)
    if first_unpublished is None:
        line, status = f"foundation-job-outcome {'changed' if written else 'unchanged'}", 0
    else:
        line, status = verdict(now, first_unpublished, said)
    if misplaced:
        # A layout finding fails the run at once; the 48 hours do not apply to it.
        line = (f"foundation-job-outcome failed {len(misplaced)} tables are laid out or written against their "
                f"cluster key (root ADR-0314)" + ("" if first_unpublished is None else f"; {said}"))
        status = 1
    return line, status


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    capture = load_capture()
    lakehouse = capture.capture()
    files = {Path(LAKEHOUSE).name: capture.render(lakehouse),
             Path(POSTGRES).name: subprocess.run(["bash", str(RELEASE / "scripts/ops/postgres-production-shape.sh")],
                                                 capture_output=True, text=True, check=True, timeout=600).stdout}
    # How production moved since this host's last capture: named, never failed (CI reads it next).
    for name, key in ((Path(LAKEHOUSE).name, "tables"), (Path(POSTGRES).name, "tables_with_rows")):
        last = read_state(name)
        for line in differences(last, json.loads(files[name]), key) if last else []:
            print(f"shape-ledger-check: MOVED {name}: {line}")
        (STATE / name).write_text(files[name], encoding="utf-8")
    now = datetime.now(timezone.utc)
    previous = read_state(PUBLISHED)
    said, head, written = publish(files, now, previous and previous.get("head"))
    print(f"shape-ledger-check: captures {said}")
    if head is not None:
        (STATE / PUBLISHED).write_text(json.dumps({"at": now.isoformat(), "head": head}) + "\n", encoding="utf-8")
    trino_url = os.environ.get("FOUNDATION_PLATFORM_TRINO_URL", "http://127.0.0.1:18081").rstrip("/")
    misplaced, notes = layout(capture.trino_rows, trino_url,
                              json.loads((RELEASE / CONTRACTS).read_text(encoding="utf-8"))["contracts"], lakehouse)
    for line in notes:
        print(f"shape-ledger-check: NOTE {line}")
    for line in misplaced:
        print(f"shape-ledger-check: LAYOUT {line}")
    line, status = outcome(now, head is not None, written, said, misplaced)
    print(line)
    return status


if __name__ == "__main__":
    sys.exit(main())
