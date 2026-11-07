#!/usr/bin/env python3
"""The media reconciler: the content-addressed media in R2 against the Silver index that names it
(root ADR-0387, ADR-0299 G-recon, ADR-0300 §2).

    media_reconcile.py index <state dir> <delta.json>
    media_reconcile.py compare <state dir> <delta.json> <listing.json> <report dir> <metrics file>

The onbid collector writes each photo or attachment once, to `<object_prefix>/sha256/<hash>.<ext>`
(data-go-kr-onbid.contract.json `media`, foundation-shared-kernel media.rs), and the Silver load
indexes it in `media.reconcile.index_tables` (공매 and 경매 photos share the store, root ADR-0429); a
parcel page shows a photo from the index's hash. Two
ways those can disagree, and nothing said so before this:

- missing: the index names a key R2 does not hold. A user sees a broken photo. The collector puts the
  blob before the Bronze line that names it, so an index key absent from a listing taken after the
  index was read is missing, with no grace.
- orphan: R2 holds a key no index row names, or a key that is not a media key at all. Storage nobody
  serves. On 2026-10-26 the collector ran into its 40-minute limit with its Bronze commit at the end of
  the run: the photos it had stored landed and the lines naming them never did. An orphan younger than
  `orphan_min_age_hours` is not counted: its line may still be on its way to Silver.

Partial (root ADR-0314, scope new-objects): the index is append-only, so the set of keys it names only
grows. The state directory keeps that set and the newest `ingested_at_utc` read (`referenced.json`);
`index` reads only the rows ingested since then, less `INDEX_OVERLAP_HOURS` for a load that stamped
its rows before an earlier one committed (a key read twice is one key). The first run, or a run whose
state is gone, reads the whole index once. The state is written only after a comparison succeeded.

`listing.json` is `list-lakehouse-objects` of the prefix (the publisher, the lakehouse reader key),
taken after the index read (media-reconcile.sh orders them): keys and sizes, no object read, about one
list call per thousand objects. The listing cannot be narrowed by time (the keys are hashes and
ListObjectsV2 filters by prefix only), so every run lists the prefix, as Iceberg's remove_orphan_files
lists a table location; the expensive side, the index, is read only for what changed.

Writes one report per run under <report dir> (kept, append-only) and the gauges for node-exporter;
the FoundationMediaObject* rules alert on them. Changes no data: the outcome is `unchanged`.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.dont_write_bytecode = True
PLATFORM = Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json"
TRINO_CONTAINER = os.environ.get("FOUNDATION_MEDIA_RECONCILE_TRINO_CONTAINER", "foundation-platform-trino")
STATE_FILE = "referenced.json"
HASH = re.compile(r"^[0-9a-f]{64}$")
FORMAT = re.compile(r"^[a-z0-9]+$")
# A Silver load stamps its rows when it starts and commits within its 40-minute limit; two hours cover
# any commit that lands after a later-stamped one.
INDEX_OVERLAP_HOURS = 2


def settings(contract_path: Path = CONTRACT) -> dict:
    media = json.loads(contract_path.read_text(encoding="utf-8"))["media"]
    reconcile = media["reconcile"]
    return {
        "prefix": media["object_prefix"].rstrip("/") + "/sha256/",
        # Every Silver index that names a key of the one store (root ADR-0429): 공매 and 경매 photos
        # share the prefix, so a key either names is referenced. The set is the state's identity.
        "index_tables": list(reconcile["index_tables"]),
        "index_table": "+".join(reconcile["index_tables"]),
        "orphan_min_age_hours": int(reconcile["orphan_min_age_hours"]),
    }


def load_state(state_dir: Path) -> dict:
    """The keys the index named up to the watermark; empty when there is none yet."""
    try:
        state = json.loads((state_dir / STATE_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"index_table": None, "watermark": None, "keys": []}
    return state


def index_sql(tables: list[str], watermark: str | None) -> str:
    for table in tables:
        if not re.fullmatch(r"[a-z_]+\.[a-z_]+", table):
            raise ValueError(f"index_table is not a schema.table name: {table}")
    since = ""
    if watermark is not None:
        start = parse_time(watermark) - INDEX_OVERLAP_HOURS * 3600
        stamp = datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        since = f" WHERE ingested_at_utc >= TIMESTAMP '{stamp} UTC'"
    union = " UNION ALL ".join(f"SELECT sha256, media_format, ingested_at_utc FROM {table}{since}" for table in tables)
    return (f"SELECT sha256, media_format, "
            f"to_iso8601(max(ingested_at_utc) AT TIME ZONE 'UTC') FROM ({union}) GROUP BY 1, 2")


def trino(sql: str) -> list[list[str]]:
    completed = subprocess.run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--catalog", "r2", "--output-format", "CSV_UNQUOTED",
         "--execute", sql],
        capture_output=True, text=True, timeout=600, check=False,
    )
    if completed.returncode != 0:
        last = completed.stderr.strip().splitlines()[-1:] or ["trino failed"]
        raise RuntimeError(f"the index read failed: {last[0]}")
    return [row for row in csv.reader(io.StringIO(completed.stdout)) if row]


def existing(tables: list[str], present: set[str]) -> list[str]:
    """The index tables that exist yet: a source whose first Silver load has not run names no key."""
    return [table for table in tables if table in present]


def read_index_delta(state_dir: Path) -> dict:
    config = settings()
    state = load_state(state_dir)
    # Another index table is another reference set: start it whole.
    watermark = state["watermark"] if state["index_table"] == config["index_table"] else None
    present = {f"silver.{row[0]}" for row in trino(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'")}
    tables = existing(config["index_tables"], present)
    rows = trino(index_sql(tables, watermark)) if tables else []
    # Until every table exists the set is not the configured one: the next run reads it whole again.
    identity = config["index_table"] if tables == config["index_tables"] else "+".join(tables)
    return {"index_table": identity, "since": watermark,
            "keys": [[row[0], row[1]] for row in rows], "newest": max((row[2] for row in rows), default=None)}


def parse_time(raw: str) -> float:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def key_of(prefix: str, sha256: str, media_format: str) -> str:
    if not HASH.match(sha256) or not FORMAT.match(media_format):
        raise ValueError(f"the index holds a row that names no media key: {sha256!r} {media_format!r}")
    return f"{prefix}{sha256}.{media_format}"


def merge(state: dict, delta: dict, prefix: str) -> dict:
    """The state after the delta: every key named so far, and the newest ingestion read."""
    same = state["index_table"] == delta["index_table"]
    keys = set(state["keys"]) if same else set()
    keys.update(key_of(prefix, sha256, media_format) for sha256, media_format in delta["keys"])
    marks = [m for m in (state["watermark"] if same else None, delta["newest"]) if m]
    newest = max(marks, key=parse_time) if marks else None
    return {"index_table": delta["index_table"], "watermark": newest, "keys": sorted(keys)}


def reconcile(referenced: set[str], listing: dict, prefix: str, orphan_min_age_hours: int, now: float) -> dict:
    """Missing and orphaned keys of the reference set against one listing of its prefix."""
    if listing["prefix"] != prefix:
        raise ValueError(f"the listing is of {listing['prefix']!r}, not {prefix!r}")
    listed = {o["key"]: o for o in listing["objects"]}
    cutoff = now - orphan_min_age_hours * 3600
    orphans, young = [], 0
    for key, obj in sorted(listed.items()):
        if key in referenced:
            continue
        modified = obj.get("last_modified")
        # An object R2 gives no time for is old enough: nothing says its line is still on its way.
        if modified and parse_time(modified) > cutoff:
            young += 1
            continue
        orphans.append({"key": key, "size_bytes": obj.get("size_bytes"), "last_modified": modified})
    missing = sorted(referenced - listed.keys())
    return {
        "prefix": prefix,
        "referenced_keys": len(referenced),
        "listed_objects": len(listed),
        "list_request_count": listing.get("list_request_count"),
        "missing_count": len(missing),
        "orphan_count": len(orphans),
        "orphan_bytes": sum(o["size_bytes"] or 0 for o in orphans),
        "young_unreferenced_count": young,
        "orphan_min_age_hours": orphan_min_age_hours,
        "missing": missing,
        "orphans": orphans,
    }


def metrics(result: dict, now: float) -> str:
    lines = []
    for name, help_text, value in (
        ("missing_objects", "Keys the media index names that R2 does not hold (a broken photo).", result["missing_count"]),
        ("orphan_objects", "R2 media keys no index row names, older than the grace.", result["orphan_count"]),
        ("orphan_bytes", "Bytes of the orphan media objects.", result["orphan_bytes"]),
        ("referenced_keys", "Distinct media keys the index names.", result["referenced_keys"]),
        ("index_rows_read", "Index keys this run read (the rows ingested since the last run).", result["index_keys_read"]),
        ("listed_objects", "Media objects R2 listed.", result["listed_objects"]),
        ("timestamp_seconds", "When the media reconciler last compared.", int(now)),
    ):
        metric = f"foundation_media_reconcile_{name}"
        lines += [f"# HELP {metric} {help_text}", f"# TYPE {metric} gauge", f'{metric}{{prefix="{result["prefix"]}"}} {value}']
    return "\n".join(lines) + "\n"


def write_atomically(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def run(state_dir: Path, delta_path: Path, listing_path: Path, report_dir: Path, metrics_path: Path,
        now: float | None = None) -> dict:
    config = settings()
    now = time.time() if now is None else now
    delta = json.loads(delta_path.read_text(encoding="utf-8"))
    state = merge(load_state(state_dir), delta, config["prefix"])
    listing = json.loads(listing_path.read_text(encoding="utf-8"))
    result = reconcile(set(state["keys"]), listing, config["prefix"], config["orphan_min_age_hours"], now)
    stamp = datetime.fromtimestamp(now, timezone.utc)
    result.update(index_table=config["index_table"], index_read_since=delta["since"],
                  index_keys_read=len(delta["keys"]), generated_at_utc=stamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
    report_dir.mkdir(parents=True, exist_ok=True)
    write_atomically(report_dir / f"{stamp:%Y%m%dT%H%M%SZ}.json", json.dumps(result, indent=1, sort_keys=True) + "\n")
    write_atomically(metrics_path, metrics(result, now))
    write_atomically(state_dir / STATE_FILE, json.dumps(state) + "\n")
    return result


USAGE = __doc__.split("\n\n")[1]


def main(argv: list[str]) -> int:
    if argv[:1] == ["index"] and len(argv) == 3:
        delta = read_index_delta(Path(argv[1]))
        Path(argv[2]).write_text(json.dumps(delta), encoding="utf-8")
        print(f"media-reconcile index since={delta['since']} keys={len(delta['keys'])}")
        return 0
    if argv[:1] == ["compare"] and len(argv) == 6:
        result = run(*(Path(arg) for arg in argv[1:]))
        print("media-reconcile " + " ".join(
            f"{key}={result[key]}" for key in ("index_keys_read", "referenced_keys", "listed_objects",
                                                 "list_request_count", "missing_count", "orphan_count",
                                                 "orphan_bytes", "young_unreferenced_count")))
        return 0
    print(USAGE, file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
