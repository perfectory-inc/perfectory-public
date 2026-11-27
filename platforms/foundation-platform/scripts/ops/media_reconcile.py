#!/usr/bin/env python3
"""The media reconciler: the content-addressed media in R2 against the Silver index that names it
(root ADR-0387, ADR-0299 G-recon, ADR-0300 §2).

    media_reconcile.py compare <state dir> <delta.json> <listing.jsonl> <report dir> <metrics file>

The onbid collector writes each photo or attachment once, to `<object_prefix>/display/sha256/<hash>.<ext>`
(data-go-kr-onbid.contract.json `media`, foundation-shared-kernel media.rs), and the Silver load
indexes it in `media.reconcile.index_tables` (공매 and 경매 photos and the industrial complex notice
attachments share the store, root ADR-0429, ADR-0484); a parcel page shows a photo from the index's
hash. Two ways those can disagree, and nothing said so before this:

- missing: the index names a key R2 does not hold. A user sees a broken photo. The collector puts the
  blob before the Bronze line that names it, so an index key absent from a listing taken after the
  index was read is missing, with no grace.
- orphan: R2 holds a key no index row names, or a key that is not a media key at all. Storage nobody
  serves. On 2026-10-26 the collector ran into its 40-minute limit with its Bronze commit at the end of
  the run: the photos it had stored landed and the lines naming them never did. An orphan younger than
  `orphan_min_age_hours` is not counted: its line may still be on its way to Silver.

Partial (root ADR-0314, scope new-objects): the index is append-only, so the set of keys it names only
grows. The state directory keeps that set and, per index table, the data files already read
(`referenced.json`). `delta.json` is the publisher's `media-index-delta`: the keys of the data files
the tables' current snapshots hold beyond those, and every file they hold (Iceberg's incremental
append read). The first run, or a run whose state is of another table set, reads the tables whole
once. The state is written only after a comparison succeeded. Until 2026-11-18 the delta was a Trino
query since an `ingested_at_utc` watermark less two hours; Trino is being retired (root ADR-0559).

`listing.jsonl` is `list-lakehouse-objects` of the prefix (the publisher, the lakehouse reader key),
taken after the index read (media-reconcile.sh orders them): a header line, then one line per object,
keys and sizes, no object read, about one list call per thousand objects. It is compared line by line,
so the run holds the reference set and the orphans, never the listing: from 2026-11-16 the listing of
188,000 objects loaded as one document held the run at its memory limit until its 10 minutes ran out,
every run. The listing cannot be narrowed by time (the keys are hashes and ListObjectsV2 filters by
prefix only), so every run lists the prefix, as Iceberg's remove_orphan_files lists a table location;
the expensive side, the index, is read only for what changed.

Writes one report per run under <report dir> (kept, append-only) and the gauges for node-exporter;
the FoundationMediaObject* rules alert on them. Changes no data: the outcome is `unchanged`.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True
PLATFORM = Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "infra/lakehouse/contracts/data-go-kr-onbid.contract.json"
STATE_FILE = "referenced.json"
HASH = re.compile(r"^[0-9a-f]{64}$")
FORMAT = re.compile(r"^[a-z0-9]+$")


def settings(contract_path: Path = CONTRACT) -> dict:
    media = json.loads(contract_path.read_text(encoding="utf-8"))["media"]
    reconcile = media["reconcile"]
    return {
        "prefix": media["object_prefix"].rstrip("/") + "/display/sha256/",
        # Every Silver index that names a key of the one store (root ADR-0429): 공매 and 경매 photos
        # share the prefix, so a key either names is referenced. The set is the state's identity.
        "index_tables": list(reconcile["index_tables"]),
        "index_table": "+".join(reconcile["index_tables"]),
        "orphan_min_age_hours": int(reconcile["orphan_min_age_hours"]),
    }


def load_state(state_dir: Path) -> dict:
    """The keys the index named, and the data files read, so far; empty when there is none yet."""
    try:
        state = json.loads((state_dir / STATE_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"index_table": None, "read_files": {}, "keys": []}
    # A state written while the delta was a Trino watermark names no files: the next delta reads
    # every file once, and its keys join the ones kept.
    state.setdefault("read_files", {})
    state.pop("watermark", None)
    return state


def parse_time(raw: str) -> float:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def key_of(prefix: str, sha256: str, media_format: str) -> str:
    if not HASH.match(sha256) or not FORMAT.match(media_format):
        raise ValueError(f"the index holds a row that names no media key: {sha256!r} {media_format!r}")
    return f"{prefix}{sha256}.{media_format}"


def merge(state: dict, delta: dict, prefix: str) -> dict:
    """The state after the delta: every key named so far, and the files the snapshots now hold."""
    same = state["index_table"] == delta["index_table"]
    keys = set(state["keys"]) if same else set()
    keys.update(key_of(prefix, sha256, media_format) for sha256, media_format in delta["keys"])
    return {"index_table": delta["index_table"], "read_files": delta["read_files"], "keys": sorted(keys)}


def read_listing(path: Path):
    """The listing's header, and an iterator over its objects, one line at a time."""
    lines = path.open(encoding="utf-8")
    try:
        header = json.loads(lines.readline() or "null")
    except ValueError:
        header = None
    if not isinstance(header, dict) or "prefix" not in header:
        lines.close()
        raise ValueError(f"{path} does not start with a listing header")

    def objects():
        with lines:
            for line in lines:
                if line.strip():
                    yield json.loads(line)

    return header, objects()


def reconcile(referenced: set[str], header: dict, objects, prefix: str, orphan_min_age_hours: int,
              now: float) -> dict:
    """Missing and orphaned keys of the reference set against one listing of its prefix.

    The objects are read once, as they come: only the reference set, the referenced keys not listed
    yet and the orphans are held."""
    if header["prefix"] != prefix:
        raise ValueError(f"the listing is of {header['prefix']!r}, not {prefix!r}")
    cutoff = now - orphan_min_age_hours * 3600
    unseen = set(referenced)
    orphans, young, listed = [], 0, 0
    for obj in objects:
        listed += 1
        key = obj["key"]
        if key in referenced:
            unseen.discard(key)
            continue
        modified = obj.get("last_modified")
        # An object R2 gives no time for is old enough: nothing says its line is still on its way.
        if modified and parse_time(modified) > cutoff:
            young += 1
            continue
        orphans.append({"key": key, "size_bytes": obj.get("size_bytes"), "last_modified": modified})
    if header.get("object_count") not in (None, listed):
        raise ValueError(f"the listing holds {listed} objects, its header says {header['object_count']}")
    orphans.sort(key=lambda o: o["key"])
    missing = sorted(unseen)
    return {
        "prefix": prefix,
        "referenced_keys": len(referenced),
        "listed_objects": listed,
        "list_request_count": header.get("list_request_count"),
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
        ("index_rows_read", "Index keys this run read (those of the data files added since the last run).", result["index_keys_read"]),
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
    header, objects = read_listing(listing_path)
    result = reconcile(set(state["keys"]), header, objects, config["prefix"], config["orphan_min_age_hours"], now)
    stamp = datetime.fromtimestamp(now, timezone.utc)
    result.update(index_table=config["index_table"], index_tables_read=delta.get("tables", {}),
                  index_keys_read=len(delta["keys"]), generated_at_utc=stamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
    report_dir.mkdir(parents=True, exist_ok=True)
    write_atomically(report_dir / f"{stamp:%Y%m%dT%H%M%SZ}.json", json.dumps(result, indent=1, sort_keys=True) + "\n")
    write_atomically(metrics_path, metrics(result, now))
    write_atomically(state_dir / STATE_FILE, json.dumps(state) + "\n")
    return result


USAGE = __doc__.split("\n\n")[1]


def main(argv: list[str]) -> int:
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
