#!/usr/bin/env python3
"""Is the history a lane serves whole enough for its first publish? (root ADR-0350 §6, ADR-0371 §5)

    serving_history_readiness.py <lane>       `ready ...` (exit 0) or `waiting ...` (exit 1)
    serving_history_readiness.py --report     every 실거래 dataset's months, one line each (exit 0)

A lane line of config/r2-connections.contract.json may name `first_publish_waits_for_history`: the
실거래 datasets whose whole history must be in `silver.real_transaction` before the lane is first cut
over, because a half-filled history reads as "no deal" on the screen. What is loaded is what the
Silver load's own commits record: each commit names the Bronze export it loaded
(`lakehouse_ingest.append_source_object`, `foundation.ingest-batch-objects`, and the durable table
property copy of root ADR-0332), one export per (dataset, month), `source=rtmolitgokr__<dataset>/
period=YYYY-MM/` in its key. A dataset is whole when every month from its `first_month`
(rt-molit-real-transaction.contract.json) to the last whole month before today has a loaded export.
The same record `plan-silver-load-objects` decides what a load owes from, read here over the catalog's
REST API (iceberg_metadata.py), no Spark and no data file.

Exits 65 when it cannot tell (no catalog, no table): the cut-over then stops rather than publish.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
for relative in ("infra/lakehouse/spark/jobs", "scripts/ops"):
    sys.path.insert(0, str(PLATFORM / relative))

TABLE = "silver.real_transaction"
CONTRACT = PLATFORM / "infra/lakehouse/contracts/rt-molit-real-transaction.contract.json"
LANES = PLATFORM / "config/r2-connections.contract.json"
# lakehouse_ingest.INGEST_BATCH_OBJECTS_KEY and its durable property prefix (root ADR-0332); spelled
# here too because importing lakehouse_ingest needs nothing but is the Spark jobs' module.
RECORD_KEY = "foundation.ingest-batch-objects"
OBJECT = re.compile(r"source=rtmolitgokr__([a-z_]+)/period=(\d{4})-(\d{2})/")


def months(first: str, last: str) -> list[str]:
    """`YYYY-MM` months from `first` to `last`, both included."""
    year, month = map(int, first.split("-"))
    end = tuple(map(int, last.split("-")))
    found = []
    while (year, month) <= end:
        found.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return found


def last_whole_month(today: date) -> str:
    return f"{today.year - 1:04d}-12" if today.month == 1 else f"{today.year:04d}-{today.month - 1:02d}"


def recorded_objects(history: dict) -> set[str]:
    """Every object the table's commits record as loaded: the snapshot summaries and the durable
    table properties (a summary expires with its snapshot; the property does not)."""
    names = set()
    for snapshot in history.get("snapshots") or []:
        names.update(n for n in ((snapshot.get("summary") or {}).get(RECORD_KEY) or "").split(",") if n)
    for key, value in (history.get("properties") or {}).items():
        if key.startswith(RECORD_KEY + "."):
            names.update(n for n in (value or "").split(",") if n)
    return names


def completeness(objects: set[str], datasets: dict[str, dict], through: str) -> dict[str, dict]:
    """{dataset: loaded, expected, missing (the oldest few), share} for every contract dataset."""
    loaded: dict[str, set[str]] = {}
    for name in objects:
        found = OBJECT.search(name)
        if found:
            loaded.setdefault(found.group(1), set()).add(f"{found.group(2)}-{found.group(3)}")
    report = {}
    for dataset, spec in datasets.items():
        expected = months(spec["first_month"], through)
        missing = [m for m in expected if m not in loaded.get(dataset, set())]
        report[dataset] = {"loaded": len(expected) - len(missing), "expected": len(expected),
                           "share": round((len(expected) - len(missing)) / len(expected), 4) if expected else 1.0,
                           "missing_first": missing[:3], "through": through}
    return report


def verdict(lane: str, report: dict[str, dict], lanes: dict) -> tuple[bool, str]:
    waits = lanes[lane].get("first_publish_waits_for_history") or []
    if not waits:
        return True, f"ready {lane}: its line waits for no history"
    unknown = sorted(set(waits) - set(report))
    if unknown:
        raise ValueError(f"{lane} waits for datasets the 실거래 contract does not name: {unknown}")
    short = [f"{d} {report[d]['loaded']}/{report[d]['expected']} (from {', '.join(report[d]['missing_first'])})"
             for d in waits if report[d]["loaded"] < report[d]["expected"]]
    if short:
        return False, f"waiting {lane}: history not whole through {report[waits[0]]['through']}: " + "; ".join(short)
    return True, f"ready {lane}: " + ", ".join(f"{d} {report[d]['expected']} months" for d in waits)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 64
    import iceberg_metadata  # noqa: PLC0415

    datasets = json.loads(CONTRACT.read_text(encoding="utf-8"))["datasets"]
    lanes = json.loads(LANES.read_text(encoding="utf-8"))["by_pnu_lanes"]
    if argv[0] != "--report" and argv[0] not in lanes:
        print(f"{argv[0]} is not a by_pnu_lanes lane", file=sys.stderr)
        return 64
    try:
        catalog = iceberg_metadata.Catalog("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN",
                                           "foundation-serving-history-readiness")
        history = catalog.table(TABLE)
    except Exception as error:  # noqa: BLE001 - any failure to read means "cannot tell"
        print(f"cannot tell: {TABLE} history unreadable: {error}", file=sys.stderr)
        return 65
    report = completeness(recorded_objects(history), datasets,
                          last_whole_month(datetime.now(timezone.utc).date()))
    if argv[0] == "--report":
        for dataset, line in report.items():
            print(json.dumps({"dataset": dataset, **line}, ensure_ascii=False, sort_keys=True))
        return 0
    ready, line = verdict(argv[0], report, lanes)
    print(line)
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
