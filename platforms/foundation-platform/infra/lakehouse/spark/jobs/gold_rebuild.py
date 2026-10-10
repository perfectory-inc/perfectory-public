#!/usr/bin/env python3
"""When a by-PNU panel Gold table is rebuilt, and what a rebuild may commit (root ADR-0139).

The panel producers (`parcel_panel_silver_to_gold.py`, `building_panel_silver_to_gold.py`) pin
every Silver input to one Iceberg snapshot before Spark reads anything (ADR-0130). A producer
that commits also records those pins in the Gold snapshot's summary, under
SOURCE_SNAPSHOTS_PROPERTY. That record is what this module plans from:

- A Silver input has changed when a snapshot in its `main` ancestry after the recorded pin
  added or deleted rows. A compaction (`replace`) or an empty commit changes nothing. Snapshot
  expiry may have removed the pin itself; the oldest kept snapshot still names it as its parent,
  so the walk stops there. A gap the kept history cannot close counts as a change.
- A Gold snapshot without the record (a schema backfill rewrote it, or it was built by hand
  before this record existed) is read through its ancestry back to the newest snapshot that has
  one. A table that has none at all falls back to the rows' own `published_at_utc`, the time the
  producer started: a Silver change committed after it cannot be in that Gold. A change committed
  between the producer's pin and that stamp can be missed, so the fallback is for the supervised
  first run only: an enabled job plans with `time_fallback=False`, which refuses such a Gold.
- An unconditional rebuild (`unconditional=<reason>`) rebuilds whatever the history says, under the
  same row floor; the supervised first run is one, so every Gold carries pins before the job is on.
- An input the contract lists under `unmeasured_inputs` has not been part of a measured run: the
  Spark size was not shown to hold with it. Once it exists the plan refuses, except for a dry run
  (`measuring=True`), which is how it gets measured.
- Nothing changed: nothing to do. Otherwise the plan pins every input to its current snapshot and
  states the fewest rows the new Gold may have: the current Gold's row count less the table's
  `max_row_loss_fraction` (contracts/gold-panel-rebuild.contract.json). The producer refuses
  fewer before it writes, so a refused rebuild commits nothing.
- A rebuild has a `mode` (root ADR-0180): `incremental` when the contract allows it, the current
  Gold head is a producer commit with pins (compactions may follow), every changed input's pinned
  snapshot is still kept, and no changed input is one the producer reads whole; the producer then
  recomputes only the PNUs whose inputs changed (gold_incremental.py). Otherwise `full`, with
  `mode_reasons`.

`main` is the Spark entry the scheduled job runs (scripts/ops/gold-panel-rebuild.sh). Everything
else is pure Python, so the CI runner, which has no PySpark, tests the decisions themselves.
"""
from __future__ import annotations

import argparse
import importlib
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse_ingest import SNAPSHOT_PROPERTY_PREFIX

SOURCE_SNAPSHOTS_PROPERTY = "foundation.source-iceberg-snapshots"
PLAN_SCHEMA_VERSION = "foundation-platform.gold_rebuild_plan.v1"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "gold-panel-rebuild.contract.json"
# Summary counters of a commit that changes rows (Iceberg SnapshotSummary).
ROW_CHANGE_COUNTERS = ("added-records", "deleted-records", "added-position-deletes",
                       "added-equality-deletes")


class PlanError(ValueError):
    pass


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = json.loads(path.read_text(encoding="utf-8"))
    tables = contract.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise PlanError("gold rebuild contract names no tables")
    for name, table in tables.items():
        fraction = table.get("max_row_loss_fraction")
        if type(fraction) not in (int, float) or not 0 <= fraction < 1:
            raise PlanError(f"{name}: max_row_loss_fraction must be a number from 0 up to 1")
        if not str(table.get("max_row_loss_reason", "")).strip():
            raise PlanError(f"{name}: max_row_loss_fraction needs its reason")
        if not isinstance(table.get("producer"), str):
            raise PlanError(f"{name}: names no producer")
        args = table.get("producer_arguments", [])
        if not (isinstance(args, list) and all(isinstance(a, str) and a.startswith("--") for a in args)):
            raise PlanError(f"{name}: producer_arguments must be a list of flags")
        unmeasured = table.get("unmeasured_inputs", {})
        if not (isinstance(unmeasured, dict)
                and all(isinstance(why, str) and why.strip() for why in unmeasured.values())):
            raise PlanError(f"{name}: unmeasured_inputs must map each input to why it is unmeasured")
        incremental = table.get("incremental")
        if incremental is not None:
            fraction = incremental.get("max_changed_key_fraction")
            sample = incremental.get("parity_sample_keys")
            if type(fraction) not in (int, float) or not 0 < fraction <= 1:
                raise PlanError(f"{name}: incremental.max_changed_key_fraction must be in (0, 1]")
            if type(sample) is not int or sample < 0:
                raise PlanError(f"{name}: incremental.parity_sample_keys must be a non-negative integer")
            if not str(incremental.get("reason", "")).strip():
                raise PlanError(f"{name}: incremental needs its reason")
    return contract


def producer_inputs(module: Any) -> tuple[tuple[str, ...], dict[str, str], str]:
    """(required inputs, optional input -> flag the producer takes without it, anchor input).

    The lists are the producer's own constants (ADR-0130: the producer owns its input list).
    """
    optional = {module.LINEAGE_SOURCE: "--no-carry-lineage"} if hasattr(module, "LINEAGE_SOURCE") else {}
    anchor = getattr(module, "PARCEL_SOURCE", None) or module.TITLE_SOURCE
    return tuple(module.ALL_SOURCES), optional, anchor


def changes_row_data(snapshot: dict[str, Any]) -> bool:
    if snapshot.get("operation") == "replace":
        return False
    summary = snapshot.get("summary") or {}
    return any(int(summary.get(counter) or 0) > 0 for counter in ROW_CHANGE_COUNTERS)


def ancestry(snapshots: list[dict[str, Any]], head: str) -> list[dict[str, Any]]:
    """The kept snapshots from `head` back through parent_id, newest first."""
    by_id = {s["snapshot_id"]: s for s in snapshots}
    chain, current = [], head
    while current in by_id and len(chain) <= len(by_id):
        chain.append(by_id[current])
        current = by_id[current].get("parent_id")
    if not chain:
        raise PlanError(f"head snapshot {head} is not in the table's snapshot history")
    return chain


def input_change_since_pin(snapshots: list[dict[str, Any]], head: str, pin: str) -> str | None:
    """Why the input changed after `pin`, or None when no kept snapshot after it changed rows."""
    for snapshot in ancestry(snapshots, head):
        if snapshot["snapshot_id"] == pin:
            return None
        if changes_row_data(snapshot):
            return f"snapshot {snapshot['snapshot_id']} ({snapshot.get('operation')}) changed rows after pin {pin}"
        if snapshot.get("parent_id") == pin:
            return None
    return f"pin {pin} is not in the kept history of head {head}"


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise PlanError(f"timestamp {value!r} has no time zone")
    return parsed.astimezone(timezone.utc)


def input_change_since_time(snapshots: list[dict[str, Any]], head: str, built_at: str) -> str | None:
    """Why the input changed after `built_at` (ISO UTC), or None."""
    chain = ancestry(snapshots, head)
    for snapshot in chain:
        if utc(snapshot["committed_at"]) <= utc(built_at):
            return None
        if changes_row_data(snapshot):
            return f"snapshot {snapshot['snapshot_id']} ({snapshot.get('operation')}) changed rows at {snapshot['committed_at']}, after the Gold was built at {built_at}"
    if chain[-1].get("parent_id") is not None:
        return f"the kept history starts after the Gold was built at {built_at}"
    return None


def recorded_pins(gold_snapshots: list[dict[str, Any]], gold_head: str) -> dict[str, str] | None:
    """The source pins of the newest Gold snapshot in the head's ancestry that records them."""
    for snapshot in ancestry(gold_snapshots, gold_head):
        value = (snapshot.get("summary") or {}).get(SOURCE_SNAPSHOTS_PROPERTY)
        if value is not None:
            pins = json.loads(value)
            if not (isinstance(pins, dict) and all(isinstance(v, str) for v in pins.values())):
                raise PlanError(f"Gold snapshot {snapshot['snapshot_id']} records malformed source pins")
            return pins
    return None


def producer_whole_table_inputs(module: Any) -> tuple[str, ...]:
    """Inputs whose change the producer's incremental rebuild does not map to PNUs (ADR-0180)."""
    return tuple(getattr(module, "WHOLE_TABLE_INPUTS", ()))


def head_build_pins(gold_snapshots: list[dict[str, Any]], gold_head: str) -> dict[str, str] | None:
    """The pins of the producer commit the current Gold rows are, or None.

    Only a compaction (`replace`) may sit between that commit and the head: it moves rows between
    files without changing them. Any other commit without pins (a backfill, a hand write) changed
    the rows outside the producer, so the Gold is not what the build makes of those pins.
    """
    for snapshot in ancestry(gold_snapshots, gold_head):
        value = (snapshot.get("summary") or {}).get(SOURCE_SNAPSHOTS_PROPERTY)
        if value is not None:
            return recorded_pins([snapshot], snapshot["snapshot_id"])
        if snapshot.get("operation") != "replace":
            return None
    return None


def incremental_mode(entry: dict[str, Any], gold: dict[str, Any] | None,
                     silver: dict[str, dict[str, Any] | None], pins: dict[str, str],
                     whole_table_inputs: tuple[str, ...], unconditional: str | None
                     ) -> tuple[str, list[str], dict[str, str] | None]:
    """("incremental", [], previous pins) when a rebuild may merge only changed PNUs, else
    ("full", reasons, None) (root ADR-0180 §3)."""
    if entry.get("incremental") is None:
        return "full", ["the contract declares no incremental rebuild for this table"], None
    if unconditional:
        return "full", ["an unconditional rebuild is a full rebuild"], None
    if gold is None or gold.get("head") is None:
        return "full", ["there is no Gold to merge into"], None
    previous = head_build_pins(gold["snapshots"], gold["head"])
    if previous is None:
        return "full", ["the current Gold head is not a producer commit with recorded pins "
                        "(only compactions may follow one)"], None
    reasons = []
    if set(previous) != set(pins):
        reasons.append(f"the inputs changed from {sorted(previous)} to {sorted(pins)}")
    changed = [name for name in sorted(pins) if previous.get(name) not in (None, pins[name])]
    whole = [name for name in changed if name in whole_table_inputs]
    if whole:
        reasons.append(f"{whole} changed, and the producer does not map a change there to PNUs")
    for name in changed:
        kept = {s["snapshot_id"] for s in (silver.get(name) or {}).get("snapshots", [])}
        if previous[name] not in kept:
            reasons.append(f"{name}: snapshot {previous[name]} the Gold was built from is no longer kept, "
                           "so the change cannot be read")
    return ("full", reasons, None) if reasons else ("incremental", [], previous)


def plan(table: str, entry: dict[str, Any], inputs: tuple, gold: dict[str, Any] | None,
         silver: dict[str, dict[str, Any] | None], *, unconditional: str | None = None,
         time_fallback: bool = True, measuring: bool = False,
         whole_table_inputs: tuple[str, ...] = ()) -> dict[str, Any]:
    """Decide one Gold table's rebuild.

    `gold`: {"head", "snapshots", "row_count", "published_at_utc"} or None when the table does
    not exist. `silver`: input -> {"head", "snapshots"}, or None for a table that does not exist.
    `unconditional`: the reason to rebuild regardless of the history. `time_fallback`: whether a
    Gold without recorded pins may be judged by its publish time (the job is not enabled yet).
    `measuring`: a dry run, which may read an input the contract lists as unmeasured.
    """
    if unconditional is not None and not unconditional.strip():
        raise PlanError(f"{table}: an unconditional rebuild needs its reason")
    required, optional, anchor = inputs
    missing = [name for name in required if silver.get(name) is None]
    if missing:
        raise PlanError(f"{table}: required Silver inputs do not exist: {missing}")
    present = [*required, *(name for name in optional if silver.get(name) is not None)]
    unmeasured = {name: why for name, why in entry.get("unmeasured_inputs", {}).items() if name in present}
    if unmeasured and not measuring:
        raise PlanError(
            f"{table}: {sorted(unmeasured)} now exist but no run was measured with them "
            f"({'; '.join(unmeasured.values())}); measure a dry run (gold-panel-rebuild.sh --dry-run) "
            "within the Spark cap, then remove them from unmeasured_inputs "
            "(contracts/gold-panel-rebuild.contract.json)")
    pins = {name: silver[name]["head"] for name in present}
    flags = [flag for name, flag in optional.items() if silver.get(name) is None]
    reasons: list[str] = [f"unconditional rebuild: {unconditional.strip()}"] if unconditional else []
    previous = None
    if gold is None or gold.get("head") is None:
        reasons.append("the Gold table has no snapshot")
    else:
        previous = int(gold["row_count"])
        recorded = recorded_pins(gold["snapshots"], gold["head"])
        if recorded is not None:
            vanished = [name for name in recorded if silver.get(name) is None]
            if vanished:
                raise PlanError(f"{table}: inputs the current Gold was built from no longer exist: {vanished}")
            for name in present:
                if name not in recorded:
                    reasons.append(f"{name}: a new input the current Gold was not built from")
                    continue
                why = input_change_since_pin(silver[name]["snapshots"], silver[name]["head"], recorded[name])
                if why:
                    reasons.append(f"{name}: {why}")
        elif unconditional:
            pass
        elif not time_fallback:
            raise PlanError(
                f"{table}: the current Gold records no source pins, and an enabled job does not plan "
                "from its published_at_utc (stamped when the producer started, so it can miss a "
                "change); rebuild it once with gold-panel-rebuild.sh --unconditional <reason>")
        else:
            built_at = gold.get("published_at_utc")
            if not built_at:
                reasons.append("the Gold records no source pins and no published_at_utc")
            else:
                for name in present:
                    why = input_change_since_time(silver[name]["snapshots"], silver[name]["head"], built_at)
                    if why:
                        reasons.append(f"{name}: {why}")
    minimum = None if previous is None else math.ceil(previous * (1 - entry["max_row_loss_fraction"]))
    mode, mode_reasons, previous_pins = ("none", [], None)
    if reasons:
        mode, mode_reasons, previous_pins = incremental_mode(entry, gold, silver, pins, whole_table_inputs,
                                                             unconditional)
    settings = entry.get("incremental") or {}
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "gold_table": table,
        "action": "rebuild" if reasons else "nothing_to_do",
        "reasons": reasons,
        "producer": entry["producer"],
        "producer_arguments": [*entry.get("producer_arguments", []), *flags],
        "anchor_input": anchor,
        "source_snapshots": pins,
        "previous_gold_snapshot_id": None if gold is None else gold.get("head"),
        "previous_row_count": previous,
        "minimum_row_count": minimum,
        "max_row_loss_fraction": entry["max_row_loss_fraction"],
        # How a rebuild runs (root ADR-0180): merge only the changed PNUs, or rebuild whole and why.
        "mode": mode,
        "mode_reasons": mode_reasons,
        "previous_source_snapshots": previous_pins,
        "max_changed_key_fraction": settings.get("max_changed_key_fraction"),
        "parity_sample_keys": settings.get("parity_sample_keys"),
    }


def assert_minimum_row_count(row_count: int, minimum: int | None) -> None:
    """Refuse a Gold that lost more rows than its contract allows (checked before the write)."""
    if minimum is not None and row_count < minimum:
        raise ValueError(
            f"Gold has {row_count} rows, fewer than the {minimum} the previous snapshot allows "
            "(contracts/gold-panel-rebuild.contract.json max_row_loss_fraction); nothing was written"
        )


def pins_property_value(pins: dict[str, str]) -> str:
    """The SOURCE_SNAPSHOTS_PROPERTY value of a Gold snapshot built from `pins`."""
    return json.dumps(pins, sort_keys=True, separators=(",", ":"))


def write_gold_snapshot(frame: Any, table: str, mode: str, pins: dict[str, str], everything: Any) -> None:
    """Commit `frame` to `table` with the Silver pins it was built from in the snapshot summary.

    `overwrite` replaces every row in one new snapshot (the old one stays in the history, as with
    INSERT OVERWRITE); `append` adds one. `everything` is the always-true filter (F.lit(True)).
    """
    writer = frame.writeTo(table)
    if pins:
        writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{SOURCE_SNAPSHOTS_PROPERTY}", pins_property_value(pins))
    if mode == "overwrite":
        writer.overwrite(everything)
    elif mode == "append":
        writer.append()
    else:
        raise ValueError(f"unknown Iceberg write mode {mode}")


# ---- Spark entry -------------------------------------------------------------------------------

def _snapshots(spark: Any, table: str) -> list[dict[str, Any]]:
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, committed_at, operation, summary FROM {table}.snapshots"
    ).collect()
    return [{
        "snapshot_id": str(r.snapshot_id),
        "parent_id": None if r.parent_id is None else str(r.parent_id),
        "committed_at": r.committed_at.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        "operation": r.operation,
        "summary": dict(r.summary or {}),
    } for r in rows]


def _table_state(spark: Any, catalog: str, name: str) -> dict[str, Any] | None:
    from lakehouse_engine import current_snapshot
    namespace, table = name.split(".", 1)
    qualified = f"`{catalog}`.`{namespace}`.`{table}`"
    if not spark.catalog.tableExists(f"{catalog}.{namespace}.{table}"):
        return None
    refs = spark.sql(f"SELECT name FROM {qualified}.refs WHERE name = 'main'").collect()
    if not refs:
        return {"head": None, "snapshots": []}
    return {"head": current_snapshot(spark, qualified), "snapshots": _snapshots(spark, qualified),
            "qualified": qualified}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--iceberg-catalog-name",
                        default=os.getenv("FOUNDATION_PLATFORM_SPARK_ICEBERG_CATALOG_NAME", "r2"))
    parser.add_argument("--plan-output", required=True)
    parser.add_argument("--pins-output", required=True)
    parser.add_argument("--unconditional-reason", default=None,
                        help="rebuild regardless of the Silver history, for this logged reason")
    parser.add_argument("--no-time-fallback", dest="time_fallback", action="store_false",
                        help="refuse a Gold without recorded pins instead of judging it by publish time")
    parser.add_argument("--measuring", action="store_true",
                        help="a dry run: an input the contract lists as unmeasured may be read")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    contract = load_contract()
    entry = contract["tables"].get(args.gold_table)
    if entry is None:
        raise PlanError(f"{args.gold_table} is not in the gold rebuild contract")
    producer = importlib.import_module(entry["producer"])
    inputs = producer_inputs(producer)

    from pyspark.sql import SparkSession, functions as F
    from lakehouse_engine import apply_catalog_settings

    builder = SparkSession.builder.appName("gold-rebuild-plan").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        required, optional, _ = inputs
        silver = {name: _table_state(spark, args.iceberg_catalog_name, name) for name in (*required, *optional)}
        gold = _table_state(spark, args.iceberg_catalog_name, args.gold_table)
        if gold is not None and gold["head"] is not None:
            head = next(s for s in gold["snapshots"] if s["snapshot_id"] == gold["head"])
            gold["row_count"] = int(head["summary"]["total-records"])
            if recorded_pins(gold["snapshots"], gold["head"]) is None and args.time_fallback:
                built = spark.table(gold["qualified"]).agg(F.max("published_at_utc").alias("at")).first().at
                if built is not None and not isinstance(built, str):
                    built = built.replace(tzinfo=timezone.utc).isoformat()  # session time zone is UTC
                gold["published_at_utc"] = built
        decision = plan(args.gold_table, entry, inputs, gold, silver,
                        unconditional=args.unconditional_reason, time_fallback=args.time_fallback,
                        measuring=args.measuring,
                        whole_table_inputs=producer_whole_table_inputs(producer))
    finally:
        spark.stop()
    for path, value in ((args.pins_output, decision["source_snapshots"]), (args.plan_output, decision)):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1) + "\n",
                              encoding="utf-8")
    print("gold-rebuild-plan " + json.dumps(
        {k: decision[k] for k in ("gold_table", "action", "reasons", "previous_row_count", "minimum_row_count",
                                  "mode", "mode_reasons")},
        ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
