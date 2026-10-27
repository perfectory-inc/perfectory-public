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
  Spark size was not shown to hold with it. Once a required one exists the plan refuses; an optional
  one is not read (the producer runs without it, as if absent, and the plan names it under
  `unmeasured_inputs_unread`), so its appearance never stops a scheduled rebuild. A dry run
  (`measuring=True`) reads it, which is how it gets measured.
- An optional input (the parcel lineage) that does not exist is refused unless the contract names
  it under `absent_inputs_accepted` with its reason; the producer then runs without it and the plan
  says so. That entry is refused once the input exists, and an optional input with no rows is
  refused like a missing one, so a Gold is never built without it unnoticed.
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

import section_coverage
from lakehouse_ingest import SNAPSHOT_PROPERTY_PREFIX

SOURCE_SNAPSHOTS_PROPERTY = "foundation.source-iceberg-snapshots"
PLAN_SCHEMA_VERSION = "foundation-platform.gold_rebuild_plan.v1"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "gold-panel-rebuild.contract.json"
# Summary counters of a commit that changes rows (Iceberg SnapshotSummary).
ROW_CHANGE_COUNTERS = ("added-records", "deleted-records", "added-position-deletes",
                       "added-equality-deletes")


class PlanError(ValueError):
    pass


def kind_tables(template: dict[str, Any], kinds: dict[str, Any],
                panels: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """One table entry per by-PNU data kind (root ADR-0193), from the contract's `kind_tables`.

    A section of the host panel reads a subset of the panel's inputs, pinned to the panel's current
    snapshots and checked against it. A kind with its own producer (root ADR-0287) reads its own
    inputs at their own heads, so it carries no pin and no parity check.
    """
    tables = {}
    for lane, kind in kinds.items():
        pin = template["pins_from_host_panel"]
        entry = {
            "unit": lane, "lane": lane, "producer": kind.producer or template["producer"],
            "producer_arguments": ["--lane", lane],
            **{k: template[k] for k in ("max_row_loss_fraction", "max_row_loss_reason", "spark") if k in template},
            # The lineage is optional to every parcel kind: the host's leave to go without it.
            "absent_inputs_accepted": panels.get(kind.host_panel, {}).get("absent_inputs_accepted", {}),
        }
        # A kind whose rows are not a slowly-growing register (e.g. open public-auction rounds that open
        # and close daily) carries its own row-loss bound and reason under `overrides.<lane>`, instead
        # of the stable-panel default; both must be given together (root ADR-0287).
        override = template.get("overrides", {}).get(lane, {})
        for key in ("max_row_loss_fraction", "max_row_loss_reason"):
            if key in override:
                entry[key] = override[key]
        if kind.follows_host_panel:
            # Root ADR-0180's incremental merge reads the panel's input map; an own producer rebuilds whole.
            if "incremental" in template:
                entry["incremental"] = template["incremental"]
            # Its host panel's: a kind reads a subset of the panel's inputs at the panel's pins.
            entry.update({
                "unmeasured_inputs": panels.get(kind.host_panel, {}).get("unmeasured_inputs", {}),
                "pins_from": {"gold_table": kind.host_panel, "inputs": "all",
                              "parity_argument": pin["parity_argument"], "reason": pin["reason"]},
            })
        tables[kind.gold_table] = entry
    return tables


def load_contract(path: Path = CONTRACT_PATH, kinds: dict[str, Any] | None = None) -> dict[str, Any]:
    contract = json.loads(path.read_text(encoding="utf-8"))
    tables = contract.get("tables")
    if not isinstance(tables, dict) or not tables:
        raise PlanError("gold rebuild contract names no tables")
    if "kind_tables" in contract:
        import by_pnu_kinds
        found = by_pnu_kinds.kinds() if kinds is None else kinds
        derived = kind_tables(contract["kind_tables"], found, tables)
        clash = sorted(set(derived) & set(tables))
        if clash:
            raise PlanError(f"{clash} are kind tables: their line in the R2 connection contract is their entry")
        tables = contract["tables"] = {**tables, **derived}
        # Root ADR-0272: every table that carries served sections (a kind's host panel, and each kind)
        # is held to the one section coverage bound.
        carriers = {kind.host_panel for kind in found.values()} | set(derived)
        coverage = contract.get("section_coverage")
        if carriers and coverage is None:
            raise PlanError(f"{sorted(carriers)} carry served sections and the contract declares no section_coverage")
        if coverage is not None:
            try:
                section_coverage.bound_from_contract(coverage, shares=True)
            except ValueError as error:
                raise PlanError(str(error)) from None
            for name in carriers & set(tables):
                tables[name]["section_coverage"] = coverage
    for name, table in tables.items():
        fraction = table.get("max_row_loss_fraction")
        if type(fraction) not in (int, float) or not 0 <= fraction < 1:
            raise PlanError(f"{name}: max_row_loss_fraction must be a number from 0 up to 1")
        if not str(table.get("max_row_loss_reason", "")).strip():
            raise PlanError(f"{name}: max_row_loss_fraction needs its reason")
        if not isinstance(table.get("producer"), str):
            raise PlanError(f"{name}: names no producer")
        args = table.get("producer_arguments", [])
        # Flags and their values; the rebuild script splits them on whitespace.
        if not (isinstance(args, list) and all(isinstance(a, str) and a and not any(c.isspace() for c in a)
                                               for a in args) and (not args or args[0].startswith("--"))):
            raise PlanError(f"{name}: producer_arguments must be a list of flags and their values")
        unmeasured = table.get("unmeasured_inputs", {})
        if not (isinstance(unmeasured, dict)
                and all(isinstance(why, str) and why.strip() for why in unmeasured.values())):
            raise PlanError(f"{name}: unmeasured_inputs must map each input to why it is unmeasured")
        accepted = table.get("absent_inputs_accepted", {})
        if not (isinstance(accepted, dict)
                and all(isinstance(why, str) and why.strip() for why in accepted.values())):
            raise PlanError(f"{name}: absent_inputs_accepted must map each input to why it may be absent")
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
        shrink = table.get("max_price_history_shrink_fraction")
        if shrink is not None and (type(shrink) not in (int, float) or not 0 <= shrink < 1
                                   or not str(table.get("max_price_history_shrink_reason", "")).strip()):
            raise PlanError(f"{name}: max_price_history_shrink_fraction must be a number from 0 up to 1 with its reason")
        follows = table.get("pins_from")
        if follows is not None:
            inputs = follows.get("inputs")
            if (follows.get("gold_table") not in tables or follows.get("gold_table") == name
                    or not (inputs == "all" or isinstance(inputs, list) and inputs
                            and all(isinstance(i, str) for i in inputs))
                    or not str(follows.get("reason", "")).strip()):
                raise PlanError(f"{name}: pins_from names another table of this contract, the inputs it "
                                "pins and its reason")
            parity = follows.get("parity_argument")
            if parity is not None and not (isinstance(parity, str) and parity.startswith("--")):
                raise PlanError(f"{name}: pins_from.parity_argument must be a producer flag")
    return contract


def producer_inputs(module: Any, entry: dict[str, Any] | None = None) -> tuple[tuple[str, ...], dict[str, str], str]:
    """(required inputs, optional input -> flag the producer takes without it, anchor input).

    The lists are the producer's own constants (ADR-0130: the producer owns its input list); a kind
    table's are its section's (`lane_inputs`).
    """
    if entry is not None and "lane" in entry:
        return module.lane_inputs(entry["lane"])[:3]
    optional = {module.LINEAGE_SOURCE: "--no-carry-lineage"} if hasattr(module, "LINEAGE_SOURCE") else {}
    anchor = getattr(module, "PARCEL_SOURCE", None) or module.TITLE_SOURCE
    return tuple(module.ALL_SOURCES), optional, anchor


def holds_no_rows(state: dict[str, Any]) -> bool:
    """True when the table has no `main` snapshot or its head snapshot records zero rows."""
    if state.get("head") is None:
        return True
    head = next((s for s in state["snapshots"] if s["snapshot_id"] == state["head"]), None)
    return head is not None and (head.get("summary") or {}).get("total-records") == "0"


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


def producer_whole_table_inputs(module: Any, entry: dict[str, Any] | None = None) -> tuple[str, ...]:
    """Inputs whose change the producer's incremental rebuild does not map to PNUs (ADR-0180)."""
    if entry is not None and "lane" in entry:
        return module.lane_inputs(entry["lane"])[3]
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
         whole_table_inputs: tuple[str, ...] = (),
         pinned_gold: dict[str, Any] | None = None) -> dict[str, Any]:
    """Decide one Gold table's rebuild.

    `gold`: {"head", "snapshots", "row_count", "published_at_utc"} or None when the table does
    not exist. `silver`: input -> {"head", "snapshots"}, or None for a table that does not exist.
    `unconditional`: the reason to rebuild regardless of the history. `time_fallback`: whether a
    Gold without recorded pins may be judged by its publish time (the job is not enabled yet).
    `measuring`: a dry run, which may read an input the contract lists as unmeasured.
    `pinned_gold`: the state of the table the contract's `pins_from` names, like `gold`: the inputs
    it lists are pinned to the snapshots that table's current Gold was built from, not to their heads.
    With `pins_from.parity_argument` every input must be pinned so, and the producer gets that flag with
    the followed table's head snapshot, against which it checks its rows before it writes.
    """
    if unconditional is not None and not unconditional.strip():
        raise PlanError(f"{table}: an unconditional rebuild needs its reason")
    required, optional, anchor = inputs
    missing = [name for name in required if silver.get(name) is None]
    if missing:
        raise PlanError(f"{table}: required Silver inputs do not exist: {missing}")
    accepted = entry.get("absent_inputs_accepted", {})
    # An optional input no run was measured with is not read by a real rebuild: the Gold is built
    # without it, as if it were absent, and the plan says so. Only a dry run reads it, which is how
    # it is measured. Its appearance therefore never refuses a scheduled rebuild.
    held_back = {name: why for name, why in entry.get("unmeasured_inputs", {}).items()
                 if name in optional and silver.get(name) is not None and not measuring}
    absent = [name for name in optional if silver.get(name) is None]
    unread = [*absent, *held_back]
    unaccepted = [name for name in absent if name not in accepted]
    if unaccepted:
        raise PlanError(
            f"{table}: {unaccepted} do not exist, so the Gold would be built without them and every column "
            "they feed left empty; load them, or name each with its reason under absent_inputs_accepted "
            "(contracts/gold-panel-rebuild.contract.json)")
    stale = sorted(name for name in accepted if silver.get(name) is not None and name not in held_back)
    # A dry run measures the input that just appeared, before the entry can go (unmeasured_inputs).
    if stale and not measuring:
        raise PlanError(f"{table}: {stale} now exist; remove them from absent_inputs_accepted so the Gold reads them")
    empty = [name for name in optional if name not in unread and holds_no_rows(silver[name])]
    if empty:
        raise PlanError(f"{table}: {empty} exist but hold no rows; an empty input is refused like a missing one")
    present = [*required, *(name for name in optional if name not in unread)]
    unmeasured = {name: why for name, why in entry.get("unmeasured_inputs", {}).items() if name in present}
    if unmeasured and not measuring:
        raise PlanError(
            f"{table}: {sorted(unmeasured)} now exist but no run was measured with them "
            f"({'; '.join(unmeasured.values())}); measure a dry run (gold-panel-rebuild.sh --dry-run) "
            "within the Spark cap, then remove them from unmeasured_inputs "
            "(contracts/gold-panel-rebuild.contract.json)")
    pins = {name: silver[name]["head"] for name in present}
    follows = entry.get("pins_from")
    if follows is not None:
        followed = None
        if pinned_gold is not None and pinned_gold.get("head") is not None:
            followed = recorded_pins(pinned_gold["snapshots"], pinned_gold["head"])
        if followed is None:
            raise PlanError(f"{table}: its {follows['inputs']} are pinned to what {follows['gold_table']} was "
                            "built from, and that table records no source pins")
        for name in present if follows["inputs"] == "all" else follows["inputs"]:
            if name not in pins or name not in followed:
                raise PlanError(f"{table}: {name} is pinned to {follows['gold_table']}, but it is not an input "
                                "of both producers")
            pins[name] = followed[name]
    flags = [flag for name, flag in optional.items() if name in unread]
    if follows is not None and follows.get("parity_argument"):
        # Root ADR-0193 §6-2: the producer checks its rows against the followed table's, which is only
        # a check when every input it reads is pinned where that table's were.
        unpinned = [] if follows["inputs"] == "all" else sorted(set(present) - set(follows["inputs"]))
        if unpinned:
            raise PlanError(f"{table}: checks parity with {follows['gold_table']}, but {unpinned} are not "
                            "pinned to what that table was built from")
        flags += [follows["parity_argument"], pinned_gold["head"]]
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
                why = input_change_since_pin(silver[name]["snapshots"], pins[name], recorded[name])
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
                    why = input_change_since_time(silver[name]["snapshots"], pins[name], built_at)
                    if why:
                        reasons.append(f"{name}: {why}")
    minimum = None if previous is None else math.ceil(previous * (1 - entry["max_row_loss_fraction"]))
    mode, mode_reasons, previous_pins = ("none", [], None)
    if reasons:
        mode, mode_reasons, previous_pins = incremental_mode(entry, gold, silver, pins, whole_table_inputs,
                                                             unconditional)
    settings = entry.get("incremental") or {}
    coverage = entry.get("section_coverage")
    # Root ADR-0272: the producer measures its sections against the served Gold before it writes.
    against = None if coverage is None or previous is None else gold["head"]
    if against is not None:
        flags += ["--section-coverage-snapshot", str(against),
                  "--max-section-coverage-drop", repr(float(coverage["max_share_drop_fraction"])),
                  "--section-coverage-min-pnus", str(coverage["min_region_pnus"])]
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "gold_table": table,
        "action": "rebuild" if reasons else "nothing_to_do",
        "reasons": reasons,
        "producer": entry["producer"],
        "producer_arguments": [*entry.get("producer_arguments", []), *flags],
        # Inputs built without, by the contract's leave; the run log says so on every run.
        "absent_inputs": {name: accepted[name] for name in absent},
        "unmeasured_inputs_unread": held_back,
        "anchor_input": anchor,
        "source_snapshots": pins,
        "previous_gold_snapshot_id": None if gold is None else gold.get("head"),
        "previous_row_count": previous,
        "minimum_row_count": minimum,
        "max_row_loss_fraction": entry["max_row_loss_fraction"],
        "section_coverage_against": against,
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


def write_gold_snapshot(frame: Any, table: str, mode: str, pins: dict[str, str], everything: Any,
                        properties: dict[str, str] | None = None) -> None:
    """Commit `frame` to `table` with the Silver pins it was built from in the snapshot summary.

    `overwrite` replaces every row in one new snapshot (the old one stays in the history, as with
    INSERT OVERWRITE); `append` adds one. `everything` is the always-true filter (F.lit(True)).
    `properties` are further snapshot summary entries (the content fingerprint, root ADR-0274).
    """
    writer = frame.writeTo(table)
    if pins:
        writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{SOURCE_SNAPSHOTS_PROPERTY}", pins_property_value(pins))
    for key, value in (properties or {}).items():
        writer = writer.option(f"{SNAPSHOT_PROPERTY_PREFIX}{key}", value)
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
    inputs = producer_inputs(producer, entry)

    from pyspark.sql import SparkSession, functions as F
    from lakehouse_engine import apply_catalog_settings

    builder = SparkSession.builder.appName("gold-rebuild-plan").config("spark.sql.session.timeZone", "UTC")
    spark = apply_catalog_settings(builder, args.iceberg_catalog_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        required, optional, _ = inputs
        silver = {name: _table_state(spark, args.iceberg_catalog_name, name) for name in (*required, *optional)}
        gold = _table_state(spark, args.iceberg_catalog_name, args.gold_table)
        follows = entry.get("pins_from")
        pinned_gold = None if follows is None else _table_state(spark, args.iceberg_catalog_name, follows["gold_table"])
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
                        whole_table_inputs=producer_whole_table_inputs(producer, entry), pinned_gold=pinned_gold)
    finally:
        spark.stop()
    for path, value in ((args.pins_output, decision["source_snapshots"]), (args.plan_output, decision)):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1) + "\n",
                              encoding="utf-8")
    print("gold-rebuild-plan " + json.dumps(
        {k: decision[k] for k in ("gold_table", "action", "reasons", "previous_row_count", "minimum_row_count",
                                  "mode", "mode_reasons", "absent_inputs", "unmeasured_inputs_unread")},
        ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
