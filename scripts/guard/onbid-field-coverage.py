#!/usr/bin/env python3
"""Every field the Onbid APIs return is either carried to the screen or says why not (root ADR-0340).

Prevents: an Onbid field we already receive silently stopping short of the screen. On 2026-10-25 the
bid results (낙찰가·입찰자 수) had been collected and loaded into Silver for six days and were served
nowhere, the item detail's appraisal, tenancy and notice fields reached no table, and ADR-0296 §4
planned a separate usage-code collection for names every response already carried inline; nobody
could tell, because no file said which provider field went where.

`data-go-kr-onbid.fields.json` is that file. This guard holds its claims to the contracts:

- every field is collected, stored, served and shown (`web`), or names a `reason`, or points at the
  field of another API that carries the same value (`same_as`);
- a claim follows the chain: shown needs served, served needs stored, stored needs collected;
- `silver` names a column of the lakehouse contract, `gold` a path of the public-auction subject's
  current revision in the schema registry (root ADR-0336), `web` a path the registry's `web`
  consumer pact reads and whose leaf the panel uses; a collected API is in the source contract;
- the other way: every content column of an Onbid Silver table and every path of the served round
  is claimed by a field or declared `derived`, so the inventory cannot fall behind a new column;
- a field the inventory marks `pii` (a private person's name) is claimed by no Silver column.

`refresh --check` (network, not CI) compares the field paths with the live data.go.kr swaggerJson.
"""

from __future__ import annotations

import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FP = ROOT / "platforms" / "foundation-platform"
INVENTORY = FP / "infra" / "lakehouse" / "contracts" / "data-go-kr-onbid.fields.json"
SOURCE = FP / "infra" / "lakehouse" / "contracts" / "data-go-kr-onbid.contract.json"
LAKEHOUSE = FP / "infra" / "lakehouse" / "contracts" / "industrial_complex_lakehouse_contracts.json"
SUBJECT = "foundation-platform.parcel_public_auction_by_pnu"
PANEL = ROOT / "products" / "gongzzang" / "apps" / "web" / "components" / "panels" / "parcel" / "public-auction.tsx"
LOAD_COLUMNS = {"source_record_id", "source_line_number", "source_snapshot_id", "ingested_at_utc", "row_state"}
ROUND = "public_auction.rounds[]."
ENVELOPE = {"header.resultCode", "header.resultMsg", "body.totalCount", "body.numOfRows", "body.pageNo"}


def registry_view() -> tuple[list[str], set[str]]:
    """(every path of the subject's current revision, the paths the web consumer reads), from the
    schema registry (root ADR-0336) through its own reader."""

    import importlib.util  # noqa: PLC0415

    spec = importlib.util.spec_from_file_location("schema_registry", FP / "scripts" / "schema_registry.py")
    registry_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(registry_module)
    registry = registry_module.load(FP)
    entry = next(e for e in registry["subjects"] if e["subject"] == SUBJECT)
    schema = registry_module.schema_of(FP, SUBJECT, registry_module.current(entry))
    record = [r["path"] for r in registry_module.record_of(schema)]
    web = next(c for c in entry["consumers"] if c["name"] == "web")
    return record, {r["path"] for r in web["reads"]}


def silver_columns(lakehouse: dict) -> dict[str, set[str]]:
    return {name: {c["name"] for c in contract["columns"]}
            for name, contract in lakehouse["contracts"].items() if name.startswith("silver.onbid_")}


def leaf(path: str) -> str:
    return path.split(".")[-1].replace("[]", "")


def check(inventory: dict, source: dict, lakehouse: dict, record: list[str], web_reads: set[str],
          panel: str) -> list[str]:
    failures: list[str] = []
    tables = silver_columns(lakehouse)
    claimed_columns: set[str] = set()
    claimed_paths: set[str] = set()
    derived = inventory.get("derived", {})
    known: set[str] = set()
    for api, spec in inventory["apis"].items():
        if spec["collected"] and api not in source:
            failures.append(f"{api}: marked collected, but data-go-kr-onbid.contract.json has no {api} endpoint")
        known.update(f"{api}.{f['path']}" for f in spec["fields"])
    for api, spec in inventory["apis"].items():
        seen = set()
        for field in spec["fields"]:
            name = f"{api}.{field['path']}"
            if field["path"] in seen:
                failures.append(f"{name}: listed twice")
            seen.add(field["path"])
            silver, gold, web = field.get("silver", []), field.get("gold", []), field.get("web", False)
            if field.get("bronze") is not spec["collected"]:
                failures.append(f"{name}: bronze must equal its API's collected ({spec['collected']})")
            if "same_as" in field:
                if field["same_as"] not in known:
                    failures.append(f"{name}: same_as {field['same_as']} is no inventory field")
                if silver or gold or web:
                    failures.append(f"{name}: a same_as field claims nothing itself")
                continue
            if not web and not str(field.get("reason", "")).strip():
                failures.append(f"{name}: not shown and gives no reason")
            if web and not gold:
                failures.append(f"{name}: shown but not served")
            if gold and not silver:
                failures.append(f"{name}: served but not stored")
            if silver and not field.get("bronze"):
                failures.append(f"{name}: stored but not collected")
            if field.get("pii") and silver:
                failures.append(f"{name}: a private person's name must not reach Silver")
            for column in silver:
                table, _, col = column.rpartition(".")
                if col not in tables.get(table, set()):
                    failures.append(f"{name}: {column} is no column of the lakehouse contract")
                claimed_columns.add(column)
            for path in gold:
                if path not in record:
                    failures.append(f"{name}: {path} is no path of the public-auction subject's current revision")
                claimed_paths.add(path)
            if web and not any(path in web_reads and re.search(rf"\b{re.escape(leaf(path))}\b", panel)
                               for path in gold):
                failures.append(f"{name}: shown, but the web pact does not read, or the panel does not use, "
                                f"any of {gold}")
        stray = set(spec.get("envelope", [])) - ENVELOPE
        if stray:
            failures.append(f"{api}: envelope names response fields {sorted(stray)}")
    for table, columns in tables.items():
        for col in sorted(columns - LOAD_COLUMNS):
            column = f"{table}.{col}"
            if column not in claimed_columns and column not in derived:
                failures.append(f"{column}: no inventory field claims it and it is not declared derived")
    for path in record:
        if path.startswith(ROUND) and path not in claimed_paths and path not in derived:
            failures.append(f"{path}: no inventory field claims it and it is not declared derived")
    for section in inventory.get("web_only", []) + inventory.get("site_sections", []):
        for ref in section["api_fields"]:
            if ref not in known:
                failures.append(f"{section['section']}: {ref} is no inventory field")
    return failures


def spec_paths(dataset: str) -> set[str]:
    page = urllib.request.urlopen(f"https://www.data.go.kr/data/{dataset}/openapi.do", timeout=60).read().decode()
    found = re.search(r"swaggerJson = `(.*?)`", page, re.S)
    if not found:
        raise SystemExit(f"{dataset}: the page carries no swaggerJson")
    swagger = json.loads(found.group(1))
    paths: list[str] = []

    def walk(schema: dict, at: str) -> None:
        if schema.get("properties"):
            for key, value in schema["properties"].items():
                walk(value, f"{at}.{key}" if at else key)
        elif schema.get("type") == "array":
            walk(schema.get("items", {}), at + "[]")
        else:
            paths.append(at)

    walk(next(iter(swagger["definitions"].values())), "")
    return {p[len("body.items.item."):] for p in paths if p not in ENVELOPE}


def refresh(inventory: dict) -> int:
    drift = 0
    for api, spec in inventory["apis"].items():
        live = spec_paths(spec["dataset"].split(":", 1)[1])
        held = {f["path"] for f in spec["fields"]}
        for path in sorted(live - held):
            print(f"{api}: the provider added {path}")
            drift += 1
        for path in sorted(held - live):
            print(f"{api}: the provider no longer lists {path}")
            drift += 1
    print(f"onbid-field-coverage refresh: {drift} difference(s)")
    return 1 if drift else 0


def main(argv: list[str]) -> int:
    inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
    if argv[:1] == ["refresh"]:
        return refresh(inventory)
    record, web_reads = registry_view()
    failures = check(
        inventory,
        json.loads(SOURCE.read_text(encoding="utf-8")),
        json.loads(LAKEHOUSE.read_text(encoding="utf-8")),
        record,
        web_reads,
        PANEL.read_text(encoding="utf-8"),
    )
    for failure in failures:
        print(f"  {failure}", file=sys.stderr)
    if failures:
        print("FAIL onbid-field-coverage: data-go-kr-onbid.fields.json and the contracts disagree. Carry the "
              "field to where the inventory says, or say in `reason` why it stops (root ADR-0340).", file=sys.stderr)
        return 1
    fields = [f for spec in inventory["apis"].values() for f in spec["fields"]]
    print(f"OK onbid-field-coverage: {len(fields)} fields, {sum(bool(f.get('web')) for f in fields)} shown")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
