#!/usr/bin/env python3
"""Render, from the declared definitions, whether each Silver table reaches the gongzzang screen.

One row per Silver table of the pipeline graph (root ADR-0086): the by-PNU lanes and tile sets it
reaches, whether that surface feeds the gongzzang panel, and whether the job that fills the surface
is on. The rows replace the block between the two markers in docs/roadmap/production-readiness.md;
`--check` fails when the block is not what the definitions generate now. Rows are sorted by what an
industrial real-estate user looks for first (factory and warehouse, land, building, then trades),
the order root ADR-0442 sorts the readiness table by. No totals line in the file: a line every new
dataset rewrites makes unrelated PRs conflict (root ADR-0176); `--summary` prints the counts.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import declared_definitions as dd

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = Path("docs/roadmap/production-readiness.md")
BEGIN = "<!-- BEGIN GENERATED showing-inventory: python3 scripts/catalog/render-showing-inventory.py -->"
END = "<!-- END GENERATED showing-inventory -->"
PANEL = "gongzzang-panel"
DATA = {"feeds", "joins", "serves"}
LIVE = {"implemented", "partial"}
# What an industrial real-estate user asks first. First matching prefix wins; the rest go last.
TIERS = (
    ("공장·산업단지", ("silver.registered_factory", "silver.industrial_complex", "silver.complex_parcel")),
    ("토지", ("silver.land_", "silver.parcel_")),
    ("건물", ("silver.building_", "silver.housing_permit_", "silver.unit_official_price")),
    ("거래·공매·경매", ("silver.real_transaction", "silver.onbid_", "silver.court_auction_")),
    ("물류 입지", ("silver.highway_", "silver.port", "silver.airport", "silver.railway_", "silver.subway_")),
    ("주변 생활", ("silver.commercial_", "silver.school_", "silver.university", "silver.dong_population",
                 "silver.household_")),
)
VERDICTS = ("화면", "길 있음·꺼짐", "서빙만", "적재 꺼짐", "없음")
# Bookkeeping a user never reads directly: a table's version log, its rejected rows, the ID ledger, and
# the PNU change record the 법정동 pairing reads as evidence (the same 토지이동 events, with their old
# 지번 in the provider's words, reach the parcel panel from silver.land_transfer_history).
INTERNAL = ("_versions", "_rejected", "silver.parcel_registry", "silver.parcel_number_change_history")


def tier(table: str) -> int:
    if table.endswith(INTERNAL) or table in INTERNAL:
        return len(TIERS)
    return next((i for i, (_, prefixes) in enumerate(TIERS) if table.startswith(prefixes)), len(TIERS))


def inventory(documents: dict) -> list[dict]:
    graph, jobs = documents[dd.GRAPH], documents[dd.JOBS]["jobs"]
    index = {n["id"]: n for n in graph["nodes"]}
    out: dict[str, list[str]] = {}
    for e in graph["edges"]:
        if e["relation"] in DATA and e["status"] in LIVE:
            out.setdefault(e["from"], []).append(e["to"])
    lanes = {node: lane["lane"] for lane in dd.lane_index(documents)["lanes"] for node in lane["serving_nodes"]}
    web = {(s.get("owner") or {}).get("lane") for s in documents[dd.REGISTRY]["subjects"]
           if any(c["name"] == "web" for c in s.get("consumers", []))}
    on = {e.split("-to-", 1)[1] for j in jobs if j.get("enabled") for e in j.get("pipeline_graph_edges", [])}
    # A Silver with no path whose every loading job is off holds no rows yet: the step it waits for is
    # its loader's first run, not a Gold (2026-11-09: 19 such tables read as 없음 beside contract-only ones).
    loaded = {e.split("-to-", 1)[1] for j in jobs for e in j.get("pipeline_graph_edges", [])}

    def walk(start: str, stop_at_gold: bool) -> set[str]:
        # A Silver reaches what its own Gold serves; going on through a second Gold would credit
        # every table that feeds the parcel edition with every lane the edition gates.
        seen, pending = set(), [start]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            kind = index[current]["type"]
            if current != start and (kind == "serving_surface" or (stop_at_gold and kind == "gold_table")):
                continue
            pending.extend(out.get(current, []))
        return seen

    rows = []
    for n in graph["nodes"]:
        if n["type"] != "silver_table":
            continue
        reached = walk(n["id"], True)
        for gold in [x for x in reached if index[x]["type"] == "gold_table"]:
            reached |= walk(gold, True)
        surfaces = sorted(x for x in reached if index[x]["type"] == "serving_surface" and x != PANEL)
        shown = lambda s: PANEL in out.get(s, []) or lanes.get(s) in web
        live = lambda s: s in on
        verdict = ("화면" if any(shown(s) and live(s) for s in surfaces) else
                   "길 있음·꺼짐" if any(shown(s) for s in surfaces) else
                   "서빙만" if any(live(s) for s in surfaces) else
                   "적재 꺼짐" if n["id"] in loaded and n["id"] not in on else "없음")
        lane_names = sorted({lanes[s] + ("" if live(s) else " (꺼짐)") for s in surfaces if s in lanes})
        tiles = sorted(s + ("" if live(s) else " (꺼짐)") for s in surfaces if index[s].get("surface_kind") == "tiles")
        rows.append({"table": n["table_name"], "title": n["title"], "tier": tier(n["table_name"]),
                     "lanes": lane_names, "tiles": tiles, "verdict": verdict})
    rows.sort(key=lambda r: (r["tier"], VERDICTS.index(r["verdict"]), r["table"]))
    return rows


def md(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def block(rows: list[dict]) -> str:
    names = [name for name, _ in TIERS] + ["행정·내부"]
    lines = [BEGIN, "",
             "| 사용자 가치 | Silver 표 | by-PNU 레인 | 타일 | 공짱 화면 |", "| --- | --- | --- | --- | --- |"]
    for r in rows:
        lines.append(f"| {names[r['tier']]} | {md(r['title'])} (`{r['table']}`) | "
                     f"{'<br>'.join(f'`{x}`' for x in r['lanes']) or '—'} | "
                     f"{'<br>'.join(f'`{x}`' for x in r['tiles']) or '—'} | {r['verdict']} |")
    return "\n".join([*lines, "", END])


def replace(text: str, generated: str) -> str:
    start, end = text.find(BEGIN), text.find(END)
    if start < 0 or end < start:
        raise ValueError(f"{OUTPUT}: the {BEGIN!r} … {END!r} markers are missing")
    return text[:start] + generated + text[end + len(END):]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--summary", action="store_true", help="print the counts; write nothing")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    try:
        rows = inventory(dd.generate(*dd.read_definitions(args.root / dd.DEFINITIONS)))
        if args.summary:
            for verdict in VERDICTS:
                count = sum(r["verdict"] == verdict for r in rows)
                print(f"{verdict}\t{count}\t{count / len(rows):.0%}")
            print(f"Silver\t{len(rows)}")
            return 0
        target = args.root / OUTPUT
        text = target.read_text(encoding="utf-8")
        rendered = replace(text, block(rows))
        if args.check:
            if rendered != text:
                print(f"stale showing inventory: {OUTPUT}; run python3 scripts/catalog/render-showing-inventory.py",
                      file=sys.stderr)
                return 1
        elif rendered != text:
            target.write_text(rendered, encoding="utf-8", newline="\n")
            print(OUTPUT.as_posix())
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f"showing inventory: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
