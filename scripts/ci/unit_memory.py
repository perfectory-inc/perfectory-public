#!/usr/bin/env python3
"""Every systemd unit's memory lines, rendered from the host memory budget (root ADR-0455).

`tools/host-memory-budget.contract.json` `systemd_units` declares, once per unit file, its class and
its hard cap (`memory_max`), sized from the unit's measured `memory peak` with headroom. The unit's
`MemoryHigh=` (the soft bound where the kernel throttles and reclaims before it kills), `MemoryMax=`
and `OOMScoreAdjust=` (from the class: batch is the OOM killer's first choice, recovery its last)
are rendered from that entry and nowhere else. `--write` renders them into the committed units;
every-container-has-a-memory-cap.sh holds the units to their rendering, refuses a unit file with no
entry, and refuses a cap that alone, beside every standing service and host_reserved, would not fit
the host.

What real incident does failing this prevent? 2026-11-08 03:43Z foundation-source-sweep, a unit with
no MemoryMax, reached 33 GB of anonymous memory on the 62 GB host; the kernel's OOM killer ended it
and the PostgreSQL backup running beside it lost its connection (W31, W36). 45 more units had no cap.

Standard library only, as every guard here.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import re
import sys

REPOSITORY = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = "tools/host-memory-budget.contract.json"
# Where the units are when a contract does not say: a tree with units and no systemd_units fails.
DEFAULT_UNITS = "platforms/foundation-platform/infra/systemd"
SIZE = re.compile(r"([0-9]+)([KMG])")
BYTES = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}
KEYS = ("MemoryHigh", "MemoryMax", "OOMScoreAdjust")
MARK = "# Memory: rendered from tools/host-memory-budget.contract.json systemd_units (root ADR-0455); do not edit."
ENTRY_KEYS = {"class", "memory_max", "measured_peak", "measured", "headroom_why"}


class UnitMemoryError(ValueError):
    pass


def size(raw: object) -> int:
    match = SIZE.fullmatch(raw) if isinstance(raw, str) else None
    if not match:
        raise UnitMemoryError(f"{raw!r} is not a size like 512M or 4G")
    return int(match.group(1)) * BYTES[match.group(2)]


def high_of(memory_max: int, ratio: float) -> str:
    """MemoryHigh: `ratio` of the hard cap, whole MiB rounded down."""
    return f"{math.floor(memory_max * ratio / BYTES['M'])}M"


def lines_for(entry: dict, section: dict) -> list[str]:
    memory_max = size(entry["memory_max"])
    adjust = section["classes"][entry["class"]]["oom_score_adjust"]
    return [
        MARK,
        f"MemoryHigh={high_of(memory_max, section['memory_high_of_max'])}",
        f"MemoryMax={entry['memory_max']}",
        f"OOMScoreAdjust={adjust}",
    ]


def render(unit: str, entry: dict, section: dict) -> str:
    """The unit with its memory block where its first memory line was, else at the end of [Service].

    Every earlier MemoryHigh=, MemoryMax=, OOMScoreAdjust= and mark line goes; the comments a unit
    writes about its cap stay where they are.
    """
    lines = unit.split("\n")
    ours = {index for index, line in enumerate(lines) if line == MARK or line.split("=", 1)[0] in KEYS}
    kept = [line for index, line in enumerate(lines) if index not in ours]
    if ours:
        at = min(ours)
    else:
        start = next((i for i, line in enumerate(kept) if line.strip() == "[Service]"), None)
        if start is None:
            raise UnitMemoryError("has no [Service] section")
        at = next((i for i in range(start + 1, len(kept)) if kept[i].startswith("[")), len(kept))
        while at > start + 1 and kept[at - 1].strip() == "":
            at -= 1
    return "\n".join(kept[:at] + lines_for(entry, section) + kept[at:])


def unit_files(units: pathlib.Path) -> list[str]:
    return sorted(path.name for path in units.glob("*.service"))


def job_unit_files(root: pathlib.Path, jobs_relative: str) -> set[str]:
    jobs = json.loads((root / jobs_relative).read_text(encoding="utf-8"))
    found = set()
    for job in jobs["jobs"]:
        service = job["systemd_service"]
        name, at, rest = service.partition("@")
        found.add(f"{name}@.{rest.rsplit('.', 1)[-1]}" if at else service)
    return found


def problems(root: pathlib.Path, contract: dict, room: int | None = None) -> list[str]:
    """Every way the units and `systemd_units` disagree. `room` is what one unit may hold beside every
    standing service and host_reserved; a cap above it alone could take the host down."""
    section = contract.get("systemd_units")
    if not isinstance(section, dict):
        return [f"{CONTRACT}: systemd_units is missing; every systemd unit declares its memory (root ADR-0455)"]
    found: list[str] = []
    units = root / section["units"]
    classes = section["classes"]
    ratio = section["memory_high_of_max"]
    headroom = section["memory_max_over_peak"]
    floor = size(section["floor"])
    declared = section["unit_files"]
    batch_only = job_unit_files(root, section["jobs"])
    files = unit_files(units)
    for file in files:
        if file not in declared:
            found.append(f"{section['units']}/{file}: no systemd_units entry in {CONTRACT}; declare its class and memory_max from its measured peak")
    for file, entry in sorted(declared.items()):
        where = f"{CONTRACT}: systemd_units {file}"
        if file not in files:
            found.append(f"{where}: no such unit under {section['units']}")
            continue
        try:
            if not isinstance(entry, dict) or not set(entry) <= ENTRY_KEYS or not {"class", "memory_max", "measured"} <= set(entry):
                raise UnitMemoryError(f"names class, memory_max and measured, optionally measured_peak and headroom_why; not {sorted(entry)}")
            if entry["class"] not in classes:
                raise UnitMemoryError(f"class {entry['class']!r} is not one of {sorted(classes)}")
            if file in batch_only and entry["class"] != "batch":
                raise UnitMemoryError("runs a scheduled job (orchestration/jobs.v1.json), so its class is batch: the OOM killer takes jobs first")
            if not isinstance(entry["measured"], str) or not entry["measured"].strip():
                raise UnitMemoryError("measured says where the peak comes from, or why there is none yet")
            cap = size(entry["memory_max"])
            if cap < floor:
                raise UnitMemoryError(f"memory_max {entry['memory_max']} is under the floor {section['floor']}")
            peak = entry.get("measured_peak")
            if peak is not None and cap < size(peak) * headroom and not entry.get("headroom_why"):
                raise UnitMemoryError(
                    f"memory_max {entry['memory_max']} is under {headroom} x the measured peak {peak}; raise it, or say why in headroom_why"
                )
            if room is not None and cap > room:
                raise UnitMemoryError(
                    f"memory_max {entry['memory_max']} alone does not fit beside every standing service and host_reserved ({room >> 20}M left)"
                )
            text = (units / file).read_text(encoding="utf-8")
            if render(text, entry, section) != text:
                found.append(f"{section['units']}/{file}: its memory lines are not what {CONTRACT} says; run: python3 scripts/ci/unit_memory.py --write")
        except (UnitMemoryError, KeyError, TypeError) as error:
            found.append(f"{where}: {error}")
    return found


def write(root: pathlib.Path, contract: dict) -> list[str]:
    section = contract["systemd_units"]
    units = root / section["units"]
    changed = []
    for file, entry in sorted(section["unit_files"].items()):
        path = units / file
        text = path.read_text(encoding="utf-8")
        rendered = render(text, entry, section)
        if rendered != text:
            path.write_bytes(rendered.encode("utf-8"))
            changed.append(file)
    return changed


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=pathlib.Path, default=REPOSITORY)
    parser.add_argument("--write", action="store_true", help="render the memory lines into the units")
    arguments = parser.parse_args(argv)
    contract = json.loads((arguments.root / CONTRACT).read_text(encoding="utf-8"))
    if arguments.write:
        for file in write(arguments.root, contract):
            print(f"rendered {file}")
    found = problems(arguments.root, contract)
    for problem in found:
        print(f"FAIL unit-memory: {problem}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
