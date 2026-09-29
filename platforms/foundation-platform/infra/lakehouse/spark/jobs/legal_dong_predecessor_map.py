#!/usr/bin/env python3
"""Write the legal-dong predecessor map {new code: old code} for one pair of cadastral snapshots.

The administrative-boundary Silver load keeps each place's id across renumbering (root ADR-0113 §9)
and needs to know which new legal-dong code came from which old one. That is exactly the dong
pairing of the parcel lineage (`parcel_lineage.pair_legal_dongs`): the official code list plus the
lot numbers each dong held before and after. This command runs that pairing on PNU lists and writes
only the dongs whose code changed.

Inputs are plain files so it runs anywhere the snapshots are: the official 법정동코드 전체자료
(.zip/.txt) and, per side, CSV files with a `PNU` column (the cadastral extract) or one PNU per line.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path

import parcel_lineage as pl
from legal_dong_code_snapshot_to_reference import read_code_file


def read_pnus(patterns: list[str]) -> set[str]:
    out: set[str] = set()
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            with open(path, encoding="utf-8", newline="") as fh:
                first = fh.readline()
                fh.seek(0)
                if "PNU" in first:
                    out.update(row["PNU"] for row in csv.DictReader(fh))
                else:
                    out.update(line.strip() for line in fh if line.strip())
    return pl.partition_valid(out)[0]


def predecessor_map(pairing: pl.DongPairing, lots_before: dict[str, set[str]]) -> dict[str, str]:
    """{new: old} for renumbered dongs. Two old dongs landing on one new code (a merger of dongs)
    leave the id with the one that brought the most lots; the other keeps its lineage only."""

    best: dict[str, tuple[int, str]] = {}
    for old, new in pairing.pairs.items():
        if old == new:
            continue
        weight = len(lots_before.get(old, ()))
        if new not in best or weight > best[new][0]:
            best[new] = (weight, old)
    # Rural parcels are numbered at the 리 level (the last two digits), while the administrative
    # boundary is the 읍·면·동 (`xxxxxxxx00`). Roll the 리 pairs up: the 읍면 that brought the most
    # lots into a new 읍면 is its predecessor, unless that level was paired directly.
    rolled: dict[str, dict[str, int]] = {}
    for old, new in pairing.pairs.items():
        if old[:8] == new[:8]:
            continue
        rolled.setdefault(new[:8] + "00", {}).setdefault(old[:8] + "00", 0)
        rolled[new[:8] + "00"][old[:8] + "00"] += len(lots_before.get(old, ()))
    for new_emd, olds in rolled.items():
        if new_emd not in best:
            old_emd, weight = max(olds.items(), key=lambda item: (item[1], item[0]))
            best[new_emd] = (weight, old_emd)
    return {new: old for new, (_, old) in sorted(best.items())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--code-list", required=True)
    parser.add_argument("--before", required=True, action="append", help="glob of the earlier snapshot's PNU files")
    parser.add_argument("--after", required=True, action="append", help="glob of the later snapshot's PNU files")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    codes = pl.parse_code_list(read_code_file(Path(args.code_list)))
    before, after = read_pnus(args.before), read_pnus(args.after)
    lots_before, lots_after = pl.lots_by_dong(before), pl.lots_by_dong(after)
    pairing = pl.pair_legal_dongs(codes, lots_before, lots_before, lots_after)
    mapping = predecessor_map(pairing, lots_before)
    Path(args.output).write_text(json.dumps(mapping, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    summary = {
        "renumbered_dongs": len(mapping),
        "paired_how": {how: sum(1 for c, h in pairing.how.items() if h == how and pairing.pairs[c] != c) for how in set(pairing.how.values())},
        "unpaired": pairing.unpaired,
        "split_signals": {c: round(pairing.lot_overlap[c], 4) for c in pairing.split_signals()},
    }
    print("legal-dong-predecessor-map-json " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
