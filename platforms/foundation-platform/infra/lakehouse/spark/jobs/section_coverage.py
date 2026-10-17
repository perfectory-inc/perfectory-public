"""A served section may not silently lose a region (root ADR-0272).

On 2026-10-16 gold.parcel_panel was built with the land-rights section of 시도 12 (the former 29 and
46) empty. Every parcel still had its row, so the row floor (`max_row_loss_fraction`) saw nothing; only
the land-rights kind table, whose rows are that section, lost enough rows to be refused. A section's
loss is a loss of coverage, not of rows, and it is regional.

So a rebuild measures, for every served section (a kind line of `config/r2-connections.contract.json`
names the section and its columns, `by_pnu_kinds`), how many PNUs of each 시도 have it non-empty, and
compares that with the Gold snapshot being served (the plan's previous Gold head). The bound is
`section_coverage` in `contracts/gold-panel-rebuild.contract.json`, declared once for every table that
carries sections:

- a panel compares the share of its PNUs whose section is non-empty; a kind table, whose rows are
  exactly those PNUs, compares their count;
- per 시도 and over the whole table; a 시도 whose served count is below `min_region_pnus` is too small
  for a share to mean anything and is judged only inside the whole;
- a reorganization the code change table declares (`reference.legal_dong_code_change`, root
  ADR-0145) is accounted for, not loosened for: 시도 codes a recorded pair joins are compared as one
  region when the pair's two sides are split between the served snapshot and the new build
  (29 and 46 served, 12 built), so moving PNUs to a new code is not a loss and emptying one is.

A breach refuses the rebuild before it writes, like the row floor, naming each section and region.
Everything but `measure` is plain Python, so the CI runner without PySpark tests the decisions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

import legal_dong_code_change_views as code_change_views

# A PNU's first two digits are its 시도 code.
REGION_DIGITS = 2
WHOLE = "all"
# A section column holding one of these is empty; the panel writes "[]" for an absent list section.
EMPTY_VALUES = ("[]", "{}", "null", "")

# (section, region) -> (PNUs with the section non-empty, PNUs)
Counts = dict[tuple[str, str], tuple[int, int]]


@dataclass(frozen=True)
class Bound:
    max_drop_fraction: float
    min_region_pnus: int
    # True: compare the share of PNUs with the section (a panel); False: their count (a kind table).
    shares: bool


def bound_from_contract(declared: Mapping[str, Any], shares: bool) -> Bound:
    drop, minimum = declared.get("max_share_drop_fraction"), declared.get("min_region_pnus")
    if type(drop) not in (int, float) or not 0 <= drop < 1:
        raise ValueError("section_coverage.max_share_drop_fraction must be a number from 0 up to 1")
    if type(minimum) is not int or minimum < 0:
        raise ValueError("section_coverage.min_region_pnus must be a non-negative integer")
    if not str(declared.get("reason", "")).strip():
        raise ValueError("section_coverage needs its reason")
    return Bound(float(drop), minimum, shares)


def merged_regions(served: Iterable[str], built: Iterable[str], pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Region -> the region it is compared as: a pair whose old side only the served snapshot holds
    and whose new side the build holds (or the reverse) joins the two; every other region is itself."""
    served, built = set(served), set(built)
    parent = {region: region for region in served | built}

    def root(region: str) -> str:
        while parent[region] != region:
            region = parent[region]
        return region

    for old, new in pairs:
        if old == new or old not in parent or new not in parent:
            continue
        if (old in served - built and new in built) or (new in served - built and old in built):
            parent[root(old)] = root(new)
    groups: dict[str, list[str]] = {}
    for region in parent:
        groups.setdefault(root(region), []).append(region)
    return {region: "+".join(sorted(groups[root(region)])) for region in parent}


def regrouped(counts: Counts, regions: Mapping[str, str]) -> Counts:
    out: dict[tuple[str, str], list[int]] = {}
    for (section, region), (present, total) in counts.items():
        for key in ((section, regions.get(region, region)), (section, WHOLE)):
            slot = out.setdefault(key, [0, 0])
            slot[0] += present
            slot[1] += total
    return {key: (present, total) for key, (present, total) in out.items()}


def merged_counts(served: Counts, removed: Counts, added: Counts) -> Counts:
    """The counts of `served` with the rows of `removed` taken out and those of `added` put in: an
    incremental merge replaces the affected PNUs' rows (root ADR-0180)."""
    out = {key: list(value) for key, value in served.items()}
    for sign, counts in ((-1, removed), (1, added)):
        for key, (present, total) in counts.items():
            slot = out.setdefault(key, [0, 0])
            slot[0] += sign * present
            slot[1] += sign * total
    return {key: (present, total) for key, (present, total) in out.items() if total or present}


def breaches(served: Counts, built: Counts, pairs: Iterable[tuple[str, str]], bound: Bound) -> list[str]:
    """Every (section, region) whose coverage fell by more than the bound, as readable lines."""
    regions = merged_regions({r for _, r in served}, {r for _, r in built}, pairs)
    before, after = regrouped(served, regions), regrouped(built, regions)
    found = []
    for (section, region), (present, total) in sorted(before.items()):
        if region != WHOLE and present < bound.min_region_pnus:
            continue
        now_present, now_total = after.get((section, region), (0, 0))
        if bound.shares:
            was = present / total if total else 0.0
            now = now_present / now_total if now_total else 0.0
            unit = f"{was:.2%} -> {now:.2%} of PNUs ({present:,} -> {now_present:,})"
        else:
            was, now = float(present), float(now_present)
            unit = f"{present:,} -> {now_present:,} PNUs"
        if was > 0 and now < was * (1 - bound.max_drop_fraction):
            found.append(f"{section} in {'the whole table' if region == WHOLE else '시도 ' + region}: {unit}")
    return found


def assert_coverage(table: str, served: Counts, built: Counts, pairs: Iterable[tuple[str, str]],
                    bound: Bound) -> None:
    """Refuse, before the write, a Gold whose sections lost a region's coverage."""
    found = breaches(served, built, pairs, bound)
    if found:
        raise ValueError(
            f"{table}: section coverage fell by more than {bound.max_drop_fraction:.0%} against the served "
            f"snapshot (contracts/gold-panel-rebuild.contract.json section_coverage): " + "; ".join(found)
            + "; nothing was written")


def region_pairs(change_rows: Iterable[Mapping[str, Any]]) -> list[tuple[str, str]]:
    """The 시도 pairs the code change table records (old 시도 -> new 시도), from its plain rows."""
    return code_change_views.region_pairs(change_rows, REGION_DIGITS)


@dataclass
class Guard:
    """What a producer hands the build: how to measure a frame, and the served snapshot's counts."""
    table: str
    measure: Callable[[Any], Counts]
    served: Counts
    pairs: list[tuple[str, str]]
    bound: Bound

    def check(self, built: Counts) -> None:
        assert_coverage(self.table, self.served, built, self.pairs, self.bound)


def measure(frame: Any, sections: Mapping[str, str]) -> Counts:
    """(section, 시도) -> (PNUs whose `sections[section]` column is non-empty, PNUs) of `frame`."""
    from pyspark.sql import functions as F

    region = F.substring("pnu", 1, REGION_DIGITS).alias("region")
    present = [F.sum(F.when(F.col(column).isNotNull() & ~F.trim(F.col(column).cast("string")).isin(*EMPTY_VALUES),
                            1).otherwise(0)).alias(f"present_{index}")
               for index, column in enumerate(sections.values())]
    rows = frame.groupBy(region).agg(F.count(F.lit(1)).alias("total"), *present).collect()
    return {(section, row["region"]): (int(row[f"present_{index}"] or 0), int(row["total"]))
            for row in rows for index, section in enumerate(sections)}


def add_arguments(parser: Any) -> None:
    """The producer flags the rebuild plan passes (gold_rebuild.plan): all three, or none."""
    parser.add_argument("--section-coverage-snapshot",
                        help="Refuse, before writing, a Gold whose sections lost a region's coverage against "
                             "this served snapshot of the target table (root ADR-0272).")
    parser.add_argument("--max-section-coverage-drop", type=float, default=None)
    parser.add_argument("--section-coverage-min-pnus", type=int, default=None)


def validate_arguments(args: Any) -> None:
    given = [args.section_coverage_snapshot is not None, args.max_section_coverage_drop is not None,
             args.section_coverage_min_pnus is not None]
    if any(given) and not all(given):
        raise ValueError("--section-coverage-snapshot, --max-section-coverage-drop and "
                         "--section-coverage-min-pnus go together")
    if all(given):
        if not str(args.section_coverage_snapshot).isdigit():
            raise ValueError("--section-coverage-snapshot is an Iceberg snapshot id")
        if args.input_mode != "iceberg":
            raise ValueError("--section-coverage-snapshot compares against an Iceberg snapshot (iceberg input)")
        if not 0 <= args.max_section_coverage_drop < 1 or args.section_coverage_min_pnus < 0:
            raise ValueError("--max-section-coverage-drop is from 0 up to 1 and --section-coverage-min-pnus non-negative")


def guard_from_arguments(spark: Any, args: Any, table: str, sections: Mapping[str, str],
                         change_frame: Any, shares: bool) -> Guard | None:
    """The guard a producer passes gold_incremental.build_or_merge, or None without the flags.

    `table` is the qualified target; `change_frame` the code change table the build read (None when
    it read none); `sections` section -> its first content column."""
    if args.section_coverage_snapshot is None:
        return None
    served = measure(spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(args.section_coverage_snapshot)}"),
                     sections)
    rows = [] if change_frame is None else [row.asDict() for row in change_frame.collect()]
    bound = Bound(args.max_section_coverage_drop, args.section_coverage_min_pnus, shares)
    return Guard(table, lambda frame: measure(frame, sections), served, region_pairs(rows), bound)
