#!/usr/bin/env python3
"""Proves the lakehouse parcel boundaries are the ones the PostGIS copies hold (root ADR-0194 5·6).

Before `serving_postgis.parcel_boundary_mirror` and `parcel_boundary_publication` are dropped, the
lakehouse that replaces them must hold the same parcels with the same geometry. Four sides are
read:

- `silver`: `silver.parcel_boundaries` rows of one `source_snapshot_id` (Trino);
- `served`: `gold.parcel_boundary_served` at one snapshot, what the lakehouse tile bake reads (Trino);
- `mirror`: the national mirror rebuild run of that snapshot (PostgreSQL);
- `publication`: the publication rows behind the PostGIS parcel release (PostgreSQL).

Every side streams one line per parcel — PNU, source checksum, whether that checksum names the
row's bytes, area and centroid in EPSG:4326 (planar, degrees; PostGIS reprojects its EPSG:5179
geometry back) — sorted by PNU. Trino reads one 시도 range after the other (`pnu >= '11' AND
pnu < '12'`); a PostgreSQL side is one sequential scan sorted on the data disk's temporary space
(a range per 시도 would walk the (run, pnu) key into random heap reads: measured 80 s for one
시군구 of 49,080 parcels). With --region every side reads that one range. The streams are merged by
PNU, and for every parcel:

1. **It is on every side, once.** A PNU missing from a side, or held twice by one, fails.
2. **Its shape is the same on every side.** Area within `AREA_TOLERANCE` of itself and centroid
   within `CENTROID_TOLERANCE_DEGREES` (about a centimetre): the reprojection round trip moves a
   vertex by about 1e-9 m. A moved, reshaped or swapped parcel fails.
3. **Its source bytes are one within each engine.** silver = served and mirror = publication on
   the checksum, and the lakehouse checksum is the SHA-256 of the row's WKB. Between the engines a
   difference is counted, not refused: the mirror keeps the checksum of the handoff it was loaded
   from, and a Silver re-encoding of the same shape changes the bytes but not the parcel (2.
   decides that).

A side that holds no parcel at all fails: a wrong snapshot id or load id must not pass as equal.
`--region` limits the read to PNUs starting with it (a sample region) before the national run. The
report is one JSON document on stdout; exit 1 on any failure, 2 when a side cannot be read.
Read-only.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterator

TRINO_CONTAINER = os.environ.get("FOUNDATION_PARCEL_EQUALITY_TRINO_CONTAINER", "foundation-platform-trino")
POSTGRES_CONTAINER = os.environ.get(
    "FOUNDATION_PARCEL_EQUALITY_POSTGRES_CONTAINER", "foundation-platform-runtime-postgres-1"
)
SIDES = ("silver", "served", "mirror", "publication")
ENGINE_OF = {"silver": "trino", "served": "trino", "mirror": "postgres", "publication": "postgres"}
AREA_TOLERANCE = 1e-6
CENTROID_TOLERANCE_DEGREES = 1e-7
SAMPLES = 20
DIGITS = re.compile(r"^[0-9]{1,19}$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
ICEBERG_SNAPSHOT = re.compile(r"^[1-9][0-9]*$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass(frozen=True)
class Parcel:
    pnu: str
    checksum: str
    bytes_match: bool
    area: float
    x: float
    y: float

    def same_shape(self, other: "Parcel") -> bool:
        return (abs(self.area - other.area) <= AREA_TOLERANCE * max(abs(self.area), abs(other.area))
                and abs(self.x - other.x) <= CENTROID_TOLERANCE_DEGREES
                and abs(self.y - other.y) <= CENTROID_TOLERANCE_DEGREES)


def ranges(region: str | None) -> list[tuple[str, str]]:
    """The PNU ranges read one after the other: each 2-digit 시도 prefix, or the one region."""
    if region is not None:
        if not DIGITS.match(region):
            raise ValueError(f"--region {region!r} is not a PNU prefix of digits")
        return [(region, str(int(region) + 1).zfill(len(region)))]
    # The last bound is twenty nines: past every 19-digit PNU under any collation, unlike a symbol.
    return [(f"{n:02d}", f"{n + 1:02d}") for n in range(99)] + [("99", "9" * 20)]


def trino_sql(table: str, where: str, low: str, high: str) -> str:
    return (
        "SELECT pnu, geometry_checksum_sha256, "
        "lower(to_hex(sha256(geometry_wkb))) = geometry_checksum_sha256, "
        "ST_Area(g), ST_X(ST_Centroid(g)), ST_Y(ST_Centroid(g)) "
        "FROM (SELECT pnu, geometry_wkb, geometry_checksum_sha256, ST_GeomFromBinary(geometry_wkb) AS g "
        f"FROM {table} WHERE {where} AND pnu >= '{low}' AND pnu < '{high}') ORDER BY pnu"
    )


def postgres_sql(table: str, where: str, low: str, high: str) -> str:
    """The checksum column is the Silver one the row was loaded with; the stored geometry is not
    those bytes, so it is not checked against them."""
    return (
        "COPY (SELECT pnu, geometry_checksum_sha256, true, ST_Area(g), ST_X(ST_Centroid(g)), "
        f"ST_Y(ST_Centroid(g)) FROM (SELECT pnu, geometry_checksum_sha256, ST_Transform(geom, 4326) AS g "
        f"FROM {table} WHERE {where} AND pnu >= '{low}' AND pnu < '{high}') AS side ORDER BY pnu) "
        "TO STDOUT WITH (FORMAT csv)"
    )


def side_queries(args: argparse.Namespace, low: str, high: str) -> dict[str, tuple[str, str]]:
    """Side name -> (engine, SQL) for one PNU range."""
    return {
        "silver": ("trino", trino_sql(
            "r2.silver.parcel_boundaries", f"source_snapshot_id = '{args.source_snapshot_id}'", low, high)),
        "served": ("trino", trino_sql(
            f"r2.gold.parcel_boundary_served FOR VERSION AS OF {args.served_snapshot}", "TRUE", low, high)),
        "mirror": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_mirror", f"rebuild_run_id = '{args.mirror_run_id}'", low, high)),
        "publication": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_publication",
            f"projection_load_id = '{args.publication_load_id}'", low, high)),
    }


def parse_line(line: str) -> Parcel:
    fields = line.strip().replace('"', "").split(",")
    if len(fields) != 6 or not DIGITS.match(fields[0]) or len(fields[0]) != 19:
        raise ValueError(f"unexpected parcel row {line.strip()[:80]!r}")
    pnu, checksum, matches, area, x, y = fields
    return Parcel(pnu, checksum.strip(), matches.strip().lower() in ("true", "t"), float(area), float(x), float(y))


def engine_lines(engine: str, sql: str) -> Iterator[str]:
    """The query's rows as they arrive; a failed query raises when its stream ends."""
    if engine == "trino":
        command = ["docker", "exec", TRINO_CONTAINER, "trino", "--output-format", "CSV_UNQUOTED", "--execute", sql]
    else:
        command = ["docker", "exec", POSTGRES_CONTAINER, "sh", "-c",
                   'psql -X -U "$POSTGRES_USER" -d foundation -v ON_ERROR_STOP=1 -c "$0"', sql]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
        assert child.stdout is not None
        yield from child.stdout
        error = child.stderr.read() if child.stderr else ""
        if child.wait() != 0:
            raise LookupError(f"{engine}: {error.strip()[-500:]}")


def stream(engine: str, sql: str) -> Iterator[Parcel]:
    for line in engine_lines(engine, sql):
        if line.strip():
            yield parse_line(line)


class Verdict:
    def __init__(self, sides: list[str]):
        self.sides = sides
        self.rows: Counter[str] = Counter()
        self.failures: dict[str, Counter[str]] = defaultdict(Counter)
        self.samples: dict[str, list] = defaultdict(list)
        self.between_engines = 0

    def fail(self, reason: str, region: str, sample) -> None:
        self.failures[reason][region] += 1
        if len(self.samples[reason]) < SAMPLES:
            self.samples[reason].append(sample)

    def parcel(self, pnu: str, held: dict[str, list[Parcel]]) -> None:
        region = pnu[:2]
        for name in self.sides:
            self.rows[name] += len(held.get(name, []))
        missing = [name for name in self.sides if not held.get(name)]
        if missing:
            self.fail("missing", region, {"pnu": pnu, "missing_from": missing})
        twice = [name for name in self.sides if len(held.get(name, [])) > 1]
        if twice:
            self.fail("held_twice", region, {"pnu": pnu, "sides": twice})
        present = {name: held[name][0] for name in self.sides if held.get(name)}
        if not present:
            return
        first = next(iter(present.values()))
        if not all(first.same_shape(p) for p in present.values()):
            self.fail("shape", region, {"pnu": pnu, **{n: [p.area, p.x, p.y] for n, p in present.items()}})
        unmatched = [name for name, p in present.items() if not p.bytes_match]
        if unmatched:
            self.fail("checksum_not_the_bytes", region, {"pnu": pnu, "sides": unmatched})
        by_engine: dict[str, set[str]] = defaultdict(set)
        for name, p in present.items():
            by_engine[ENGINE_OF[name]].add(p.checksum)
        if any(len(found) > 1 for found in by_engine.values()):
            self.fail("source_bytes_split_within_an_engine", region, {"pnu": pnu})
        elif len({next(iter(found)) for found in by_engine.values()}) > 1:
            self.between_engines += 1

    def report(self) -> dict:
        empty = [name for name in self.sides if self.rows[name] == 0]
        for name in empty:
            self.failures["empty_side"][name] += 1
        return {
            "equal": not self.failures,
            "rows": dict(self.rows),
            "failures": {reason: {"total": sum(c.values()), "by_region": dict(sorted(c.items()))}
                         for reason, c in sorted(self.failures.items())},
            "samples": dict(self.samples),
            "source_bytes_differ_between_engines": self.between_engines,
        }


def merge(verdict: Verdict, streams: dict[str, Iterator[Parcel]]) -> None:
    """Walks the sorted streams together, handing the verdict every PNU with what each side holds."""
    def keyed(name, parcels):
        for parcel in parcels:
            yield parcel.pnu, name, parcel

    current, held = None, defaultdict(list)
    for pnu, name, parcel in heapq.merge(*(keyed(n, s) for n, s in streams.items())):
        if pnu != current:
            if current is not None:
                verdict.parcel(current, held)
            current, held = pnu, defaultdict(list)
        held[name].append(parcel)
    if current is not None:
        verdict.parcel(current, held)


def side_stream(args: argparse.Namespace, name: str) -> Iterator[Parcel]:
    """One side's parcels in PNU order: one range for a region or a PostgreSQL side, else the
    시도 ranges one after the other."""
    if args.region is not None or ENGINE_OF[name] == "postgres":
        low, high = ranges(args.region)[0] if args.region is not None else ("00", "9" * 20)
        return stream(*side_queries(args, low, high)[name])
    return itertools.chain.from_iterable(
        stream(*side_queries(args, low, high)[name]) for low, high in ranges(None)
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id")
    parser.add_argument("--served-snapshot", required=True, help="gold.parcel_boundary_served Iceberg snapshot")
    parser.add_argument("--mirror-run-id", required=True, help="the mirror's national rebuild_run_id")
    parser.add_argument("--publication-load-id", required=True, help="the publication's projection_load_id")
    parser.add_argument("--region", help="only PNUs starting with these digits (a sample region)")
    parser.add_argument("--sides", default=",".join(SIDES), help="comma-separated subset of " + ",".join(SIDES))
    args = parser.parse_args(argv)
    if not SNAPSHOT_ID.match(args.source_snapshot_id):
        raise SystemExit("--source-snapshot-id is not a plain snapshot id")
    if not ICEBERG_SNAPSHOT.match(args.served_snapshot):
        raise SystemExit("--served-snapshot is not an Iceberg snapshot id")
    for name in ("mirror_run_id", "publication_load_id"):
        if not UUID.match(getattr(args, name)):
            raise SystemExit(f"--{name.replace('_', '-')} is not a lowercase UUID")
    if args.region is not None and not DIGITS.match(args.region):
        raise SystemExit("--region is not a PNU prefix of digits")
    args.sides = [side for side in args.sides.split(",") if side]
    if len(args.sides) < 2 or any(side not in SIDES for side in args.sides):
        raise SystemExit(f"--sides needs at least two of {', '.join(SIDES)}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    verdict = Verdict(args.sides)
    started = time.monotonic()
    try:
        merge(verdict, {name: side_stream(args, name) for name in args.sides})
    except (LookupError, ValueError) as error:
        print(json.dumps({"equal": False, "unreadable": str(error)}))
        return 2
    report = verdict.report()
    report.update({
        "region": args.region or "national",
        "sides": args.sides,
        "source_snapshot_id": args.source_snapshot_id,
        "served_snapshot": args.served_snapshot,
        "mirror_run_id": args.mirror_run_id,
        "publication_load_id": args.publication_load_id,
        "seconds": round(time.monotonic() - started, 1),
    })
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["equal"] else 1


if __name__ == "__main__":
    sys.exit(main())
