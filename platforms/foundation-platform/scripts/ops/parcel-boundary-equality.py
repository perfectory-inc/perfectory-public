#!/usr/bin/env python3
"""Proves the lakehouse parcel boundaries are the ones the PostGIS copies hold (root ADR-0194 5·6).

Before `serving_postgis.parcel_boundary_mirror` and `parcel_boundary_publication` are dropped, the
lakehouse that replaces them must hold the same parcels with the same geometry. Four sides are
read, per region (the first `--prefix-length` digits of the PNU; 2 is the 시도):

- `silver`: `silver.parcel_boundaries` rows of one `source_snapshot_id` (Trino);
- `served`: `gold.parcel_boundary_served`, what the lakehouse tile bake reads (Trino);
- `mirror`: the national mirror rebuild run of that snapshot (PostgreSQL);
- `publication`: the publication rows behind the PostGIS parcel release (PostgreSQL).

Every side gives each region one fingerprint, and three things are compared:

1. **The parcels** — exact on every side: the row count, the distinct PNU count, and two
   order-free sums of 28-bit slices of sha256(pnu).
2. **The shapes** — on every side, the sums of each parcel's area and centroid in EPSG:4326
   (planar, degrees; PostGIS reprojects its EPSG:5179 geometry back). They must agree within
   `AREA_TOLERANCE` of the area and `CENTROID_TOLERANCE_DEGREES` per parcel: the reprojection
   round trip moves a vertex by about 1e-9 m, a changed parcel by far more.
3. **The source bytes** — the same order-free sums over sha256(pnu ':' geometry_checksum_sha256),
   where the checksum is the SHA-256 of the Silver WKB a row came from. Exact within each engine
   (silver = served, mirror = publication); the lakehouse sides also recompute sha256 of their WKB
   and count every row whose checksum does not name its bytes. Between the engines a difference
   is reported, not refused: the mirror keeps the checksum of the handoff it was loaded from, and
   a Silver re-encoding of the same shapes changes the bytes but not the parcels (2. decides that).

`--region` limits the read to PNUs starting with it (a sample region, e.g. one 시도) before the
national run. The report is one JSON document on stdout. Exits 1 when the parcels or the shapes of
any region differ, any side holds a PNU twice, an engine disagrees with itself on the source bytes,
or a lakehouse checksum does not match its bytes; 2 when a side cannot be read. Read-only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass

TRINO_CONTAINER = os.environ.get("FOUNDATION_PARCEL_EQUALITY_TRINO_CONTAINER", "foundation-platform-trino")
POSTGRES_CONTAINER = os.environ.get(
    "FOUNDATION_PARCEL_EQUALITY_POSTGRES_CONTAINER", "foundation-platform-runtime-postgres-1"
)
SIDES = ("silver", "served", "mirror", "publication")
ENGINE_OF = {"silver": "trino", "served": "trino", "mirror": "postgres", "publication": "postgres"}
AREA_TOLERANCE = 1e-6
CENTROID_TOLERANCE_DEGREES = 1e-7
DIGITS = re.compile(r"^[0-9]{1,19}$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
ICEBERG_SNAPSHOT = re.compile(r"^[1-9][0-9]*$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
FIELDS = 10


@dataclass(frozen=True)
class Fingerprint:
    rows: int
    distinct_pnus: int
    pnu_a: int
    pnu_b: int
    source_a: int
    source_b: int
    checksum_mismatches: int
    area: float
    centroid_x: float
    centroid_y: float

    def parcels(self) -> tuple[int, int, int, int]:
        return (self.rows, self.distinct_pnus, self.pnu_a, self.pnu_b)

    def source(self) -> tuple[int, int]:
        return (self.source_a, self.source_b)

    def same_shapes(self, other: "Fingerprint") -> bool:
        reach = CENTROID_TOLERANCE_DEGREES * max(self.rows, 1)
        return (abs(self.area - other.area) <= AREA_TOLERANCE * max(abs(self.area), abs(other.area))
                and abs(self.centroid_x - other.centroid_x) <= reach
                and abs(self.centroid_y - other.centroid_y) <= reach)


def _slices(digest: str, to_int: str) -> str:
    return f"sum({to_int.format(f'substr({digest}, 1, 7)')}), sum({to_int.format(f'substr({digest}, 8, 7)')})"


def trino_sql(table: str, where: str, prefix_length: int) -> str:
    """One row per region, the FIELDS of a Fingerprint after the region."""
    to_int = "from_base({}, 16)"
    pnu = "lower(to_hex(sha256(to_utf8(pnu))))"
    source = "lower(to_hex(sha256(to_utf8(pnu || ':' || geometry_checksum_sha256))))"
    return (
        f"SELECT substr(pnu, 1, {prefix_length}) AS region, count(*), count(DISTINCT pnu), "
        f"{_slices(pnu, to_int)}, {_slices(source, to_int)}, "
        f"count_if(lower(to_hex(sha256(geometry_wkb))) <> geometry_checksum_sha256), "
        f"sum(ST_Area(g)), sum(ST_X(ST_Centroid(g))), sum(ST_Y(ST_Centroid(g))) "
        f"FROM (SELECT pnu, geometry_wkb, geometry_checksum_sha256, ST_GeomFromBinary(geometry_wkb) AS g "
        f"FROM {table} WHERE {where}) GROUP BY 1 ORDER BY 1"
    )


def postgres_sql(table: str, where: str, prefix_length: int) -> str:
    """The same fingerprint in PostgreSQL. The checksum column is the Silver one the row was loaded
    with; the stored geometry is not those bytes, so there is no mismatch count."""
    to_int = "('x' || {})::bit(28)::bigint"
    pnu = "encode(sha256(convert_to(pnu::text, 'UTF8')), 'hex')"
    source = "encode(sha256(convert_to(pnu::text || ':' || geometry_checksum_sha256::text, 'UTF8')), 'hex')"
    return (
        f"SELECT substr(pnu, 1, {prefix_length}) AS region, count(*), count(DISTINCT pnu), "
        f"{_slices(pnu, to_int)}, {_slices(source, to_int)}, 0, "
        f"sum(ST_Area(g)), sum(ST_X(ST_Centroid(g))), sum(ST_Y(ST_Centroid(g))) "
        f"FROM (SELECT pnu, geometry_checksum_sha256, ST_Transform(geom, 4326) AS g "
        f"FROM {table} WHERE {where}) AS side GROUP BY 1 ORDER BY 1"
    )


def region_clause(region: str | None) -> str:
    if region is None:
        return "TRUE"
    if not DIGITS.match(region):
        raise ValueError(f"--region {region!r} is not a PNU prefix of digits")
    return f"pnu LIKE '{region}%'"


def side_queries(args: argparse.Namespace) -> dict[str, tuple[str, str]]:
    """Side name -> (engine, SQL)."""
    region = region_clause(args.region)
    length = args.prefix_length
    return {
        "silver": ("trino", trino_sql(
            "r2.silver.parcel_boundaries",
            f"source_snapshot_id = '{args.source_snapshot_id}' AND {region}", length)),
        "served": ("trino", trino_sql(
            f"r2.gold.parcel_boundary_served FOR VERSION AS OF {args.served_snapshot}", region, length)),
        "mirror": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_mirror",
            f"rebuild_run_id = '{args.mirror_run_id}' AND {region}", length)),
        "publication": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_publication",
            f"projection_load_id = '{args.publication_load_id}' AND {region}", length)),
    }


def parse_rows(text: str) -> dict[str, Fingerprint]:
    """Region -> fingerprint, from comma-separated rows (Trino CSV_UNQUOTED or psql -A -F,)."""
    regions: dict[str, Fingerprint] = {}
    for line in text.splitlines():
        line = line.strip().replace('"', "")
        if not line:
            continue
        region, *values = line.split(",")
        if len(values) != FIELDS or not DIGITS.match(region):
            raise ValueError(f"unexpected fingerprint row {line!r}")
        if region in regions:
            raise ValueError(f"region {region} reported twice")
        whole = [int(value) for value in values[:7]]
        regions[region] = Fingerprint(*whole, *(float(value) for value in values[7:]))
    return regions


def compare(sides: dict[str, dict[str, Fingerprint]]) -> dict:
    """The verdict over every region any side holds."""
    regions = sorted(set().union(*(found.keys() for found in sides.values())))
    parcels, shapes, engine_split, across, duplicated, mismatched = [], [], [], [], [], []
    for region in regions:
        seen = {name: found.get(region) for name, found in sides.items()}
        described = {name: None if fp is None else asdict(fp) for name, fp in seen.items()}
        if len({None if fp is None else fp.parcels() for fp in seen.values()}) != 1:
            parcels.append({"region": region, "sides": described})
            continue
        held = [fp for fp in seen.values() if fp is not None]
        if not all(held[0].same_shapes(fp) for fp in held[1:]):
            shapes.append({"region": region, "sides": described})
        by_engine: dict[str, set] = {}
        for name, fp in seen.items():
            by_engine.setdefault(ENGINE_OF[name], set()).add(fp.source())
            if fp.rows != fp.distinct_pnus:
                duplicated.append({"region": region, "side": name, "rows": fp.rows, "distinct_pnus": fp.distinct_pnus})
            if fp.checksum_mismatches:
                mismatched.append({"region": region, "side": name, "rows": fp.checksum_mismatches})
        if any(len(found) != 1 for found in by_engine.values()):
            engine_split.append({"region": region, "sides": described})
        elif len({next(iter(found)) for found in by_engine.values()}) != 1:
            across.append(region)
    return {
        "equal": not (parcels or shapes or engine_split or duplicated or mismatched),
        "regions": len(regions),
        "totals": {name: {"rows": sum(fp.rows for fp in found.values()), "regions": len(found)}
                   for name, found in sides.items()},
        "differing_parcels": parcels,
        "differing_shapes": shapes,
        "source_bytes_split_within_an_engine": engine_split,
        "source_bytes_differ_between_engines": across,
        "duplicated_pnus": duplicated,
        "checksum_mismatches": mismatched,
    }


def run_engine(engine: str, sql: str) -> str:
    if engine == "trino":
        command = ["docker", "exec", TRINO_CONTAINER, "trino", "--output-format", "CSV_UNQUOTED", "--execute", sql]
    else:
        command = ["docker", "exec", POSTGRES_CONTAINER, "sh", "-c",
                   'psql -U "$POSTGRES_USER" -d foundation -v ON_ERROR_STOP=1 -At -F, -c "$0"', sql]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=6 * 3600)
    if completed.returncode != 0:
        raise LookupError(f"{engine}: {completed.stderr.strip()[-500:]}")
    return completed.stdout


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-snapshot-id", required=True, help="silver.parcel_boundaries source_snapshot_id")
    parser.add_argument("--served-snapshot", required=True, help="gold.parcel_boundary_served Iceberg snapshot")
    parser.add_argument("--mirror-run-id", required=True, help="the mirror's national rebuild_run_id")
    parser.add_argument("--publication-load-id", required=True, help="the publication's projection_load_id")
    parser.add_argument("--region", help="only PNUs starting with these digits (a sample region)")
    parser.add_argument("--prefix-length", type=int, default=2, help="PNU digits that name a region (2 = 시도)")
    parser.add_argument("--sides", default=",".join(SIDES), help="comma-separated subset of " + ",".join(SIDES))
    args = parser.parse_args(argv)
    if not SNAPSHOT_ID.match(args.source_snapshot_id):
        raise SystemExit("--source-snapshot-id is not a plain snapshot id")
    if not ICEBERG_SNAPSHOT.match(args.served_snapshot):
        raise SystemExit("--served-snapshot is not an Iceberg snapshot id")
    for name in ("mirror_run_id", "publication_load_id"):
        if not UUID.match(getattr(args, name)):
            raise SystemExit(f"--{name.replace('_', '-')} is not a lowercase UUID")
    if not 1 <= args.prefix_length <= 19:
        raise SystemExit("--prefix-length must be 1..19")
    args.sides = [side for side in args.sides.split(",") if side]
    if len(args.sides) < 2 or any(side not in SIDES for side in args.sides):
        raise SystemExit(f"--sides needs at least two of {', '.join(SIDES)}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    queries = side_queries(args)
    sides, seconds = {}, {}
    for name in args.sides:
        engine, sql = queries[name]
        started = time.monotonic()
        try:
            sides[name] = parse_rows(run_engine(engine, sql))
        except (LookupError, ValueError, subprocess.TimeoutExpired) as error:
            print(json.dumps({"equal": False, "unreadable_side": name, "error": str(error)}))
            return 2
        seconds[name] = round(time.monotonic() - started, 1)
    report = compare(sides)
    report.update({
        "region": args.region or "national",
        "prefix_length": args.prefix_length,
        "source_snapshot_id": args.source_snapshot_id,
        "served_snapshot": args.served_snapshot,
        "mirror_run_id": args.mirror_run_id,
        "publication_load_id": args.publication_load_id,
        "seconds": seconds,
    })
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["equal"] else 1


if __name__ == "__main__":
    sys.exit(main())
