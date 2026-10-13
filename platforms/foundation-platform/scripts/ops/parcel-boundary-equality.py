#!/usr/bin/env python3
"""Proves the lakehouse parcel boundaries are the ones the PostGIS copies hold (root ADR-0194 5·6).

Before `serving_postgis.parcel_boundary_mirror` and `parcel_boundary_publication` are dropped, the
lakehouse that replaces them must hold the same parcels with the same geometry. Four sides are
read, per region (the first `--prefix-length` digits of the PNU; 2 is the 시도):

- `silver`: `silver.parcel_boundaries` rows of one `source_snapshot_id` (Trino);
- `served`: `gold.parcel_boundary_served`, what the lakehouse tile bake reads (Trino);
- `mirror`: the national mirror rebuild run of that snapshot (PostgreSQL);
- `publication`: the publication rows behind the active parcel release (PostgreSQL).

Every side gives each region the same fingerprint: its row count, its distinct PNU count, and two
order-free sums of 28-bit slices of sha256(pnu ':' geometry_checksum_sha256). Every row carries the
SHA-256 of the Silver WKB it came from (`vworld_cadastral_silver_plan.rs`); the mirror keeps it
through the PostGIS reprojection. Equal fingerprints therefore mean the same PNUs with the same
source geometry. The lakehouse sides also recompute sha256 of their WKB bytes and count each row
whose checksum does not match them: the hash names the bytes the bake reads, not a stale value.

`--region` limits the read to PNUs starting with it (a sample region, e.g. one 시도) before the
national run. The report is one JSON document on stdout. Exits 1 when any region differs between
sides, any side holds a PNU twice, or a lakehouse checksum does not match its bytes; 2 when a side
cannot be read. Read-only: nothing is written anywhere.
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
DIGITS = re.compile(r"^[0-9]{1,19}$")
SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
ICEBERG_SNAPSHOT = re.compile(r"^[1-9][0-9]*$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass(frozen=True)
class Fingerprint:
    rows: int
    distinct_pnus: int
    hash_a: int
    hash_b: int
    checksum_mismatches: int = 0

    def identity(self) -> tuple[int, int, int, int]:
        """What must be equal across sides; the checksum count is checked on its own."""
        return (self.rows, self.distinct_pnus, self.hash_a, self.hash_b)


def trino_sql(table: str, where: str, prefix_length: int) -> str:
    """One row per region: region, rows, distinct PNUs, the two hash sums, checksum mismatches."""
    digest = "lower(to_hex(sha256(to_utf8(pnu || ':' || geometry_checksum_sha256))))"
    return (
        f"SELECT substr(pnu, 1, {prefix_length}) AS region, count(*), count(DISTINCT pnu), "
        f"sum(from_base(substr({digest}, 1, 7), 16)), sum(from_base(substr({digest}, 8, 7), 16)), "
        f"count_if(lower(to_hex(sha256(geometry_wkb))) <> geometry_checksum_sha256) "
        f"FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"
    )


def postgres_sql(table: str, where: str, prefix_length: int) -> str:
    """The same fingerprint in PostgreSQL. The checksum column is the Silver one, kept through the
    reprojection; the stored geometry is not the Silver bytes, so it has no mismatch count."""
    digest = "encode(sha256(convert_to(pnu::text || ':' || geometry_checksum_sha256::text, 'UTF8')), 'hex')"
    return (
        f"SELECT substr(pnu, 1, {prefix_length}) AS region, count(*), count(DISTINCT pnu), "
        f"sum(('x' || substr({digest}, 1, 7))::bit(28)::bigint), "
        f"sum(('x' || substr({digest}, 8, 7))::bit(28)::bigint), 0 "
        f"FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"
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
            f'r2.gold.parcel_boundary_served FOR VERSION AS OF {args.served_snapshot}', region, length)),
        "mirror": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_mirror",
            f"rebuild_run_id = '{args.mirror_run_id}' AND {region}", length)),
        "publication": ("postgres", postgres_sql(
            "serving_postgis.parcel_boundary_publication",
            f"projection_load_id = '{args.publication_load_id}' AND {region}", length)),
    }


def parse_rows(text: str) -> dict[str, Fingerprint]:
    """Region -> fingerprint, from comma-separated rows (Trino CSV unquoted or psql -A -F,)."""
    regions: dict[str, Fingerprint] = {}
    for line in text.splitlines():
        line = line.strip().replace('"', "")
        if not line:
            continue
        region, *numbers = line.split(",")
        if len(numbers) != 5 or not DIGITS.match(region):
            raise ValueError(f"unexpected fingerprint row {line!r}")
        if region in regions:
            raise ValueError(f"region {region} reported twice")
        regions[region] = Fingerprint(*(int(value) for value in numbers))
    return regions


def compare(sides: dict[str, dict[str, Fingerprint]]) -> dict:
    """The verdict over every region any side holds."""
    regions = sorted(set().union(*(found.keys() for found in sides.values())))
    differing, duplicated, mismatched = [], [], []
    for region in regions:
        seen = {name: found.get(region) for name, found in sides.items()}
        if len({None if fp is None else fp.identity() for fp in seen.values()}) != 1:
            differing.append({
                "region": region,
                "sides": {name: None if fp is None else asdict(fp) for name, fp in seen.items()},
            })
        for name, fp in seen.items():
            if fp is not None and fp.rows != fp.distinct_pnus:
                duplicated.append({"region": region, "side": name, "rows": fp.rows, "distinct_pnus": fp.distinct_pnus})
            if fp is not None and fp.checksum_mismatches:
                mismatched.append({"region": region, "side": name, "rows": fp.checksum_mismatches})
    totals = {
        name: {"rows": sum(fp.rows for fp in found.values()), "regions": len(found)}
        for name, found in sides.items()
    }
    return {
        "equal": not (differing or duplicated or mismatched),
        "regions": len(regions),
        "totals": totals,
        "differing_regions": differing,
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
