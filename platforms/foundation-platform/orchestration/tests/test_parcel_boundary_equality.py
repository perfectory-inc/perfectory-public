"""The lakehouse-versus-PostGIS parcel boundary proof (scripts/ops/parcel-boundary-equality.py, root ADR-0194).

The engines are replaced by a fake that answers each range query from in-memory parcels, in each
dialect's CSV (Trino booleans `true`/`false`, PostgreSQL `t`), sorted by PNU. Every way the copies
can differ — a missing, extra or doubled parcel, a reshaped, moved or swapped geometry, an engine
that disagrees with itself on the source bytes, a checksum that does not name the lakehouse bytes,
a side that holds nothing — must turn the verdict false and the exit code non-zero. Source bytes
that differ only between the engines (the same shapes encoded again) are counted and pass.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import re
import sys
import unittest
from dataclasses import dataclass, replace
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "ops" / "parcel-boundary-equality.py"

spec = importlib.util.spec_from_file_location("parcel_boundary_equality", SCRIPT)
equality = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = equality
spec.loader.exec_module(equality)

RUN = "01a06e42-0000-7000-8000-000000000001"
LOAD = "3d24bbff-0000-4000-8000-000000000002"
ARGS = ["--source-snapshot-id", "vworldkr__parcel-202606", "--served-snapshot", "4772",
        "--mirror-run-id", RUN, "--publication-load-id", LOAD]
TABLE_OF = {"r2.silver.parcel_boundaries": "silver", "r2.gold.parcel_boundary_served": "served",
            "serving_postgis.parcel_boundary_mirror": "mirror",
            "serving_postgis.parcel_boundary_publication": "publication"}


@dataclass(frozen=True)
class Row:
    pnu: str
    checksum: str
    matches: bool
    area: float
    x: float
    y: float


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# The repository-reserved 99999 PNUs and coordinates outside Korea: ten parcels.
LAKE = [Row(f"99999110101100{n:05d}", sha(f"wkb-{n}"), True, 1e-8 * n, 10.0 + n * 1e-3, 20.0 + n * 1e-3)
        for n in range(1, 11)]


def postgis(rows, noise=1e-12):
    """PostGIS holds the same parcels, reprojected and back: a tiny float difference."""
    return [replace(r, area=r.area * (1 + noise), x=r.x + noise, y=r.y - noise) for r in rows]


def sides(**override):
    found = {"silver": LAKE, "served": LAKE, "mirror": postgis(LAKE), "publication": postgis(LAKE)}
    found.update(override)
    return found


def changed(rows, index, **fields):
    return [replace(r, **fields) if i == index else r for i, r in enumerate(rows)]


def run(rows_by_side, extra=()):
    """Runs main() with every range query answered from rows_by_side; returns (exit, report)."""
    def lines(engine, sql):
        table = next(name for name in sorted(TABLE_OF, key=len, reverse=True) if name in sql)
        low, high = re.search(r"pnu >= '(\d+)' AND pnu < '(\d+)'", sql).groups()
        for r in sorted(rows_by_side[TABLE_OF[table]], key=lambda r: r.pnu):
            if low <= r.pnu < high:
                flag = ("true" if r.matches else "false") if engine == "trino" else "t"
                yield f"{r.pnu},{r.checksum},{flag},{r.area!r},{r.x!r},{r.y!r}\n"

    out = io.StringIO()
    with mock.patch.object(equality, "engine_lines", lines), contextlib.redirect_stdout(out):
        code = equality.main(ARGS + list(extra))
    return code, json.loads(out.getvalue())


def failed(report, reason):
    return [sample["pnu"] for sample in report["samples"].get(reason, [])]


class ParcelBoundaryEqualityTest(unittest.TestCase):
    def test_the_same_parcels_and_shapes_are_equal(self):
        code, report = run(sides())
        self.assertEqual(code, 0, report)
        self.assertEqual(report["rows"], {name: 10 for name in equality.SIDES})
        self.assertEqual(report["source_bytes_differ_between_engines"], 0)

    def test_shapes_encoded_again_differ_only_in_source_bytes_and_pass(self):
        again = [replace(r, checksum=sha(r.checksum)) for r in postgis(LAKE)]
        code, report = run(sides(mirror=again, publication=again))
        self.assertEqual(code, 0, report)
        self.assertEqual(report["source_bytes_differ_between_engines"], 10)

    def test_a_reshaped_parcel_fails(self):
        code, report = run(sides(mirror=changed(postgis(LAKE), 7, area=LAKE[7].area * 1.01)))
        self.assertEqual(code, 1)
        self.assertEqual(failed(report, "shape"), [LAKE[7].pnu])

    def test_a_parcel_moved_far_fails_alone_among_many(self):
        code, report = run(sides(served=changed(LAKE, 2, x=LAKE[2].x + 0.4)))
        self.assertEqual(code, 1)
        self.assertEqual(failed(report, "shape"), [LAKE[2].pnu])

    def test_two_swapped_geometries_fail_although_every_sum_is_kept(self):
        a, b = LAKE[3], LAKE[4]
        swapped = changed(changed(postgis(LAKE), 3, area=b.area, x=b.x, y=b.y), 4, area=a.area, x=a.x, y=a.y)
        code, report = run(sides(publication=swapped))
        self.assertEqual(code, 1)
        self.assertEqual(failed(report, "shape"), [a.pnu, b.pnu])

    def test_a_parcel_missing_from_one_side_fails(self):
        code, report = run(sides(publication=postgis(LAKE[1:])))
        self.assertEqual(code, 1)
        self.assertEqual(report["samples"]["missing"], [{"pnu": LAKE[0].pnu, "missing_from": ["publication"]}])

    def test_a_parcel_only_one_side_holds_fails(self):
        extra = Row("9999947010110001001", sha("x"), True, 1e-8, 10.5, 20.5)
        code, report = run(sides(served=LAKE + [extra]))
        self.assertEqual(code, 1)
        self.assertEqual(report["samples"]["missing"][0]["missing_from"], ["silver", "mirror", "publication"])

    def test_a_pnu_held_twice_fails(self):
        code, report = run(sides(silver=LAKE + LAKE[:1]))
        self.assertEqual(code, 1)
        self.assertEqual(report["samples"]["held_twice"], [{"pnu": LAKE[0].pnu, "sides": ["silver"]}])

    def test_an_engine_that_disagrees_with_itself_on_the_source_bytes_fails(self):
        code, report = run(sides(served=changed(LAKE, 0, checksum=sha("other"))))
        self.assertEqual(code, 1)
        self.assertEqual(failed(report, "source_bytes_split_within_an_engine"), [LAKE[0].pnu])

    def test_a_checksum_that_does_not_name_the_bytes_fails(self):
        both = changed(LAKE, 0, matches=False)
        code, report = run(sides(silver=both, served=both))
        self.assertEqual(code, 1)
        self.assertEqual(report["samples"]["checksum_not_the_bytes"], [{"pnu": LAKE[0].pnu, "sides": ["silver", "served"]}])

    def test_sides_that_hold_nothing_are_not_equal(self):
        # A wrong snapshot or load id reads no rows on every side; that is not a proof.
        code, report = run({name: [] for name in equality.SIDES})
        self.assertEqual(code, 1)
        self.assertEqual(report["failures"]["empty_side"]["total"], 4)

    def test_one_empty_side_fails(self):
        code, report = run(sides(mirror=[]))
        self.assertEqual(code, 1)
        self.assertIn("mirror", report["failures"]["empty_side"]["by_region"])
        self.assertEqual(report["failures"]["missing"]["total"], 10)

    def test_an_unreadable_side_exits_2(self):
        def broken(engine, _sql):
            raise LookupError(f"{engine}: down")
            yield  # pragma: no cover

        out = io.StringIO()
        with mock.patch.object(equality, "engine_lines", broken), contextlib.redirect_stdout(out):
            self.assertEqual(equality.main(ARGS), 2)
        self.assertFalse(json.loads(out.getvalue())["equal"])

    def test_the_ranges_cover_every_pnu_and_a_region_reads_only_itself(self):
        national = equality.ranges(None)
        self.assertEqual(national[0][0], "00")
        self.assertTrue(all(a[1] == b[0] for a, b in zip(national, national[1:])))
        self.assertTrue("9" * 19 < national[-1][1])
        self.assertEqual(equality.ranges("9999911"), [("9999911", "9999912")])
        code, report = run(sides(), extra=["--region", "9999911"])
        self.assertEqual((code, report["rows"]["silver"]), (0, 10))

    def test_postgis_is_read_in_one_sorted_scan_and_the_lakehouse_by_range(self):
        asked = []
        real = equality.side_queries

        def recording(args, low, high):
            asked.append((low, high))
            return real(args, low, high)

        with mock.patch.object(equality, "side_queries", recording):
            code, _ = run(sides())
        self.assertEqual(code, 0)
        # Two PostgreSQL sides ask the whole range once; two Trino sides ask each 시도 range.
        self.assertEqual(asked.count(("00", "9" * 20)), 2)
        self.assertEqual(len(asked), 2 + 2 * len(equality.ranges(None)))

    def test_each_side_reads_its_own_copy_by_range_in_one_crs(self):
        queries = equality.side_queries(equality.parse_args(ARGS), "11", "12")
        self.assertIn("source_snapshot_id = 'vworldkr__parcel-202606'", queries["silver"][1])
        self.assertIn("FOR VERSION AS OF 4772", queries["served"][1])
        self.assertIn(f"rebuild_run_id = '{RUN}'", queries["mirror"][1])
        self.assertIn(f"projection_load_id = '{LOAD}'", queries["publication"][1])
        self.assertTrue(all("pnu >= '11' AND pnu < '12'" in sql and "ORDER BY pnu" in sql for _, sql in queries.values()))
        self.assertTrue(all("ST_Transform(geom, 4326)" in queries[name][1] for name in ("mirror", "publication")))

    def test_arguments_that_are_not_ids_are_refused(self):
        for flag, bad in (("--mirror-run-id", "x'; DROP TABLE t; --"), ("--served-snapshot", "0"),
                          ("--source-snapshot-id", "a b")):
            argv = list(ARGS)
            argv[argv.index(flag) + 1] = bad
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                equality.parse_args(argv)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            equality.parse_args(ARGS + ["--region", "11' OR '1'='1"])

    def test_a_malformed_engine_row_is_refused(self):
        with self.assertRaises(ValueError):
            equality.parse_line("9999911010110000001,abc,true,1.0\n")
        with self.assertRaises(ValueError):
            equality.parse_line("12345,abc,true,1.0,2.0,3.0\n")
        self.assertTrue(equality.parse_line("9999911010110000001,abc,t,1.0,2.0,3.0\n").bytes_match)


if __name__ == "__main__":
    unittest.main()
