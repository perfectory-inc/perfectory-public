"""The lakehouse-versus-PostGIS parcel boundary proof (scripts/ops/parcel-boundary-equality.py, root ADR-0194).

The engines are replaced by a fake that fingerprints in-memory parcels the way both SQL dialects
do: 28-bit slices of sha256(pnu) and of sha256(pnu ':' checksum) summed per region, plus the sums
of each parcel's area and centroid. Every way the copies can differ — a missing or extra parcel,
a moved or reshaped parcel, a PNU held twice, an engine that disagrees with itself on the source
bytes, a checksum that does not name the lakehouse bytes — must turn the verdict false and the
exit code non-zero. Source bytes that differ only between the engines (the same shapes encoded
again) are reported and pass.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
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
        "--mirror-run-id", RUN, "--publication-load-id", LOAD, "--prefix-length", "7"]


@dataclass(frozen=True)
class Parcel:
    pnu: str
    checksum: str
    area: float
    x: float
    y: float
    wkb: bytes | None = None  # the lakehouse sides carry their bytes; PostGIS does not


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def fingerprint_rows(parcels, prefix_length=7):
    """What the SQL returns: one CSV line per region with the ten fingerprint fields."""
    regions = {}
    for p in parcels:
        r = regions.setdefault(p.pnu[:prefix_length], [0, set(), 0, 0, 0, 0, 0, 0.0, 0.0, 0.0])
        key, source = sha(p.pnu), sha(f"{p.pnu}:{p.checksum}")
        r[0] += 1
        r[1].add(p.pnu)
        r[2] += int(key[0:7], 16)
        r[3] += int(key[7:14], 16)
        r[4] += int(source[0:7], 16)
        r[5] += int(source[7:14], 16)
        r[6] += p.wkb is not None and hashlib.sha256(p.wkb).hexdigest() != p.checksum
        r[7] += p.area
        r[8] += p.x
        r[9] += p.y
    return "".join(
        f"{name},{r[0]},{len(r[1])},{r[2]},{r[3]},{r[4]},{r[5]},{int(r[6])},{r[7]!r},{r[8]!r},{r[9]!r}\n"
        for name, r in sorted(regions.items())
    )


def lake_parcel(pnu, n):
    wkb = bytes([n, int(pnu[5:7])]) * 9
    return Parcel(pnu, hashlib.sha256(wkb).hexdigest(), 1e-8 * n, 10.0 + n * 1e-3, 20.0 + n * 1e-3, wkb)


# The repository-reserved 99999 PNUs, coordinates outside Korea: two regions, five parcels each.
LAKE = [lake_parcel(f"99999{region}010110001{n:03d}", n) for region in ("11", "26") for n in range(1, 6)]


def postgis(parcels, noise=1e-13):
    """PostGIS holds the same parcels without their bytes, reprojected (a tiny float difference)."""
    return [replace(p, wkb=None, area=p.area * (1 + noise), x=p.x + noise, y=p.y - noise) for p in parcels]


def sides(**override):
    found = {"silver": LAKE, "served": LAKE, "mirror": postgis(LAKE), "publication": postgis(LAKE)}
    found.update(override)
    return found


def run(parcels_by_side, extra=()):
    """Runs main() with each side's query answered from its parcels; returns (exit, report)."""
    queries = equality.side_queries(equality.parse_args(ARGS + list(extra)))
    by_sql = {sql: name for name, (_, sql) in queries.items()}

    def engine(_engine, sql):
        return fingerprint_rows(parcels_by_side[by_sql[sql]])

    out = io.StringIO()
    with mock.patch.object(equality, "run_engine", engine), contextlib.redirect_stdout(out):
        code = equality.main(ARGS + list(extra))
    return code, json.loads(out.getvalue())


def changed(parcels, index, **fields):
    return [replace(p, **fields) if i == index else p for i, p in enumerate(parcels)]


class ParcelBoundaryEqualityTest(unittest.TestCase):
    def test_the_same_parcels_and_shapes_are_equal(self):
        code, report = run(sides())
        self.assertEqual(code, 0, report)
        self.assertTrue(report["equal"])
        self.assertEqual(report["regions"], 2)
        self.assertEqual({side["rows"] for side in report["totals"].values()}, {10})
        self.assertEqual(report["source_bytes_differ_between_engines"], [])

    def test_shapes_encoded_again_differ_only_in_source_bytes_and_pass(self):
        again = [replace(p, checksum=sha(p.checksum)) for p in postgis(LAKE)]
        code, report = run(sides(mirror=again, publication=again))
        self.assertEqual(code, 0, report)
        self.assertEqual(report["source_bytes_differ_between_engines"], ["9999911", "9999926"])

    def test_a_reshaped_parcel_in_the_mirror_names_its_region(self):
        mirror = changed(postgis(LAKE), 7, area=LAKE[7].area * 1.01)
        code, report = run(sides(mirror=mirror))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["differing_shapes"]], ["9999926"])

    def test_a_moved_parcel_differs_even_with_its_area(self):
        served = changed(LAKE, 2, x=LAKE[2].x + 1e-5)
        code, report = run(sides(served=served))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["differing_shapes"]], ["9999911"])

    def test_a_parcel_missing_from_one_side_differs(self):
        code, report = run(sides(publication=postgis(LAKE[1:])))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["differing_parcels"]], ["9999911"])

    def test_a_region_only_one_side_holds_differs(self):
        extra = lake_parcel("9999947010110001001", 9)
        code, report = run(sides(served=LAKE + [extra]))
        self.assertEqual(code, 1)
        self.assertEqual(report["differing_parcels"][0]["region"], "9999947")
        self.assertIsNone(report["differing_parcels"][0]["sides"]["silver"])

    def test_a_pnu_held_twice_fails_even_when_every_side_agrees(self):
        doubled = {name: parcels + parcels[:1] for name, parcels in sides().items()}
        code, report = run(doubled)
        self.assertEqual(code, 1)
        self.assertEqual({d["side"] for d in report["duplicated_pnus"]}, set(equality.SIDES))

    def test_an_engine_that_disagrees_with_itself_on_the_source_bytes_fails(self):
        served = changed(LAKE, 0, checksum=sha("other"), wkb=b"other")
        code, report = run(sides(served=served))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["source_bytes_split_within_an_engine"]], ["9999911"])

    def test_a_checksum_that_does_not_name_the_bytes_fails(self):
        both = changed(LAKE, 0, wkb=LAKE[0].wkb + b"!")
        code, report = run(sides(silver=both, served=both))
        self.assertEqual(code, 1)
        self.assertEqual({(m["region"], m["side"]) for m in report["checksum_mismatches"]},
                         {("9999911", "silver"), ("9999911", "served")})

    def test_an_unreadable_side_exits_2(self):
        def broken(engine, _sql):
            raise LookupError(f"{engine}: down")

        out = io.StringIO()
        with mock.patch.object(equality, "run_engine", broken), contextlib.redirect_stdout(out):
            self.assertEqual(equality.main(ARGS), 2)
        self.assertFalse(json.loads(out.getvalue())["equal"])

    def test_the_region_limits_every_side_and_refuses_anything_but_digits(self):
        queries = equality.side_queries(equality.parse_args(ARGS + ["--region", "9999911"]))
        self.assertTrue(all("pnu LIKE '9999911%'" in sql for _, sql in queries.values()))
        with self.assertRaises(ValueError):
            equality.side_queries(equality.parse_args(ARGS + ["--region", "11' OR '1'='1"]))

    def test_each_side_reads_its_own_copy_in_one_crs(self):
        queries = equality.side_queries(equality.parse_args(ARGS))
        self.assertIn("source_snapshot_id = 'vworldkr__parcel-202606'", queries["silver"][1])
        self.assertIn("FOR VERSION AS OF 4772", queries["served"][1])
        self.assertIn(f"rebuild_run_id = '{RUN}'", queries["mirror"][1])
        self.assertIn(f"projection_load_id = '{LOAD}'", queries["publication"][1])
        self.assertTrue(all("ST_Transform(geom, 4326)" in queries[name][1] for name in ("mirror", "publication")))
        self.assertEqual({engine for engine, _ in queries.values()}, {"trino", "postgres"})

    def test_ids_that_are_not_ids_are_refused(self):
        for flag, bad in (("--mirror-run-id", "x'; DROP TABLE t; --"), ("--served-snapshot", "0"),
                          ("--source-snapshot-id", "a b")):
            argv = list(ARGS)
            argv[argv.index(flag) + 1] = bad
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                equality.parse_args(argv)

    def test_a_malformed_engine_row_is_refused(self):
        with self.assertRaises(ValueError):
            equality.parse_rows("9999911,1,1,1,1\n")
        row = "9999911,1,1,1,1,1,1,0,1.0,2.0,3.0\n"
        self.assertEqual(equality.parse_rows(row)["9999911"].centroid_y, 3.0)
        with self.assertRaises(ValueError):
            equality.parse_rows(row + row)


if __name__ == "__main__":
    unittest.main()
