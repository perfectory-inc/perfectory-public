"""The lakehouse-versus-PostGIS parcel boundary proof (scripts/ops/parcel-boundary-equality.py, root ADR-0194).

The engines are replaced by a fake that fingerprints in-memory rows the way both SQL dialects do
(28-bit slices of sha256(pnu ':' checksum), summed per region). Every way the copies can differ —
a changed geometry, a missing or extra parcel, a PNU held twice, a checksum that does not name the
lakehouse bytes — must turn the verdict false and the exit code non-zero.
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest
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


def checksum(geometry: bytes) -> str:
    return hashlib.sha256(geometry).hexdigest()


def fingerprint_rows(rows, prefix_length=2):
    """What the SQL returns for rows of (pnu, checksum, wkb or None): one CSV line per region."""
    regions = {}
    for pnu, digest, wkb in rows:
        region = regions.setdefault(pnu[:prefix_length], {"rows": 0, "pnus": set(), "a": 0, "b": 0, "bad": 0})
        key = hashlib.sha256(f"{pnu}:{digest}".encode()).hexdigest()
        region["rows"] += 1
        region["pnus"].add(pnu)
        region["a"] += int(key[0:7], 16)
        region["b"] += int(key[7:14], 16)
        region["bad"] += wkb is not None and checksum(wkb) != digest
    return "\n".join(
        f"{name},{r['rows']},{len(r['pnus'])},{r['a']},{r['b']},{int(r['bad'])}"
        for name, r in sorted(regions.items())
    ) + "\n"


# Fictional PNUs (99999 is no real 시군구): two regions.
PARCELS = [(f"{region}99900101100010{n:03d}", bytes([n, int(region)]) * 9) for region in ("11", "26") for n in range(1, 6)]


def sides(mirror=None, served=None):
    """Engine output per side: Silver and served carry their WKB, PostGIS only the checksum."""
    lake = [(pnu, checksum(wkb), wkb) for pnu, wkb in PARCELS]
    postgis = [(pnu, checksum(wkb), None) for pnu, wkb in PARCELS]
    return {
        "silver": lake,
        "served": served if served is not None else lake,
        "mirror": mirror if mirror is not None else postgis,
        "publication": postgis,
    }


def run(rows_by_side, extra=()):
    """Runs main() with each side's query answered from rows_by_side; returns (exit, report)."""
    queries = equality.side_queries(equality.parse_args(ARGS + list(extra)))
    by_sql = {sql: name for name, (_, sql) in queries.items()}

    def engine(_engine, sql):
        return fingerprint_rows(rows_by_side[by_sql[sql]])

    out = io.StringIO()
    with mock.patch.object(equality, "run_engine", engine), contextlib.redirect_stdout(out):
        code = equality.main(ARGS + list(extra))
    return code, json.loads(out.getvalue())


class ParcelBoundaryEqualityTest(unittest.TestCase):
    def test_identical_copies_are_equal(self):
        code, report = run(sides())
        self.assertEqual(code, 0)
        self.assertTrue(report["equal"])
        self.assertEqual(report["regions"], 2)
        self.assertEqual({side["rows"] for side in report["totals"].values()}, {10})

    def test_a_changed_geometry_in_the_mirror_names_its_region(self):
        mirror = [(pnu, checksum(wkb + (b"x" if pnu.startswith("26") and pnu.endswith("3") else b"")), None)
                  for pnu, wkb in PARCELS]
        code, report = run(sides(mirror=mirror))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["differing_regions"]], ["26"])

    def test_a_parcel_missing_from_one_side_differs(self):
        code, report = run(sides(served=[(p, checksum(w), w) for p, w in PARCELS[1:]]))
        self.assertEqual(code, 1)
        self.assertEqual([d["region"] for d in report["differing_regions"]], ["11"])

    def test_a_region_only_one_side_holds_differs(self):
        extra = ("4799900101100010001", b"\x01" * 9)
        served = [(p, checksum(w), w) for p, w in PARCELS + [extra]]
        code, report = run(sides(served=served))
        self.assertEqual(code, 1)
        self.assertEqual(report["differing_regions"][0]["region"], "47")
        self.assertIsNone(report["differing_regions"][0]["sides"]["silver"])

    def test_a_pnu_held_twice_fails_even_when_every_side_agrees(self):
        pnu, wkb = PARCELS[0]
        doubled = [(pnu, checksum(wkb), wkb)]
        rows = {name: rows + [(r[0], r[1], r[2]) for r in doubled if name in ("silver", "served")]
                + ([(pnu, checksum(wkb), None)] if name in ("mirror", "publication") else [])
                for name, rows in sides().items()}
        code, report = run(rows)
        self.assertEqual(code, 1)
        self.assertEqual({d["side"] for d in report["duplicated_pnus"]}, set(equality.SIDES))

    def test_a_checksum_that_does_not_name_the_bytes_fails(self):
        served = [(p, checksum(w), w + (b"!" if i == 0 else b"")) for i, (p, w) in enumerate(PARCELS)]
        code, report = run(sides(served=served))
        self.assertEqual(code, 1)
        self.assertEqual(report["checksum_mismatches"], [{"region": "11", "side": "served", "rows": 1}])
        self.assertEqual(report["differing_regions"], [])

    def test_an_unreadable_side_exits_2(self):
        def broken(engine, _sql):
            raise LookupError(f"{engine}: down")

        out = io.StringIO()
        with mock.patch.object(equality, "run_engine", broken), contextlib.redirect_stdout(out):
            self.assertEqual(equality.main(ARGS), 2)
        self.assertFalse(json.loads(out.getvalue())["equal"])

    def test_the_region_limits_every_side_and_refuses_anything_but_digits(self):
        queries = equality.side_queries(equality.parse_args(ARGS + ["--region", "11"]))
        self.assertTrue(all("pnu LIKE '11%'" in sql for _, sql in queries.values()))
        with self.assertRaises(ValueError):
            equality.side_queries(equality.parse_args(ARGS + ["--region", "11' OR '1'='1"]))

    def test_each_side_reads_its_own_copy(self):
        queries = equality.side_queries(equality.parse_args(ARGS))
        self.assertIn("source_snapshot_id = 'vworldkr__parcel-202606'", queries["silver"][1])
        self.assertIn("FOR VERSION AS OF 4772", queries["served"][1])
        self.assertIn(f"rebuild_run_id = '{RUN}'", queries["mirror"][1])
        self.assertIn(f"projection_load_id = '{LOAD}'", queries["publication"][1])
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
            equality.parse_rows("11,1,1,1\n")
        with self.assertRaises(ValueError):
            equality.parse_rows("11,1,1,1,1,0\n11,1,1,1,1,0\n")


if __name__ == "__main__":
    unittest.main()
