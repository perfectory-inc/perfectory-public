"""The matching gate at national scale: the same verdict as `map_matching_gate`, from joins (ADR-0113 §7).

`ParcelGateSparkTest` plants every refusal the gate names and checks the Spark checks report
exactly what the pure-Python checks report over the same values, while no collect returns more
than `SAMPLE` rows: the PNU sets, the code list and the registry stay on the executors. Skipped
where pyspark is missing. `VerdictContractTest` needs no Spark.
"""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import map_matching_gate as gate  # noqa: E402
import parcel_identity as pi  # noqa: E402
import parcel_lineage as pl  # noqa: E402
import parcel_matching_gate as job  # noqa: E402
from lineage_review_queue import steward_resolved  # noqa: E402

PLATFORM = Path(__file__).resolve().parents[4]


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


# Reserved 99999 range. 갑동 exists and has a boundary, 을동 exists without one, 병동 was abolished.
GAP, EUL, BYEONG = "9999910100", "9999910200", "9999920100"
OFFICIAL = {GAP: ("합성 갑동", gate.EXISTS), EUL: ("합성 을동", gate.EXISTS), BYEONG: ("합성 병동", "폐지")}
ADMIN = {GAP}


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


GAP_PARCELS = [pnu(GAP, n) for n in range(1, 31)]
PARCELS = GAP_PARCELS + [pnu(EUL, 1), pnu(BYEONG, 1), "123"]
REGISTRY = (
    [pi.RegistryRow(f"id-{n}", pnu(GAP, n), pi.CURRENT, "2099-01-01") for n in range(1, 27)]
    # 27: closed later, so it has no current id.
    + [pi.RegistryRow("id-27", pnu(GAP, 27), pi.CURRENT, "2099-01-01"),
       pi.RegistryRow("id-27", pnu(GAP, 27), pi.HISTORIC, "2099-02-01", "2099-02-01")]
    # 28: renumbered id; the later current row wins and the earlier id's closing does not matter.
    + [pi.RegistryRow("id-28-old", pnu(GAP, 28), pi.CURRENT, "2099-01-01"),
       pi.RegistryRow("id-28-old", pnu(GAP, 28), pi.HISTORIC, "2099-02-01", "2099-02-01"),
       pi.RegistryRow("id-28", pnu(GAP, 28), pi.CURRENT, "2099-02-01")]
    # 29 and 30 carry one id between them; nothing for 을동 and 병동.
    + [pi.RegistryRow("id-shared", pnu(GAP, 29), pi.CURRENT, "2099-01-01"),
       pi.RegistryRow("id-shared", pnu(GAP, 30), pi.CURRENT, "2099-01-01")]
)
# The price is held for 1..20; 21 lost it under an old number its lineage names, 22 never had one.
HELD = {pnu(GAP, n) for n in range(1, 21)} | {pnu(GAP, 90)}
LINEAGE = [
    {"predecessor_pnu": pnu(GAP, 90), "successor_pnu": pnu(GAP, 21), "relation": "code_change",
     "grade": "code_derived", "evidence_kind": "derived", "evidence_ref": "synthetic"},
    {"predecessor_pnu": pnu(GAP, 91), "successor_pnu": pnu(GAP, 22), "relation": "code_change",
     "grade": "code_derived", "evidence_kind": "derived", "evidence_ref": "synthetic"},
]


class VerdictContractTest(unittest.TestCase):
    def test_the_bake_guards_read_the_schema_the_gate_writes(self):
        migration = next((PLATFORM / "migrations").glob("*_a_lakehouse_bake_binds_the_silver_snapshot_it_read.sql"))
        verdict_rs = PLATFORM / "services/foundation-outbox-publisher/src/lakehouse_bake_verdict.rs"
        for path in (migration, verdict_rs):
            with self.subTest(path=path.name):
                self.assertIn(f'{job.VERDICT_SCHEMA_VERSION}', path.read_text(encoding="utf-8"))


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class ParcelGateSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("parcel-matching-gate")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, parcels):
        s = self.spark
        return (
            s.createDataFrame([(p,) for p in parcels], "pnu string"),
            s.createDataFrame([(c, st) for c, (_, st) in OFFICIAL.items()], "region_cd string, status string"),
            s.createDataFrame([(c,) for c in ADMIN], "canonical_code string"),
            s.createDataFrame(
                [(r.parcel_id, r.pnu, r.status, r.valid_from, r.valid_to) for r in REGISTRY],
                "parcel_id string, pnu string, status string, valid_from string, valid_to string",
            ),
        )

    def small_collects_only(self):
        """Every collect in the block must return at most `SAMPLE` rows; more is a set on the driver."""

        from pyspark.sql import DataFrame

        real = DataFrame.collect
        largest = []

        def bounded(frame):
            rows = real(frame)
            largest.append(len(rows))
            if len(rows) > gate.SAMPLE:
                raise AssertionError(f"a collect brought {len(rows)} rows to the driver")
            return rows

        return patch.object(DataFrame, "collect", bounded), largest

    def test_the_parcel_checks_report_what_the_python_gate_reports(self):
        expected = gate.check_parcels(PARCELS, OFFICIAL, ADMIN, pi.fold(REGISTRY).current).as_dict()
        parcels, official, admin, registry = self.frames(PARCELS)
        guard, largest = self.small_collects_only()
        with guard:
            got = job.check_parcels(parcels, official, admin, job.current_ids(registry))
        self.assertEqual(got, expected)
        self.assertFalse(got["passed"])
        self.assertEqual(
            set(got["violations"]),
            {gate.MALFORMED_PNU, gate.NO_LEGAL_DONG, gate.NO_BOUNDARY, gate.NO_CURRENT_ID, gate.ID_ON_TWO_PARCELS},
            "every planted refusal is named",
        )
        self.assertTrue(largest, "the checks did collect their samples")

    def test_without_a_registry_only_place_is_checked(self):
        expected = gate.check_parcels(PARCELS, OFFICIAL, ADMIN, None).as_dict()
        parcels, official, admin, _ = self.frames(PARCELS)
        self.assertEqual(job.check_parcels(parcels, official, admin, None), expected)

    def test_a_clean_snapshot_passes(self):
        clean = [pnu(GAP, n) for n in range(1, 27)]
        parcels, official, admin, registry = self.frames(clean)
        self.assertTrue(job.check_parcels(parcels, official, admin, job.current_ids(registry))["passed"])

    def test_an_attribute_left_under_an_old_number_is_refused(self):
        predecessors = {}
        for r in steward_resolved(LINEAGE):
            if r["predecessor_pnu"] and pl.GRADE_RANK[r["grade"]] <= pl.GRADE_RANK["evidence_strong"]:
                predecessors.setdefault(r["successor_pnu"], []).append(r["predecessor_pnu"])
        expected = gate.check_attribute("silver.land_individual_price", GAP_PARCELS, HELD, predecessors).as_dict()
        s = self.spark
        parcels = s.createDataFrame([(p,) for p in GAP_PARCELS], "pnu string")
        held = s.createDataFrame([(p,) for p in HELD], "pnu string")
        lineage = s.createDataFrame(
            [tuple(r.values()) for r in LINEAGE],
            "predecessor_pnu string, successor_pnu string, relation string, grade string, "
            "evidence_kind string, evidence_ref string",
        )
        guard, _ = self.small_collects_only()
        with guard:
            got = job.check_attribute("silver.land_individual_price", parcels, held, lineage)
        self.assertEqual(got, expected)
        self.assertFalse(got["passed"])
        self.assertEqual(
            got["violations"][gate.attribute_held_elsewhere("silver.land_individual_price")]["sample"],
            [f"{pnu(GAP, 21)}<-{pnu(GAP, 90)}"],
        )

    def test_the_collect_guard_catches_a_set_brought_to_the_driver(self):
        parcels, *_ = self.frames(PARCELS)
        guard, _ = self.small_collects_only()
        with guard, self.assertRaisesRegex(AssertionError, "brought 33 rows"):
            parcels.collect()


if __name__ == "__main__":
    unittest.main()
