"""The parcel registry as of an edition, and its next step as joins (root ADR-0490 §4a).

`EditionReadTest` needs no Spark: which editions a read "as of" holds, since when the served read holds
each, and which edition readiness names next. `RegistryFramesSparkTest` checks that the joins of
`parcel_registry_frames` append exactly the rows `parcel_identity` appends over the same values, and
that a candidate's step stays out of the served edition's read. Skipped where pyspark is missing.
"""

import importlib.util
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_identity as pi  # noqa: E402
import parcel_lineage as pl  # noqa: E402
import parcel_registry_frames as frames  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402


def has_pyspark():
    try:
        return importlib.util.find_spec("pyspark") is not None
    except ValueError:  # another test module left a stand-in `pyspark` in sys.modules
        return False


# Synthetic editions and the reserved 99999 range only.
CONTRACT = {
    "snapshot_id_prefix": "synthetic__parcel-",
    "served_edition": "209906",
    "editions": {"209906": {}, "209909": {}, "209910": {}},
    "promotions": [{"edition": "209906", "promoted_at_utc": "2099-07-01T00:00:00Z"}],
}
FIRST, NEXT, LATER = (editions.snapshot_id(CONTRACT, n) for n in ("209906", "209909", "209910"))
OLD, NEW, GONE = "9999910100", "9999920100", "9999930100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


class EditionReadTest(unittest.TestCase):
    def test_a_read_as_of_an_edition_holds_it_and_the_ones_before(self):
        self.assertEqual(editions.snapshot_ids_through(CONTRACT, NEXT), [FIRST, NEXT])
        self.assertEqual(frames.through_predicate(FIRST, CONTRACT), f"to_snapshot_id IN ('{FIRST}')")
        with self.assertRaises(editions.EditionError):
            editions.snapshot_ids_through(CONTRACT, "synthetic__parcel-209912")

    def test_an_edition_reaches_the_served_read_at_the_promotion_that_includes_it(self):
        ledger = dict(CONTRACT, promotions=[
            {"edition": "209906", "promoted_at_utc": "t0"},
            {"edition": "209910", "promoted_at_utc": "t1"},  # 209909 skipped: included with 209910
            {"edition": "209906", "promoted_at_utc": "t2"},  # rolled back
            {"edition": "209909", "promoted_at_utc": "t3"},
        ])
        self.assertEqual(editions.included_since(ledger), {FIRST: "t0", NEXT: "t3"})
        self.assertEqual(editions.included_since(dict(ledger, promotions=ledger["promotions"][:2])),
                         {FIRST: "t0", NEXT: "t1", LATER: "t1"})

    def test_the_candidate_is_the_newest_newer_edition_waiting_only_for_its_rehearsal(self):
        report = {"editions": [{"edition": "209906", "first_failing_stage": None},
                               {"edition": "209909", "first_failing_stage": "rehearsal"},
                               {"edition": "209910", "first_failing_stage": "silver"}]}
        self.assertEqual(editions.candidate(CONTRACT, report), "209909")
        self.assertIsNone(editions.candidate(CONTRACT, None))
        self.assertIsNone(editions.candidate(dict(CONTRACT, served_edition="209909"), report))

    def test_a_link_across_sido_reads_them_as_one_region(self):
        # 29·46 → 12 on 2026-07-01, in synthetic codes.
        groups = frames.advance_groups([("97", "95"), ("96", "95")], ["95", "98"], ["96", "97", "98"])
        self.assertEqual(groups, [["95", "95,96,97"], ["98", "98"]])
        self.assertEqual(frames.advance_groups([], ["98"], ["98"]), [["98", "98"]])


REGISTRY = pi.bootstrap([pnu(OLD, n) for n in range(1, 8)] + [pnu(GONE, 1)], "2099-06-01")
# 1-3 renumbered by a code change, 4 by a weak transfer (fresh id), 5 and 6 both claim 5's land (neither
# carries), 7 kept; GONE·1 vanished; NEW·9 is a new parcel.
AFTER = [pnu(NEW, n) for n in range(1, 7)] + [pnu(OLD, 7), pnu(NEW, 9)]
LINKS = (
    [pl.Link(pnu(OLD, n), pnu(NEW, n), "code_change", "code_derived", "t") for n in range(1, 4)]
    + [pl.Link(pnu(OLD, 4), pnu(NEW, 4), "jurisdiction_transfer", "evidence_weak", "t"),
       pl.Link(pnu(OLD, 5), pnu(NEW, 5), "registration_conversion", "official", "t"),
       pl.Link(pnu(OLD, 5), pnu(NEW, 6), "registration_conversion", "official", "t")]
)


@unittest.skipUnless(has_pyspark(), "requires pyspark")
class RegistryFramesSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (SparkSession.builder.master("local[2]").appName("parcel-registry-frames")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def registry(self, rows, snapshot=FIRST):
        return self.spark.createDataFrame(
            [(r.parcel_id, r.pnu, r.status, r.valid_from or None, r.valid_to, r.redirect_to, snapshot) for r in rows],
            "parcel_id string, pnu string, status string, valid_from string, valid_to string, redirect_to string, "
            "to_snapshot_id string")

    def lineage(self, links):
        return self.spark.createDataFrame(
            [(link.predecessor_pnu, link.successor_pnu, link.relation, link.grade, link.evidence_kind, None, None,
              FIRST, NEXT) for link in links],
            ", ".join(f"{c} string" for c in frames.LINEAGE_COLUMNS[:6])
            + ", derived_at_utc timestamp, from_snapshot_id string, to_snapshot_id string")

    def pnus(self, values):
        return self.spark.createDataFrame([(p,) for p in values], "pnu string")

    @staticmethod
    def as_set(rows):
        return {(r.parcel_id, r.pnu, r.status, r.valid_from or None, r.valid_to, r.redirect_to) for r in rows}

    def test_the_fold_is_parcel_identitys(self):
        rows = REGISTRY + pi.advance(pi.fold(REGISTRY).current, AFTER, pl.effective_links(LINKS)[0], "2099-09-01").rows
        state = pi.fold(rows)
        frame = self.registry(rows)
        self.assertEqual({(r.pnu, r.parcel_id) for r in frames.current_ids(frame).collect()}, set(state.current.items()))
        self.assertEqual({(r.pnu, r.parcel_id) for r in frames.closed_ids(frame).collect()},
                         set(state.last_id_of_closed.items()))

    def test_an_advance_appends_the_rows_parcel_identity_appends(self):
        expected = pi.advance(pi.fold(REGISTRY).current, AFTER, pl.effective_links(LINKS)[0], "2099-09-01")
        registry = self.registry(REGISTRY)
        carry, conflicts, _ = frames.carrying(frames.resolved_lineage(self.lineage(LINKS)))
        self.assertEqual(conflicts, 0)
        rows, counts = frames.advance_rows(frames.current_ids(registry), self.pnus(AFTER), carry, "2099-09-01")
        self.assertEqual(self.as_set(rows.collect()), self.as_set(expected.rows))
        self.assertEqual(counts, {"kept": expected.kept, "carried": expected.carried, "issued": expected.issued,
                                  "retired": expected.retired, "redirected": 0})
        self.assertEqual((counts["carried"], counts["kept"]), (3, 1))
        self.assertEqual(frames.problems(registry, rows, self.pnus(AFTER)), [])

    def test_a_stronger_link_later_redirects_the_interim_id_as_parcel_identity_does(self):
        advanced = REGISTRY + pi.advance(pi.fold(REGISTRY).current, AFTER, pl.effective_links(LINKS)[0], "2099-09-01").rows
        stronger = [pl.Link(pnu(OLD, 4), pnu(NEW, 4), "jurisdiction_transfer", "official", "t")] + LINKS[:3]
        expected = pi.upgrade(pi.fold(advanced), pl.effective_links(stronger)[0], "2099-10-01")
        registry = self.registry(advanced)
        carry, _, _ = frames.carrying(frames.resolved_lineage(self.lineage(stronger)))
        rows, counts = frames.upgrade_rows(frames.current_ids(registry), frames.closed_ids(registry), carry, "2099-10-01")
        self.assertEqual(self.as_set(rows.collect()), self.as_set(expected.rows))
        self.assertEqual(counts, {"redirected": 1})

    def test_two_identity_claims_on_one_successor_are_a_conflict(self):
        conflicting = [pl.Link(pnu(OLD, 1), pnu(NEW, 1), "jurisdiction_transfer", "official", "t"),
                       pl.Link(pnu(OLD, 2), pnu(NEW, 1), "jurisdiction_transfer", "evidence_strong", "t")]
        _, conflicts, sample = frames.carrying(frames.resolved_lineage(self.lineage(conflicting)))
        self.assertEqual((conflicts, sample), (1, [pnu(NEW, 1)]))

    def test_a_parcel_without_an_id_is_refused(self):
        registry = self.registry(REGISTRY)
        empty = self.registry([]).select(*frames.REGISTRY_ROW)
        self.assertEqual(frames.problems(registry, empty, self.pnus([pnu(NEW, 9)])), [f"no current id: {pnu(NEW, 9)}"])

    def test_the_candidates_step_stays_out_of_the_served_read(self):
        # 2026-11-11: the registry knew only 202606; appending 202609's step must not move the served ids.
        step = pi.advance(pi.fold(REGISTRY).current, AFTER, pl.effective_links(LINKS)[0], "2099-09-01").rows
        both = self.registry(REGISTRY).unionByName(self.registry(step, NEXT))
        served = {(r.pnu, r.parcel_id) for r in frames.current_ids(frames.registry_through(both, FIRST, CONTRACT)).collect()}
        self.assertEqual(served, set(pi.fold(REGISTRY).current.items()))
        candidate = {r.pnu for r in frames.current_ids(frames.registry_through(both, NEXT, CONTRACT)).collect()}
        self.assertEqual(candidate, set(AFTER))


if __name__ == "__main__":
    unittest.main()
