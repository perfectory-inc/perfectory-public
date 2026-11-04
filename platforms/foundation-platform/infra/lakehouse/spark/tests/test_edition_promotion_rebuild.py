"""A promotion rebuilds every served-universe Gold, partially, against the new edition (root ADR-0381).

The editions are the synthetic 209906/209910 and the snapshot ids are synthetic. A promotion moves no
Silver snapshot (both editions are loaded side by side), so before ADR-0381 the planner said "nothing to
do" and every lane kept serving the old edition; each case below is that planted and shown handled.
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_industrial_complex_gold_schema_evolution  # noqa: F401,E402  Import-only PySpark stub.
import gold_incremental as incremental  # noqa: E402
import gold_rebuild as rebuild  # noqa: E402
import parcel_edition_readiness as readiness  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402
from test_parcel_edition_readiness import CANDIDATE, NOW, SERVED, contract, evidence  # noqa: E402

PARCELS = "silver.parcel_boundaries"
ENTRY = {"producer": "fixture", "producer_arguments": [], "max_row_loss_fraction": 0.01,
         "incremental": {"max_changed_key_fraction": 0.3, "parity_sample_keys": 10}}
INPUTS = ((PARCELS, "silver.a"), {}, PARCELS)


def snap(snapshot_id, parent, at, **summary):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": at, "operation": "append",
            "summary": {"added-records": "1", "deleted-records": "0", **summary}}


def silver():
    return {PARCELS: {"head": "11", "snapshots": [snap("11", None, "2099-09-01T00:00:00Z")]},
            "silver.a": {"head": "21", "snapshots": [snap("21", None, "2099-09-01T00:00:00Z")]}}


def gold(edition=None, at="2099-10-01T00:00:00Z"):
    summary = {"total-records": "1000",
               rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps({PARCELS: "11", "silver.a": "21"})}
    if edition is not None:
        summary[rebuild.SERVED_EDITION_PROPERTY] = edition
    return {"head": "90", "snapshots": [{**snap("90", None, at, **summary), "operation": "overwrite"}],
            "row_count": 1000}


def promoted():
    held = contract()
    return readiness.promote(held, readiness.readiness(held, evidence(), NOW), CANDIDATE, "op", "why", NOW)


class PromotionPlanTest(unittest.TestCase):
    def test_a_promotion_rebuilds_incrementally_from_the_edition_the_gold_was_built_from(self):
        old = editions.snapshot_id(contract(), SERVED)
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(old), silver(), parcel_contract=promoted())
        self.assertEqual(decision["action"], "rebuild")
        self.assertIn("the served edition moved", decision["reasons"][0])
        self.assertEqual(decision["mode"], "incremental", decision["mode_reasons"])
        self.assertEqual(decision["previous_parcel_edition"], old)
        flags = decision["producer_arguments"]
        self.assertEqual(flags[flags.index("--incremental-from-parcel-edition") + 1], old)

    def test_the_same_served_edition_is_nothing_to_do(self):
        served = editions.snapshot_id(contract(), SERVED)
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(served), silver(), parcel_contract=contract())
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])

    def test_a_gold_from_before_the_record_is_dated_by_the_promotions_ledger(self):
        # The synthetic ledger's first promotion (2026-10-05) served 209906; the promotion to 209910 is
        # dated 2099-10-20, after this Gold commit, so the commit was built from 209906.
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold(None), silver(), parcel_contract=promoted())
        self.assertEqual(decision["previous_parcel_edition"], editions.snapshot_id(contract(), SERVED))
        after = rebuild.plan("gold.x", ENTRY, INPUTS, gold(None, "2099-10-21T00:00:00Z"), silver(),
                             parcel_contract=promoted())
        self.assertEqual(after["action"], "nothing_to_do", after["reasons"])

    def test_a_table_that_does_not_read_the_parcels_is_not_moved(self):
        inputs = (("silver.a",), {}, "silver.a")
        state = {"silver.a": silver()["silver.a"]}
        summary = {"total-records": "10", rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps({"silver.a": "21"})}
        table = {"head": "90", "snapshots": [snap("90", None, "2099-10-01T00:00:00Z", **summary)], "row_count": 10}
        decision = rebuild.plan("gold.y", ENTRY, inputs, table, state, parcel_contract=promoted())
        self.assertEqual(decision["action"], "nothing_to_do")


class ChangedInputsTest(unittest.TestCase):
    """The producer's side: the parcels count as changed when the comparison edition is not the served one."""

    def test_a_moved_edition_changes_the_parcels_only(self):
        current = {PARCELS: "11", "silver.a": "21"}
        previous = {**current, editions.SERVED_EDITION_PIN: "vworldkr__parcel-209906"}
        self.assertEqual(incremental.changed_inputs(previous, current, "vworldkr__parcel-209910"), [PARCELS])
        self.assertEqual(incremental.changed_inputs(previous, current, "vworldkr__parcel-209906"), [])
        self.assertEqual(incremental.changed_inputs(dict(current), current, None), [])


if __name__ == "__main__":
    unittest.main()
