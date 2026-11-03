"""Edition readiness and the gated promotion (root ADR-0291 §2, §4; ADR-0379), on synthetic editions.

The editions are dated 2099 and the 시도 are the synthetic 97–98, so nothing here is a real place or
a real release. Each refusal is proven by planting what it refuses: an edition missing a stage, a
report made from another contract or too old, a hand-set `ready`, and a served edition moved by hand.
"""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import parcel_edition_readiness as readiness  # noqa: E402
import vworld_parcel_editions as editions  # noqa: E402
from test_parcel_edition_pairs import contract as synthetic_contract  # noqa: E402

NOW = datetime(2099, 10, 20, 12, tzinfo=timezone.utc)
SERVED, CANDIDATE = "209906", "209910"


def contract() -> dict:
    held = synthetic_contract(SERVED, CANDIDATE, served=SERVED)
    for entry in held["editions"].values():
        entry["measured_at_utc"] = "2099-10-15"
    return held


def evidence(**overrides) -> dict:
    found = {
        "measured_at_utc": "2099-10-20T11:00:00Z",
        "provider_reuploads": {SERVED: [], CANDIDATE: []},
        "silver": {"rows_by_edition": {SERVED: 1000, CANDIDATE: 1002}},
        "rehearsals": {CANDIDATE: {"gold": {"passed": True, "detail": "served Gold 1002 rows"},
                                   "bake": {"passed": True, "detail": "equivalence 0 differences"}}},
    }
    found.update(overrides)
    return found


def row(report: dict, name: str) -> dict:
    return next(entry for entry in report["editions"] if entry["edition"] == name)


class ReadinessTest(unittest.TestCase):
    def test_an_edition_with_every_stage_is_ready(self):
        report = readiness.readiness(contract(), evidence(), NOW)
        self.assertTrue(row(report, CANDIDATE)["ready"])
        self.assertIsNone(row(report, CANDIDATE)["first_failing_stage"])
        self.assertEqual(report["served_edition"], SERVED)
        self.assertEqual(report["promotions"][-1]["edition"], SERVED)

    def test_the_first_missing_stage_is_named(self):
        unmeasured = contract()
        del unmeasured["editions"][CANDIDATE]["measured_at_utc"]
        cases = {
            "source": (unmeasured, evidence()),
            "source ": (contract(), evidence(provider_reuploads={SERVED: [], CANDIDATE: ["30563-1.zip 2099-10-18"]})),
            "source  ": (contract(), evidence(provider_reuploads={SERVED: []})),
            "silver": (contract(), evidence(silver={"rows_by_edition": {SERVED: 1000}})),
            "silver ": (contract(), evidence(silver={"rows_by_edition": {SERVED: 1000, CANDIDATE: 980}})),
            "silver  ": (contract(), evidence(silver=None)),
            "rehearsal": (contract(), evidence(rehearsals={CANDIDATE: {"gold": {"passed": True}}})),
            "rehearsal ": (contract(), evidence(rehearsals={})),
        }
        for stage, (held, found) in cases.items():
            with self.subTest(stage=stage):
                candidate = row(readiness.readiness(held, found, NOW), CANDIDATE)
                self.assertFalse(candidate["ready"])
                self.assertEqual(candidate["first_failing_stage"], stage.strip())

    def test_no_evidence_is_not_ready(self):
        report = readiness.readiness(contract(), {}, NOW)
        self.assertEqual([entry["ready"] for entry in report["editions"]], [False, False])


class EvidenceTest(unittest.TestCase):
    """The host's files read as the tile bake and Trino write them (formats of 2026-10-15/16)."""

    def test_partition_counts_become_rows_per_edition(self):
        csv = f'"vworldkr__parcel-{SERVED}","1000","14"\n"vworldkr__parcel-{CANDIDATE}","1002","15"\n"other-1","5","1"\n'
        self.assertEqual(readiness.silver_rows(contract(), csv), {SERVED: 1000, CANDIDATE: 1002})

    def test_the_newest_rehearsal_of_each_edition_counts(self):
        snap = f"vworldkr__parcel-{CANDIDATE}"
        refused_then_built = [
            f"2099-10-15T15:07:02Z rehearsal: the matching gate refused {snap} in 3s (verdict v.json); continuing",
            f"2099-10-15T15:21:25Z rehearsal-gold: served Gold of {snap} in 863s, handoff 1 bytes, total 866s",
            f"2099-10-15T16:43:08Z rehearsal-bake {snap} FAILED at line 215 (log bake.log)",
        ]
        found = readiness.rehearsals(contract(), refused_then_built)[CANDIDATE]
        self.assertFalse(found["gold"]["passed"])
        self.assertFalse(found["bake"]["passed"])
        passed = refused_then_built + [
            f"2099-10-16T01:00:00Z rehearsal-gold: served Gold of {snap} in 800s, handoff 1 bytes, total 801s",
            f"2099-10-16T03:00:00Z rehearsal baked {snap} in 5000s: lakehouse-tile-bake-rehearsal tiles=9 differences=0",
        ]
        found = readiness.rehearsals(contract(), passed)[CANDIDATE]
        self.assertTrue(found["gold"]["passed"] and found["bake"]["passed"])
        # A newer Gold run makes the bake before it stale.
        rebuilt = passed + [f"2099-10-17T01:00:00Z rehearsal-gold {snap} FAILED at line 188 (log run.log)"]
        self.assertNotIn("bake", readiness.rehearsals(contract(), rebuilt)[CANDIDATE])

    def test_what_the_host_did_not_give_is_left_out(self):
        found = readiness.evidence(contract(), None, None, None, NOW)
        self.assertEqual(found, {"measured_at_utc": "2099-10-20T12:00:00Z"})
        report = readiness.readiness(contract(), found, NOW)
        self.assertEqual(row(report, CANDIDATE)["first_failing_stage"], "source")
        self.assertTrue(row(report, SERVED)["stages"]["rehearsal"]["passed"])


class PartialRunTest(unittest.TestCase):
    """The scheduled check judges only the editions whose inputs moved (root ADR-0380)."""

    def test_an_edition_whose_inputs_did_not_move_keeps_its_row(self):
        held, found = contract(), evidence()
        first = readiness.readiness(held, found, NOW)
        digests = {name: readiness.edition_inputs(held, name, found) for name in editions.names(held)}
        # Mark the kept rows, so a row judged again shows itself by losing the mark.
        for entry in first["editions"]:
            entry["stages"]["source"]["detail"] = "kept"
        moved = copy.deepcopy(found)
        moved["rehearsals"][CANDIDATE]["bake"] = {"passed": False, "detail": "equivalence differs"}
        second = readiness.readiness(held, moved, NOW, previous=first, previous_inputs=digests)
        self.assertEqual(row(second, SERVED)["stages"]["source"]["detail"], "kept")
        self.assertNotEqual(row(second, CANDIDATE)["stages"]["source"]["detail"], "kept")
        self.assertEqual(row(second, CANDIDATE)["first_failing_stage"], "rehearsal")

    def test_a_moved_served_edition_judges_every_edition_again(self):
        held, found = contract(), evidence()
        digests = {name: readiness.edition_inputs(held, name, found) for name in editions.names(held)}
        moved = readiness.promote(held, readiness.readiness(held, found, NOW), CANDIDATE, "op", "why", NOW)
        self.assertTrue(all(readiness.edition_inputs(moved, name, found) != digests[name] for name in digests))


class PromotionTest(unittest.TestCase):
    def promote(self, held: dict, report: dict, now: datetime = NOW) -> dict:
        return readiness.promote(held, report, CANDIDATE, "operator@synthetic", "the newer edition is all there", now)

    def test_a_ready_edition_is_served_and_its_promotion_appended(self):
        held = contract()
        promoted = self.promote(held, readiness.readiness(held, evidence(), NOW))
        self.assertEqual(promoted["served_edition"], CANDIDATE)
        last = promoted["promotions"][-1]
        self.assertEqual((last["edition"], last["from_edition"], last["by"]), (CANDIDATE, SERVED, "operator@synthetic"))
        self.assertEqual(sorted(last["readiness"]["stages"]), sorted(readiness.STAGES))
        self.assertEqual(promoted["promotions"][:-1], held["promotions"])
        editions.validate(promoted)

    def test_an_incomplete_edition_is_refused(self):
        held = contract()
        report = readiness.readiness(held, evidence(rehearsals={}), NOW)
        with self.assertRaisesRegex(editions.EditionError, "not ready: stage rehearsal failed"):
            self.promote(held, report)

    def test_a_hand_set_ready_does_not_pass(self):
        held = contract()
        report = readiness.readiness(held, evidence(rehearsals={}), NOW)
        row(report, CANDIDATE).update(ready=True, first_failing_stage=None)
        with self.assertRaisesRegex(editions.EditionError, "not ready"):
            self.promote(held, report)

    def test_a_report_of_another_contract_or_too_old_is_refused(self):
        held = contract()
        report = readiness.readiness(held, evidence(), NOW)
        changed = copy.deepcopy(held)
        changed["editions"][CANDIDATE]["measured_at_utc"] = "2099-10-16"
        with self.assertRaisesRegex(editions.EditionError, "another contract"):
            self.promote(changed, report)
        with self.assertRaisesRegex(editions.EditionError, "older than"):
            self.promote(held, report, NOW + timedelta(hours=49))

    def test_a_promotion_names_who_and_why(self):
        held = contract()
        report = readiness.readiness(held, evidence(), NOW)
        with self.assertRaisesRegex(editions.EditionError, "who asked"):
            readiness.promote(held, report, CANDIDATE, "operator", "two\nlines", NOW)


class LedgerTest(unittest.TestCase):
    def test_a_served_edition_moved_by_hand_is_refused(self):
        held = contract()
        held["served_edition"] = CANDIDATE
        with self.assertRaisesRegex(editions.EditionError, "last promotion"):
            editions.validate(held)

    def test_a_forged_or_rewritten_promotion_is_refused(self):
        held = contract()
        promoted = readiness.promote(held, readiness.readiness(held, evidence(), NOW), CANDIDATE, "op", "why", NOW)
        cases = {
            "carries no readiness": lambda c: c["promotions"][-1].update(readiness=None),
            "does not carry the readiness": lambda c: c["promotions"][-1]["readiness"]["stages"].pop("rehearsal"),
            "only grows": lambda c: c["promotions"][-1].update(from_edition=CANDIDATE),
            "holds no promotions": lambda c: c.update(promotions=[]),
        }
        for message, plant in cases.items():
            with self.subTest(message=message):
                broken = copy.deepcopy(promoted)
                plant(broken)
                with self.assertRaisesRegex(editions.EditionError, message):
                    editions.validate(broken)

    def test_the_real_contract_serves_its_last_promotion(self):
        real = editions.load()
        self.assertEqual(real["promotions"][-1]["edition"], editions.served(real))


class ConsoleShapeTest(unittest.TestCase):
    def test_the_report_has_the_shape_foundation_api_reads(self):
        # foundation-api's parcel_editions route reads this file (its example is what its test parses).
        example = json.loads((Path(__file__).resolve().parents[4] / "services/foundation-api/src/routes/data_catalog/"
                              "parcel-editions.example.json").read_text(encoding="utf-8"))
        made = readiness.readiness(contract(), evidence(), NOW)
        self.assertEqual(sorted(made), sorted(example))
        self.assertEqual(sorted(made["editions"][0]), sorted(example["editions"][0]))
        self.assertEqual(sorted(made["editions"][0]["stages"]), sorted(example["editions"][0]["stages"]))


class CommandLineTest(unittest.TestCase):
    def test_the_command_refuses_an_incomplete_edition_and_leaves_the_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, report = Path(tmp) / "contract.json", Path(tmp) / "report.json"
            body = json.dumps(contract(), ensure_ascii=False, indent=2) + "\n"
            path.write_text(body, encoding="utf-8")
            report.write_text(json.dumps(readiness.readiness(contract(), evidence(rehearsals={}))), encoding="utf-8")
            code = readiness.main(["promote", "--contract", str(path), "--report", str(report), "--edition", CANDIDATE,
                                   "--by", "op", "--reason", "why"])
            self.assertEqual(code, 2)
            self.assertEqual(path.read_text(encoding="utf-8"), body)


if __name__ == "__main__":
    unittest.main()
