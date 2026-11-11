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
        # `promotion_requests` is added by `grant`, which runs after the judgement (root ADR-0381).
        self.assertEqual(sorted([*made, "promotion_requests"]), sorted(example))
        self.assertEqual(sorted(made["editions"][0]), sorted(example["editions"][0]))
        self.assertEqual(sorted(made["editions"][0]["stages"]), sorted(example["editions"][0]["stages"]))


class ConsoleRequestTest(unittest.TestCase):
    """A promotion asked in the console is granted by the readiness check, once (root ADR-0381)."""

    def request(self, folder: Path, request_id: str, **overrides) -> None:
        fields = {"request": request_id, "actor": "00000000-0000-0000-0000-000000000099",
                  "requested_at": "2099-10-20T11:00:00+00:00", "reason": "the newer edition is all there",
                  "job": "parcel_edition_readiness", "expires_at": "2099-10-22T11:00:00+00:00", "edition": CANDIDATE}
        fields.update(overrides)
        (folder / request_id).write_text("".join(f"{key}={value}\n" for key, value in fields.items()), encoding="utf-8")

    def test_each_request_is_answered_once_and_only_a_ready_edition_is_granted(self):
        held = contract()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            requests, answers = root / "promote.d", root / "promotions"
            requests.mkdir()
            report = root / "readiness.json"
            report.write_text(json.dumps(readiness.readiness(held, evidence(), datetime.now(timezone.utc))), encoding="utf-8")
            self.request(requests, "a")
            self.request(requests, "b", edition=SERVED)
            self.request(requests, "c", expires_at="2000-01-01T00:00:00+00:00")
            self.request(requests, "d", job="shape_ledger_check")
            self.assertEqual(readiness.grant(held, report, requests, answers), ["a", "b", "c", "d"])
            outcome = {row["request"]: row["outcome"] for row in json.loads(report.read_text(encoding="utf-8"))["promotion_requests"]}
            self.assertEqual(outcome["a"], "granted")
            self.assertTrue(outcome["b"].startswith("refused: ") and "already the served edition" in outcome["b"])
            self.assertEqual(outcome["c"], "expired")
            self.assertTrue(outcome["d"].startswith("refused: asked of shape_ledger_check"))
            granted = json.loads((answers / "a.contract.json").read_text(encoding="utf-8"))
            self.assertEqual(granted["served_edition"], CANDIDATE)
            self.assertEqual(granted["promotions"][-1]["by"], "00000000-0000-0000-0000-000000000099")
            editions.validate(granted)
            self.assertFalse((answers / "b.contract.json").exists())
            # A second run answers nothing again and keeps every answer.
            self.assertEqual(readiness.grant(held, report, requests, answers), [])
            self.assertEqual(len(json.loads(report.read_text(encoding="utf-8"))["promotion_requests"]), 4)


class LagTest(unittest.TestCase):
    """Root ADR-0490 §7: 202609, measured 2026-10-05, waited 37 days unserved and nothing said so."""

    def test_the_newest_edition_in_silver_waits_from_its_measurement(self):
        found = readiness.lag(contract(), readiness.readiness(contract(), evidence(), NOW), NOW)
        self.assertEqual((found["served"], found["newest"]), (SERVED, CANDIDATE))
        self.assertEqual(found["behind_seconds"], 5 * 86400 + 12 * 3600)  # measured 2099-10-15, now 10-20 12:00
        self.assertEqual(found["budget_seconds"], 14 * 86400)
        self.assertTrue(found["silver_measured"])
        lines = "\n".join(readiness.lag_series(found, NOW))
        self.assertIn(f'foundation_parcel_served_edition_behind_seconds{{served="{SERVED}",newest="{CANDIDATE}"}} 475200', lines)
        self.assertIn("foundation_parcel_edition_silver_measured 1", lines)

    def test_an_edition_not_in_silver_or_the_served_one_is_no_lag(self):
        for found in (evidence(silver={"rows_by_edition": {SERVED: 1000}}), ):
            report = readiness.readiness(contract(), found, NOW)
            self.assertEqual(readiness.lag(contract(), report, NOW)["behind_seconds"], 0)

    def test_unmeasured_silver_is_said_not_taken_for_no_lag(self):
        found = readiness.lag(contract(), readiness.readiness(contract(), evidence(silver=None), NOW), NOW)
        self.assertFalse(found["silver_measured"])
        self.assertEqual(found["behind_seconds"], 0)
        self.assertIn("foundation_parcel_edition_silver_measured 0", readiness.lag_series(found, NOW))

    def test_the_bound_is_the_contracts(self):
        held = contract()
        del held["readiness"]["max_served_lag_days_reason"]
        with self.assertRaises(editions.EditionError):
            editions.validate(held)
class ControllerTest(unittest.TestCase):
    """Root ADR-0490 §5: a ready edition is promoted on the judgement's own request, with no person.
    2026-10-05..11-11 the console was asked nothing and 202609 was never promoted."""

    def run_grant(self, found: dict) -> tuple[list[str], Path, dict]:
        held = contract()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        report = root / "readiness.json"
        report.write_text(json.dumps(readiness.readiness(held, found, datetime.now(timezone.utc))), encoding="utf-8")
        answered = readiness.grant(held, report, root / "promote.d", root / "promotions", controller=True)
        return answered, root / "promotions", held

    def test_the_newest_ready_edition_is_granted_once(self):
        answered, answers, held = self.run_grant(evidence())
        self.assertEqual(len(answered), 1)
        self.assertTrue(answered[0].startswith(f"{readiness.CONTROLLER}-{CANDIDATE}-"))
        granted = json.loads((answers / f"{answered[0]}.contract.json").read_text(encoding="utf-8"))
        self.assertEqual(granted["served_edition"], CANDIDATE)
        self.assertEqual(granted["promotions"][-1]["by"], readiness.CONTROLLER)
        editions.validate(granted)
        report = answers.parent / "readiness.json"
        self.assertEqual(readiness.grant(held, report, answers.parent / "promote.d", answers, controller=True), [])

    def test_an_edition_not_ready_is_not_asked_for(self):
        answered, answers, _ = self.run_grant(evidence(rehearsals={}))
        self.assertEqual(answered, [])
        self.assertEqual(list(answers.glob("*")), [])

    def test_without_the_flag_nothing_is_asked(self):
        held = contract()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = root / "readiness.json"
            report.write_text(json.dumps(readiness.readiness(held, evidence(), datetime.now(timezone.utc))), encoding="utf-8")
            self.assertEqual(readiness.grant(held, report, root / "promote.d", root / "promotions"), [])


class HostLedgerTest(unittest.TestCase):
    """Root ADR-0490 §5: a granted promotion takes effect through the host's append-only ledger, with no
    pull request. 2026-10-21..11-11 a grant could only become a contract file a person had to turn into a
    reviewed change, and none was ever opened."""

    def granted(self) -> tuple[dict, Path, dict]:
        held = contract()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        report = root / "readiness.json"
        report.write_text(json.dumps(readiness.readiness(held, evidence(), datetime.now(timezone.utc))), encoding="utf-8")
        self.request_twice(root)
        ledger = root / "served-ledger.jsonl"
        answered = readiness.grant(held, report, root / "promote.d", root / "promotions", controller=True, ledger=ledger)
        self.assertEqual(len(answered), 2)
        return held, ledger, json.loads(report.read_text(encoding="utf-8"))

    def request_twice(self, root: Path) -> None:
        (root / "promote.d").mkdir()
        ConsoleRequestTest.request(self, root / "promote.d", "console", reason="asked in the console too")

    def test_a_grant_is_appended_and_every_reader_serves_it(self):
        held, ledger, report = self.granted()
        lines = ledger.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)  # the console's request after the controller's is one promotion too many
        self.assertEqual(json.loads(lines[0])["by"], readiness.CONTROLLER)
        outcome = {row["request"]: row["outcome"] for row in report["promotion_requests"]}
        self.assertTrue(outcome["console"].startswith("refused: ") and "already the served edition" in outcome["console"])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contract.json"
            path.write_text(json.dumps(held), encoding="utf-8")
            served = editions.load(path, ledger)
            self.assertEqual(editions.served(served), CANDIDATE)
            self.assertEqual([entry["edition"] for entry in served["promotions"]], [SERVED, CANDIDATE])
            self.assertEqual(editions.served(editions.load(path, Path(tmp) / "absent.jsonl")), SERVED)

    def test_a_promotion_copied_into_the_contract_is_not_counted_twice(self):
        held, ledger, _ = self.granted()
        copied = editions.overlay(held, editions.read_ledger(ledger))
        self.assertEqual(editions.overlay(copied, editions.read_ledger(ledger))["promotions"], copied["promotions"])

    def test_a_ledger_that_does_not_continue_the_contract_stops_every_reader(self):
        held, ledger, _ = self.granted()
        entry = json.loads(ledger.read_text(encoding="utf-8"))
        with self.assertRaises(editions.EditionError):
            editions.validate(editions.overlay(held, [{**entry, "from_edition": CANDIDATE}]))
        with self.assertRaises(editions.EditionError):
            editions.validate(editions.overlay(held, [{**entry, "readiness": None}]))
        torn = ledger.with_name("torn.jsonl")
        torn.write_text("{", encoding="utf-8")
        with self.assertRaises(editions.EditionError):
            editions.read_ledger(torn)

    def test_the_ledger_is_readable_by_the_spark_containers(self):
        if sys.platform == "win32":
            self.skipTest("POSIX modes")
        _, ledger, _ = self.granted()
        self.assertEqual(ledger.stat().st_mode & 0o777, 0o644)

    def test_a_rollback_returns_to_the_edition_the_promotion_left(self):
        held, ledger, _ = self.granted()
        promoted = editions.overlay(held, editions.read_ledger(ledger))
        last = promoted["promotions"][-1]
        back = {"edition": SERVED, "from_edition": CANDIDATE, "promoted_at_utc": "2100-01-01T00:00:00Z",
                "by": readiness.CONTROLLER, "reason": "the first publish after the promotion failed",
                "rollback_of": last["promoted_at_utc"], "cause": "edge probe 3 of 5 failed", "readiness": None}
        rolled = editions.overlay(promoted, [back])
        editions.validate(rolled)
        self.assertEqual(editions.served(rolled), SERVED)
        for wrong in ({"rollback_of": "2000-01-01T00:00:00Z"}, {"cause": ""}, {"edition": CANDIDATE}):
            with self.assertRaises(editions.EditionError):
                editions.validate(editions.overlay(promoted, [{**back, **wrong}]))

    def test_the_real_contract_names_where_the_host_ledger_lives(self):
        declared = json.loads(editions.CONTRACT_PATH.read_text(encoding="utf-8"))["host_promotions_ledger"]
        self.assertEqual(Path(declared["path"]).name, Path(declared["container_path"]).name)
        compose = (editions.CONTRACT_PATH.parents[3] / "compose.lakehouse.yml").read_text(encoding="utf-8")
        mount = f"/var/lib/foundation-platform/parcel-editions}}:{Path(declared['container_path']).parent.as_posix()}:ro"
        self.assertEqual(Path(declared["path"]).parent.as_posix(), "/var/lib/foundation-platform/parcel-editions")
        self.assertEqual(compose.count(mount), 2, "both Spark services mount the host ledger")


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
