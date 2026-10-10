"""When a panel Gold table is rebuilt (root ADR-0139): the plan, the row-loss refusal, the write.

Snapshot ids are synthetic. Each refusal is planted and shown refused; each "nothing to do" is the
case that would otherwise start a multi-day re-bake for no change.
"""
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # noqa: F401  Import-only PySpark stub.
import gold_rebuild as rebuild  # noqa: E402
import building_panel_silver_to_gold as building  # noqa: E402
import parcel_panel_silver_to_gold as parcel  # noqa: E402
import gold_incremental as incremental  # noqa: E402

ENTRY = {"producer": "fixture", "producer_arguments": [], "max_row_loss_fraction": 0.01}
INPUTS = (("silver.a", "silver.b"), {}, "silver.a")


def snap(snapshot_id, parent, at, operation="append", added=1, deleted=0, **summary):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": at, "operation": operation,
            "summary": {"added-records": str(added), "deleted-records": str(deleted), **summary}}


def table(*snapshots):
    return {"head": snapshots[-1]["snapshot_id"], "snapshots": list(snapshots)}


def gold(pins=None, rows=1000, published="2026-01-10T00:00:00Z"):
    summary = {"total-records": str(rows)}
    if pins is not None:
        summary[rebuild.SOURCE_SNAPSHOTS_PROPERTY] = json.dumps(pins)
    return {**table(snap("90", None, "2026-01-10T01:00:00Z", "overwrite", rows, **summary)),
            "row_count": rows, "published_at_utc": published}


A1 = snap("11", None, "2026-01-01T00:00:00Z")
B1 = snap("21", None, "2026-01-01T00:00:00Z")


class Plan(unittest.TestCase):
    def test_no_newer_silver_is_nothing_to_do(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold({"silver.a": "11", "silver.b": "21"}),
                                {"silver.a": table(A1), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])

    def test_a_newer_silver_snapshot_that_changed_rows_rebuilds_at_the_current_heads(self):
        a2 = snap("12", "11", "2026-01-20T00:00:00Z", "overwrite", added=5, deleted=4)
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold({"silver.a": "11", "silver.b": "21"}),
                                {"silver.a": table(A1, a2), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "rebuild")
        self.assertIn("silver.a", decision["reasons"][0])
        self.assertEqual(decision["source_snapshots"], {"silver.a": "12", "silver.b": "21"})
        self.assertEqual(decision["minimum_row_count"], 990)

    def test_compaction_empty_commits_and_an_expired_pin_change_nothing(self):
        # Maintenance compacts (replace), then expires the pin: the kept history still names it
        # as the parent of the compaction. A schema-only commit adds no rows.
        a2 = snap("12", "11", "2026-01-20T00:00:00Z", "replace", added=7, deleted=7)
        a3 = snap("13", "12", "2026-01-21T00:00:00Z", "append", added=0)
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold({"silver.a": "11", "silver.b": "21"}),
                                {"silver.a": table(a2, a3), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])

    def test_a_pin_the_kept_history_cannot_reach_counts_as_a_change(self):
        a5 = snap("15", "14", "2026-01-25T00:00:00Z", "replace")
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, gold({"silver.a": "11", "silver.b": "21"}),
                                {"silver.a": table(a5), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "rebuild")
        self.assertIn("not in the kept history", decision["reasons"][0])

    def test_a_backfill_snapshot_without_pins_is_read_through_to_the_build_that_has_them(self):
        built = gold({"silver.a": "11", "silver.b": "21"})
        backfill = snap("91", "90", "2026-02-01T00:00:00Z", "overwrite", 1000, deleted=1000,
                        **{"total-records": "1000"})
        built["snapshots"].append(backfill)
        built["head"] = "91"
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, built,
                                {"silver.a": table(A1), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "nothing_to_do", decision["reasons"])

    def test_a_gold_without_any_record_falls_back_to_when_its_rows_were_published(self):
        a2 = snap("12", "11", "2026-01-05T00:00:00Z", "overwrite", added=3)
        silver = {"silver.a": table(A1, a2), "silver.b": table(B1)}
        older = rebuild.plan("gold.x", ENTRY, INPUTS, gold(published="2026-01-10T00:00:00Z"), silver)
        self.assertEqual(older["action"], "nothing_to_do", older["reasons"])
        newer = rebuild.plan("gold.x", ENTRY, INPUTS, gold(published="2026-01-04T23:59:59Z"), silver)
        self.assertEqual(newer["action"], "rebuild")
        # Fractional commit times compare as times, not strings ('Z' sorts after '.').
        same_second = rebuild.plan("gold.x", ENTRY, INPUTS, gold(published="2026-01-05T00:00:00Z"),
                                   {"silver.a": table(A1, snap("12", "11", "2026-01-05T00:00:00.500000Z")),
                                    "silver.b": table(B1)})
        self.assertEqual(same_second["action"], "rebuild")

    def test_an_enabled_job_refuses_to_plan_from_publish_time(self):
        # published_at_utc is stamped when the producer started, so it can miss a change. The
        # fallback serves the supervised first run only; once the job is on, a Gold without pins
        # needs an unconditional rebuild.
        silver = {"silver.a": table(A1), "silver.b": table(B1)}
        first_run = rebuild.plan("gold.x", ENTRY, INPUTS, gold(), silver)
        self.assertEqual(first_run["action"], "nothing_to_do", first_run["reasons"])
        with self.assertRaisesRegex(rebuild.PlanError, "records no source pins.*--unconditional"):
            rebuild.plan("gold.x", ENTRY, INPUTS, gold(), silver, time_fallback=False)
        # A Gold that records its pins plans the same way either way.
        pinned = rebuild.plan("gold.x", ENTRY, INPUTS, gold({"silver.a": "11", "silver.b": "21"}), silver,
                              time_fallback=False)
        self.assertEqual(pinned["action"], "nothing_to_do")

    def test_an_unconditional_rebuild_states_its_reason_and_keeps_the_row_floor(self):
        silver = {"silver.a": table(A1), "silver.b": table(B1)}
        for current in (gold(), gold({"silver.a": "11", "silver.b": "21"})):
            decision = rebuild.plan("gold.x", ENTRY, INPUTS, current, silver,
                                    unconditional="first supervised run", time_fallback=False)
            self.assertEqual(decision["action"], "rebuild")
            self.assertEqual(decision["reasons"], ["unconditional rebuild: first supervised run"])
            self.assertEqual(decision["source_snapshots"], {"silver.a": "11", "silver.b": "21"})
            self.assertEqual(decision["minimum_row_count"], 990)
        with self.assertRaisesRegex(rebuild.PlanError, "needs its reason"):
            rebuild.plan("gold.x", ENTRY, INPUTS, gold(), silver, unconditional="  ")
        args = rebuild.parse_args(["--gold-table", "gold.x", "--plan-output", "p", "--pins-output", "q",
                                   "--unconditional-reason", "why", "--no-time-fallback"])
        self.assertEqual((args.unconditional_reason, args.time_fallback), ("why", False))
        defaults = rebuild.parse_args(["--gold-table", "gold.x", "--plan-output", "p", "--pins-output", "q"])
        self.assertEqual((defaults.unconditional_reason, defaults.time_fallback), (None, True))

    def test_an_unmeasured_input_is_refused_once_it_exists_except_by_a_dry_run(self):
        entry = {**ENTRY, "unmeasured_inputs": {"silver.lineage": "never measured"}}
        inputs = (("silver.a",), {"silver.lineage": "--no-carry-lineage"}, "silver.a")
        absent = rebuild.plan("gold.x", entry, inputs, gold({"silver.a": "11"}),
                              {"silver.a": table(A1), "silver.lineage": None})
        self.assertEqual(absent["action"], "nothing_to_do")
        present = {"silver.a": table(A1), "silver.lineage": table(snap("31", None, "2026-01-02T00:00:00Z"))}
        for unconditional in (None, "first supervised run"):
            with self.subTest(unconditional=unconditional), \
                    self.assertRaisesRegex(rebuild.PlanError, "silver.lineage.*never measured.*unmeasured_inputs"):
                rebuild.plan("gold.x", entry, inputs, gold({"silver.a": "11"}), present, unconditional=unconditional)
        measured = rebuild.plan("gold.x", entry, inputs, gold({"silver.a": "11"}), present, measuring=True)
        self.assertEqual(measured["action"], "rebuild")
        self.assertEqual(measured["producer_arguments"], [])
        self.assertTrue(rebuild.parse_args(["--gold-table", "g", "--plan-output", "p", "--pins-output", "q",
                                            "--measuring"]).measuring)

    def test_an_optional_input_is_pinned_when_it_exists_and_flagged_off_when_it_does_not(self):
        inputs = (("silver.a",), {"silver.lineage": "--no-carry-lineage"}, "silver.a")
        absent = rebuild.plan("gold.x", ENTRY, inputs, gold({"silver.a": "11"}),
                              {"silver.a": table(A1), "silver.lineage": None})
        self.assertEqual(absent["action"], "nothing_to_do")
        self.assertEqual(absent["producer_arguments"], ["--no-carry-lineage"])
        appeared = rebuild.plan("gold.x", ENTRY, inputs, gold({"silver.a": "11"}),
                                {"silver.a": table(A1), "silver.lineage": table(snap("31", None, "2026-01-02T00:00:00Z"))})
        self.assertEqual(appeared["action"], "rebuild")
        self.assertEqual(appeared["source_snapshots"], {"silver.a": "11", "silver.lineage": "31"})
        self.assertEqual(appeared["producer_arguments"], [])

    def test_a_missing_required_input_or_a_vanished_pinned_input_is_refused(self):
        with self.assertRaisesRegex(rebuild.PlanError, "required Silver inputs do not exist"):
            rebuild.plan("gold.x", ENTRY, INPUTS, gold(), {"silver.a": table(A1), "silver.b": None})
        inputs = (("silver.a",), {"silver.lineage": "--no-carry-lineage"}, "silver.a")
        with self.assertRaisesRegex(rebuild.PlanError, "no longer exist"):
            rebuild.plan("gold.x", ENTRY, inputs, gold({"silver.a": "11", "silver.lineage": "31"}),
                         {"silver.a": table(A1), "silver.lineage": None})

    def test_a_table_without_gold_is_built_with_no_row_floor(self):
        decision = rebuild.plan("gold.x", ENTRY, INPUTS, None, {"silver.a": table(A1), "silver.b": table(B1)})
        self.assertEqual(decision["action"], "rebuild")
        self.assertIsNone(decision["minimum_row_count"])


class RowLoss(unittest.TestCase):
    def test_row_loss_beyond_the_tolerance_is_refused_and_within_it_is_not(self):
        minimum = rebuild.plan("gold.x", ENTRY, INPUTS, gold(rows=1000),
                               {"silver.a": table(A1), "silver.b": table(B1)})["minimum_row_count"]
        rebuild.assert_minimum_row_count(990, minimum)
        rebuild.assert_minimum_row_count(5000, minimum)
        with self.assertRaisesRegex(ValueError, "nothing was written"):
            rebuild.assert_minimum_row_count(989, minimum)

    def test_both_producers_refuse_before_writing(self):
        """The refusal sits in the checked build both producers run before they write.

        `build_or_merge` returns a full or an incremental outcome only after the row floor; each
        producer's `run` calls it before it commits anything (root ADR-0139, ADR-0180).
        """
        for module, write in ((building, "outcome.commit("), (parcel, "write_gold_iceberg(spark")):
            source = Path(module.__file__).read_text(encoding="utf-8")
            run = source[source.index("def run("):]
            self.assertLess(run.index("incremental.build_or_merge("), run.index(write), module.JOB_NAME)
            self.assertIn("assert_minimum_row_count)", run[:run.index(write)], module.JOB_NAME)
        source = Path(incremental.__file__).read_text(encoding="utf-8")
        body = source[source.index("def build_or_merge("):source.index("def pins_seed(")]
        returns = [i for i in range(len(body)) if body.startswith("return Outcome(", i)]
        self.assertEqual(len(returns), 2, "one full and one incremental outcome")
        for at in returns:
            self.assertIn("assert_minimum(", body[:at].rsplit("return Outcome(", 1)[-1])


class Write(unittest.TestCase):
    def test_the_commit_records_the_pins_and_overwrites_everything_in_one_snapshot(self):
        frame = mock.MagicMock()
        rebuild.write_gold_snapshot(frame, "`r2`.`gold`.`t`", "overwrite", {"silver.b": "2", "silver.a": "1"}, "ALL")
        writer = frame.writeTo.return_value
        writer.option.assert_called_once_with(rebuild.SNAPSHOT_PROPERTY_PREFIX + rebuild.SOURCE_SNAPSHOTS_PROPERTY,
                                              '{"silver.a":"1","silver.b":"2"}')
        writer.option.return_value.overwrite.assert_called_once_with("ALL")
        rebuild.write_gold_snapshot(frame, "t", "append", {"silver.a": "1"}, "ALL")
        writer.option.return_value.append.assert_called_once_with()
        with self.assertRaises(ValueError):
            rebuild.write_gold_snapshot(frame, "t", "merge", {}, "ALL")


class Contract(unittest.TestCase):
    def test_the_contract_names_each_producer_and_its_inputs_come_from_the_producer(self):
        contract = rebuild.load_contract()
        self.assertEqual(set(contract["tables"]), {"gold.parcel_panel", "gold.building_panel"})
        required, optional, anchor = rebuild.producer_inputs(parcel)
        self.assertEqual(required, parcel.ALL_SOURCES)
        self.assertEqual(optional, {parcel.LINEAGE_SOURCE: "--no-carry-lineage"})
        self.assertEqual(anchor, parcel.PARCEL_SOURCE)
        self.assertEqual(rebuild.producer_inputs(building), (building.ALL_SOURCES, {}, building.TITLE_SOURCE))
        # Every unmeasured input is one the producer actually reads.
        for name, entry in contract["tables"].items():
            producer = parcel if name == "gold.parcel_panel" else building
            required, optional, _ = rebuild.producer_inputs(producer)
            self.assertLessEqual(set(entry.get("unmeasured_inputs", {})), {*required, *optional})

    def test_a_contract_without_a_tolerance_or_its_reason_is_refused(self):
        import tempfile
        good = json.loads(rebuild.CONTRACT_PATH.read_text(encoding="utf-8"))
        for field, value in (("max_row_loss_fraction", 1), ("max_row_loss_fraction", "0.01"),
                             ("max_row_loss_reason", ""), ("producer_arguments", ["x"]),
                             ("unmeasured_inputs", {"silver.x": ""}), ("unmeasured_inputs", ["silver.x"])):
            bad = json.loads(json.dumps(good))
            bad["tables"]["gold.parcel_panel"][field] = value
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "c.json"
                path.write_text(json.dumps(bad), encoding="utf-8")
                with self.assertRaises(rebuild.PlanError):
                    rebuild.load_contract(path)


if __name__ == "__main__":
    unittest.main()
