"""The incremental panel Gold's decisions without Spark (root ADR-0180).

The planner's choice between merging the changed PNUs and the full rebuild, the producer's
change-fraction refusal, the sample bound, the MERGE statement, and the producers' declared build
columns. The Spark proof that a merge equals the full rebuild is test_gold_incremental_iceberg.py.
Snapshot ids are synthetic.
"""
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import test_industrial_complex_gold_schema_evolution  # noqa: F401  Import-only PySpark stub.
import gold_incremental as incremental  # noqa: E402
import gold_rebuild as rebuild  # noqa: E402
import building_panel_silver_to_gold as building  # noqa: E402
import parcel_panel_silver_to_gold as parcel  # noqa: E402
from building_link_evidence import evidence_policy  # noqa: E402
from platform_contracts import column_names, load_lakehouse_contract  # noqa: E402

INCREMENTAL = {"max_changed_key_fraction": 0.3, "parity_sample_keys": 10, "reason": "fixture"}
ENTRY = {"producer": "fixture", "producer_arguments": [], "max_row_loss_fraction": 0.01,
         "incremental": INCREMENTAL}
INPUTS = (("silver.a", "silver.b"), {}, "silver.a")


def snap(snapshot_id, parent, operation="append", added=1, **summary):
    return {"snapshot_id": snapshot_id, "parent_id": parent, "committed_at": "2026-01-01T00:00:00Z",
            "operation": operation, "summary": {"added-records": str(added), **summary}}


def gold(*extra, pins=None):
    pins = {"silver.a": "11", "silver.b": "21"} if pins is None else pins
    built = snap("90", None, "overwrite", 10, **{"total-records": "1000",
                                                 rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps(pins)})
    snapshots = [built, *extra]
    return {"head": snapshots[-1]["snapshot_id"], "snapshots": snapshots, "row_count": 1000}


def silver(a=("11", "12"), b=("21",)):
    def table(ids):
        snapshots = [snap(i, ids[n - 1] if n else None) for n, i in enumerate(ids)]
        return {"head": ids[-1], "snapshots": snapshots}
    return {"silver.a": table(a), "silver.b": table(b)}


class PlanMode(unittest.TestCase):
    def plan(self, gold_state, silver_state, **options):
        return rebuild.plan("gold.x", options.pop("entry", ENTRY), INPUTS, gold_state, silver_state, **options)

    def test_a_changed_input_whose_pin_is_kept_merges_from_the_recorded_pins(self):
        decision = self.plan(gold(), silver())
        self.assertEqual((decision["action"], decision["mode"]), ("rebuild", "incremental"), decision)
        self.assertEqual(decision["previous_source_snapshots"], {"silver.a": "11", "silver.b": "21"})
        self.assertEqual(decision["source_snapshots"], {"silver.a": "12", "silver.b": "21"})
        self.assertEqual((decision["max_changed_key_fraction"], decision["parity_sample_keys"]), (0.3, 10))

    def test_nothing_changed_has_no_mode(self):
        decision = self.plan(gold(), silver(a=("11",)))
        self.assertEqual((decision["action"], decision["mode"]), ("nothing_to_do", "none"))

    def test_each_fallback_is_full_with_its_reason(self):
        cases = {
            "declares no incremental": dict(entry={k: v for k, v in ENTRY.items() if k != "incremental"}),
            "unconditional": dict(unconditional="operator"),
            "no longer kept": dict(silver_state=silver(a=("12",))),  # the pin expired
            "does not map a change there to PNUs": dict(whole_table_inputs=("silver.a",)),
            "not a producer commit": dict(gold_state=gold(snap("91", "90", "overwrite"))),  # a backfill
        }
        for needle, options in cases.items():
            with self.subTest(needle):
                decision = self.plan(options.pop("gold_state", gold()), options.pop("silver_state", silver()),
                                     **options)
                self.assertEqual((decision["action"], decision["mode"]), ("rebuild", "full"), decision)
                self.assertIn(needle, "; ".join(decision["mode_reasons"]))
                self.assertIsNone(decision["previous_source_snapshots"])

    def test_a_compaction_after_the_build_still_merges(self):
        decision = self.plan(gold(snap("91", "90", "replace", 5)), silver())
        self.assertEqual(decision["mode"], "incremental", decision["mode_reasons"])

    def test_a_merge_commit_is_the_next_plans_starting_point(self):
        merged = snap("91", "90", "overwrite", 3, **{rebuild.SOURCE_SNAPSHOTS_PROPERTY: json.dumps(
            {"silver.a": "12", "silver.b": "21"})})
        decision = self.plan(gold(merged), silver(a=("11", "12", "13")))
        self.assertEqual(decision["previous_source_snapshots"], {"silver.a": "12", "silver.b": "21"})

    def test_the_contract_declares_incremental_for_both_tables_with_a_reason(self):
        contract = rebuild.load_contract()
        for name, table in contract["tables"].items():
            self.assertIn("incremental", table, name)
        broken = json.loads(rebuild.CONTRACT_PATH.read_text(encoding="utf-8"))
        broken["tables"]["gold.building_panel"]["incremental"]["max_changed_key_fraction"] = 0
        path = Path(self.id().replace(".", "_") + ".json")
        try:
            path.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(rebuild.PlanError, "max_changed_key_fraction"):
                rebuild.load_contract(path)
        finally:
            path.unlink(missing_ok=True)


class ProducerDecisions(unittest.TestCase):
    def test_changed_inputs_are_the_moved_pins_and_a_new_input_refuses(self):
        self.assertEqual(incremental.changed_inputs({"a": "1", "b": "2"}, {"a": "1", "b": "3"}), ["b"])
        with self.assertRaisesRegex(incremental.IncrementalRefusal, "not built from"):
            incremental.changed_inputs({"a": "1"}, {"a": "1", "b": "3"})

    def test_the_change_fraction_bound(self):
        self.assertIsNone(incremental.change_fraction_verdict(30, 100, 0.3))
        self.assertIn("max_changed_key_fraction", incremental.change_fraction_verdict(31, 100, 0.3))
        self.assertIn("empty", incremental.change_fraction_verdict(0, 0, 0.3))
        with self.assertRaises(ValueError):
            incremental.change_fraction_verdict(1, 100, 0)

    def test_the_sample_bound_takes_about_the_asked_share(self):
        self.assertEqual(incremental.sample_threshold(0, 100), 0)
        self.assertEqual(incremental.sample_threshold(100, 100), incremental.HASH_SPACE)
        self.assertEqual(incremental.sample_threshold(20_000, 5_950_000),
                         -(-incremental.HASH_SPACE * 20_000 // 5_950_000))

    def test_the_merge_deletes_inserts_and_updates_only_a_changed_digest(self):
        sql = incremental.merge_sql("`c`.`gold`.`t`", ["pnu", "body", "row_digest"])
        self.assertIn(f"WHEN MATCHED AND s.`{incremental.DELETE_FLAG}` THEN DELETE", sql)
        self.assertIn("NOT (t.`row_digest` <=> s.`row_digest`)", sql)
        self.assertIn(f"WHEN NOT MATCHED AND NOT s.`{incremental.DELETE_FLAG}` THEN INSERT", sql)
        self.assertNotIn(incremental.DELETE_FLAG, sql.split("INSERT", 1)[1].split("VALUES")[0])

    def test_incremental_arguments_are_refused_where_a_merge_cannot_follow(self):
        path = Path(self.id().replace(".", "_") + ".json")
        path.write_text(json.dumps({building.TITLE_SOURCE: "11"}), encoding="utf-8")
        self.addCleanup(path.unlink)
        base = dict(incremental_from_snapshots=str(path), input_mode="iceberg", write_mode="iceberg",
                    iceberg_write_mode="overwrite", region_prefix=None, pnu_prefix=None,
                    max_changed_key_fraction=0.3, parity_sample_keys=0)
        self.assertEqual(incremental.validate_arguments(SimpleNamespace(**base), building.ALL_SOURCES),
                         {building.TITLE_SOURCE: "11"})
        self.assertIsNone(incremental.validate_arguments(
            SimpleNamespace(**{**base, "incremental_from_snapshots": None}), building.ALL_SOURCES))
        for change, needle in (({"region_prefix": "11"}, "region"), ({"write_mode": "parquet"}, "Iceberg"),
                               ({"max_changed_key_fraction": None}, "fraction"),
                               ({"iceberg_write_mode": "append"}, "overwrite")):
            with self.subTest(change), self.assertRaisesRegex(ValueError, needle):
                incremental.validate_arguments(SimpleNamespace(**{**base, **change}), building.ALL_SOURCES)
        with self.assertRaisesRegex(ValueError, "does not read"):
            incremental.validate_arguments(SimpleNamespace(**base), parcel.ALL_SOURCES)


class BuildColumns(unittest.TestCase):
    def test_every_input_declares_contract_columns_and_no_lineage_column(self):
        lineage = {"source_snapshot_id", "source_record_id", "ingested_at_utc", "bronze_object_key",
                   "source_line_number", "valid_from_utc", "valid_to_utc", "row_checksum_sha256"}
        for job in (building, parcel):
            self.assertEqual(set(job.BUILD_COLUMNS), set(job.ALL_SOURCES), job.JOB_NAME)
            for name, columns in job.BUILD_COLUMNS.items():
                with self.subTest(job=job.JOB_NAME, input=name):
                    self.assertLessEqual(set(columns), set(column_names(load_lakehouse_contract(name))))
                    self.assertFalse(set(columns) & lineage, "a lineage column would make every release a change")

    def test_the_unit_evidence_columns_are_declared(self):
        # verified_building_links fills a missing evidence column with NULL instead of failing.
        self.assertLessEqual(set(evidence_policy()["columns"]), set(building.BUILD_COLUMNS[building.UNIT_SOURCE]))

    def test_whole_table_inputs_are_inputs(self):
        self.assertEqual(parcel.WHOLE_TABLE_INPUTS, (parcel.ZONE_CODE_SOURCE,))
        self.assertEqual(rebuild.producer_whole_table_inputs(parcel), (parcel.ZONE_CODE_SOURCE,))
        self.assertEqual(rebuild.producer_whole_table_inputs(building), ())


if __name__ == "__main__":
    unittest.main()
