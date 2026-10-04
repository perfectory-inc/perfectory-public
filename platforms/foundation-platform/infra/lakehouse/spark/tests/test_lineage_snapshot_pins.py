"""A lineage derivation binds its actual inputs before starting Spark."""
import copy
import hashlib
import io
import json
import sys
import types
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))
import parcel_lineage_to_silver as job


class LineageInputTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "inputs.json"
        self.sources = {
            "boundaries_from": {"table": "silver.parcel_boundaries", "snapshot_id": "11"},
            "boundaries_to": {"table": "staging.parcel_boundaries_september", "snapshot_id": "22"},
            "code_changes": {"table": "reference.legal_dong_code_change", "snapshot_id": "33"},
            "history": {"table": "silver.land_transfer_history", "snapshot_id": "44"},
        }
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.sources), encoding="utf-8")

    def argv(self, *extra):
        return [
            "--from-snapshot-id", "vworldkr__parcel-209906", "--to-snapshot-id", "vworldkr__parcel-209909",
            "--from-date", "2099-06-01", "--to-date", "2099-09-01",
            "--from-sido", "99", "--to-sido", "99", "--iceberg-table", "lineage_smoke",
            "--source-snapshots-path", str(self.path), *extra,
        ]

    def load(self, *extra):
        with mock.patch.object(job, "assert_catalog_env"):
            args = job.parse_args(self.argv(*extra))
            return args, job.validate_args(args)

    def identity(self, args, inputs):
        return job.derivation_run_id(job.input_provenance(args, inputs))

    def test_every_source_has_an_exact_physical_table_and_snapshot(self):
        args, inputs = self.load()
        document = job.input_provenance(args, inputs)
        self.assertEqual(document["iceberg_inputs"], {
            role: {"table": "lakehouse." + binding["table"], "snapshot_id": binding["snapshot_id"]}
            for role, binding in self.sources.items()
        })
        self.assertNotIn("code_snapshot_date", document, "the code change table snapshot is pinned like every input")

    def test_missing_extra_or_ambiguous_bindings_are_rejected(self):
        malformed = [[], {}, {**self.sources, "unused": self.sources["code_changes"]}]
        for role in self.sources:
            incomplete = copy.deepcopy(self.sources)
            del incomplete[role]
            malformed.append(incomplete)
        for value in [None, "11", {}, {"table": "silver.t"},
                      {"table": "silver.t", "snapshot_id": "1", "unused": 1}]:
            malformed.append({**self.sources, "history": value})
        for document in malformed:
            with self.subTest(document=document):
                self.path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.load()

    def test_shared_document_bounds_and_duplicate_key_rejection_apply(self):
        for document in [
            '{"history": {}, "history": {}}',
            json.dumps(self.sources).replace('"snapshot_id": "11"', '"snapshot_id": "11", "snapshot_id": "12"'),
            " " * (64 * 1024 + 1),
        ]:
            with self.subTest(document=document[:80]):
                self.path.write_text(document, encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.load()

    def test_invalid_snapshot_ids_and_physical_identifiers_are_rejected(self):
        for value in [None, True, 1.0, "0", "01", "-1", str(2**63), "1 OR true"]:
            with self.subTest(snapshot=value):
                self.sources["code_changes"]["snapshot_id"] = value
                self.save()
                with self.assertRaises(ValueError):
                    self.load()
        self.sources["code_changes"]["snapshot_id"] = "33"
        for table in [None, "t", "other.silver.t", "silver.t where true", "silver.t;DROP", "silver.`t`"]:
            with self.subTest(table=table):
                self.sources["code_changes"]["table"] = table
                self.save()
                with self.assertRaises(ValueError):
                    self.load()

    def test_impossible_dates_and_aliases_of_one_vintage_fail_before_spark(self):
        cases = [
            ("--from-date", "2099-02-29"), ("--to-date", "2099-13-01"),
            ("--to-date", "2099-06-01"),
            ("--to-snapshot-id", "vworldkr__parcel-209906"),
            ("--to-snapshot-id", "vworldkr__parcel:209906"),
        ]
        for extra in cases:
            with self.subTest(extra=extra), mock.patch.object(job, "assert_catalog_env"):
                with mock.patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
                    with self.assertRaises(ValueError):
                        job.main(self.argv(*extra))

    def test_optional_buildings_require_both_logical_ids_and_both_pins(self):
        building_flags = ("--building-from-snapshot-id", "building-june", "--building-to-snapshot-id", "building-september")
        with self.assertRaisesRegex(ValueError, "go together"):
            self.load(*building_flags[:2])
        with self.assertRaises(ValueError):
            self.load(*building_flags)
        self.sources.update({
            "buildings_from": {"table": "silver.building_register_titles", "snapshot_id": "55"},
            "buildings_to": {"table": "staging.building_register_titles", "snapshot_id": "66"},
        })
        self.save()
        self.assertEqual(len(self.load(*building_flags)[1].sources), 6)
        with self.assertRaises(ValueError):
            self.load()

    def test_provenance_changes_with_every_semantic_selection(self):
        args, inputs = self.load()
        original = self.identity(args, inputs)
        for attribute, value in [
            ("from_date", "2099-05-01"), ("to_date", "2099-10-01"),
            ("from_snapshot_id", "other-june"), ("to_snapshot_id", "other-september"),
            ("from_sido", "98"), ("to_sido", "98"), ("iceberg_catalog_name", "other"),
        ]:
            with self.subTest(attribute=attribute):
                changed = copy.copy(args)
                setattr(changed, attribute, value)
                self.assertNotEqual(original, self.identity(changed, inputs))
        with mock.patch.object(job.pl, "RULES_VERSION", "future-rules"):
            self.assertNotEqual(original, self.identity(args, inputs))
        for role in self.sources:
            for key, value in [("snapshot_id", "99"), ("table", "staging.other")]:
                with self.subTest(role=role, key=key):
                    saved = self.sources[role][key]
                    self.sources[role][key] = value
                    self.save()
                    self.assertNotEqual(original, self.identity(*self.load()))
                    self.sources[role][key] = saved
        self.save()

    def test_region_order_and_numeric_pin_spelling_do_not_change_identity(self):
        args, inputs = self.load("--from-sido", "99,98", "--to-sido", "99,98")
        original = self.identity(args, inputs)
        for binding in self.sources.values():
            binding["snapshot_id"] = int(binding["snapshot_id"])
        self.save()
        self.assertEqual(original, self.identity(*self.load("--from-sido", "98,99,99", "--to-sido", "98,99")))

    def test_ownership_bytes_are_read_once_and_match_the_hashed_evidence(self):
        path = Path(self.directory.name) / "ownership.jsonl"
        raw = b'{"pnu":"9999910100100010000","owner_kind":"01"}\n'
        path.write_bytes(raw)
        args, inputs = self.load("--ownership-old-jsonl", str(path))
        original = self.identity(args, inputs)
        self.assertEqual(job.input_provenance(args, inputs)["ownership_sha256"]["old"], hashlib.sha256(raw).hexdigest())
        path.write_bytes(raw.replace(b'"01"', b'"02"'))
        self.path.write_text("not JSON", encoding="utf-8")
        self.assertEqual(inputs.ownership_old.rows["9999910100100010000"]["owner_kind"], "01")
        self.assertEqual(original, self.identity(args, inputs))
        self.save()
        self.assertNotEqual(original, self.identity(*self.load("--ownership-old-jsonl", str(path))))

    def test_every_view_is_loaded_once_through_the_shared_pinned_reader(self):
        from parcel_lineage_inputs import bind_input_views

        args, inputs = self.load()
        spark = mock.Mock()
        views = bind_input_views(spark, args.iceberg_catalog_name, inputs)
        self.assertEqual(set(views), set(self.sources))
        self.assertEqual(spark.read.format.call_args_list, [mock.call("iceberg")] * 4)
        reader = spark.read.format.return_value
        self.assertEqual(reader.option.call_args_list, [mock.call("snapshot-id", v["snapshot_id"]) for v in self.sources.values()])
        self.assertEqual(reader.option.return_value.load.call_args_list, [mock.call("lakehouse." + v["table"]) for v in self.sources.values()])

    def test_every_sql_reader_uses_its_supplied_pinned_relation(self):
        spark = mock.Mock()
        spark.sql.return_value.collect.return_value = []
        job.read_pnus(spark, "unused", "old", "99", table="`pinned_before`")
        job.read_code_changes(spark, "`pinned_codes`")
        job.read_window_events(spark, "`pinned_history`", "99", "2099-06-01", "2099-09-01")
        job.read_facts_before(spark, "`pinned_history`", "2099-06-01", {"9999910100100010000"})
        job.read_buildings(spark, "`pinned_buildings`", "old", "99")
        for call, relation in zip(spark.sql.call_args_list, [
            "pinned_before", "pinned_codes", "pinned_history", "pinned_history", "pinned_buildings",
        ]):
            self.assertIn(f"FROM `{relation}`", call.args[0])
            self.assertNotIn(".silver.", call.args[0])
            self.assertNotIn(".reference.", call.args[0])

    def test_main_reuses_the_plan_after_files_change_and_reports_consumed_inputs(self):
        for buildings in [False, True]:
            with self.subTest(buildings=buildings):
                flags = []
                if buildings:
                    self.sources.update({
                        "buildings_from": {"table": "silver.building_register_titles", "snapshot_id": "55"},
                        "buildings_to": {"table": "staging.building_register_titles", "snapshot_id": "66"},
                    })
                    flags = ["--building-from-snapshot-id", "old-building", "--building-to-snapshot-id", "new-building"]
                self.save()
                ownership = Path(self.directory.name) / "ownership.jsonl"
                ownership.write_text('{"pnu":"9999910100100010000","owner_kind":"01"}\n', encoding="utf-8")
                flags += ["--ownership-old-jsonl", str(ownership)]
                args, inputs = self.load(*flags)
                expected = job.input_provenance(args, inputs)
                spark, builder = mock.Mock(), mock.Mock()
                builder.appName.return_value = builder
                builder.config.return_value = builder
                spark.sql.return_value.collect.return_value = []

                def start():
                    self.path.write_text("invalid changed file", encoding="utf-8")
                    ownership.write_text("invalid changed file", encoding="utf-8")
                    return spark

                def derive(*values):
                    values[6]({"9999910100100010000"})  # Lazy history read uses the same pinned view.
                    self.assertEqual(values[9]["9999910100100010000"]["owner_kind"], "01")
                    return [], {}

                builder.getOrCreate.side_effect = start
                module = types.ModuleType("pyspark.sql")
                module.SparkSession = types.SimpleNamespace(builder=builder)
                with ExitStack() as stack:
                    stack.enter_context(mock.patch.dict(sys.modules, {"pyspark": types.ModuleType("pyspark"), "pyspark.sql": module}))
                    stack.enter_context(mock.patch.object(job, "assert_catalog_env"))
                    stack.enter_context(mock.patch.object(job, "apply_catalog_settings", return_value=builder))
                    pnus = stack.enter_context(mock.patch.object(job, "read_pnus", return_value={"9999910100100010000"}))
                    codes = stack.enter_context(mock.patch.object(job, "read_code_changes", return_value=[]))
                    events = stack.enter_context(mock.patch.object(job, "read_window_events", return_value=set()))
                    titles = stack.enter_context(mock.patch.object(job, "read_buildings", return_value={}))
                    stack.enter_context(mock.patch.object(job, "derive", side_effect=derive))
                    stack.enter_context(mock.patch.object(job, "evolve_iceberg_table_to_contract"))
                    stack.enter_context(mock.patch.object(job, "append_batch_once", return_value={"appended": True}))
                    output = io.StringIO()
                    stack.enter_context(redirect_stdout(output))
                    self.assertEqual(job.main(self.argv(*flags)), 0)
                summary = json.loads(output.getvalue().split("parcel-lineage-summary-json ")[1])
                self.assertEqual(summary["input_provenance"], expected)
                self.assertEqual(summary["derivation_run_id"], job.derivation_run_id(expected))
                self.assertEqual([c.kwargs["table"] for c in pnus.call_args_list], [
                    "`_parcel_lineage_boundaries_from`", "`_parcel_lineage_boundaries_to`",
                ])
                self.assertEqual(codes.call_args.args[1], "`_parcel_lineage_code_changes`")
                self.assertEqual(events.call_args.args[1], "`_parcel_lineage_history`")
                self.assertIn("FROM `_parcel_lineage_history` h", spark.sql.call_args_list[0].args[0])
                self.assertEqual(titles.call_count, 2 if buildings else 0)
                if buildings:
                    self.assertEqual([c.args[1] for c in titles.call_args_list], [
                        "`_parcel_lineage_buildings_from`", "`_parcel_lineage_buildings_to`",
                    ])
                spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
