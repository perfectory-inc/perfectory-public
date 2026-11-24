"""The court auction Silver load runs as Dagster assets with no host under it
(orchestration/dagster/court_auction_lane.py, the reference lane of root ADR-0616).

The publisher is replaced by a script that answers its two commands as the real one does: a plan file
per (table, source), then the load's Pipes lines and summary. Everything after it is Dagster's own.
Run by the CI `dagster-observe` lane in the code location's image; skipped elsewhere, where a skip
fails that lane.
"""

import importlib.util
import json
import os
import pathlib
import re
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "orchestration" / "dags"))
sys.path.insert(0, str(PLATFORM / "orchestration" / "dagster"))

HAS_DAGSTER = importlib.util.find_spec("dagster") is not None
NEEDS_DAGSTER = "requires dagster (the CI dagster-observe lane runs it)"
LANE = PLATFORM / "orchestration" / "dagster" / "court_auction_lane.py"
WRAPPER = PLATFORM / "scripts" / "ops" / "court-auction-silver-load.sh"


def pipes_line(method, params):
    return json.dumps({"__dagster_pipes_version": "0.1", "method": method, "params": params})


class NoHostTest(unittest.TestCase):
    """What ADR-0616 asks of a lane: it runs anywhere its process environment is set."""

    def test_the_lane_names_no_host_path_unit_or_journal(self):
        text = LANE.read_text(encoding="utf-8")
        code = "\n".join(line for line in text.split('"""')[2].splitlines() if not line.lstrip().startswith("#"))
        for forbidden in ("/var/lib", "/opt/", "/etc/", "/data/", "systemctl", "journal", ".service", "ssh"):
            self.assertNotIn(forbidden, code, f"the lane's code names {forbidden}")

    def test_the_plans_are_the_wrappers(self):
        # Until the wrapper is deleted the two must ask for the same (table, source) pairs.
        wrapper = WRAPPER.read_text(encoding="utf-8")
        planned = re.findall(r'plan (silver\.[a-z_]+) "\$\{([a-z]+)_slug\}" "\$\{work\}/duckdb/([a-z]+)"', wrapper)
        self.assertEqual(len(planned), 14)
        # The lane module imports Dagster only inside its functions that need it; without Dagster
        # its plan list is still read through the same contract by the stub modules below.
        if not HAS_DAGSTER:
            self.skipTest("the plan list is compared where the module imports (the dagster-observe lane)")
        import court_auction_lane as lane

        self.assertEqual([(table, name) for name, table, _source in lane.plans()],
                         [(table, name) for table, _slug, name in planned])
        contract = json.loads(lane.CONTRACT.read_text(encoding="utf-8"))
        slugs = {"item": contract["item_search"], "result": contract["sale_result_search"],
                 "detail": contract["item_detail"], "code": contract["common_codes"],
                 "report": contract["appraisal_reports"], "spec": contract["sale_specifications"],
                 "notice": contract["sale_notices"], "dividend": contract["dividend_notices"],
                 **dict(zip(("case", "survey", "appraisal", "filing", "tenant"), contract["case_documents"]["documents"]))}
        self.assertEqual([source for _name, _table, source in lane.plans()],
                         [lane.SOURCE_PREFIX + slugs[slug]["dataset_slug"] for _table, slug, _name in planned])


@unittest.skipUnless(HAS_DAGSTER, NEEDS_DAGSTER)
class SilverLaneTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)

    def publisher(self, objects, late=0, load_lines=(), load_status=0):
        """A stand-in for the publisher binary: `plan-silver-load-objects` writes the plan the
        environment asks for (`objects` per plan, `late` of them past the window in the first), the
        load prints `load_lines` and exits `load_status`. Every call is recorded."""
        script = pathlib.Path(self.scratch.name) / "publisher.py"
        calls = pathlib.Path(self.scratch.name) / "calls.jsonl"
        script.write_text(textwrap.dedent(f"""
            import json, os, pathlib, sys
            command = sys.argv[1]
            with open({str(calls)!r}, "a", encoding="utf-8") as handle:
                handle.write(json.dumps({{"command": command, "env": {{k: v for k, v in os.environ.items()
                                                                   if k.startswith("FOUNDATION_PLATFORM_")}}}}) + "\\n")
            if command == "plan-silver-load-objects":
                out = pathlib.Path(os.environ["FOUNDATION_PLATFORM_SILVER_LOAD_OUTPUT_DIR"])
                out.mkdir(parents=True, exist_ok=True)
                first = out.name == "items"
                plan = {{"objects": [f"bronze/synthetic/{{out.name}}-{{i}}.gz" for i in range({objects})],
                        "selection": {{"late": ["bronze/synthetic/late.gz"] * ({late} if first else 0)}}}}
                (out / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
                print("silver-load-plan " + json.dumps({{"owed": {objects}}}))
            elif command == "load-courtauction-go-kr-silver":
                assert pathlib.Path(os.environ["FOUNDATION_PLATFORM_COURT_AUCTION_PLAN_ROOT"], "items", "plan.json").exists()
                for line in {list(load_lines)!r}:
                    print(line, flush=True)
                sys.exit({load_status})
            else:
                sys.exit(64)
        """), encoding="utf-8")
        launcher = pathlib.Path(self.scratch.name) / ("publisher.cmd" if os.name == "nt" else "publisher")
        if os.name == "nt":
            launcher.write_text(f'@"{sys.executable}" "{script}" %*\r\n', encoding="utf-8")
        else:
            launcher.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8")
            launcher.chmod(0o755)
        return str(launcher), calls

    def materialized(self, program):
        import court_auction_lane as lane
        from dagster import DagsterInstance, materialize

        instance = DagsterInstance.ephemeral()
        with mock.patch.dict(os.environ, {lane.PUBLISHER_ENV: program}):
            result = materialize([lane.silver()], instance=instance, raise_on_error=False)
        return lane, instance, result

    def calls(self, path):
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

    def test_a_load_materializes_every_table_and_logs_the_checks_it_reported(self):
        import court_auction_lane as lane

        item = "iceberg/silver/court_auction_item"
        program, calls = self.publisher(objects=2, load_lines=[
            "loading",
            pipes_line("report_asset_check", {"asset_key": item, "check_name": "silver_load_rows", "passed": True,
                                              "metadata": {"rows": {"raw_value": 7, "type": "__infer__"}}}),
            pipes_line("report_asset_check", {"asset_key": item, "check_name": "silver_load_key_unique", "passed": False,
                                              "metadata": {}}),
            lane.SUMMARY_PREFIX + json.dumps({"tables": 1}),
        ])
        lane, instance, result = self.materialized(program)
        self.assertTrue(result.success)
        keys = lane.output_keys()
        self.assertEqual(len(keys), 28)
        self.assertEqual({event.asset_key for event in result.get_asset_materialization_events()}, set(keys))
        evaluations = {(e.asset_key.to_user_string(), e.check_name): e.passed for e in result.get_asset_check_evaluations()}
        self.assertEqual(evaluations, {(item, "silver_load_rows"): True, (item, "silver_load_key_unique"): False})
        made = self.calls(calls)
        self.assertEqual([call["command"] for call in made], [lane.PLAN_COMMAND] * 14 + [lane.LOAD_COMMAND])
        self.assertEqual([(call["env"]["FOUNDATION_PLATFORM_SILVER_LOAD_TABLE"],
                           call["env"]["FOUNDATION_PLATFORM_SILVER_LOAD_SOURCE_SLUGS"]) for call in made[:14]],
                         [(table, source) for _name, table, source in lane.plans()])
        self.assertEqual({call["env"]["FOUNDATION_PLATFORM_SILVER_LOAD_WINDOW_DAYS"] for call in made[:14]},
                         {str(lane.window()[0])})

    def test_nothing_owed_writes_no_table_and_does_not_start_the_load(self):
        program, calls = self.publisher(objects=0)
        lane, _instance, result = self.materialized(program)
        self.assertTrue(result.success)
        self.assertEqual(result.get_asset_materialization_events(), [])
        self.assertNotIn(lane.LOAD_COMMAND, [call["command"] for call in self.calls(calls)])

    def test_an_object_that_waited_past_the_window_is_loaded_and_the_run_fails(self):
        program, calls = self.publisher(objects=1, late=1, load_lines=["loaded"])
        lane, _instance, result = self.materialized(program)
        self.assertFalse(result.success)
        self.assertEqual(len(result.get_asset_materialization_events()), 28, "the late object is loaded all the same")
        self.assertIn(lane.LOAD_COMMAND, [call["command"] for call in self.calls(calls)])

    def test_a_load_that_exits_non_zero_fails_and_materializes_nothing(self):
        program, _calls = self.publisher(objects=1, load_lines=["Error: DuckDB failed to compare the editions"], load_status=1)
        _lane, _instance, result = self.materialized(program)
        self.assertFalse(result.success)
        self.assertEqual(result.get_asset_materialization_events(), [])

    def test_a_report_on_a_table_the_load_does_not_write_is_refused(self):
        program, _calls = self.publisher(objects=1, load_lines=[
            pipes_line("report_asset_check", {"asset_key": "iceberg/gold/parcel_panel", "check_name": "silver_load_rows",
                                              "passed": True, "metadata": {}})])
        _lane, _instance, result = self.materialized(program)
        self.assertFalse(result.success)

    def test_the_lane_is_its_own_definitions_and_the_code_location_does_not_load_it(self):
        import court_auction_lane as lane

        defs = lane.lane_definitions()
        defs.resolve_asset_graph()
        self.assertNotIn("court_auction_lane", (PLATFORM / "orchestration" / "dagster" / "definitions.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
