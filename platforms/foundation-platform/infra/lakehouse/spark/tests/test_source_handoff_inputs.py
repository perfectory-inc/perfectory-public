import copy
import importlib.util
import json
from pathlib import Path
import unittest

SPARK = Path(__file__).resolve().parents[1]


class SourceHandoffInputsTest(unittest.TestCase):
    def planner(self):
        path = SPARK / "jobs/source_handoff_inputs.py"
        self.assertTrue(path.is_file(), "contract-driven input planner is missing")
        spec = importlib.util.spec_from_file_location("source_handoff_inputs", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.plan_inputs

    def contract(self):
        return json.loads((SPARK.parent / "contracts/hub-building-register-apartment-price-source-objects.json").read_text(encoding="utf-8"))

    def manifest(self, c):
        obj = next(o for o in c["objects"] if o["vintage"] == c["selected_vintage"])
        base = obj["object_key"].rsplit("/", 1)[1][:-4]
        return {"schema_version": 1, "status": "complete", "input_object_key": obj["object_key"],
                "source_snapshot_id": "SYNTHETIC", "vintage": c["selected_vintage"],
                "rows_per_part": 10_000_000, "rows_read": 10_000_003, "rows_emitted": 10_000_002,
                "rejected_rows": 1, "pnu_ok": 10_000_001, "pnu_bad": 1,
                "output_object_prefix": c["handoff_prefix"],
                "parts": [{"object_key": f'{c["handoff_prefix"]}/{base}/attempt=test/part-{i:04d}.jsonl.gz',
                           "rows": count, "bytes": 123} for i, count in enumerate([10_000_000, 2], 1)]}

    def test_national_manifest_names_exact_parts_and_counts(self):
        c = self.contract()
        manifest = self.manifest(c)
        plan = self.planner()(c, "test-bucket", manifest)
        self.assertEqual(plan, [("s3a://test-bucket/" + p["object_key"], p["rows"]) for p in manifest["parts"]])

    def test_partial_wrong_vintage_and_duplicate_manifests_are_rejected(self):
        c = self.contract()
        plan = self.planner()
        with self.assertRaises(ValueError):
            plan(c, "test", None)
        for mutation in [
            lambda m: m.update(status="pending"),
            lambda m: m.update(vintage="202607"),
            lambda m: m.update(input_object_key=c["objects"][0]["object_key"]),
            lambda m: m["parts"].pop(),
            lambda m: m["parts"].append(copy.deepcopy(m["parts"][0])),
            lambda m: m.update(pnu_bad=0),
            lambda m: m["parts"][0].update(object_key="unrelated/part-0001.jsonl.gz"),
            lambda m: m["parts"][1].update(object_key=m["parts"][1]["object_key"].replace("part-0002", "part-0003")),
        ]:
            manifest = self.manifest(c)
            mutation(manifest)
            with self.subTest(manifest=manifest):
                with self.assertRaises(ValueError):
                    plan(c, "test", manifest)

    def test_existing_sido_contract_uses_declared_count_and_propagates_missing_regions(self):
        c = json.loads((SPARK.parent / "contracts/vworld-land-right-registration-source-objects.json").read_text(encoding="utf-8"))
        plan = self.planner()
        self.assertEqual(len(plan(c, "test", None)), 17)
        c["objects"] = [o for o in c["objects"] if not (o["region_code"] == "11" and o["vintage"] == c["selected_vintage"])]
        with self.assertRaises(ValueError):
            plan(c, "test", None)


if __name__ == "__main__":
    unittest.main()
