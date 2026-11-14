"""A lane whose Gold is a table of appended row versions names its change set by version_seq
(root ADR-0564 §5, ADR-0567), against the fake publisher of test_by_pnu_serving_bake.

parcel-nearby bakes gold.parcel_nearby_versions. Its change set is the publisher's
`export-gold-versions-change-set` (the keys whose version_seq lies between the reflected snapshot
and the new one), not the Spark delta job, which would read every version of a key as a row; the
command's verdict is the delta job's exit code. PNUs are synthetic (99999...).
"""

import json
import shutil
import unittest

import test_by_pnu_serving_bake as base

LANE = "parcel-nearby"
# The fake answers the change-set command as the publisher would: the lists and a summary whose
# verdict FAKE_VERDICT names. Inserted before the fake's generic `export-` branch.
VERSIONS_FAKE = r'''elif command == "export-gold-versions-change-set":
    p = "FOUNDATION_PLATFORM_GOLD_VERSIONS_CHANGE_SET_"
    ups = json.loads(os.environ.get("FAKE_DELTA_UPSERTS", "[]"))
    dels = json.loads(os.environ.get("FAKE_DELTA_DELETES", "[]"))
    with open(os.environ["FAKE_LOG"], "a") as log:
        log.write(json.dumps({"command": "versions-change-set", "env": {k: v for k, v in os.environ.items() if k.startswith(p)}}) + "\n")
    open(os.environ[p + "UPSERTS"], "w").write("".join(pnu + "\n" for pnu in ups))
    open(os.environ[p + "DELETES"], "w").write("".join(pnu + "\n" for pnu in dels))
    json.dump({"job_name": "gold_versions_change_set", "contract": os.environ[p + "TABLE"],
               "verdict": os.environ.get("FAKE_VERDICT", "delta"),
               "quality_metrics": {"upsert_count": len(ups), "delete_count": len(dels), "new_count": 0},
               "input": {"baseline_snapshot_id": os.environ[p + "BASELINE_SNAPSHOT_ID"],
                         "current_snapshot_id": os.environ[p + "CURRENT_SNAPSHOT_ID"]}},
              open(os.environ[p + "SUMMARY"], "w"))
'''
ANCHOR = 'elif command.startswith("export-") and command.endswith("-section-packs"):'
if VERSIONS_FAKE not in base.FAKE_PUBLISHER:
    assert ANCHOR in base.FAKE_PUBLISHER, "the fake publisher's pack export branch moved"
    base.FAKE_PUBLISHER = base.FAKE_PUBLISHER.replace(ANCHOR, VERSIONS_FAKE + ANCHOR, 1)


class VersionsTableLane(unittest.TestCase):
    exports = base.ByPnuServingBake.exports
    published = base.ByPnuServingBake.published
    PREFIX = "FOUNDATION_PLATFORM_PARCEL_NEARBY_BY_PNU_SERVING_"

    def setUp(self):
        base.ByPnuServingBake.setUp(self)
        # The release ships the contract that names the kind lanes (root ADR-0190, ADR-0193 §9).
        shutil.copytree(base.job_specs.PLATFORM_ROOT / "config", self.script.parents[2] / "config",
                        dirs_exist_ok=True)

    def bake(self, **env):
        state = base.packs_state(sections=[{"name": "documents", "generation": 1, "patch_floor": 0}])
        return base.ByPnuServingBake.bake(self, LANE, FAKE_PACKS=env.pop("FAKE_PACKS", state), **env)

    def test_the_change_set_is_the_publisher_s_version_range_and_no_spark_runs(self):
        result, calls = self.bake(FAKE_DELTA_UPSERTS=json.dumps(base.PNUS[:2]),
                                  FAKE_DELTA_DELETES=json.dumps(["9999920000000000001"]))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [change] = [call for call in calls if call["command"] == "versions-change-set"]
        env = change["env"]
        prefix = "FOUNDATION_PLATFORM_GOLD_VERSIONS_CHANGE_SET_"
        self.assertEqual(env[prefix + "TABLE"], "gold.parcel_nearby_versions")
        self.assertEqual((env[prefix + "BASELINE_SNAPSHOT_ID"], env[prefix + "CURRENT_SNAPSHOT_ID"]), ("150", "202"))
        self.assertFalse([call for call in calls if call["command"] == "delta"], "the Spark delta job ran")
        [export] = [call for call in self.exports(calls) if call["command"].endswith("-section-packs")]
        self.assertIn(self.PREFIX + "TARGET_PATCH", export["env"])
        self.assertEqual(open(export["env"][self.PREFIX + "PNU_ALLOWLIST_PATH"]).read().split(), base.PNUS[:2])
        self.assertEqual(len(self.published(calls)), 1)

    def test_a_verdict_is_the_delta_job_s_exit_code(self):
        # comparison_gone (3) and not_a_delta (4) refuse and publish nothing; explained_full (6) bakes full.
        for verdict, refused in (("comparison_gone", True), ("not_a_delta", True), ("explained_full", False)):
            with self.subTest(verdict=verdict):
                self.log.unlink(missing_ok=True)
                result, calls = self.bake(FAKE_VERDICT=verdict, FAKE_DELTA_UPSERTS=json.dumps(base.PNUS[:2]))
                if refused:
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertEqual(self.published(calls), [])
                else:
                    self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                    self.assertIn("a full bake is cheaper past the break-even", result.stdout)
                self.assertFalse([call for call in calls if call["command"] == "delta"])


if __name__ == "__main__":
    unittest.main()
