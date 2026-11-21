"""A takeover's switch (root ADR-0564 §6-5), against the fake publisher of test_by_pnu_serving_bake.

The lane's served state was baked from the replaced table (gold.lane); the lane now bakes its versions
table (gold.lane_versions). No change set spans two tables: the switch is one whole generation, baked
from the build pair the verdict compared (show-gold-takeover-switch) and refused unless the packs
reflect the replaced table's build. Only packs are held to what they serve, so an object lane's
switch is refused. PNUs are synthetic (99999...).
"""

import json
import unittest

import test_by_pnu_serving_bake as base

# The fake answers the build pair as the publisher would. Inserted before the fake's `show-`
# branches, which would otherwise take the command for a lane's state.
SWITCH_FAKE = r'''if command == "show-gold-takeover-switch":
    print(json.dumps({"table": os.environ["FOUNDATION_PLATFORM_GOLD_TAKEOVER_TABLE"],
                      "replaced_build": os.environ.get("FAKE_REPLACED_BUILD", "150"),
                      "shadow_build": os.environ.get("FAKE_SHADOW_BUILD", "170")}))
el'''
ANCHOR = 'if command.startswith("show-") and os.environ.get("FAKE_NOT_CUT_OVER") == command:'
if SWITCH_FAKE not in base.FAKE_PUBLISHER:
    assert ANCHOR in base.FAKE_PUBLISHER, "the fake publisher's show- branch moved"
    base.FAKE_PUBLISHER = base.FAKE_PUBLISHER.replace(ANCHOR, SWITCH_FAKE + ANCHOR, 1)


class ObjectLaneTakeover(unittest.TestCase):
    setUp = base.ByPnuServingBake.setUp
    exports = base.ByPnuServingBake.exports
    published = base.ByPnuServingBake.published
    patch_bake = base.ByPnuServingBake.patch_bake
    bake = base.ByPnuServingBake.bake

    def test_an_object_lane_whose_gold_was_taken_over_is_refused_not_baked_unchecked(self):
        result, calls = self.patch_bake(FAKE_SERVED_GOLD_TABLE="gold.lane",
                                        FAKE_LANE_GOLD_TABLE="gold.lane_versions")
        self.assertEqual(result.returncode, 64, result.stderr + result.stdout)
        self.assertIn("a takeover's switch is checked only for section packs", result.stdout + result.stderr)
        self.assertEqual(self.exports(calls), [])
        self.assertEqual(self.published(calls), [])


class PackLaneTakeover(unittest.TestCase):
    setUp = base.ByPnuServingBake.setUp
    exports = base.ByPnuServingBake.exports
    published = base.ByPnuServingBake.published
    PREFIX = base.SectionPackBake.PREFIX
    bake = base.SectionPackBake.bake

    def test_a_takeover_re_bases_the_packs_once_at_the_shadow_commit(self):
        # The packs were baked from gold.lane at its build 150; the switch is a whole generation
        # pinned to the shadow commit 170 the verdict compared, though Gold is at 202.
        result, calls = self.bake(FAKE_PACKS=base.packs_state(gold_table="gold.lane"),
                                  FAKE_LANE_GOLD_TABLE="gold.lane_versions")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual([call for call in calls if call["command"] == "delta"], [])
        exports = [call for call in self.exports(calls) if call["command"].endswith("-section-packs")]
        self.assertTrue(exports)
        for call in exports:
            self.assertIn(self.PREFIX + "PACK_GENERATION", call["env"])
            self.assertEqual(call["env"][self.PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "170")
        [publish] = self.published(calls)
        self.assertEqual(publish["env"][self.PREFIX + "EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID"], "170")
        summary = json.loads(next(self.state_root.glob("building/runs/*/run-summary.json")).read_text())
        self.assertEqual((summary["mode"], summary["takeover_from"]), ("full", "gold.lane"))
        self.assertIn("baked from gold.lane, the lane now bakes gold.lane_versions", summary["reason"])

    def test_a_takeover_is_refused_until_the_packs_reflect_the_verdict_build(self):
        # The packs reflect gold.lane snapshot 150, but its build the verdict compared is 160.
        result, calls = self.bake(FAKE_PACKS=base.packs_state(gold_table="gold.lane"),
                                  FAKE_LANE_GOLD_TABLE="gold.lane_versions", FAKE_REPLACED_BUILD="160")
        self.assertEqual(result.returncode, 1, result.stderr + result.stdout)
        self.assertIn("not its build 160 the verdict compared", result.stdout + result.stderr)
        self.assertEqual(self.exports(calls), [])
        self.assertEqual(self.published(calls), [])


if __name__ == "__main__":
    unittest.main()
