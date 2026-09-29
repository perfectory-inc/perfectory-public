import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import parcel_identity as pi  # noqa: E402
import place_id_release as rel  # noqa: E402
from parcel_lineage import Link  # noqa: E402

OLD, NEW = "9999910100", "9999920100"


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


class ParcelReleaseTest(unittest.TestCase):
    def setUp(self):
        self.boot = pi.bootstrap([pnu(OLD, 1), pnu(OLD, 2), pnu(OLD, 3)], "2099-06-01")
        current = {r.pnu: r.parcel_id for r in self.boot}
        effective = {
            pnu(NEW, 1): Link(pnu(OLD, 1), pnu(NEW, 1), "code_change", "code_derived", "t"),
            pnu(NEW, 50): Link(pnu(OLD, 2), pnu(NEW, 50), "merge", "official", "t"),
        }
        self.step = pi.advance(current, [pnu(NEW, 1), pnu(NEW, 50)], effective, "2099-09-01").rows
        self.rows = self.boot + self.step
        self.id_of = current

    def test_the_changelog_names_every_kind_of_change(self):
        changes = {c.change: c for c in rel.parcel_changelog(self.rows, "2099-06-01", "2099-09-01")}
        self.assertEqual(changes["renumbered"].place_id, self.id_of[pnu(OLD, 1)])
        self.assertEqual((changes["renumbered"].from_code, changes["renumbered"].to_code), (pnu(OLD, 1), pnu(NEW, 1)))
        self.assertEqual(changes["added"].to_code, pnu(NEW, 50))
        retired = [c for c in rel.parcel_changelog(self.rows, "2099-06-01", "2099-09-01") if c.change == "retired"]
        self.assertEqual({c.from_code for c in retired}, {pnu(OLD, 2), pnu(OLD, 3)})

    def test_an_old_reference_resolves_to_the_same_land_today(self):
        registry, bridge = rel.parcel_registry(self.rows), rel.parcel_bridge(self.rows)
        self.assertEqual(rel.resolve(pnu(OLD, 1), bridge, registry), pnu(NEW, 1), "renumbered: same land, new number")
        self.assertIsNone(rel.resolve(pnu(OLD, 3), bridge, registry), "closed and not continued: no current land")
        periods = [b for b in bridge if b.place_id == self.id_of[pnu(OLD, 1)]]
        self.assertEqual([(b.code, b.valid_from, b.valid_to) for b in periods],
                         [(pnu(OLD, 1), "2099-06-01", "2099-09-01"), (pnu(NEW, 1), "2099-09-01", None)])

    def test_a_redirect_is_followed(self):
        registry = [rel.RegistryEntry("interim", pi.REDIRECTED, None, "kept"), rel.RegistryEntry("kept", pi.CURRENT, pnu(NEW, 3), None)]
        bridge = [rel.BridgeEntry("interim", pnu(NEW, 3), "2099-09-01", "2099-10-01")]
        self.assertEqual(rel.resolve(pnu(NEW, 3), bridge, registry), pnu(NEW, 3))


class AdminReleaseTest(unittest.TestCase):
    def test_renumbered_added_and_retired_are_told_apart(self):
        july = {"a": "9999910100", "b": "9999910200", "c": "9999910300"}
        september = {"a": "9999920100", "b": "9999910200", "d": "9999920400"}
        registry, bridge, changes = rel.admin_release([("202606", july), ("202609", september)])
        kinds = {c.place_id: (c.change, c.from_code, c.to_code) for c in changes}
        self.assertEqual(kinds, {
            "a": ("renumbered", "9999910100", "9999920100"),
            "c": ("retired", "9999910300", None),
            "d": ("added", None, "9999920400"),
        })
        self.assertEqual({r.place_id: r.status for r in registry}, {"a": "current", "b": "current", "c": "historic", "d": "current"})
        self.assertEqual(rel.resolve("9999910100", bridge, registry), "9999920100")


class ReleaseJobArgsTest(unittest.TestCase):
    def test_parcel_releases_need_their_scope_and_dates(self):
        from place_id_release_to_gold import parse_args, validate_args

        args = parse_args(["--unit", "parcel", "--release-id", "r1", "--allow-non-smoke-write"])
        with self.assertRaisesRegex(ValueError, "--sido"):
            validate_args(args)
        args = parse_args(["--unit", "admin", "--release-id", "r1; DROP", "--allow-non-smoke-write"])
        with self.assertRaisesRegex(ValueError, "release id"):
            validate_args(args)


if __name__ == "__main__":
    unittest.main()
