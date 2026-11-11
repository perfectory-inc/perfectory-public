import sys
import unittest
from pathlib import Path


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import map_matching_gate as gate  # noqa: E402

# Reserved 99999 range; sido 99 exists, 98 was abolished.
OFFICIAL = {
    "9999900000": ("합성시", gate.EXISTS),
    "9999910100": ("합성시 가구 갑동", gate.EXISTS),
    "9999910200": ("합성시 가구 을동", gate.EXISTS),
    "9999910121": ("합성시 가구 을동 일리", gate.EXISTS),
    "9999920100": ("합성시 나구 병동", "폐지"),
}


def pnu(dong, main):
    return f"{dong}1{main:04d}0000"


class AdminGateTest(unittest.TestCase):
    def test_a_clean_layer_passes(self):
        report = gate.check_admin_units([("a", "9999910100"), ("b", "9999910200")], OFFICIAL, {"a": "9999910100"})
        self.assertTrue(report.passed, report.as_dict())

    def test_each_defect_is_named(self):
        report = gate.check_admin_units(
            [("a", "9999910100"), ("a", "9999920100"), ("c", "9999910100")],
            OFFICIAL,
            {"a": "9999910100", "gone": "9999910200"},
        )
        found = set(report.as_dict()["violations"])
        self.assertEqual(
            found,
            {"code not existing in the official list", "id served twice", "code served twice", "id served before is gone"},
        )
        self.assertIn("gone(9999910200)", report.message())

    def test_a_sido_with_no_unit_refuses(self):
        official = dict(OFFICIAL, **{"9800000000": ("다른시", gate.EXISTS)})
        report = gate.check_admin_units([("a", "9999910100")], official)
        self.assertEqual(report.as_dict()["violations"]["existing sido with no unit"]["sample"], ["98"])


class ParcelGateTest(unittest.TestCase):
    def test_parcels_need_their_dong_their_boundary_and_one_id(self):
        admin = {"9999910100", "9999910200"}
        ok = [pnu("9999910100", 1), pnu("9999910121", 2)]
        self.assertTrue(gate.check_parcels(ok, OFFICIAL, admin, {ok[0]: "x", ok[1]: "y"}).passed)
        bad = gate.check_parcels(
            # 병동 is abolished (no place, so no boundary is asked of it); 합성시 exists without a boundary.
            [pnu("9999920100", 1), pnu("9999900000", 1), pnu("9999910100", 2), pnu("9999910100", 3),
             "99999101001 00010000"],
            OFFICIAL,
            admin,
            {pnu("9999910100", 2): "same", pnu("9999910100", 3): "same"},
        )
        self.assertEqual(
            set(bad.as_dict()["violations"]),
            {
                "legal dong not existing in the official list",
                "no administrative boundary for the parcel's 읍면동",
                "no current parcel id",
                "parcel id on two parcels",
                "malformed PNU",
            },
        )

    def test_a_dong_renumbered_after_the_edition_is_allowed_under_its_successor(self):
        # 2026-11-11: 202609 holds 22,738 parcels of a 면 renumbered on 2026-09-30, after the edition was
        # cut; the zero-tolerance gate refused the whole edition for them. Here 병동 (abolished) was
        # renumbered into 갑동, which exists and has its boundary.
        admin = {"9999910100"}
        moved = [pnu("9999920100", 1), pnu("9999920100", 2)]
        report = gate.check_parcels(moved, OFFICIAL, admin, None, {"9999920100": "9999910100"})
        self.assertTrue(report.passed, report.as_dict())
        self.assertEqual(report.as_dict()["allowed"], {gate.RENUMBERED_LEGAL_DONG: 2})
        # Without the pair, or into a code the list does not hold, it is refused as before.
        for renumbered in (None, {"9999920100": "9999930100"}):
            with self.subTest(renumbered=renumbered):
                refused = gate.check_parcels(moved, OFFICIAL, admin, None, renumbered).as_dict()["violations"]
                self.assertEqual(refused[gate.NO_LEGAL_DONG]["count"], 2)
        # A stray code nothing explains is allowed up to the caller's bound, and sampled; past it, refused.
        within = gate.check_parcels(moved, OFFICIAL, admin, None, None, unexplained_bound=2).as_dict()
        self.assertTrue(within["passed"], within)
        self.assertEqual(within["allowed"], {gate.UNEXPLAINED_LEGAL_DONG: 2})
        self.assertEqual(within["unexplained_sample"], sorted(moved))
        over = gate.check_parcels(moved, OFFICIAL, admin, None, None, unexplained_bound=1).as_dict()
        self.assertEqual(over["violations"][gate.NO_LEGAL_DONG]["count"], 2)
        self.assertEqual(over["allowed"], {})
        # The successor's own boundary is what the parcel needs.
        refused = gate.check_parcels(moved, OFFICIAL, set(), None, {"9999920100": "9999910100"}).as_dict()
        self.assertEqual(set(refused["violations"]), {gate.NO_BOUNDARY})


class AttributeGateTest(unittest.TestCase):
    def test_a_value_left_behind_under_the_old_number_refuses_and_a_missing_one_is_allowed(self):
        new_a, new_b, new_c = pnu("9999910100", 1), pnu("9999910100", 2), pnu("9999910100", 3)
        old_b = pnu("9999920100", 2)
        report = gate.check_attribute(
            "land price",
            [new_a, new_b, new_c],
            has_value={new_a, old_b},
            predecessors={new_b: [old_b], new_c: [pnu("9999920100", 3)]},
        )
        self.assertFalse(report.passed)
        self.assertEqual(report.as_dict()["violations"]["land price held under a predecessor number but not attached"]["count"], 1)
        self.assertEqual(report.as_dict()["allowed"], {"land price: no source value": 1})


class ParcelGateArgsTest(unittest.TestCase):
    def test_attribute_specs_are_validated_before_any_sql(self):
        from parcel_matching_gate import parse_args, validate_args

        for spec in ("land_individual_price", "silver.x; DROP", "silver.land_individual_price:base_year='2026'"):
            args = parse_args(["--snapshot-id", "s", "--allow-non-served-edition", "--sido", "99", "--attribute", spec])
            with self.subTest(spec=spec), self.assertRaises(ValueError):
                validate_args(args)


if __name__ == "__main__":
    unittest.main()
