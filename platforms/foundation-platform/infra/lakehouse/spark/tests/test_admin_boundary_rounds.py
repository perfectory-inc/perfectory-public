"""`scripts/tiles/admin-boundary/rounds.py` reads the round from the ZIP members (root ADR-0236).

Bronze keeps every round side by side under keys that say nothing of the round, so the scheduled
release job must take the newest round from the layer names and convert only that round's ZIPs.
"""

import importlib.util
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
_spec = importlib.util.spec_from_file_location("admin_boundary_rounds", ROOT / "scripts" / "tiles" / "admin-boundary" / "rounds.py")
rounds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rounds)


class NewestRound(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)

    def zip(self, name: str, *members: str) -> None:
        with zipfile.ZipFile(self.dir / name, "w") as archive:
            for member in members:
                archive.writestr(member, b"")

    def newest(self):
        return rounds.newest(sorted(self.dir.glob("*.zip")))

    def test_the_newest_round_and_its_zip_per_sido(self):
        self.zip("30603-1.zip", "LSMD_ADM_SECT_UMD_98_209901.shp", "LSMD_ADM_SECT_UMD_98_209901.dbf")
        self.zip("30603-2.zip", "LSMD_ADM_SECT_UMD_99_209901.shp")
        self.zip("30603-3--sha256-ab.zip", "LSMD_ADM_SECT_UMD_98_209902.shp")
        self.zip("30603-4--sha256-cd.zip", "LSMD_ADM_SECT_UMD_99_209902.shp")
        self.assertEqual(self.newest(), {"round": "209902", "files": {"98": "30603-3--sha256-ab.zip", "99": "30603-4--sha256-cd.zip"}})

    def test_a_zip_without_a_boundary_layer_is_no_round(self):
        self.zip("30603-1.zip", "LSMD_ADM_SECT_UMD.xlsx", "readme.txt")
        self.assertEqual(self.newest(), {"round": None, "files": {}})

    def test_two_zips_for_one_sido_of_the_round_are_refused(self):
        self.zip("30603-1.zip", "LSMD_ADM_SECT_UMD_99_209901.shp")
        self.zip("30603-2--sha256-ef.zip", "LSMD_ADM_SECT_UMD_99_209901.shp")
        with self.assertRaisesRegex(SystemExit, "more than one ZIP for sido"):
            self.newest()


if __name__ == "__main__":
    unittest.main()
