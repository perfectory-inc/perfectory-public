"""numbering_collisions refuses the collisions it exists to refuse, and only those (planted below)."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numbering_collisions as check  # noqa: E402

MAIN = ["docs/adr/0258-a.md", "docs/adr/README.md",
        "platforms/foundation-platform/migrations/20261016090000_x.sql",
        "products/gongzzang/migrations/20261016120000_y.sql"]


def run(mine, others, me=640):
    return check.collisions({path: path for path in mine}, MAIN, others, me)


class Adr(unittest.TestCase):
    def test_a_number_an_older_open_pull_request_adds_is_refused_with_the_next_free_one(self):
        failures, _ = run(["docs/adr/0259-mine.md"], {633: ["docs/adr/0259-theirs.md"], 635: ["docs/adr/0260-z.md"]})
        self.assertEqual(len(failures), 1)
        self.assertIn("#633", failures[0])
        self.assertIn("the next free one is 0261", failures[0])

    def test_a_number_main_holds_is_refused(self):
        failures, _ = run(["docs/adr/0258-mine.md"], {})
        self.assertIn("main (docs/adr/0258-a.md)", failures[0])

    def test_a_newer_pull_request_on_the_same_number_is_named_not_refused(self):
        failures, notes = run(["docs/adr/0259-mine.md"], {650: ["docs/adr/0259-later.md"]})
        self.assertEqual(failures, [])
        self.assertIn("#650", notes[0])

    def test_a_pull_request_stacked_on_another_carries_its_adr_without_colliding(self):
        # The same path in both is the same file; a different file on the number still collides.
        self.assertEqual(run(["docs/adr/0259-mine.md"], {633: ["docs/adr/0259-mine.md"]}), ([], []))
        failures, _ = run(["docs/adr/0259-mine.md"], {633: ["docs/adr/0259-mine.md"], 634: ["docs/adr/0259-x.md"]})
        self.assertIn("#634", failures[0])

    def test_a_free_number_passes(self):
        self.assertEqual(run(["docs/adr/0259-mine.md"], {633: ["docs/adr/0260-theirs.md"]}), ([], []))

    def test_an_area_adr_or_the_index_is_not_numbered_here(self):
        self.assertEqual(run(["products/gongzzang/docs/adr/0258-x.md", "docs/adr/README.md"], {}), ([], []))


class Migrations(unittest.TestCase):
    def test_a_version_an_older_pull_request_adds_to_the_same_directory_is_refused(self):
        # 2026-10-16: #633 and #636 both added 20261016120000.
        mine = ["platforms/foundation-platform/migrations/20261016120000_mine.sql"]
        failures, _ = run(mine, {633: ["platforms/foundation-platform/migrations/20261016120000_theirs.sql",
                                       "platforms/foundation-platform/migrations/20261016120001_more.sql"]}, me=636)
        self.assertEqual(len(failures), 1)
        self.assertIn("the next free one is 20261016120002", failures[0])

    def test_the_same_version_in_another_directory_is_no_collision(self):
        self.assertEqual(run(["platforms/foundation-platform/migrations/20261016120000_mine.sql"], {}), ([], []))

    def test_a_version_main_holds_in_the_same_directory_is_refused(self):
        failures, _ = run(["platforms/foundation-platform/migrations/20261016090000_mine.sql"], {})
        self.assertIn("main (platforms/foundation-platform/migrations/20261016090000_x.sql)", failures[0])

    def test_own_pull_request_is_not_its_own_collision(self):
        mine = ["docs/adr/0259-mine.md"]
        self.assertEqual(run(mine, {640: mine}), ([], []))


if __name__ == "__main__":
    unittest.main()
