"""foundation_spark_shards.py: which Spark runtime tests a shard runs (root ADR-0365). No Spark needed."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import foundation_spark_shards as shards  # noqa: E402

TESTS = shards.TESTS
GATED = ["test_a_spark.py", "test_b_iceberg.py", "test_c_spark.py"]
ONE_CLASS = '''import unittest

@unittest.skipUnless(False, "requires pyspark")
class T(unittest.TestCase):
    def test_one(self): pass
    def test_two(self): pass
    def helper(self): pass
'''
TWO_CLASSES = '''import unittest

class Pure(unittest.TestCase):
    def test_logic(self): pass

class Spark(TestCase):
    def test_spark(self): pass

class Empty(unittest.TestCase):
    pass
'''


def answer(*tests: str, kind: str = "some") -> str:
    return "\n".join([f"{kind}\twhy"] + [f"test\t{path}" for path in tests]) + "\n"


class SelectedFilesTest(unittest.TestCase):
    def test_an_event_that_does_not_narrow_runs_every_file(self):
        files, _ = shards.selected_files(GATED, None, lambda: self.fail("asked the suites"))
        self.assertEqual(files, GATED)

    def test_a_change_to_the_runtime_or_the_selection_runs_every_file(self):
        for path in ("platforms/foundation-platform/compose.lakehouse.yml", "scripts/ci/foundation_spark_shards.py",
                     "scripts/ci/foundation-spark-runtime-durations.json", ".github/workflows/foundation-ci.yml"):
            files, why = shards.selected_files(GATED, {path}, lambda: answer())
            self.assertEqual(files, GATED, path)
            self.assertIn(path, why)

    def test_a_suite_answer_of_all_or_nothing_readable_runs_every_file(self):
        for text in (answer(kind="all"), ""):
            files, _ = shards.selected_files(GATED, {"platforms/foundation-platform/x.py"}, lambda: text)
            self.assertEqual(files, GATED)

    def test_a_narrow_answer_runs_the_gated_files_it_reaches(self):
        text = answer(f"{TESTS}/test_b_iceberg.py", f"{TESTS}/test_pure.py", "elsewhere/tests/test_a_spark.py")
        files, _ = shards.selected_files(GATED, {"platforms/foundation-platform/x.py"}, lambda: text)
        self.assertEqual(files, ["test_b_iceberg.py"])

    def test_a_narrow_answer_reaching_no_gated_file_runs_none(self):
        files, _ = shards.selected_files(GATED, {"platforms/foundation-platform/x.py"}, lambda: answer())
        self.assertEqual(files, [])


class PiecesTest(unittest.TestCase):
    def test_one_class_splits_into_its_test_methods(self):
        self.assertEqual(shards.pieces("test_x.py", 100, ONE_CLASS), [("test_x.T.test_one", 50), ("test_x.T.test_two", 50)])

    def test_several_classes_split_into_the_classes_with_tests(self):
        self.assertEqual(shards.pieces("test_x.py", 100, TWO_CLASSES), [("test_x.Pure", 50), ("test_x.Spark", 50)])

    def test_a_class_that_may_inherit_tests_keeps_the_file_whole(self):
        source = ONE_CLASS + "\nclass More(Base):\n    pass\n"
        self.assertEqual(shards.pieces("test_x.py", 100, source), [("test_x.py", 100)])

    def test_a_module_loading_its_own_tests_stays_whole(self):
        source = ONE_CLASS + "\ndef load_tests(loader, tests, pattern):\n    return tests\n"
        self.assertEqual(shards.pieces("test_x.py", 100, source), [("test_x.py", 100)])


class PlanTest(unittest.TestCase):
    def test_every_target_runs_once_and_a_long_file_is_split_across_shards(self):
        files = ["test_long.py", "test_a.py", "test_b.py", "test_c.py", "test_new.py"]
        durations = {"test_long.py": 400, "test_a.py": 90, "test_b.py": 80, "test_c.py": 30}
        sources = {name: "" for name in files} | {"test_long.py": ONE_CLASS}

        planned = shards.plan(files, durations, 2, sources.__getitem__)

        targets = [target for shard in planned for target, _ in shard]
        self.assertCountEqual(
            targets, ["test_long.T.test_one", "test_long.T.test_two", "test_a.py", "test_b.py", "test_c.py", "test_new.py"]
        )
        self.assertEqual(len({t for t, _ in planned[0]} & {"test_long.T.test_one", "test_long.T.test_two"}), 1)
        loads = [sum(s for _, s in shard) for shard in planned]
        self.assertLessEqual(max(loads) - min(loads), 90)
        self.assertEqual(dict(planned[1] + planned[0])["test_new.py"], 85)  # the median

    def test_a_piece_measured_on_its_own_keeps_its_own_time(self):
        durations = {"test_long.py": 400, "test_long.T.test_one": 350, "test_a.py": 50}
        sources = {"test_long.py": ONE_CLASS, "test_a.py": ""}

        planned = shards.plan(["test_long.py", "test_a.py"], durations, 2, sources.__getitem__)

        self.assertEqual(planned[0], [("test_long.T.test_one", 350.0)])
        self.assertEqual(planned[1], [("test_a.py", 50.0), ("test_long.T.test_two", 200.0)][::-1])

    def test_a_short_file_is_not_split(self):
        planned = shards.plan(["test_x.py"], {"test_x.py": shards.MIN_SPLIT_SECONDS}, 4, lambda _: ONE_CLASS)
        self.assertEqual([t for shard in planned for t, _ in shard], ["test_x.py"])

    def test_no_files_gives_empty_shards(self):
        self.assertEqual(shards.plan([], {"a": 1}, 3, lambda _: ""), [[], [], []])


if __name__ == "__main__":
    unittest.main()
