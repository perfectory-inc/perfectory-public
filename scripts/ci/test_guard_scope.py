#!/usr/bin/env python3
"""Unit tests for scripts/ci/guard_scope.py (root ADR-0207)."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import guard_scope as gs  # noqa: E402


class Fixture:
    """A synthetic repository: a few guards, their inputs, one contract."""

    def __init__(self, test: unittest.TestCase) -> None:
        self.root = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.root)
        self.run = [
            {"name": "alpha", "paths": ["areas/a/**"]},
            {"name": "alpha-self-test", "paths": []},
            {"name": "beta", "always": "reads every file"},
            {"name": "gamma", "paths": ["areas/g/**"]},
        ]
        self.write("areas/a/one.txt", "a\n")
        self.write("areas/g/one.txt", "g\n")
        self.write("tools/beta.json", "{}\n")
        self.write("scripts/guard/monorepo-guard.sh", "#!/usr/bin/env bash\n")
        self.write("scripts/guard/lib/fixture-repo.sh", "#!/usr/bin/env bash\n")
        self.write("scripts/guard/alpha.sh", '#!/usr/bin/env bash\nbash scripts/guard/check-alpha.sh\n')
        self.write("scripts/guard/check-alpha.sh", '#!/usr/bin/env bash\ncat "tools/beta.json"\n')
        self.write("scripts/guard/alpha-self-test.sh", '#!/usr/bin/env bash\nbash "$dir/alpha.sh"\n')
        self.write("scripts/guard/beta.sh", "#!/usr/bin/env bash\n")
        self.write("scripts/guard/gamma.sh", "#!/usr/bin/env bash\n")
        self.write(".github/workflows/ci.yml", "on: push\n")
        self.save()

    def write(self, path: str, text: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def save(self) -> None:
        self.write("tools/ci/change-scope.json", json.dumps({"workflows": {}, "unscoped": {}, "guards": {"run": self.run}}))

    def contract(self) -> dict:
        return json.loads((self.root / "tools/ci/change-scope.json").read_text(encoding="utf-8"))

    def tracked(self) -> list[str]:
        return sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob("*") if p.is_file())

    def select(self, changed: set[str] | None, event: str = "pull_request") -> list[str]:
        return gs.select(self.contract(), event, changed, self.root)[0]

    def check(self) -> list[str]:
        return gs.check(self.root, self.tracked())


ALL = ["alpha", "alpha-self-test", "beta", "gamma"]


class SelectTest(unittest.TestCase):
    def test_untouched_guard_does_not_run(self) -> None:
        self.assertEqual(Fixture(self).select({"areas/g/one.txt"}), ["beta", "gamma"])

    def test_listed_path_runs_the_guard_and_not_its_self_test(self) -> None:
        self.assertEqual(Fixture(self).select({"areas/a/one.txt"}), ["alpha", "beta"])

    def test_always_runs_on_any_change(self) -> None:
        self.assertEqual(Fixture(self).select({"README.md"}), ["beta"])

    def test_own_script_and_named_helpers_are_inputs(self) -> None:
        f = Fixture(self)
        self.assertIn("alpha", f.select({"scripts/guard/alpha.sh"}))
        self.assertIn("alpha", f.select({"scripts/guard/check-alpha.sh"}))
        # check-alpha.sh names tools/beta.json: followed through the helper.
        self.assertIn("alpha", f.select({"tools/beta.json"}))

    def test_self_test_runs_when_its_guard_changes(self) -> None:
        f = Fixture(self)
        self.assertIn("alpha-self-test", f.select({"scripts/guard/check-alpha.sh"}))
        self.assertIn("alpha-self-test", f.select({"scripts/guard/alpha-self-test.sh"}))
        self.assertNotIn("gamma", f.select({"scripts/guard/alpha.sh"}))

    def test_other_events_and_unknown_comparison_run_everything(self) -> None:
        f = Fixture(self)
        for event in ("push", "merge_group", "schedule", "workflow_dispatch", ""):
            self.assertEqual(f.select(set(), event), ALL, event)
        self.assertEqual(f.select(None), ALL)

    def test_runner_library_contract_and_selectors_run_everything(self) -> None:
        f = Fixture(self)
        for path in (
            "scripts/guard/monorepo-guard.sh",
            "scripts/guard/lib/fixture-repo.sh",
            "tools/ci/change-scope.json",
            "scripts/ci/change_scope.py",
            "scripts/ci/guard_scope.py",
        ):
            self.assertEqual(f.select({path}), ALL, path)

    def test_order_is_the_contract_order(self) -> None:
        f = Fixture(self)
        f.run.reverse()
        f.save()
        self.assertEqual(f.select({"areas/a/one.txt", "areas/g/one.txt"}), ["gamma", "beta", "alpha"])


class CheckTest(unittest.TestCase):
    def assertRefused(self, f: Fixture, needle: str) -> None:
        errors = f.check()
        self.assertTrue(any(needle in e for e in errors), errors)

    def test_valid_fixture_passes(self) -> None:
        self.assertEqual(Fixture(self).check(), [])

    def test_empty_list_is_refused(self) -> None:
        f = Fixture(self)
        f.run = []
        f.save()
        self.assertRefused(f, "lists no guard")

    def test_guard_listed_twice_is_refused(self) -> None:
        f = Fixture(self)
        f.run.append({"name": "gamma", "paths": ["areas/g/**"]})
        f.save()
        self.assertRefused(f, "listed 2 times")

    def test_listed_guard_without_script_is_refused(self) -> None:
        f = Fixture(self)
        f.run.append({"name": "delta", "always": "x"})
        f.save()
        self.assertRefused(f, "no scripts/guard/delta.sh")

    def test_entry_must_be_scoped_or_justified_not_both(self) -> None:
        for entry in ({"name": "gamma"}, {"name": "gamma", "paths": ["areas/g/**"], "always": "x"}):
            f = Fixture(self)
            f.run[3] = entry
            f.save()
            self.assertRefused(f, "exactly one of")

    def test_always_without_reason_is_refused(self) -> None:
        f = Fixture(self)
        f.run[2] = {"name": "beta", "always": " "}
        f.save()
        self.assertRefused(f, "states why")

    def test_stale_glob_is_refused(self) -> None:
        f = Fixture(self)
        f.run[3] = {"name": "gamma", "paths": ["areas/gone/**"]}
        f.save()
        self.assertRefused(f, "names no tracked file")

    def test_quoted_read_outside_the_inputs_is_refused_and_listing_it_clears_it(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/gamma.sh", '#!/usr/bin/env bash\ngit grep x -- "areas/a"\n')
        self.assertRefused(f, "reads `areas/a`")
        f.run[3] = {"name": "gamma", "paths": ["areas/g/**", "areas/a/**"]}
        f.save()
        self.assertEqual(f.check(), [])

    def test_comment_is_not_a_read(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/gamma.sh", '#!/usr/bin/env bash\n# see "areas/a"\n')
        self.assertEqual(f.check(), [])

    def test_script_nothing_runs_is_refused(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/orphan-self-test.sh", "#!/usr/bin/env bash\n")
        self.assertRefused(f, "orphan-self-test.sh: nothing runs it")
        f.write(".github/workflows/ci.yml", "on: push\njobs:\n  x:\n    steps:\n      - run: bash scripts/guard/orphan-self-test.sh\n")
        self.assertEqual(f.check(), [])

    def test_listed_guard_started_again_by_a_guard_is_refused(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/beta.sh", "#!/usr/bin/env bash\nbash scripts/guard/gamma.sh\n")
        self.assertRefused(f, "gamma.sh: runs 2 times")

    def test_helper_started_by_two_guards_is_refused(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/gamma.sh", '#!/usr/bin/env bash\n  bash "$dir/check-alpha.sh" || rc=1\n')
        self.assertRefused(f, "check-alpha.sh: runs 2 times")

    def test_self_test_running_its_guard_on_a_fixture_is_not_a_second_run(self) -> None:
        f = Fixture(self)
        f.write("scripts/guard/alpha-self-test.sh", "#!/usr/bin/env bash\nexec bash scripts/guard/alpha.sh fixture\n")
        self.assertEqual(f.check(), [])


class RepositoryTest(unittest.TestCase):
    def test_the_repository_guard_list_holds(self) -> None:
        self.assertEqual(gs.check(), [])

    def test_every_listed_guard_runs_outside_a_pull_request(self) -> None:
        contract = gs.cs.load_contract()
        self.assertEqual(gs.select(contract, "merge_group", set())[0], [e["name"] for e in gs.guards(contract)])


if __name__ == "__main__":
    unittest.main()
