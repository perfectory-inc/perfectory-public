#!/usr/bin/env python3
"""The change-scope selector and its contract check (root ADR-0201)."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import change_scope as cs  # noqa: E402

SCOPE_JOB = """\
  scope:
    name: Change scope
    runs-on: ubuntu-24.04
    outputs:
      run: ${{ steps.select.outputs.run }}
    steps:
      - name: Select
        id: select
        run: python3 scripts/ci/change_scope.py select --workflow example-ci.yml
"""
BUILD_JOB = """\
  build:
    needs: [scope]
    if: ${{ needs.scope.outputs.run == 'true' }}
    runs-on: ubuntu-24.04
    steps:
      - run: cargo xtask verify example
"""
REQUIRED_JOB = """\
  required:
    name: required/example
    if: ${{ always() }}
    needs: [scope, build]
    steps:
      - name: Require
        env:
          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.run }}
          REQUIRED_RESULT_SCOPE: ${{ needs.scope.result }}
          REQUIRED_RESULT_BUILD: ${{ needs.build.result }}
        run: bash scripts/ci/require-successful-needs.sh
"""
ON_BLOCK = "name: example\non:\n  pull_request:\n    branches: [main]\n  merge_group:\n"


class Fixture:
    """A synthetic repository: one area, one scoped workflow, one contract."""

    def __init__(self, test: unittest.TestCase) -> None:
        self.root = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.root)
        self.contract = {
            "workflows": {
                "example-ci.yml": {
                    "merge_group": "scoped",
                    "paths": ["platforms/example/**", "tools/xtask/**", ".cargo/config.toml", "rust-toolchain.toml"],
                }
            },
            "unscoped": {"guard.yml": "reads everything"},
        }
        self.files = {
            "platforms/example/src/lib.rs": "",
            "tools/xtask/src/main.rs": "",
            ".cargo/config.toml": "",
            "rust-toolchain.toml": "",
            "scripts/ci/require-successful-needs.sh": "",
            "scripts/ci/example-setup.sh": "",
            ".github/workflows/guard.yml": ON_BLOCK + "jobs:\n  guard:\n    runs-on: ubuntu-24.04\n",
        }
        self.workflow(ON_BLOCK + "jobs:\n" + SCOPE_JOB + BUILD_JOB + REQUIRED_JOB)

    def workflow(self, text: str, name: str = "example-ci.yml") -> None:
        self.files[f".github/workflows/{name}"] = text

    def write(self) -> Path:
        for path, text in self.files.items():
            (self.root / path).parent.mkdir(parents=True, exist_ok=True)
            (self.root / path).write_text(text, encoding="utf-8")
        (self.root / "tools/ci").mkdir(parents=True, exist_ok=True)
        (self.root / cs.CONTRACT).write_text(json.dumps(self.contract), encoding="utf-8")
        return self.root

    def errors(self) -> list[str]:
        root = self.write()
        return cs.check(root, tracked=list(self.files) + [cs.CONTRACT])

    def decide(self, event: str, changed: set[str] | None) -> bool:
        root = self.write()
        return cs.decide(self.contract, "example-ci.yml", event, changed, root)[0]


class GlobTest(unittest.TestCase):
    def test_github_glob_forms(self) -> None:
        cases = {
            ("a/**", "a/b/c.rs"): True,
            ("a/**", "ab/c.rs"): False,
            ("**/Cargo.lock", "Cargo.lock"): True,
            ("**/Cargo.lock", "x/y/Cargo.lock"): True,
            ("p/**/*.md", "p/README.md"): True,
            ("p/**/*.md", "p/a/b.md"): True,
            ("p/**/*.md", "p/a/b.rs"): False,
            ("a/*.json", "a/b/c.json"): False,
            ("rust-toolchain.toml", "rust-toolchain.toml"): True,
            ("rust-toolchain.toml", "x/rust-toolchain.toml"): False,
        }
        for (glob, path), expected in cases.items():
            with self.subTest(glob=glob, path=path):
                self.assertEqual(bool(cs.glob_regex(glob).match(path)), expected)


class DecideTest(unittest.TestCase):
    def test_untouched_area_does_not_run(self) -> None:
        self.assertFalse(Fixture(self).decide("pull_request", {"platforms/other/src/lib.rs"}))

    def test_area_change_runs(self) -> None:
        self.assertTrue(Fixture(self).decide("pull_request", {"platforms/example/src/lib.rs"}))

    def test_implicit_inputs_run(self) -> None:
        fixture = Fixture(self)
        for path in (
            ".github/workflows/example-ci.yml",  # the workflow itself
            "scripts/ci/require-successful-needs.sh",  # a file the workflow names
            cs.CONTRACT,
            cs.SELECTOR,
        ):
            with self.subTest(path=path):
                self.assertTrue(fixture.decide("pull_request", {path}))

    def test_named_file_relative_to_the_default_working_directory(self) -> None:
        fixture = Fixture(self)
        fixture.files["platforms/example/scripts/e2e.sh"] = ""
        fixture.contract["workflows"]["example-ci.yml"]["paths"] = ["rust-toolchain.toml"]
        fixture.workflow(
            ON_BLOCK + "defaults:\n  run:\n    working-directory: platforms/example\njobs:\n"
            "  e2e:\n    steps:\n      - run: bash scripts/e2e.sh\n"
        )
        self.assertTrue(fixture.decide("pull_request", {"platforms/example/scripts/e2e.sh"}))

    def test_unknown_comparison_and_manual_events_run(self) -> None:
        fixture = Fixture(self)
        self.assertTrue(fixture.decide("pull_request", None))
        self.assertTrue(fixture.decide("workflow_dispatch", set()))
        self.assertTrue(fixture.decide("schedule", set()))

    def test_merge_group_full_runs_everything(self) -> None:
        fixture = Fixture(self)
        unrelated = {"platforms/other/src/lib.rs"}
        self.assertFalse(fixture.decide("merge_group", unrelated))
        fixture.contract["workflows"]["example-ci.yml"]["merge_group"] = "full"
        self.assertTrue(fixture.decide("merge_group", unrelated))

    def test_all_zero_base_is_unknown(self) -> None:
        self.assertIsNone(cs.git_changed_paths(cs.ROOT, "0" * 40, "abc"))
        self.assertIsNone(cs.git_changed_paths(cs.ROOT, "", "abc"))


class CheckTest(unittest.TestCase):
    def test_valid_fixture_passes(self) -> None:
        self.assertEqual(Fixture(self).errors(), [])

    def assertRejected(self, fixture: Fixture, fragment: str) -> None:
        errors = fixture.errors()
        self.assertTrue(any(fragment in e for e in errors), errors)

    def test_new_unscoped_pull_request_workflow_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(ON_BLOCK + "jobs:\n  x:\n    runs-on: ubuntu-24.04\n", "new.yml")
        self.assertRejected(fixture, "new.yml: a pull-request workflow must be scoped")

    def test_schedule_only_workflow_is_exempt(self) -> None:
        fixture = Fixture(self)
        fixture.workflow("name: weekly\non:\n  schedule:\n    - cron: '0 0 * * 1'\njobs:\n  x:\n", "weekly.yml")
        self.assertEqual(fixture.errors(), [])

    def test_paths_filter_is_refused(self) -> None:
        fixture = Fixture(self)
        text = fixture.files[".github/workflows/example-ci.yml"].replace(
            "  merge_group:\n", '  merge_group:\n  push:\n    branches: [main]\n    paths:\n      - "x/**"\n'
        )
        fixture.workflow(text)
        self.assertRejected(fixture, "keeps no `paths:` filter")

    def test_job_not_gated_by_scope_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("    if: ${{ needs.scope.outputs.run == 'true' }}\n", ""))
        self.assertRejected(fixture, "job `build` must run only")

    def test_job_not_needing_scope_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("    needs: [scope]\n", ""))
        self.assertRejected(fixture, "job `build` must need `scope`")

    def test_missing_scope_job_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(ON_BLOCK + "jobs:\n" + BUILD_JOB + REQUIRED_JOB)
        self.assertRejected(fixture, "job `scope` must run")

    def test_scope_job_for_another_workflow_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("--workflow example-ci.yml", "--workflow other.yml"))
        self.assertRejected(fixture, "job `scope` must run")

    def test_required_job_without_scope_result_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.run }}\n", ""))
        self.assertRejected(fixture, "required job must pass")

    def test_stale_glob_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["workflows"]["example-ci.yml"]["paths"].append("platforms/gone/**")
        self.assertRejected(fixture, "`platforms/gone/**` names no tracked file")

    def test_xtask_workflow_must_take_xtask_inputs(self) -> None:
        fixture = Fixture(self)
        fixture.contract["workflows"]["example-ci.yml"]["paths"].remove("tools/xtask/**")
        self.assertRejected(fixture, "does not take `tools/xtask/**`")

    def test_listed_but_missing_workflow_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["unscoped"]["gone.yml"] = "reason"
        self.assertRejected(fixture, "gone.yml: listed")

    def test_unscoped_without_reason_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["unscoped"]["guard.yml"] = " "
        self.assertRejected(fixture, "states why")

    def test_merge_group_mode_is_closed(self) -> None:
        fixture = Fixture(self)
        fixture.contract["workflows"]["example-ci.yml"]["merge_group"] = "sometimes"
        self.assertRejected(fixture, "merge_group must be")


class RepositoryTest(unittest.TestCase):
    def test_the_repository_contract_holds(self) -> None:
        self.assertEqual(cs.check(), [])


if __name__ == "__main__":
    unittest.main()
