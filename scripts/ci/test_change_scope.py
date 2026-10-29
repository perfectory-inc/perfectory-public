#!/usr/bin/env python3
"""The change-scope selector and its contract check (root ADR-0202)."""

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
      affected: ${{ steps.select.outputs.affected }}
    steps:
      - name: Select
        id: select
        run: python3 scripts/ci/change_scope.py select --workflow example-ci.yml
"""
BUILD_JOB = """\
  build:
    needs: [scope]
    if: ${{ needs.scope.outputs.affected == 'true' }}
    runs-on: ubuntu-24.04
    steps:
      - run: cargo xtask verify example
"""
REQUIRED_JOB = """\
  required:
    name: required/example
    if: ${{ always() && (needs.scope.result != 'success' || needs.scope.outputs.affected == 'true') }}
    needs: [scope, build]
    steps:
      - name: Require
        env:
          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.affected }}
          REQUIRED_RESULT_SCOPE: ${{ needs.scope.result }}
          REQUIRED_RESULT_BUILD: ${{ needs.build.result }}
        run: bash scripts/ci/require-successful-needs.sh
"""
ON_BLOCK = "name: example\non:\n  pull_request:\n    branches: [main]\n  merge_group:\n  schedule:\n    - cron: '0 0 * * *'\n"


class Fixture:
    """A synthetic repository: one area, one scoped workflow, one contract."""

    def __init__(self, test: unittest.TestCase) -> None:
        self.root = Path(tempfile.mkdtemp())
        test.addCleanup(shutil.rmtree, self.root)
        self.contract = {
            "workflows": {
                "example-ci.yml": {
                    "paths": ["platforms/example/**", "tools/xtask/**", ".cargo/config.toml", "rust-toolchain.toml"],
                }
            },
            "unscoped": {"guard.yml": {"reason": "reads everything", "owns": ["docs/**"]}},
            "crates": {"run_all": ["rust-toolchain.toml"], "run_all_in_workspace": [".cargo/config.toml"]},
        }
        self.files = {
            "platforms/example/src/lib.rs": "",
            "tools/xtask/src/main.rs": "",
            ".cargo/config.toml": "",
            "rust-toolchain.toml": "",
            "scripts/ci/require-successful-needs.sh": "",
            "scripts/ci/example-setup.sh": "",
            "docs/guide.md": "",
            "platforms/other/src/lib.rs": "",
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
        # A path an unscoped workflow owns is answered for; it needs no scoped run.
        self.assertFalse(Fixture(self).decide("pull_request", {"docs/guide.md"}))

    def test_path_no_area_claims_runs_everything(self) -> None:
        self.assertTrue(Fixture(self).decide("pull_request", {"platforms/other/src/lib.rs"}))
        self.assertTrue(Fixture(self).decide("pull_request", {"docs/guide.md", "lychee.toml"}))

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

    def test_merge_group_narrows_like_a_pull_request(self) -> None:
        """Root ADR-0313: the queue compares its head with its base, not everything every time."""
        fixture = Fixture(self)
        for event in ("pull_request", "merge_group"):
            with self.subTest(event=event):
                self.assertFalse(fixture.decide(event, {"docs/guide.md"}))
                self.assertTrue(fixture.decide(event, {"platforms/example/src/lib.rs"}))
                self.assertTrue(fixture.decide(event, None))

class QueueVerifiedTest(unittest.TestCase):
    """Root ADR-0313: a push to main the merge queue passed on the same tree does nothing."""

    RUNS = [{"id": 2, "head_commit": {"tree_id": "other"}}, {"id": 1, "html_url": "u1", "head_commit": {"tree_id": "t"}}]

    def test_a_push_on_a_tree_the_queue_passed_is_verified(self) -> None:
        self.assertEqual(cs.queue_verified("push", "t", lambda: self.RUNS), "u1")

    def test_another_tree_or_event_is_not(self) -> None:
        self.assertIsNone(cs.queue_verified("push", "new", lambda: self.RUNS))
        self.assertIsNone(cs.queue_verified("push", None, lambda: self.RUNS))
        for event in ("pull_request", "merge_group", "schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assertIsNone(cs.queue_verified(event, "t", lambda: self.RUNS))

    def test_unreadable_runs_run_everything(self) -> None:
        def unreadable():
            raise OSError("403")

        self.assertIsNone(cs.queue_verified("push", "t", unreadable))
        self.assertIsNone(cs.queue_verified("push", "t", lambda: [{"head_commit": None}]))

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

    def test_a_merge_group_workflow_without_a_schedule_is_refused(self) -> None:
        fixture = Fixture(self)
        text = fixture.files[".github/workflows/example-ci.yml"]
        fixture.workflow(text.replace("  schedule:\n    - cron: '0 0 * * *'\n", ""))
        self.assertRejected(fixture, "keeps a `schedule:` full run")

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
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("    if: ${{ needs.scope.outputs.affected == 'true' }}\n", ""))
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
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace("          CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.affected }}\n", ""))
        self.assertRejected(fixture, "required job must pass")

    def test_required_job_that_always_runs_is_refused(self) -> None:
        """Root ADR-0324: an untouched area's required job is skipped, not run to say so."""
        fixture = Fixture(self)
        fixture.workflow(fixture.files[".github/workflows/example-ci.yml"].replace(cs.REQUIRED_IF, "${{ always() }}"))
        self.assertRejected(fixture, "the required job must run `if:")

    def test_a_scope_job_listing_skipped_jobs_gates_each_job_and_the_result_on_it(self) -> None:
        fixture = Fixture(self)
        listing = (
            fixture.files[".github/workflows/example-ci.yml"]
            .replace("      affected: ${{ steps.select.outputs.affected }}\n",
                     "      affected: ${{ steps.select.outputs.affected }}\n      " + cs.SKIPPED_OUTPUT + "\n")
        )
        fixture.workflow(listing)
        self.assertRejected(fixture, "job `build` must run only `if: " + cs.job_if("build"))
        self.assertRejected(fixture, "must pass `CHANGE_SCOPE_SKIPPED")
        gated = listing.replace(cs.SCOPE_IF, cs.job_if("build")).replace(
            "          " + cs.SCOPE_ENV + "\n", "          " + cs.SCOPE_ENV + "\n          " + cs.SKIPPED_ENV + "\n"
        )
        fixture.workflow(gated)
        self.assertEqual(fixture.errors(), [])
        # Another job's name in the condition does not gate this one.
        fixture.workflow(gated.replace("'build') }}", "'other') }}"))
        self.assertRejected(fixture, "job `build` must run only")

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
        fixture.contract["unscoped"]["gone.yml"] = {"reason": "r", "owns": []}
        self.assertRejected(fixture, "gone.yml: listed")

    def test_unscoped_without_reason_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["unscoped"]["guard.yml"]["reason"] = " "
        self.assertRejected(fixture, "states why")

    def test_stale_owned_glob_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["unscoped"]["guard.yml"]["owns"].append("gone/**")
        self.assertRejected(fixture, "owned glob `gone/**` names no tracked file")


class ReadScanTest(unittest.TestCase):
    """A file an area's sources read outside the area must be an input (the #456 review)."""

    def scan(self, source: str, text: str, extra: dict[str, str] | None = None) -> list[str]:
        fixture = Fixture(self)
        fixture.files[source] = text
        fixture.files.update(extra or {})
        return fixture.errors()

    def test_relative_read_outside_the_area_is_refused(self) -> None:
        errors = self.scan(
            "platforms/example/web/edge.test.ts",
            'readFileSync(resolve(__dirname, "../../other/contract.json"), "utf8");\n',
            {"platforms/other/contract.json": "{}"},
        )
        self.assertTrue(any("reads `platforms/other/contract.json`" in e for e in errors), errors)

    def test_root_relative_read_is_refused_and_listing_it_clears_it(self) -> None:
        source = "platforms/example/src/lib.rs"
        text = 'const S: &str = include_str!("../../../docs/guide.md");\n'
        self.assertTrue(any("reads `docs/guide.md`" in e for e in self.scan(source, text)))
        fixture = Fixture(self)
        fixture.files[source] = text
        fixture.contract["workflows"]["example-ci.yml"]["paths"].append("docs/guide.md")
        self.assertEqual(fixture.errors(), [])

    def test_directory_is_narrowed_by_the_names_joined_onto_it(self) -> None:
        errors = self.scan(
            "platforms/example/web/check.mjs",
            'const docs = resolve(here, "../../other");\nconst pairs = [["contract.json", "x.d.ts"]];\n',
            {"platforms/other/contract.json": "{}", "platforms/other/unread.json": "{}"},
        )
        self.assertTrue(any("reads `platforms/other/contract.json`" in e for e in errors), errors)
        self.assertFalse(any("unread.json" in e or "reads `platforms/other`" in e for e in errors), errors)

    def test_comment_is_not_a_read(self) -> None:
        self.assertEqual(self.scan("platforms/example/src/a.rs", '// see "docs/guide.md"\n'), [])

    def test_read_inside_the_area_needs_nothing(self) -> None:
        self.assertEqual(self.scan("platforms/example/src/b.rs", 'include_str!("lib.rs");\n'), [])


class CratesTest(unittest.TestCase):
    """Which crates of a workspace a pull request verifies (root ADR-0269)."""

    WS = "platforms/ws"
    # kernel <- domain <- api; tool depends on nothing; app dev-depends on domain.
    MEMBERS = {
        "kernel": ("platforms/ws/crates/kernel", set()),
        "domain": ("platforms/ws/crates/domain", {"kernel"}),
        "api": ("platforms/ws/services/api", {"domain"}),
        "app": ("platforms/ws/services/app", {"domain"}),
        "tool": ("platforms/ws/tools/tool", set()),
    }
    CONTRACT = {
        "crates": {"run_all": ["**/Cargo.lock", "tools/xtask/**"], "run_all_in_workspace": ["Cargo.toml", ".config/**"]}
    }

    def decide(self, changed, event="pull_request", reads=None):
        return cs.decide_crates(self.CONTRACT, self.WS, event, changed, self.MEMBERS, reads or {})

    def test_a_change_in_one_crate_selects_it_and_its_dependents_only(self) -> None:
        selected, direct, _, _ = self.decide({"platforms/ws/crates/domain/src/lib.rs"})
        self.assertEqual(direct, {"domain"})
        self.assertEqual(selected, {"domain", "api", "app"})

    def test_a_leaf_crate_selects_itself(self) -> None:
        selected, _, _, _ = self.decide({"platforms/ws/services/api/tests/route.rs"})
        self.assertEqual(selected, {"api"})

    def test_a_full_run_trigger_selects_everything(self) -> None:
        for path in (
            "platforms/ws/Cargo.lock",
            "platforms/ws/Cargo.toml",
            "platforms/ws/.config/nextest.toml",
            "tools/xtask/src/main.rs",
            cs.CONTRACT,
            cs.SELECTOR,
            "platforms/ws/src/stray.rs",
        ):
            with self.subTest(path=path):
                selected, _, _, reason = self.decide({path, "platforms/ws/services/api/src/lib.rs"})
                self.assertIsNone(selected, reason)

    def test_events_other_than_a_pull_request_or_the_queue_select_everything(self) -> None:
        self.assertEqual(self.decide({"platforms/ws/services/api/src/lib.rs"}, "merge_group")[0], {"api"})
        for event in ("push", "schedule", "workflow_dispatch", ""):
            with self.subTest(event=event):
                self.assertIsNone(self.decide({"platforms/ws/services/api/src/lib.rs"}, event)[0])
        self.assertIsNone(self.decide(None)[0])

    def test_a_file_outside_every_crate_selects_its_readers(self) -> None:
        reads = {"api": {"platforms/ws/config/routes.json"}, "kernel": {"platforms/ws/migrations"}}
        self.assertEqual(self.decide({"platforms/ws/config/routes.json"}, reads=reads)[0], {"api"})
        selected, direct, _, _ = self.decide({"platforms/ws/migrations/0001_a.sql"}, reads=reads)
        self.assertEqual(direct, {"kernel"})
        self.assertEqual(selected, set(self.MEMBERS) - {"tool"})

    def test_a_file_no_crate_reads_selects_nothing(self) -> None:
        selected, direct, _, _ = self.decide({"platforms/ws/scripts/job.py", "docs/guide.md"})
        self.assertEqual((selected, direct), (set(), set()))

    def test_member_reads_resolve_quoted_paths_and_deleted_files(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        source = "platforms/ws/services/api/src/lib.rs"
        (root / source).parent.mkdir(parents=True)
        (root / source).write_text(
            'const C: &str = include_str!("../../../config/routes.json");\n'
            'static M: Migrator = sqlx::migrate!("../../migrations");\n',
            encoding="utf-8",
        )
        # routes.json is deleted on this branch: it is in the list only because it changed.
        tracked = [source, "platforms/ws/config/routes.json", "platforms/ws/migrations/0001_a.sql"]
        reads = cs.member_reads(root, self.WS, self.MEMBERS, tracked)
        self.assertEqual(reads["api"], {"platforms/ws/config/routes.json", "platforms/ws/migrations"})

    def test_a_test_that_walks_every_crate_reads_every_crate(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        source = "platforms/ws/tools/tool/tests/one_caller.rs"
        (root / source).parent.mkdir(parents=True)
        (root / source).write_text(
            'for top in ["crates", "services", "unrelated"] {}\n// "tools" in a comment is no read\n'
            'let doc = json!({"tools": {}}); let image = &doc["tools"]["x"];\n',
            encoding="utf-8",
        )
        reads = cs.member_reads(root, self.WS, self.MEMBERS, [source])
        self.assertEqual(reads["tool"], {"platforms/ws/crates", "platforms/ws/services"})
        selected, _, _, _ = self.decide({"platforms/ws/services/api/src/lib.rs"}, reads=reads)
        self.assertEqual(selected, {"api", "tool"})

    def test_a_walk_over_every_crate_reruns_the_walker_and_not_its_dependents(self) -> None:
        # domain's test walks every crate under services/; a data file inside api changes: api runs
        # (as the crate holding it), domain runs (it reads it), domain's other dependents do not.
        reads = {"domain": {"platforms/ws/services"}}
        selected, direct, readers, _ = self.decide({"platforms/ws/services/api/fixtures/example.json"}, reads=reads)
        self.assertEqual((direct, readers), ({"api"}, {"domain"}))
        self.assertEqual(selected, {"api", "domain"})
        # A file a crate names itself may be compiled in: that crate's dependents still run.
        reads = {"domain": {"platforms/ws/services/api/fixtures/example.json"}}
        selected, direct, _, _ = self.decide({"platforms/ws/services/api/fixtures/example.json"}, reads=reads)
        self.assertEqual(direct, {"api", "domain"})
        self.assertEqual(selected, {"api", "domain", "app"})

    def test_a_crate_whose_tests_read_another_crates_source_is_selected_with_it(self) -> None:
        # tool's test reads kernel's source as text: tool runs, but tool's dependents do not.
        reads = {"tool": {"platforms/ws/crates/kernel/src/lib.rs"}, "api": {"platforms/ws/crates/domain"}}
        selected, direct, readers, _ = self.decide({"platforms/ws/crates/kernel/src/lib.rs"}, reads=reads)
        self.assertEqual((direct, readers), ({"kernel"}, {"tool"}))
        self.assertEqual(selected, set(self.MEMBERS))
        _, direct, readers, _ = self.decide({"platforms/ws/crates/domain/src/lib.rs"}, reads=reads)
        self.assertEqual((direct, readers), ({"domain"}, set()), "api is selected as domain's dependent")

    def test_a_data_file_inside_a_crate_selects_its_readers_with_their_dependents(self) -> None:
        reads = {"domain": {"platforms/ws/tools/tool/fixtures/rows.json"}}
        selected, direct, readers, _ = self.decide({"platforms/ws/tools/tool/fixtures/rows.json"}, reads=reads)
        self.assertEqual((direct, readers), ({"tool", "domain"}, set()))
        self.assertEqual(selected, {"tool", "domain", "api", "app"})

    def test_closure_follows_dependents_transitively(self) -> None:
        self.assertEqual(cs.closure(self.MEMBERS, {"kernel"}), {"kernel", "domain", "api", "app"})
        self.assertEqual(cs.closure(self.MEMBERS, set()), set())

    def test_a_stale_run_all_glob_is_refused(self) -> None:
        fixture = Fixture(self)
        fixture.contract["crates"]["run_all"].append("gone/**")
        self.assertTrue(any("crates.run_all: `gone/**` names no tracked file" in e for e in fixture.errors()))


class SuitesTest(unittest.TestCase):
    """Whether a pull request runs a workspace's Python and Node suites (root ADR-0269)."""

    WS = CratesTest.WS
    MEMBERS = CratesTest.MEMBERS
    CONTRACT = CratesTest.CONTRACT

    def decide(self, changed, event="pull_request", reads=frozenset()):
        return cs.decide_suites(self.CONTRACT, self.WS, event, changed, self.MEMBERS, set(reads))[0]

    def test_a_rust_only_change_skips_the_suites(self) -> None:
        self.assertFalse(self.decide({"platforms/ws/services/api/src/lib.rs", "platforms/ws/crates/kernel/Cargo.toml"}))
        self.assertFalse(self.decide({"platforms/other/src/lib.rs", "docs/guide.md"}))

    def test_a_change_outside_every_crate_runs_them(self) -> None:
        self.assertTrue(self.decide({"platforms/ws/services/api/src/lib.rs", "platforms/ws/scripts/job.py"}))

    def test_a_crate_file_a_suite_reads_runs_them(self) -> None:
        reads = {"platforms/ws/crates/kernel/fixtures"}
        self.assertTrue(self.decide({"platforms/ws/crates/kernel/fixtures/rows.json"}, reads=reads))
        self.assertTrue(self.decide({"docs/contract.json"}, reads={"docs/contract.json"}))

    def test_run_all_and_other_events_run_them(self) -> None:
        self.assertTrue(self.decide({"platforms/ws/Cargo.lock"}))
        self.assertTrue(self.decide({"platforms/ws/services/api/src/lib.rs"}, event="push"))
        self.assertTrue(self.decide(None))

    def test_suite_reads_scan_non_rust_sources_only(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        files = {
            "platforms/ws/scripts/test_job.py": 'open("platforms/ws/crates/kernel/fixtures/rows.json")\n',
            "platforms/ws/crates/kernel/src/lib.rs": 'include_str!("../../../config/routes.json");\n',
            "platforms/ws/crates/kernel/fixtures/rows.json": "[]",
            "platforms/ws/config/routes.json": "{}",
        }
        for path, text in files.items():
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text(text, encoding="utf-8")
        self.assertEqual(cs.suite_reads(root, self.WS, list(files)), {"platforms/ws/crates/kernel/fixtures/rows.json"})


class SuiteModulesTest(unittest.TestCase):
    """Which Python test modules and Node packages a pull request runs (root ADR-0269)."""

    WS = "platforms/ws"
    FILES = {
        "platforms/ws/jobs/kernel.py": "VALUE = 1\n",
        "platforms/ws/jobs/gold.py": "import kernel\n",
        "platforms/ws/jobs/other.py": "X = 2\n",
        "platforms/ws/scripts/bake.sh": 'python3 "platforms/ws/jobs/gold.py"\n',
        "platforms/ws/config/routes.json": "{}\n",
        "platforms/ws/tests/test_gold.py": "from jobs import gold\n",
        "platforms/ws/tests/test_bake.py": 'subprocess.run(["bash", "platforms/ws/scripts/bake.sh"])\n',
        "platforms/ws/tests/test_routes.py": 'open("platforms/ws/config/routes.json")\n',
        "platforms/ws/tests/test_other.py": "import other\n",
        "platforms/ws/jobs/mentions.py": 'NOTE = "test_other.py"\n',
        "platforms/ws/tests/test_mentions.py": "import mentions\n",
        "platforms/ws/services/gw/package.json": "{}\n",
        "platforms/ws/services/gw/src/index.ts": 'readFileSync("platforms/ws/config/routes.json")\n',
        "platforms/ws/data/orphan.csv": "a,b\n",
    }

    def select(self, changed: set[str]):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        for path, text in self.FILES.items():
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text(text, encoding="utf-8")
        tracked = sorted(self.FILES)
        return cs.select_suite_modules(self.WS, changed, tracked, cs.dependency_graph(root, tracked))

    def test_a_module_selects_the_tests_that_import_it_at_any_depth(self) -> None:
        tests, nodes, _ = self.select({"platforms/ws/jobs/kernel.py"})
        self.assertIn("platforms/ws/tests/test_gold.py", tests, "test_gold imports gold, which imports kernel")
        self.assertIn("platforms/ws/tests/test_bake.py", tests, "test_bake starts bake.sh, which runs gold")
        self.assertNotIn("platforms/ws/tests/test_other.py", tests)
        self.assertEqual(nodes, set())

    def test_a_file_selects_the_tests_and_packages_that_name_it(self) -> None:
        tests, nodes, _ = self.select({"platforms/ws/config/routes.json"})
        self.assertEqual(tests, {"platforms/ws/tests/test_routes.py"})
        self.assertEqual(nodes, {"platforms/ws/services/gw"})

    def test_a_changed_test_runs_itself_and_pulls_in_nothing(self) -> None:
        tests, _, _ = self.select({"platforms/ws/tests/test_other.py"})
        self.assertEqual(tests, {"platforms/ws/tests/test_other.py"}, "mentions.py names it; code never depends on a test")

    def test_a_file_nothing_names_runs_every_suite(self) -> None:
        tests, _, reason = self.select({"platforms/ws/data/orphan.csv"})
        self.assertIsNone(tests, reason)

    def test_a_run_all_input_or_another_event_runs_every_suite(self) -> None:
        """#710 changed a workflow and this selector; the suites must all run, not 7 modules."""
        contract = CratesTest.CONTRACT
        members = CratesTest.MEMBERS
        unreachable = lambda: self.fail("a run-all answer must not need the graph")  # noqa: E731
        for event, changed in (
            ("pull_request", {"tools/xtask/src/main.rs", "platforms/ws/scripts/job.py"}),
            ("pull_request", {cs.SELECTOR}),
            ("push", {"platforms/ws/scripts/job.py"}),
            ("pull_request", None),
        ):
            with self.subTest(event=event, changed=changed):
                kind, tests, nodes, reason = cs.suite_answer(
                    contract, "platforms/ws", event, changed, members, set(), [], unreachable
                )
                self.assertEqual(kind, "all", reason)

    def test_documentation_and_rust_reach_no_suite(self) -> None:
        tests, nodes, _ = self.select({"platforms/ws/README.md", "platforms/ws/crates/a/src/lib.rs"})
        self.assertEqual((tests, nodes), (set(), set()))


class RepositoryTest(unittest.TestCase):
    def test_the_repository_contract_holds(self) -> None:
        self.assertEqual(cs.check(), [])


if __name__ == "__main__":
    unittest.main()
