"""A job's code version (orchestration/dags/job_code_version.py, root ADR-0499), inferred from the tree.

2026-11-10/11: data_go_kr_file_data, the zoning bake and the real transaction Gold failed, their fixes
deployed hours later, and each waited for its next daily slot. The reconciler starts such a job once
when a release changes its code version, so the version must move with the files the run executes and
stay put for everything else. A synthetic tree pins the inference rules; the real tree pins that the
three jobs reach the files their fixes changed.
"""

import json
import pathlib
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_code_version  # noqa: E402

TREE = {
    "Cargo.toml": '[workspace]\nmembers = ["crates/*"]\n',
    "crates/tool/Cargo.toml": '[package]\nname = "foundation-tool"\n\n[dependencies]\nlib = { path = "../lib" }\n',
    "crates/tool/src/main.rs": "fn main() {}\n",
    "crates/tool/src/tests.rs": "#[test] fn t() {}\n",
    "crates/lib/Cargo.toml": '[package]\nname = "foundation-lib"\n',
    "crates/lib/src/lib.rs": "pub fn f() {}\n",
    "crates/other/Cargo.toml": '[package]\nname = "foundation-other"\n',
    "crates/other/src/main.rs": "fn main() {}\n",
    "infra/systemd/foundation-a.service": "[Service]\nExecStart=/opt/foundation-platform/current/scripts/ops/a.sh\n",
    "infra/systemd/foundation-lane@.service": "[Service]\nExecStart=/opt/foundation-platform/current/scripts/ops/lane.sh %i\n",
    "scripts/ops/a.sh": (
        '#!/usr/bin/env bash\n'
        'source "$(dirname "${BASH_SOURCE[0]}")/common.sh"\n'
        '# scripts/ops/b.sh does the other half (a comment, not code)\n'
        '"${PUBLISHER_BIN}" plan  # foundation-other is named only here, in a comment\n'
        'spark /workspace/infra/lakehouse/spark/jobs/a_job.py --contract '
        '"${RELEASE_ROOT}/infra/lakehouse/contracts/a.contract.json"\n'
        'exec foundation-tool run\n'),
    "scripts/ops/common.sh": "PUBLISHER_BIN=/opt/foundation-platform/artifacts/foundation-tool\n",
    "scripts/ops/b.sh": "echo b\n",
    "scripts/ops/lane.sh": 'echo "$1"\n',
    "infra/lakehouse/spark/jobs/a_job.py": (
        '"""Reads like b_job.py does (a docstring, not code)."""\n'
        'import helper\n'
        'SETTINGS = "x.settings.json"\n'),
    "infra/lakehouse/spark/jobs/helper.py": "VALUE = 1\n",
    "infra/lakehouse/spark/jobs/b_job.py": "VALUE = 2\n",
    "config/x.settings.json": "{}\n",
    "infra/lakehouse/contracts/a.contract.json": json.dumps({"purpose": "scripts/ops/b.sh writes the rest"}),
    "infra/lakehouse/contracts/lanes.contract.json": json.dumps({"lanes": {"east": {"rows": 1}}}),
    "infra/lakehouse/contracts/other.contract.json": json.dumps({"lanes": {"west": {}}}),
    "docs/a.md": "scripts/ops/a.sh\n",
}
A = {"id": "a", "systemd_service": "foundation-a.service"}
EAST = {"id": "east", "systemd_service": "foundation-lane@east.service"}


class Inference(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="job-code-version-")
        self.addCleanup(temp.cleanup)
        self.root = pathlib.Path(temp.name)
        for relative, text in TREE.items():
            (self.root / relative).parent.mkdir(parents=True, exist_ok=True)
            (self.root / relative).write_text(text, encoding="utf-8")
        self.clear()

    def clear(self):
        for cached in (job_code_version._tree, job_code_version._crates, job_code_version._reached,
                       job_code_version._digest, job_code_version._contract_strings):
            cached.cache_clear()

    def edit(self, relative, text):
        (self.root / relative).write_text(text, encoding="utf-8")
        self.clear()

    def test_a_job_reaches_its_unit_what_its_code_names_and_its_binarys_crates(self):
        self.assertEqual(job_code_version.files(A, self.root), sorted([
            "infra/systemd/foundation-a.service", "scripts/ops/a.sh", "scripts/ops/common.sh",
            "infra/lakehouse/spark/jobs/a_job.py", "infra/lakehouse/spark/jobs/helper.py", "config/x.settings.json",
            "infra/lakehouse/contracts/a.contract.json",
            "crates/tool/Cargo.toml", "crates/tool/src/main.rs", "crates/lib/Cargo.toml", "crates/lib/src/lib.rs",
        ]))

    def test_the_version_moves_with_a_reached_file_and_with_nothing_else(self):
        before = job_code_version.version(A, self.root)
        for relative in ("scripts/ops/b.sh", "infra/lakehouse/spark/jobs/b_job.py", "crates/tool/src/tests.rs",
                         "crates/other/src/main.rs", "docs/a.md"):
            with self.subTest(unreached=relative):
                self.edit(relative, "changed\n")
                self.assertEqual(job_code_version.version(A, self.root), before)
        for relative in ("infra/lakehouse/spark/jobs/helper.py", "crates/lib/src/lib.rs", "config/x.settings.json"):
            with self.subTest(reached=relative):
                self.edit(relative, f"changed {relative}\n")
                self.assertNotEqual(job_code_version.version(A, self.root), before)
                before = job_code_version.version(A, self.root)

    def test_a_template_instance_reaches_the_contracts_that_name_it(self):
        reached = job_code_version.files(EAST, self.root)
        self.assertIn("infra/lakehouse/contracts/lanes.contract.json", reached)
        self.assertNotIn("infra/lakehouse/contracts/other.contract.json", reached)
        self.assertEqual(job_code_version.unit_files(EAST, self.root), ["infra/systemd/foundation-lane@.service"])


class TheRealTree(unittest.TestCase):
    """Each job reaches the file its 2026-11-10/11 fix changed, and not another job's script."""

    def files(self, job_id):
        return set(job_code_version.files(job_code_version.job_of(job_id)))

    def test_the_three_jobs_reach_what_their_fixes_changed(self):
        cases = {
            "data_go_kr_file_data": "infra/lakehouse/spark/jobs/data_go_kr_file_data.py",  # #1230, the CSV escape
            "gold_parcel_real_transaction_rebuild": "infra/lakehouse/spark/jobs/real_transaction_silver_to_gold.py",  # #1262
            "zoning_tile_bake": "scripts/ops/zoning-tile-bake.sh",  # #1234
        }
        for job_id, fixed in cases.items():
            with self.subTest(job_id):
                self.assertIn(fixed, self.files(job_id))
        self.assertNotIn("scripts/ops/zoning-tile-bake.sh", self.files("data_go_kr_file_data"))
        self.assertFalse({path for path in self.files("data_go_kr_file_data") if path.startswith("docs/")})

    def test_the_command_line_prints_the_version_and_the_files(self):
        import contextlib
        import io

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(job_code_version.main(["data_go_kr_file_data"]), 0)
        self.assertRegex(out.getvalue().strip(), r"^[0-9a-f]{64}$")
        self.assertEqual(job_code_version.main(["no_such_job"]), 64)


if __name__ == "__main__":
    unittest.main()
