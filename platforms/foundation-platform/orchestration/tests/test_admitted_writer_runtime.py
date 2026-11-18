"""scripts/ops/admitted-writer-runtime.sh --current: a writer runs the admitted current release, or the
release its run started with (root ADR-0441).

2026-11-07 07:46Z..10:05Z the host deployed nothing while two lane cut-overs ran: a step of a run started
after a deploy would have been refused on the release the run had pinned. The run's release is
admitted by name; the name never points the writer at another release, and the build output is still
verified. Release ids are synthetic.
"""

import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
RUNTIME = PLATFORM / "scripts/ops/admitted-writer-runtime.sh"
OLD, NEW = "a" * 40, "b" * 40


@unittest.skipUnless(shutil.which("bash") and os.name == "posix", "the runtime is a host bash script")
class TheRunsRelease(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="admitted-writer-")
        self.addCleanup(temp.cleanup)
        base = pathlib.Path(temp.name)
        for release in (OLD, NEW):
            ops = base / "releases" / release / "scripts/ops"
            ops.mkdir(parents=True)
            shutil.copy(RUNTIME, ops / RUNTIME.name)
            (ops / "writer.sh").write_text('source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current\n')
        # A deploy moved `current` on while a run of the old release was going.
        (base / "current").symlink_to(pathlib.Path("releases") / NEW)
        self.writer = base / "releases" / OLD / "scripts/ops/writer.sh"

    def run_writer(self, **env):
        clean = {key: value for key, value in os.environ.items() if key != "FOUNDATION_RUN_RELEASE"}
        return subprocess.run(["bash", str(self.writer)], env={**clean, **env}, capture_output=True, text=True,
                              timeout=30)

    def test_a_writer_of_a_release_current_has_moved_on_from_is_refused(self):
        result = self.run_writer()
        self.assertEqual(result.returncode, 65)
        self.assertIn("writer must execute the admitted current release", result.stderr)

    def test_a_step_of_a_run_is_admitted_on_the_release_the_run_started_with(self):
        result = self.run_writer(FOUNDATION_RUN_RELEASE=OLD)
        self.assertNotIn("writer must execute", result.stderr)
        # Admitted by name only: the build output it names is verified the same way (none here).
        self.assertIn("trusted release build output is missing", result.stderr)

    def admitted_build(self, release):
        """A build output of `release` the runtime verifies: a read-only publisher its manifest names."""
        root = self.writer.parents[4] / "artifacts" / release
        root.mkdir(parents=True)
        publisher = root / "foundation-outbox-publisher"
        publisher.write_bytes(b"synthetic publisher")
        publisher.chmod(0o555)
        (root / "build.json").write_text(json.dumps({
            "source": release, "tippecanoe_image": "sha256:" + "0" * 64,
            "files": {"foundation-outbox-publisher": hashlib.sha256(b"synthetic publisher").hexdigest(),
                      "jars/synthetic.jar": "0" * 64}}))

    def test_a_step_a_run_starts_after_a_deploy_keeps_the_runs_release(self):
        # 2026-11-11 04:31Z: the source sweep's members step re-checked `current` after a deploy had
        # moved it and was refused, and the whole sweep failed. A run's children are the same run.
        self.admitted_build(OLD)
        base = self.writer.parents[4]
        (base / "current").unlink()
        (base / "current").symlink_to(pathlib.Path("releases") / OLD)
        ops = self.writer.parent
        (ops / "parent.sh").write_text(
            'source "$(dirname "${BASH_SOURCE[0]}")/admitted-writer-runtime.sh" --current\n'
            f'ln -sfn releases/{NEW} "{base}/current"\n'  # the deploy switches while the run goes on
            'bash "$(dirname "${BASH_SOURCE[0]}")/writer.sh" && echo child-admitted\n')
        result = subprocess.run(["bash", str(ops / "parent.sh")], capture_output=True, text=True, timeout=30,
                                env={key: value for key, value in os.environ.items() if key != "FOUNDATION_RUN_RELEASE"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("child-admitted", result.stdout)

    def test_another_releases_name_admits_nothing(self):
        result = self.run_writer(FOUNDATION_RUN_RELEASE=NEW)
        self.assertEqual(result.returncode, 65)
        self.assertIn("writer must execute the admitted current release", result.stderr)


if __name__ == "__main__":
    unittest.main()
