"""A release's generated files come from the release CI built and signed, never rendered on the host (root ADR-0419).

Against real Git objects, as test_foundation_release_admission.py: a commit with declared definitions
(root ADR-0418) commits none of what they generate. `prepare` writes the signed generated files beside
the commit's and records their digests; `seal` and `verify` hold the installed tree to both. The signed
release is a stand-in (test_release_from_registry.py proves the registry and signature path).
"""

import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[4] / "scripts/deploy/foundation-release-admission.py"
SUBTREE = "platforms/foundation-platform"
JOBS = f"{SUBTREE}/orchestration/jobs.v1.json"
GRAPH = f"{SUBTREE}/docs/catalog/pipeline-graph.v1.json"
OUTSIDE = "tools/host-memory-budget.contract.json"


class GeneratedFiles(unittest.TestCase):
    def setUp(self):
        old_umask = os.umask(0o022)
        self.addCleanup(os.umask, old_umask)
        spec = importlib.util.spec_from_file_location("release_admission", SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.sealed = set()
        for name, fake in (("set_immutable", lambda target, sealed: self.sealed.add(Path(target))),
                           ("immutable", lambda path: any(root == Path(path) or root in Path(path).parents
                                                          for root in self.sealed))):
            patcher = mock.patch.object(self.module, name, side_effect=fake)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.signed = {JOBS: b'{"jobs": []}\n', GRAPH: b'{"nodes": []}\n', OUTSIDE: b'{"memory": {}}\n'}
        self.fetches = 0

        def signed_generated(release_id):
            self.fetches += 1
            return dict(self.signed)
        for name, value in (("signed_generated", mock.Mock(side_effect=signed_generated)),):
            patcher = mock.patch.object(self.module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="release-generated-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "canonical"
        (self.source / SUBTREE / "scripts").mkdir(parents=True)
        (self.source / SUBTREE / "scripts/job.py").write_text("print('official')\n", encoding="utf-8")
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("add", ".")
        self.git("commit", "-qm", "before the definitions")
        self.before = self.git("rev-parse", "HEAD").strip()
        (self.source / SUBTREE / "definitions").mkdir()
        (self.source / SUBTREE / "definitions/platform.definition.json").write_text("{}\n", encoding="utf-8")
        self.git("add", ".")
        self.git("commit", "-qm", "declared definitions")
        self.declared = self.git("rev-parse", "HEAD").strip()

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.source), *args], text=True)

    def files(self, sha):
        return self.module.release_files(lambda *args: subprocess.check_output(["git", "-C", str(self.source), *args]), sha)

    def install(self, sha):
        archive = self.root / f"{sha}.tar"
        archive.write_bytes(subprocess.check_output(["git", "-C", str(self.source), "archive", f"{sha}:{SUBTREE}"]))
        target = self.root / "releases" / sha
        target.mkdir(parents=True)
        files = self.files(sha)
        self.module.prepare_release(files, sha, archive, target, generated=self.module.generated_release_files(sha, files))
        self.addCleanup(self.writable, target)
        self.module.seal_release(self.module.release_expected(files, target), sha, target, os.getuid())
        return target

    def verify(self, sha, target):
        self.module.verify_release(self.module.release_expected(self.files(sha), target), sha, target, os.getuid())

    @staticmethod
    def writable(path):
        for directory, _, files in os.walk(path):
            os.chmod(directory, 0o755)
            for name in files:
                os.chmod(Path(directory) / name, 0o644)

    def rewrite(self, path, data):
        path.chmod(0o644)
        path.write_bytes(data)
        path.chmod(0o444)

    def test_a_declared_release_installs_the_signed_generated_files_and_verifies(self):
        target = self.install(self.declared)
        self.verify(self.declared, target)
        self.assertEqual((target / "orchestration/jobs.v1.json").read_bytes(), self.signed[JOBS])
        self.assertFalse((target / "tools").exists(), "only the subtree's generated files are the release's")
        recorded = json.loads((target / self.module.GENERATED_FILE).read_text(encoding="utf-8"))
        self.assertEqual(recorded, {"orchestration/jobs.v1.json": hashlib.sha256(self.signed[JOBS]).hexdigest(),
                                    "docs/catalog/pipeline-graph.v1.json": hashlib.sha256(self.signed[GRAPH]).hexdigest()})
        self.assertEqual(self.fetches, 1, "the signed release is read once, at prepare; verify reads nothing remote")

    def test_a_changed_generated_file_is_refused(self):
        target = self.install(self.declared)
        self.sealed.clear()
        self.rewrite(target / "orchestration/jobs.v1.json", b'{"jobs": ["hand"]}\n')
        with self.assertRaisesRegex(ValueError, "generated file differs"):
            self.verify(self.declared, target)

    def test_a_changed_record_cannot_make_an_unsigned_file_part_of_the_release(self):
        target = self.install(self.declared)
        self.sealed.clear()
        marker = target / self.module.GENERATED_FILE
        self.rewrite(marker, json.dumps({"../escape": "0" * 64}).encode())
        with self.assertRaisesRegex(ValueError, "invalid generated file record"):
            self.verify(self.declared, target)
        self.rewrite(marker, json.dumps({"scripts/job.py": "0" * 64}).encode())
        with self.assertRaisesRegex(ValueError, "invalid generated file record"):
            self.verify(self.declared, target)

    def test_a_declared_release_without_its_record_is_refused(self):
        target = self.install(self.declared)
        self.sealed.clear()
        target.chmod(0o755)
        (target / self.module.GENERATED_FILE).unlink()
        target.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "no generated files recorded"):
            self.verify(self.declared, target)

    def test_a_release_before_the_definitions_reads_nothing_signed_and_records_nothing(self):
        target = self.install(self.before)
        self.verify(self.before, target)
        self.assertEqual(self.fetches, 0, "a release from before root ADR-0418 needs no registry")
        self.assertFalse((target / self.module.GENERATED_FILE).exists())
        self.sealed.clear()
        target.chmod(0o755)
        (target / self.module.GENERATED_FILE).write_text("{}\n", encoding="utf-8")
        (target / self.module.GENERATED_FILE).chmod(0o444)
        target.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "holds generated files"):
            self.verify(self.before, target)

    def test_a_signed_release_cannot_name_a_path_outside_the_tree(self):
        for name in ("../escape.json", "/etc/passwd", f"{SUBTREE}/./x.json", ".git/config", f"{SUBTREE}/.hidden"):
            with self.subTest(name=name):
                self.signed = {JOBS: b"{}\n", name: b"x"}
                with self.assertRaisesRegex(ValueError, "unsafe generated path"):
                    self.module.generated_release_files(self.declared, self.files(self.declared))

    def test_an_artifact_a_newer_release_adds_installs_under_an_older_control(self):
        # The running control's generator may list fewer artifacts than the release (root ADR-0419 §2).
        self.signed[f"{SUBTREE}/config/by-pnu-lane-index.v1.json"] = b'{"lanes": []}\n'
        target = self.install(self.declared)
        self.verify(self.declared, target)
        self.assertTrue((target / "config/by-pnu-lane-index.v1.json").is_file())

    def test_a_generated_file_the_commit_also_holds_is_refused(self):
        self.signed[f"{SUBTREE}/scripts/job.py"] = b"print('generated')\n"
        with self.assertRaisesRegex(ValueError, "also committed"):
            self.module.generated_release_files(self.declared, self.files(self.declared))

    def test_a_checkout_receives_the_signed_files_and_keeps_an_equal_committed_one(self):
        checkout = self.root / "control"
        (checkout / SUBTREE / "docs/catalog").mkdir(parents=True)
        (checkout / GRAPH).write_bytes(self.signed[GRAPH])
        self.assertEqual(self.module.write_generated(self.declared, checkout), 2)
        self.assertEqual((checkout / JOBS).read_bytes(), self.signed[JOBS])
        self.assertEqual((checkout / OUTSIDE).read_bytes(), self.signed[OUTSIDE])
        self.writable(checkout)
        (checkout / GRAPH).write_bytes(b"{}\n")
        with self.assertRaisesRegex(ValueError, "not the signed generated one"):
            self.module.write_generated(self.declared, checkout)

    def test_a_release_built_before_this_writes_nothing(self):
        self.signed.clear()
        checkout = self.root / "control"
        checkout.mkdir()
        self.assertEqual(self.module.write_generated(self.declared, checkout), 0)
        self.assertEqual(list(checkout.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
