"""Exercise release admission against real Git objects, without production access."""

import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[4] / "scripts/deploy/foundation-release-admission.py"


class ReleaseAdmissionTests(unittest.TestCase):
    def setUp(self):
        old_umask = os.umask(0o022)
        self.addCleanup(os.umask, old_umask)
        self.assertTrue(SCRIPT.is_file(), "the trusted release admission entry point is missing")
        spec = importlib.util.spec_from_file_location("release_admission", SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory(prefix="foundation-release-admission-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "canonical"
        self.source.mkdir()
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Release test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.area = self.source / "platforms/foundation-platform"
        (self.area / "scripts").mkdir(parents=True)
        (self.area / "scripts/job.py").write_text("print('official')\n", encoding="utf-8")
        (self.area / "scripts/ops").mkdir()
        (self.area / "scripts/ops/admitted-writer-runtime.sh").write_text("# canonical runtime fixture\n")
        self.git("add", ".")
        self.git("commit", "-qm", "merged release")
        self.merged = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "-qb", "private-feature")
        (self.area / "scripts/job.py").write_text("print('private')\n", encoding="utf-8")
        self.git("commit", "-qam", "unmerged code")
        self.unmerged = self.git("rev-parse", "HEAD").strip()
        self.git("checkout", "-q", "main")
        self.archive = self.root / "release.tar"
        self.archive.write_bytes(subprocess.check_output([
            "git", "-C", str(self.source), "archive", f"{self.merged}:platforms/foundation-platform"
        ]))
        self.target = self.root / "releases" / self.merged
        self.target.mkdir(parents=True)
        self.target.parent.chmod(0o755)

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.source), *args], text=True)

    def expected(self, sha=None):
        # Only the network transport is substituted: ancestry and archive bytes are real Git.
        def git(*args):
            return subprocess.check_output(["git", "-C", str(self.source), *args])
        return self.module.release_files(git, sha or self.merged)

    def install(self):
        self.module.prepare_release(self.expected(), self.merged, self.archive, self.target)
        self.addCleanup(lambda: self.make_writable(self.target))

    def verify(self):
        self.module.verify_release(self.expected(), self.merged, self.target, os.getuid())

    @staticmethod
    def make_writable(path):
        if not path.exists():
            return
        for directory, _, files in os.walk(path):
            os.chmod(directory, 0o755)
            for file in files:
                os.chmod(Path(directory) / file, 0o644)

    def test_real_merged_archive_installs_and_verifies(self):
        self.install()
        self.verify()
        self.assertEqual((self.target / "scripts/job.py").read_bytes(), b"print('official')\n")

    def test_known_unmerged_commit_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "canonical main"):
            self.expected(self.unmerged)

    def test_merged_sha_cannot_label_different_archive_bytes(self):
        self.archive.write_bytes(subprocess.check_output([
            "git", "-C", str(self.source), "archive", f"{self.unmerged}:platforms/foundation-platform"
        ]))
        with self.assertRaisesRegex(ValueError, "contents"):
            self.module.prepare_release(self.expected(), self.merged, self.archive, self.target)
        self.assertEqual(list(self.target.iterdir()), [])

    def test_self_declared_markers_do_not_authorize_a_tree(self):
        (self.target / ".foundation-release-id").write_text(self.merged)
        (self.target / ".foundation-release-archive-sha256").write_text("a" * 64)
        with self.assertRaises(ValueError):
            self.verify()

    def test_changed_source_after_install_is_rejected(self):
        self.install()
        path = self.target / "scripts/job.py"
        path.chmod(0o644)
        path.write_text("print('private')\n")
        path.chmod(0o444)
        with self.assertRaisesRegex(ValueError, "contents"):
            self.verify()

    def test_writable_source_is_rejected_even_if_bytes_match(self):
        self.install()
        (self.target / "scripts/job.py").chmod(0o644)
        with self.assertRaisesRegex(ValueError, "writable"):
            self.verify()

    def test_extra_importable_file_is_rejected(self):
        self.install()
        self.target.chmod(0o755)
        (self.target / "sitecustomize.py").write_text("pass\n")
        (self.target / "sitecustomize.py").chmod(0o444)
        self.target.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "file set"):
            self.verify()

    def test_archive_links_and_path_traversal_are_rejected_before_install(self):
        for name, kind in [("../escape", tarfile.REGTYPE), ("scripts/job.py", tarfile.SYMTYPE)]:
            with self.subTest(name=name, kind=kind):
                with tarfile.open(self.archive, "w") as archive:
                    item = tarfile.TarInfo(name)
                    item.type = kind
                    item.linkname = "/tmp/other"
                    archive.addfile(item, io.BytesIO())
                with self.assertRaises(ValueError):
                    self.module.prepare_release(self.expected(), self.merged, self.archive, self.target)
                self.assertEqual(list(self.target.iterdir()), [])

    def test_world_writable_parent_is_rejected(self):
        self.install()
        self.target.parent.chmod(0o777)
        self.addCleanup(lambda: self.target.parent.chmod(0o755))
        with self.assertRaisesRegex(ValueError, "writable"):
            self.verify()

    def test_symlink_and_hardlink_in_installed_tree_are_rejected(self):
        self.install()
        original = self.target / "scripts/job.py"
        directory = original.parent
        directory.chmod(0o755)
        original.unlink()
        original.symlink_to(self.area / "scripts/job.py")
        directory.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "link"):
            self.verify()
        directory.chmod(0o755)
        original.unlink()
        os.link(self.area / "scripts/job.py", original)
        original.chmod(0o444)
        directory.chmod(0o555)
        with self.assertRaisesRegex(ValueError, "hard-linked"):
            self.verify()

    def network_fixture(self):
        source = self.module.CanonicalSource.__new__(self.module.CanonicalSource)
        source.identity = self.root / "identity.json"
        policy = {
            "hostname": "github.com", "full_name": "perfectory-inc/perfectory-public",
            "repository_id": 100, "owner": {"id": 200},
        }
        source.identity.write_text(json.dumps(policy))
        source.identity_reader = self.root / "identity-reader"
        source.git = mock.Mock()
        source.check_cache = mock.Mock()
        return source, policy

    def test_changed_remote_repository_identity_cannot_admit_source(self):
        source, identity = self.network_fixture()
        identity["repository_id"] += 1
        with mock.patch.object(self.module.subprocess, "check_output", return_value=json.dumps(identity).encode()):
            with self.assertRaisesRegex(ValueError, "repository identity"):
                source.refresh()
        source.git.assert_not_called()

    def test_admission_fetches_literal_canonical_main_without_caller_environment(self):
        source, identity = self.network_fixture()
        with mock.patch.object(self.module.subprocess, "check_output", return_value=json.dumps(identity).encode()) as reader, \
                mock.patch.object(self.module, "SOURCE_CACHE", self.source), \
                mock.patch.object(self.module, "protected_path"), \
                mock.patch.dict(os.environ, {"GIT_DIR": "/private", "GH_HOST": "untrusted.invalid", "PYTHONPATH": "/private"}):
            source.refresh()
        source.git.assert_called_once_with(
            "fetch", "--no-tags", "https://github.com/perfectory-inc/perfectory-public.git",
            "+refs/heads/main:refs/heads/main",
        )
        self.assertNotIn("GIT_DIR", reader.call_args.kwargs["env"])
        self.assertNotIn("GH_HOST", reader.call_args.kwargs["env"])
        self.assertNotIn("PYTHONPATH", reader.call_args.kwargs["env"])

    def artifacts(self):
        self.install()
        artifacts = self.root / "artifacts" / self.merged
        artifacts.mkdir(parents=True)
        (artifacts / "jars").mkdir()
        (artifacts / "jars/iceberg.jar").write_bytes(b"resolved canonical dependency")
        (artifacts / "foundation-outbox-publisher").write_bytes(b"built canonical binary")
        self.module.seal_artifacts(self.merged, artifacts, "sha256:" + "a" * 64)
        self.addCleanup(lambda: self.make_writable(artifacts))
        return artifacts

    def test_trusted_build_artifacts_verify_without_a_mutable_cache(self):
        artifacts = self.artifacts()
        self.module.verify_artifacts(self.merged, artifacts, os.getuid())

    def test_binary_or_jar_substitution_is_rejected(self):
        artifacts = self.artifacts()
        for name in ("foundation-outbox-publisher", "jars/iceberg.jar"):
            with self.subTest(name=name):
                path = artifacts / name
                original = path.read_bytes()
                path.chmod(0o644)
                path.write_bytes(b"private code")
                path.chmod(0o555 if name == "foundation-outbox-publisher" else 0o444)
                with self.assertRaisesRegex(ValueError, "artifact contents"):
                    self.module.verify_artifacts(self.merged, artifacts, os.getuid())
                path.chmod(0o644)
                path.write_bytes(original)
                path.chmod(0o555 if name == "foundation-outbox-publisher" else 0o444)

    def test_extra_jar_and_wrong_source_artifacts_are_rejected(self):
        artifacts = self.artifacts()
        with self.assertRaisesRegex(ValueError, "source"):
            self.module.verify_artifacts(self.unmerged, artifacts, os.getuid())
        (artifacts / "jars").chmod(0o755)
        (artifacts / "jars/private.jar").write_bytes(b"extra dependency")
        (artifacts / "jars/private.jar").chmod(0o444)
        (artifacts / "jars").chmod(0o555)
        with self.assertRaisesRegex(ValueError, "artifact file set"):
            self.module.verify_artifacts(self.merged, artifacts, os.getuid())

    def test_mutable_artifact_metadata_cannot_authorize_execution(self):
        artifacts = self.artifacts()
        (artifacts / "build.json").chmod(0o666)
        with self.assertRaisesRegex(ValueError, "writable"):
            self.module.verify_artifacts(self.merged, artifacts, os.getuid())

    def exercise_builder(self):
        self.install()
        calls = []
        self.build_calls = calls
        def run(args, **kwargs):
            calls.append(args)
            self.assertEqual(kwargs["cwd"], self.target)
            process_env = dict(kwargs["env"])
            docker_config = process_env.pop("DOCKER_CONFIG", None)
            self.assertEqual(process_env, self.module.control_environment())
            if args[0] == "/usr/bin/docker":
                self.assertIsNotNone(docker_config)
                self.assertEqual(Path(docker_config).stat().st_mode & 0o777, 0o700)
            self.assertNotIn("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN", kwargs["env"])
            if args[1:3] == ["buildx", "create"]:
                self.assertIn("docker-container", args)
                self.assertRegex(self.module.BUILDKIT_IMAGE, r"@sha256:[0-9a-f]{64}$")
                self.assertIn("image=" + self.module.BUILDKIT_IMAGE + ",memory=4g,memory-swap=4g,cpu-period=100000,cpu-quota=200000,restart-policy=no", args)
            elif args[1:3] == ["buildx", "build"]:
                self.assertIn("--no-cache", args)
                self.assertIn("CARGO_BUILD_JOBS=2", args)
                if getattr(self, "fail_build", False):
                    raise subprocess.CalledProcessError(97, args)
                Path(args[args.index("--iidfile") + 1]).write_text("sha256:" + "a" * 64)
            elif args[1] == "create":
                self.assertEqual(args[2], "sha256:" + "a" * 64)
                return b"fixture-container"
            elif args[1] == "cp":
                Path(args[-1]).write_bytes(b"actual fixture publisher output")
            elif args[1] == "compose":
                self.assertEqual(args[2:4], ["--env-file", "/dev/null"])
                return json.dumps({"services": {"spark": {"image": "spark@sha256:" + "b" * 64}}}).encode()
            elif args[0] == "/usr/bin/python3":
                return b"fixture:dependency:1\n"
            elif args[1] == "run":
                mount = next(arg for arg in args if arg.startswith("type=bind,src=") and arg.endswith(",dst=/resolve"))
                cache = Path(mount.removeprefix("type=bind,src=").removesuffix(",dst=/resolve"))
                self.assertEqual(cache.parent.stat().st_mode & 0o777, 0o700)
                (cache / "jars").mkdir()
                (cache / "jars/dependency.jar").write_bytes(b"actual fixture resolved jar")
                self.assertIn("fixture:dependency:1", args)
                self.assertNotIn("-e", args)
            return b""
        artifact_root = self.root / "artifacts"
        real_verify = self.module.verify_artifacts
        real_rename = Path.rename
        def privileged_rename(path, destination):
            # Production runs as root. Non-root POSIX fixtures need owner-write on the
            # directory to change its '..' when moving it to another parent.
            path.chmod(0o755)
            result = real_rename(path, destination)
            result.chmod(0o555)
            return result
        with mock.patch.object(self.module, "ARTIFACT_ROOT", artifact_root), \
                mock.patch.object(self.module, "protected_path"), \
                mock.patch.object(Path, "rename", autospec=True, side_effect=privileged_rename), \
                mock.patch.object(self.module, "verify_artifacts", side_effect=lambda sha, path: real_verify(sha, path, os.getuid())), \
                mock.patch.object(self.module.subprocess, "check_output", side_effect=run), \
                mock.patch.dict(os.environ, {"FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN": "fixture-secret"}):
            self.module.build_artifacts(self.target)
        self.addCleanup(lambda: self.make_writable(artifact_root))
        real_verify(self.merged, artifact_root / self.merged, os.getuid())
        self.assertTrue(any(call[1] == "rm" for call in calls))

    def test_builder_uses_admitted_context_and_clean_process_environment(self):
        self.exercise_builder()

    def test_build_failure_removes_owned_builder_without_publishing_artifacts(self):
        self.fail_build = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.exercise_builder()
        create = next(call for call in self.build_calls if call[1:3] == ["buildx", "create"])
        builder = create[create.index("--name") + 1]
        self.assertEqual(self.build_calls[-1], ["/usr/bin/docker", "buildx", "rm", builder])
        self.assertEqual(list((self.root / "artifacts").iterdir()), [])

    def test_legacy_release_cannot_build_artifacts_for_unbound_jobs(self):
        legacy = self.root / "legacy-release"
        legacy.mkdir()
        with self.assertRaisesRegex(ValueError, "predates"):
            self.module.build_artifacts(legacy)

    def test_real_engine_import_cannot_write_bytecode_into_release(self):
        official = SCRIPT.parents[2] / "platforms/foundation-platform/infra/lakehouse"
        lakehouse = self.area / "infra/lakehouse"
        (lakehouse / "spark/jobs").mkdir(parents=True)
        (lakehouse / "contracts").mkdir()
        for name in ("spark/jobs/lakehouse_engine.py", "contracts/lakehouse-engine.contract.json"):
            (lakehouse / name).write_bytes((official / name).read_bytes())
        # Deliberately writable: read-only permissions would hide the bug in a non-root test,
        # while the production root builder can create a pyc through 0555 source permissions.
        packages = self.module.engine_packages(self.area)
        self.assertIn("org.apache.iceberg:", packages)
        self.assertEqual(list(lakehouse.rglob("__pycache__")), [])


class RegisteredRuntimeTests(unittest.TestCase):
    area = SCRIPT.parents[2] / "platforms/foundation-platform"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="foundation-runtime-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.release = self.root / "releases" / ("a" * 40)
        (self.release / "scripts/ops").mkdir(parents=True)
        (self.root / "current").symlink_to(self.release)
        self.artifacts = self.root / "artifacts" / self.release.name
        self.artifacts.mkdir(parents=True)
        (self.artifacts / "foundation-outbox-publisher").write_text("fixture")
        (self.artifacts / "foundation-outbox-publisher").chmod(0o555)
        (self.artifacts / "build.json").write_text(json.dumps({"files": {"jars/official.jar": "fixture"}}))
        text = (self.area / "scripts/ops/admitted-writer-runtime.sh").read_text()
        # Relocate only the fixed host installation prefix in the test copy. There is no
        # production prefix override; arbitrary real operator overrides remain hostile inputs.
        self.helper = self.release / "scripts/ops/admitted-writer-runtime.sh"
        self.helper.write_text(text.replace("/opt/foundation-platform", str(self.root)))

    def test_environment_cannot_select_private_source_binary_or_ivy(self):
        env = dict(os.environ, FOUNDATION_MAP_EDIT_FOLD_RELEASE_ROOT="/private/source",
                   FOUNDATION_MAP_EDIT_FOLD_PUBLISHER_BIN="/private/binary",
                   FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="/private/ivy",
                   FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE="rw")
        result = subprocess.run(["bash", "-eu", "-c", 'source "$1"; printf "%s\\n" "$RELEASE_ROOT" "$PUBLISHER_BIN" "$FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE" "$FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE" "$SPARK_RELEASE_JARS"', "test", str(self.helper)], env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [str(self.release), str(self.artifacts / "foundation-outbox-publisher"), str(self.artifacts / "jars"), "ro", "/home/spark/.ivy2/official.jar"])

    def test_private_script_cannot_claim_the_current_release(self):
        private = self.root / "private/scripts/ops/admitted-writer-runtime.sh"
        private.parent.mkdir(parents=True)
        private.write_bytes(self.helper.read_bytes())
        result = subprocess.run(["bash", "-eu", "-c", 'source "$1"', "test", str(private)], text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("admitted current", result.stderr)

    def test_registered_spark_jobs_do_not_reenable_package_resolution(self):
        jobs = self.area / "infra/lakehouse/spark/jobs"
        found = set()
        for script in (self.area / "scripts/ops").glob("*.sh"):
            text = script.read_text()
            if "spark-submit" not in text:
                continue
            self.assertIn("admitted-writer-runtime.sh", text)
            self.assertIn('--jars "${SPARK_RELEASE_JARS}"', text)
            self.assertNotIn("--packages", text)
            for name in re.findall(r"\b[a-z_]+\.py\b", text):
                if (jobs / name).exists():
                    found.add(name)
                    self.assertNotIn("spark.jars.packages", (jobs / name).read_text())
        self.assertGreaterEqual(len(found), 4)

    def test_migration_install_and_start_use_admission_dropin(self):
        installer = (self.area / "scripts/deploy/foundation-release.sh").read_text()
        prefix = installer.split('\ncommand="${1:-}"\n', 1)[0]
        timer_case = installer.split("  timers)\n", 1)[1].split("  rollback)\n", 1)[0]
        self.assertRegex(timer_case, r"(?m)^    install_lakehouse_migration_unit$")
        harness = self.root / "migration-test.sh"
        harness.write_text(prefix + '''
install() { printf 'install %s\\n' "$*" >>"$TEST_INSTALL_LOG"; }
assert_current_release() { printf 'verified\\n' >>"$TEST_INSTALL_LOG"; }
systemctl() {
  grep -q '10-release-admission.conf' "$TEST_INSTALL_LOG" || return 90
  printf 'systemctl %s\\n' "$*" >>"$TEST_INSTALL_LOG"
}
journalctl() { printf 'lakehouse-migrate apply: fixture=ok\\n'; }
migrate_lakehouse
''')
        log = self.root / "install.log"
        env = dict(os.environ, TEST_INSTALL_LOG=str(log), FOUNDATION_PLATFORM_RELEASE_ROOT=str(self.root))
        result = subprocess.run(["bash", str(harness)], env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = log.read_text()
        self.assertTrue(calls.startswith("verified\n"))
        self.assertIn("foundation-lakehouse-migrate.service.d/10-release-admission.conf", calls)
        self.assertGreater(calls.index("systemctl start"), calls.index("10-release-admission.conf"))

    def test_trino_render_writes_only_external_state(self):
        (self.release / "scripts/deploy").mkdir()
        renderer = self.release / "scripts/deploy/render-trino-catalog.sh"
        renderer.write_bytes((self.area / "scripts/deploy/render-trino-catalog.sh").read_bytes())
        template = Path("infra/lakehouse/trino/templates/r2-iceberg.properties.template")
        (self.release / template).parent.mkdir(parents=True)
        (self.release / template).write_bytes((self.area / template).read_bytes())
        env = dict(os.environ, FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT=str(self.root / "state"))
        for name in ("LAKEHOUSE_CATALOG_URI", "LAKEHOUSE_WAREHOUSE", "LAKEHOUSE_CATALOG_READER_TOKEN", "R2_LAKEHOUSE_ENDPOINT", "R2_LAKEHOUSE_READER_ACCESS_KEY_ID", "R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY"):
            env["FOUNDATION_PLATFORM_" + name] = "fixture"
        env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN"] = "fixture-writer-must-not-leak"
        result = subprocess.run(["bash", "-c", 'source "$1"', "test", str(renderer)], env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        generated = self.root / "state/trino/catalog/r2.properties"
        self.assertIn("connector.name=iceberg", generated.read_text())
        self.assertNotIn("fixture-writer-must-not-leak", generated.read_text())
        self.assertEqual(generated.parent.stat().st_mode & 0o777, 0o700)
        self.assertFalse((self.release / "infra/lakehouse/trino/catalog").exists())
        original = generated.read_bytes()
        for reader_token in ("", "invalid\nvalue"):
            env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN"] = reader_token
            result = subprocess.run(["bash", "-c", 'source "$1"', "test", str(renderer)], env=env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(generated.read_bytes(), original)
            self.assertEqual(list(generated.parent.glob(".r2.*")), [])
        env["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN"] = "fixture"
        fake_bin = self.root / "failing-bin"
        fake_bin.mkdir()
        (fake_bin / "chmod").write_text("#!/bin/sh\nexit 17\n")
        (fake_bin / "chmod").chmod(0o755)
        result = subprocess.run(["bash", "-c", 'source "$1"', "test", str(renderer)], env=dict(env, PATH=str(fake_bin) + ":" + os.environ["PATH"]), text=True, capture_output=True)
        self.assertEqual(result.returncode, 17)
        self.assertEqual(generated.read_bytes(), original)
        env["FOUNDATION_PLATFORM_TRINO_CATALOG_DIR"] = "relative/catalog"
        result = subprocess.run(["bash", "-c", 'source "$1" && printf "%s" "$FOUNDATION_PLATFORM_TRINO_CATALOG_DIR"', "test", str(renderer)], cwd=self.root, env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, str(self.root / "relative/catalog"))
        env["FOUNDATION_PLATFORM_TRINO_CATALOG_DIR"] = str(self.release / "infra/private")
        result = subprocess.run(["bash", "-c", 'source "$1"', "test", str(renderer)], env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.release / "infra/private").exists())


if __name__ == "__main__":
    unittest.main()
