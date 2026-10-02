"""Registered jobs run the admitted release's own build output, and Trino refuses a missing catalog.

No production access: releases, artifacts and the compose project are temporary fixtures laid
out the way the host lays them out (root ADR-0134).
"""

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import unittest


AREA = Path(__file__).resolve().parents[2]
OPS = AREA / "scripts/ops"
HELPER = "admitted-writer-runtime.sh"
CATALOG_TARGET = "/etc/trino/catalog/r2.properties"


class ReleaseFixture(unittest.TestCase):
    """A host layout: <base>/releases/<sha>, <base>/artifacts/<sha>, <base>/current."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="foundation-runtime-")
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.make_writable, Path(self.temp.name))
        self.base = Path(self.temp.name) / "opt/foundation-platform"
        self.release_id = "a" * 40
        self.release = self.install_release(self.release_id)
        (self.base / "current").symlink_to(Path("releases") / self.release_id)
        self.artifacts = self.seal_artifacts(self.release_id, b"#!/bin/sh\nexit 0\n")

    @staticmethod
    def make_writable(path):
        for directory, _, files in os.walk(path):
            os.chmod(directory, 0o755)
            for name in files:
                file = Path(directory) / name
                if not file.is_symlink():
                    file.chmod(0o644)

    def install_release(self, release_id, scripts=(HELPER,)):
        release = self.base / "releases" / release_id
        (release / "scripts/ops").mkdir(parents=True)
        for name in scripts:
            shutil.copyfile(OPS / name, release / "scripts/ops" / name)
            (release / "scripts/ops" / name).chmod(0o555)
        return release

    def seal_artifacts(self, release_id, publisher, manifest_publisher=None):
        artifacts = self.base / "artifacts" / release_id
        (artifacts / "jars").mkdir(parents=True)
        (artifacts / "jars/official.jar").write_bytes(b"frozen dependency")
        binary = artifacts / "foundation-outbox-publisher"
        binary.write_bytes(publisher)
        binary.chmod(0o555)
        files = {
            "foundation-outbox-publisher": manifest_publisher or hashlib.sha256(publisher).hexdigest(),
            "jars/official.jar": hashlib.sha256(b"frozen dependency").hexdigest(),
        }
        (artifacts / "build.json").write_text(json.dumps({"source": release_id, "files": files}))
        return artifacts

    def source_helper(self, helper, mode="--current", env=None):
        script = 'source "$1" "$2"; printf "%s\\n" "$RELEASE_ROOT" "$PUBLISHER_BIN" "$RELEASE_JARS_DIR" "$SPARK_RELEASE_JARS"'
        return subprocess.run(["bash", "-eu", "-c", script, "test", str(helper), mode],
                              env=env or dict(os.environ), text=True, capture_output=True)


class AdmittedRuntimeTests(ReleaseFixture):
    """The helper binds a job to <base>/releases/<sha> and <base>/artifacts/<sha>, nothing else."""

    def test_sourcing_without_a_mode_is_refused_rather_than_reading_the_callers_arguments(self):
        # `source file` with no arguments hands it the caller's: `map-edit-fold.sh complex`.
        script = 'set -- complex; source "$0"; printf "%s" "$PUBLISHER_BIN"'
        result = subprocess.run(["bash", "-eu", "-c", script, str(self.base / "current/scripts/ops" / HELPER)],
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 65)
        self.assertEqual(result.stdout, "")
        self.assertIn("exactly one of --current or --installed", result.stderr)

    def test_environment_cannot_select_private_source_binary_or_ivy(self):
        env = dict(os.environ, FOUNDATION_MAP_EDIT_FOLD_RELEASE_ROOT="/private/source",
                   FOUNDATION_MAP_EDIT_FOLD_PUBLISHER_BIN="/private/binary",
                   PUBLISHER_BIN="/private/binary", RELEASE_ROOT="/private/source",
                   FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="/private/ivy")
        result = self.source_helper(self.base / "current/scripts/ops" / HELPER, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [
            str(self.release.resolve()), str(self.artifacts.resolve() / "foundation-outbox-publisher"),
            str(self.artifacts.resolve() / "jars"), "/home/spark/.ivy2/official.jar",
        ])

    def test_script_outside_an_installed_release_is_refused(self):
        private = Path(self.temp.name) / "private/scripts/ops" / HELPER
        private.parent.mkdir(parents=True)
        shutil.copyfile(OPS / HELPER, private)
        result = self.source_helper(private)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not inside an installed release", result.stderr)

    def test_installed_release_that_is_not_current_is_refused_unless_asked(self):
        other_id = "b" * 40
        other = self.install_release(other_id)
        self.seal_artifacts(other_id, b"other\n")
        result = self.source_helper(other / "scripts/ops" / HELPER)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("admitted current release", result.stderr)
        # FLOOR's ExecStopPost cleans up with the release it started, after `current` moved.
        result = self.source_helper(other / "scripts/ops" / HELPER, "--installed")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[0], str(other.resolve()))

    def test_publisher_with_the_wrong_sha256_is_refused(self):
        binary = self.artifacts / "foundation-outbox-publisher"
        binary.chmod(0o755)
        binary.write_bytes(b"#!/bin/sh\necho substituted\n")
        binary.chmod(0o555)
        result = self.source_helper(self.base / "current/scripts/ops" / HELPER)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("publisher sha256 differs", result.stderr)

    def test_artifacts_of_another_release_are_refused(self):
        manifest = json.loads((self.artifacts / "build.json").read_text())
        manifest["source"] = "b" * 40
        (self.artifacts / "build.json").write_text(json.dumps(manifest))
        result = self.source_helper(self.base / "current/scripts/ops" / HELPER)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("another release", result.stderr)

    def test_writable_publisher_is_refused(self):
        (self.artifacts / "foundation-outbox-publisher").chmod(0o755)
        result = self.source_helper(self.base / "current/scripts/ops" / HELPER)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("read-only regular file", result.stderr)


class MapEditFoldBinaryTests(ReleaseFixture):
    """map-edit-fold.sh runs the admitted release's publisher, never a binary outside it."""

    def run_fold(self, release, env_extra=None):
        recorder = Path(self.temp.name) / "outside-publisher"
        marker = Path(self.temp.name) / "outside-publisher-ran"
        recorder.write_text(f"#!/bin/sh\n: > {marker}\n")
        recorder.chmod(0o755)
        # The paths production used before: an override and the shared state-directory binary.
        env = dict(os.environ, FOUNDATION_MAP_EDIT_FOLD_PUBLISHER_BIN=str(recorder),
                   PUBLISHER_BIN=str(recorder), PATH=f"{recorder.parent}:{os.environ['PATH']}")
        env.update(env_extra or {})
        result = subprocess.run(["bash", str(release / "scripts/ops/map-edit-fold.sh"), "complex"],
                                env=env, text=True, capture_output=True, timeout=60)
        return result, marker

    def test_fold_without_its_release_artifacts_refuses_before_running_anything(self):
        release_id = "c" * 40
        release = self.install_release(release_id, scripts=(HELPER, "map-edit-fold.sh"))
        (self.base / "current").unlink()
        (self.base / "current").symlink_to(Path("releases") / release_id)
        result, marker = self.run_fold(release)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("trusted release build output is missing", result.stderr)
        self.assertFalse(marker.exists(), "map-edit-fold ran a publisher outside the admitted release")

    def test_fold_with_a_substituted_publisher_refuses_before_running_anything(self):
        release = self.install_release("d" * 40, scripts=(HELPER, "map-edit-fold.sh"))
        self.seal_artifacts("d" * 40, b"built\n", manifest_publisher="0" * 64)
        (self.base / "current").unlink()
        (self.base / "current").symlink_to(Path("releases") / ("d" * 40))
        result, marker = self.run_fold(release)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("publisher sha256 differs", result.stderr)
        self.assertFalse(marker.exists())

    def test_no_registered_job_names_a_publisher_outside_its_release(self):
        found = []
        for script in sorted(OPS.glob("*.sh")):
            if script.name == HELPER:
                continue
            text = script.read_text(encoding="utf-8")
            self.assertNotIn("/var/lib/foundation-platform/bin", text, script.name)
            self.assertNotRegex(text, r"PUBLISHER_BIN:-", script.name)
            if "PUBLISHER_BIN" in text or "publisher=" in text:
                self.assertRegex(text, r'source "\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)/admitted-writer-runtime\.sh"',
                                 script.name)
                found.append(script.name)
            if "spark-submit" in text:
                self.assertIn('--jars "${SPARK_RELEASE_JARS}"', text, script.name)
                self.assertNotIn("--packages", text, script.name)
                self.assertIn('FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE="${RELEASE_JARS_DIR}" '
                              "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_MODE=ro", text, script.name)
        self.assertIn("map-edit-fold.sh", found)
        self.assertIn("building-register-floor-cycle.sh", found)


def trino_catalog_mounts(services):
    """Returns the Trino catalog mounts, refusing any form that can start without the file."""
    mounts = [volume for volume in services["trino"].get("volumes", [])
              if volume.get("target", "").startswith("/etc/trino/catalog")]
    if len(mounts) != 1:
        raise ValueError("Trino must mount exactly one catalog file")
    mount = mounts[0]
    if (mount.get("type") != "bind" or mount.get("target") != CATALOG_TARGET
            or not mount.get("source", "").endswith("r2.properties") or mount.get("read_only") is not True
            or mount.get("bind", {}).get("create_host_path") is not False):
        raise ValueError("Trino's catalog must be a read-only file bind with create_host_path: false")
    return mount


def compose_services(compose_file, env):
    docker = shutil.which("docker")
    if docker is None:
        raise unittest.SkipTest("docker compose is not installed; the CI runner has it")
    result = subprocess.run([docker, "compose", "-f", str(compose_file), "--profile", "lakehouse-query",
                             "config", "--format", "json"], env=env, text=True, capture_output=True)
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)["services"]


class TrinoCatalogMountTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="foundation-trino-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.catalog = self.root / "catalog"
        self.env = dict(os.environ, FOUNDATION_PLATFORM_TRINO_CATALOG_DIR=str(self.catalog))
        self.text = (AREA / "compose.lakehouse.yml").read_text(encoding="utf-8")

    def services(self, text):
        compose = self.root / "compose.lakehouse.yml"
        compose.write_text(text, encoding="utf-8")
        return compose_services(compose, self.env)

    def test_catalog_is_one_file_that_must_already_exist(self):
        mount = trino_catalog_mounts(self.services(self.text))
        self.assertEqual(Path(mount["source"]), self.catalog / "r2.properties")

    def test_short_syntax_or_created_host_path_is_refused(self):
        long_form = re.search(r"      - type: bind\n        source: \$\{FOUNDATION_PLATFORM_TRINO_CATALOG_DIR.*?"
                              r"create_host_path: false\n", self.text, re.S)
        self.assertIsNotNone(long_form)
        planted = {
            "short form": self.text.replace(long_form.group(0),
                                            "      - ./infra/lakehouse/trino/catalog:/etc/trino/catalog:ro\n"),
            "creates the host path": self.text.replace("create_host_path: false", "create_host_path: true"),
            "writable": self.text.replace(long_form.group(0), long_form.group(0).replace(
                "read_only: true", "read_only: false")),
        }
        for name, text in planted.items():
            with self.subTest(name):
                self.assertNotEqual(text, self.text)
                with self.assertRaises(ValueError):
                    trino_catalog_mounts(self.services(text))

    @unittest.skipUnless(platform.system() == "Linux" and shutil.which("docker"),
                         "the bind-source check is the Linux engine's; Docker Desktop creates the path")
    def test_engine_refuses_to_start_trino_without_its_catalog_file(self):
        if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
            self.skipTest("no Docker daemon")
        # The real mount, on a small pinned image this compose file already uses, so the test
        # needs neither the Trino image nor credentials.
        mount = trino_catalog_mounts(self.services(self.text))
        image = re.search(r"image: (busybox:[^\s]+)", self.text).group(1)
        probe = self.root / "probe.yml"
        probe.write_text(json.dumps({"services": {"probe": {
            "image": image, "mem_limit": "32m", "command": ["cat", CATALOG_TARGET], "volumes": [mount],
        }}}))
        run = ["docker", "compose", "-p", "foundation-trino-catalog-test-" + str(os.getpid()),
               "-f", str(probe), "run", "--rm", "probe"]
        self.addCleanup(subprocess.run, run[:6] + ["down"], capture_output=True)
        missing = subprocess.run(run, capture_output=True, text=True, timeout=300)
        self.assertNotEqual(missing.returncode, 0, missing.stdout)
        self.assertIn("bind source path does not exist", missing.stderr)
        self.assertFalse((self.catalog / "r2.properties").exists(), "compose created the missing catalog")
        self.catalog.mkdir()
        (self.catalog / "r2.properties").write_text("connector.name=iceberg\n")
        present = subprocess.run(run, capture_output=True, text=True, timeout=300)
        self.assertEqual(present.returncode, 0, present.stderr)
        self.assertIn("connector.name=iceberg", present.stdout)


if __name__ == "__main__":
    unittest.main()
