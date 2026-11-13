"""A release carries exactly the DuckDB extension files its own contract pins (root ADR-0559 §6).

The publisher loads them by path, with automatic install and load off, so a file the contract did
not name must never sit where it looks, and a pinned one must never be missing or another build.
"""

import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PLATFORM = ROOT / "platforms" / "foundation-platform"
SCRIPT = ROOT / "scripts/deploy/foundation-release-admission.py"
BUILD = ROOT / "scripts/deploy/foundation-release-build.py"
COMMIT = "a" * 40
IMAGES = {"publisher": "sha256:" + "1" * 64, "tippecanoe": "sha256:" + "2" * 64}


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EngineExtensionsTest(unittest.TestCase):
    def setUp(self):
        self.module = load(SCRIPT, "release_admission_engine_extensions")
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.addCleanup(self.make_writable, Path(temporary.name))
        self.root = Path(temporary.name)
        self.target = self.root / "release"
        self.output = self.root / "output"
        (self.output / "jars").mkdir(parents=True)
        (self.output / "jars/engine.jar").write_bytes(b"PK engine")
        (self.output / "foundation-outbox-publisher").write_bytes(b"publisher")

    @staticmethod
    def make_writable(path: Path):
        for directory, _, files in os.walk(path):
            Path(directory).chmod(0o755)
            for name in files:
                (Path(directory) / name).chmod(0o644)

    def pin(self, files: dict[str, bytes]):
        """A release whose contract pins `files` (extension name -> the unpacked bytes)."""
        contract = {
            "directory": "duckdb-extensions",
            "load_order": list(files),
            "distributions": {name: {"platforms": {"linux-x86_64": {
                "url": f"https://extensions.example.invalid/{name}.duckdb_extension.gz", "sha256": "0" * 64,
                "file": {"name": f"{name}.duckdb_extension", "compression": "gzip",
                         "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}}}}
                for name, data in files.items()},
        }
        path = self.target / self.module.ENGINE_EXTENSIONS_CONTRACT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(contract), encoding="utf-8")

    def carry(self, files: dict[str, bytes]):
        (self.output / "duckdb-extensions").mkdir(exist_ok=True)
        for name, data in files.items():
            (self.output / "duckdb-extensions" / f"{name}.duckdb_extension").write_bytes(data)

    def test_the_repository_contract_pins_the_three_files_the_publisher_loads(self):
        pinned = self.module.engine_extensions(PLATFORM)
        self.assertEqual(list(pinned), ["duckdb-extensions/httpfs.duckdb_extension",
                                        "duckdb-extensions/avro.duckdb_extension",
                                        "duckdb-extensions/iceberg.duckdb_extension"])
        for pin in pinned.values():
            self.assertRegex(pin["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(pin["size"], 0)

    def test_the_contract_is_one_the_pinned_fetcher_prefetches(self):
        # main fills the cache every release build restores (root ADR-0197); a contract the registry
        # does not list is downloaded by every release build instead.
        relative = (PLATFORM / self.module.ENGINE_EXTENSIONS_CONTRACT).relative_to(ROOT).as_posix()
        self.assertIn(relative, json.loads((ROOT / "tools/pinned-artifacts.json").read_text(encoding="utf-8"))["contracts"])

    def test_a_release_from_before_the_contract_pins_none_and_carries_none(self):
        self.target.mkdir()
        self.assertEqual(self.module.engine_extensions(self.target), {})
        self.module.require_engine_extensions(self.target, self.output)
        self.module.seal_artifacts(COMMIT, self.output, IMAGES)

    def test_the_pinned_files_are_admitted_and_sealed_read_only(self):
        files = {"httpfs": b"httpfs build", "iceberg": b"iceberg build"}
        self.pin(files)
        self.carry(files)
        self.module.require_engine_extensions(self.target, self.output)
        self.module.seal_artifacts(COMMIT, self.output, IMAGES)
        manifest = json.loads((self.output / "build.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["files"]["duckdb-extensions/iceberg.duckdb_extension"],
                         hashlib.sha256(b"iceberg build").hexdigest())
        self.assertEqual((self.output / "duckdb-extensions/iceberg.duckdb_extension").stat().st_mode & 0o777, 0o444)
        self.module.verify_artifacts(COMMIT, self.output, os.getuid())

    def test_a_missing_an_extra_and_another_build_are_each_refused(self):
        files = {"httpfs": b"httpfs build", "iceberg": b"iceberg build"}
        self.pin(files)
        for carried in ({"httpfs": b"httpfs build"},
                        {**files, "spatial": b"not pinned"},
                        {**files, "iceberg": b"another build"},
                        {}):
            with self.subTest(carried=sorted(carried)):
                for stale in (self.output / "duckdb-extensions").glob("*") if (self.output / "duckdb-extensions").exists() else ():
                    stale.unlink()
                self.carry(carried)
                with self.assertRaisesRegex(ValueError, "not the ones the release's contract pins"):
                    self.module.require_engine_extensions(self.target, self.output)

    def test_a_file_that_is_neither_a_jar_nor_an_extension_is_still_refused(self):
        (self.output / "duckdb-extensions").mkdir()
        (self.output / "duckdb-extensions/run.sh").write_bytes(b"#!/bin/sh")
        with self.assertRaisesRegex(ValueError, "unexpected build output"):
            self.module.seal_artifacts(COMMIT, self.output, IMAGES)

    def test_a_contract_that_names_a_path_outside_its_directory_is_refused(self):
        self.pin({"httpfs": b"x"})
        path = self.target / self.module.ENGINE_EXTENSIONS_CONTRACT
        contract = json.loads(path.read_text(encoding="utf-8"))
        contract["distributions"]["httpfs"]["platforms"]["linux-x86_64"]["file"]["name"] = "../foundation-outbox-publisher"
        path.write_text(json.dumps(contract), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unusable file"):
            self.module.engine_extensions(self.target)

    def test_the_release_build_proves_the_offline_load_before_it_writes_the_manifest(self):
        # The release's own publisher loads the release's own files with no network, or there is no
        # release: a build for another DuckDB would otherwise fail in front of the first job.
        source = BUILD.read_text(encoding="utf-8")
        proof = source.index('extensions_load_offline(tree, images["publisher"]["config"])')
        self.assertLess(proof, source.index('(out / "manifest.json").write_text'))
        body = source[source.index("def extensions_load_offline"):source.index("def generated_paths")]
        for anchor in ('"--network", "none"', "/release/foundation-outbox-publisher", "verify-lakehouse-engine-extensions"):
            self.assertIn(anchor, body)


if __name__ == "__main__":
    unittest.main()
