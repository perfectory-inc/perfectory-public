"""Release admission takes a release from GitHub's container registry only once every byte is verified (root ADR-0276 §3, §4).

The registry, cosign and docker are stand-ins: a dictionary of manifests and blobs, a signature
check that passes or refuses, and a docker that reports the image ID it is told to.
"""

import base64
import gzip
import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[4] / "scripts/deploy/foundation-release-admission.py"
COMMIT = "a" * 40


def digest(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def layer(files: dict[str, bytes]) -> bytes:
    plain = io.BytesIO()
    with tarfile.open(fileobj=plain, mode="w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return gzip.compress(plain.getvalue())


class Registry:
    """A release as release-push.sh leaves it: images by digest, files in one image, release-<commit> last."""

    def __init__(self, files_layer_files=None, signed_extra=(), drop_signed=None, commit=COMMIT):
        self.blobs, self.manifests = {}, {}
        files = {"foundation-outbox-publisher": b"\x7fELF publisher", "jars/engine.jar": b"PK engine"}
        files_layer = layer(files_layer_files or files)
        files_manifest = self.manifest([files_layer])
        self.images = {"files": {"oci_manifest": files_manifest, "config": digest(b"files-config")}}
        for name in ("publisher", "tippecanoe", "foundation-platform-runtime", "foundation-postgres-recovery"):
            self.images[name] = {"oci_manifest": digest(f"{name}-manifest".encode()), "config": digest(f"{name}-config".encode())}
        self.release = {"source_commit": commit, "images": self.images,
                        "files": {name: {"sha256": digest(data), "size": len(data), "mode": "0555" if "/" not in name else "0444"}
                                  for name, data in files.items()}}
        release_json = json.dumps(self.release).encode()
        signed = {digest(release_json), *(d for image in self.images.values() for d in image.values()),
                  *(entry["sha256"] for entry in self.release["files"].values()), *signed_extra} - {drop_signed}
        statement = {"subject": [{"name": "x", "digest": {"sha256": d.split(":")[1]}} for d in sorted(signed)]}
        bundle = json.dumps({"dsseEnvelope": {"payload": base64.b64encode(json.dumps(statement).encode()).decode()}}).encode()
        release_layer = layer({"manifest.json": release_json, "manifest.sigstore.json": bundle})
        self.manifests["release-" + COMMIT] = json.dumps({"layers": [{"digest": self.blob(release_layer)}]}).encode()

    def blob(self, data: bytes) -> str:
        self.blobs[digest(data)] = data
        return digest(data)

    def manifest(self, layers: list[bytes]) -> str:
        body = json.dumps({"layers": [{"digest": self.blob(data)} for data in layers]}).encode()
        self.manifests[digest(body)] = body
        return digest(body)

    def get(self, repository, path, token, accept=None):
        kind, _, reference = path.partition("/")
        return (self.manifests if kind == "manifests" else self.blobs)[reference]


class FromTheRegistry(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("release_admission", SCRIPT)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory(prefix="release-from-registry-")
        self.addCleanup(self.temp.cleanup)
        self.artifacts = Path(self.temp.name) / "artifacts"
        self.artifacts.mkdir()
        self.calls, self.cosign_refuses, self.wrong_image = [], False, None
        for name, value in (("ARTIFACT_ROOT", self.artifacts), ("protected_path", mock.Mock()),
                            ("pinned_cosign", mock.Mock(return_value=Path("/pinned/cosign"))),
                            ("registry_token", mock.Mock(return_value="anonymous"))):
            patcher = mock.patch.object(self.module, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(self.module.subprocess, "check_output", side_effect=self.fake_command)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.use(Registry())

    def use(self, registry):
        self.registry = registry
        patcher = mock.patch.object(self.module, "registry_get", side_effect=registry.get)
        patcher.start()
        self.addCleanup(patcher.stop)

    def fake_command(self, args, **kwargs):
        self.calls.append(args)
        if args[0] == "/pinned/cosign":
            self.assertIn("--offline", args)
            self.assertEqual(args[args.index("--certificate-github-workflow-sha") + 1], COMMIT)
            self.assertTrue(args[args.index("--certificate-identity") + 1].endswith(
                "/.github/workflows/foundation-release-build.yml@refs/heads/main"))
            if self.cosign_refuses:
                raise subprocess.CalledProcessError(1, args)
            return b"Verified OK"
        if args[:2] == ["/usr/bin/docker", "image"]:
            name = next(n for n, image in self.registry.images.items() if image["oci_manifest"] in args[-1])
            return (self.wrong_image if name == self.wrong_image_for else self.registry.images[name]["config"]).encode()
        return b""

    wrong_image_for = None

    def test_r2_verifies_everything_and_installs_nothing(self):
        summary = self.module.verify_registry(COMMIT)
        self.assertIn("files=2", summary)
        self.assertFalse(any(call[:2] == ["/usr/bin/docker", "tag"] for call in self.calls), "R2 tags nothing")
        self.assertEqual(list(self.artifacts.iterdir()), [], "R2 leaves nothing behind")
        pulled = [call[-1] for call in self.calls if call[:2] == ["/usr/bin/docker", "pull"]]
        self.assertTrue(pulled and all("@sha256:" in reference for reference in pulled), "images are pulled by digest only")

    def test_installing_writes_the_signed_files_and_tags_the_signed_images(self):
        output = self.artifacts / "out"
        output.mkdir()
        images = self.module.registry_artifacts(COMMIT, output, tag=True)
        # The compose images are checked and returned; `tag-compose-images` tags them when the API migrates.
        self.assertEqual(images, {name: image["config"] for name, image in self.registry.images.items() if name != "files"})
        self.assertEqual((output / "foundation-outbox-publisher").read_bytes(), b"\x7fELF publisher")
        tags = [call[-1] for call in self.calls if call[:2] == ["/usr/bin/docker", "tag"]]
        self.assertEqual(tags, [f"foundation-outbox-publisher:{COMMIT}", f"foundation-tippecanoe:{COMMIT}"])

    def test_the_compose_images_are_recorded_all_or_none_and_tagged_as_compose_names_them(self):
        image = {name: "sha256:" + str(n) * 64 for n, name in enumerate(
            ("publisher", "tippecanoe", "foundation-platform-runtime", "foundation-postgres-recovery"), start=1)}
        target = self.artifacts / COMMIT
        for images, expected in ((image, 2), ({k: image[k] for k in ("publisher", "tippecanoe")}, 0)):
            with self.subTest(tagged=expected):
                if target.exists():
                    for directory, _, _ in os.walk(target):
                        Path(directory).chmod(0o755)
                    shutil.rmtree(target)
                (target / "jars").mkdir(parents=True)
                (target / "jars/engine.jar").write_bytes(b"PK engine")
                (target / "foundation-outbox-publisher").write_bytes(b"publisher")
                self.module.seal_artifacts(COMMIT, target, images)
                self.calls.clear()
                with mock.patch.object(self.module, "verify_artifacts") as verified:
                    self.assertEqual(self.module.tag_compose_images(COMMIT), expected)
                verified.assert_called_once_with(COMMIT, target)
                tags = [call[2:] for call in self.calls if call[:2] == ["/usr/bin/docker", "tag"]]
                self.assertEqual(tags, [[image["foundation-platform-runtime"], "foundation-platform-runtime:local"],
                                        [image["foundation-postgres-recovery"], "foundation-postgres-recovery:local"]][:expected])
        for directory, _, _ in os.walk(target):
            Path(directory).chmod(0o755)
        with self.assertRaisesRegex(ValueError, "immutable image ID"):
            self.module.seal_artifacts(COMMIT, target, {k: v for k, v in image.items() if k != "foundation-postgres-recovery"})

    def test_a_signature_cosign_refuses_installs_nothing(self):
        self.cosign_refuses = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.module.verify_registry(COMMIT)
        self.assertFalse(any(call[0] == "/usr/bin/docker" for call in self.calls), "nothing is pulled after a refused signature")

    def test_a_digest_the_provenance_did_not_sign_is_refused(self):
        self.use(Registry(drop_signed=Registry().images["publisher"]["config"]))
        with self.assertRaisesRegex(ValueError, "did not sign"):
            self.module.verify_registry(COMMIT)

    def test_a_manifest_for_another_commit_is_refused(self):
        self.use(Registry(commit="b" * 40))
        with self.assertRaisesRegex(ValueError, "not " + COMMIT):
            self.module.verify_registry(COMMIT)

    def test_a_file_the_manifest_does_not_name_is_refused(self):
        self.use(Registry(files_layer_files={"foundation-outbox-publisher": b"\x7fELF publisher",
                                             "jars/engine.jar": b"PK engine", "jars/extra.jar": b"PK extra"}))
        with self.assertRaisesRegex(ValueError, "does not name"):
            self.module.verify_registry(COMMIT)

    def test_bytes_that_are_not_their_digest_are_refused(self):
        registry = Registry()
        files_layer = json.loads(registry.manifests[registry.images["files"]["oci_manifest"]])["layers"][0]["digest"]
        registry.blobs[files_layer] = layer({"foundation-outbox-publisher": b"tampered", "jars/engine.jar": b"PK engine"})
        self.use(registry)
        with self.assertRaisesRegex(ValueError, "is not sha256:"):
            self.module.verify_registry(COMMIT)

    def test_a_pulled_image_that_is_not_the_signed_config_is_refused(self):
        self.wrong_image, self.wrong_image_for = "sha256:" + "f" * 64, "tippecanoe"
        with self.assertRaisesRegex(ValueError, "not the signed"):
            self.module.verify_registry(COMMIT)

    def test_the_containerd_store_names_an_image_by_its_signed_manifest(self):
        # 2026-10-19 on the host: docker image inspect gave the manifest digest, not the config.
        self.wrong_image = self.registry.images["tippecanoe"]["oci_manifest"]
        self.wrong_image_for = "tippecanoe"
        self.assertIn("files=2", self.module.verify_registry(COMMIT))
        self.wrong_image = self.registry.images["publisher"]["oci_manifest"]
        with self.assertRaisesRegex(ValueError, "not the signed"):
            self.module.verify_registry(COMMIT)

    def test_the_switch_defaults_to_the_server_build_and_refuses_anything_else(self):
        contract = Path(self.temp.name) / "release-build.contract.json"
        with mock.patch.object(self.module, "BUILD_CONTRACT", contract):
            contract.write_text("{}")
            self.assertEqual(self.module.artifacts_from(), "server-build")
            contract.write_text(json.dumps({"artifacts_from": "registry"}))
            self.assertEqual(self.module.artifacts_from(), "registry")
            contract.write_text(json.dumps({"artifacts_from": "anywhere"}))
            with self.assertRaises(ValueError):
                self.module.artifacts_from()


if __name__ == "__main__":
    unittest.main()
