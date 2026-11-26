"""An apply takes the images of one release commit only after their signatures checked out (root ADR-0616 §2-4).

scripts/deploy/aws-release-images.py with the registry and `gh` stood in for: it reads the commit's record,
verifies the record and every image it names against the contract's repository, workflow, ref and that commit,
and only then writes the two digests. 2026-11-26 (W107 review): the runbook scraped digests with curl and awk,
its `gh attestation verify` loop had no `set -e` so a failed check still exported the digest, and nothing tied
the two digests to one commit.
"""

import gzip
import hashlib
import importlib.util
import io
import json
import pathlib
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "deploy" / "aws-release-images.py"
COMMIT = "1" * 40
OLDER = "2" * 40


def load():
    spec = importlib.util.spec_from_file_location("aws_release_images", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(text):
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def record(commit):
    return {"schema_version": "foundation-platform.aws_images.v1", "source_commit": commit, "images": {
        name: {"digest": digest(f"{commit}-{name}"), "tag": f"x:{commit}-aws-{name}"}
        for name in ("publisher", "code-location", "runtime", "dagster")}}


def layer(payload):
    plain = io.BytesIO()
    with tarfile.open(fileobj=plain, mode="w") as archive:
        data = json.dumps(payload).encode()
        info = tarfile.TarInfo("aws-images.json")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return gzip.compress(plain.getvalue())


class Registry:
    """The records of a few commits, as the release package serves them."""

    def __init__(self, records, corrupt=()):
        self.layers = {commit: layer(payload) for commit, payload in records.items()}
        self.corrupt = set(corrupt)

    def get(self, images, path, accept):
        kind, name = path.split("/", 1)
        if kind == "manifests":
            commit = name.removesuffix("-aws-images")
            return json.dumps({"layers": [{"digest": "sha256:" + hashlib.sha256(self.layers[commit]).hexdigest()}]}).encode()
        for commit, data in self.layers.items():
            if name == "sha256:" + hashlib.sha256(data).hexdigest():
                return data + b"x" if commit in self.corrupt else data
        raise AssertionError(path)


class Gh:
    """`gh attestation verify`: passes every target but the ones named in `refuse`."""

    def __init__(self, refuse=()):
        self.calls, self.refuse = [], set(refuse)

    def run(self, argv, **kwargs):
        self.calls.append(argv)
        failed = any(target in argv[3] for target in self.refuse)
        return subprocess.CompletedProcess(argv, 1 if failed else 0, "", "refused" if failed else "")


class AnApplyTakesOnlyVerifiedImages(unittest.TestCase):
    def setUp(self):
        self.module = load()
        self.images = self.module.images_contract()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.out = pathlib.Path(directory.name) / "release.tfvars.json"

    def run_script(self, registry, gh, *arguments):
        with mock.patch.object(self.module, "registry_get", registry.get), \
                mock.patch.object(self.module.subprocess, "run", gh.run), \
                mock.patch("sys.argv", ["aws-release-images.py", *arguments, "--out", str(self.out)]):
            return self.module.main()

    def test_it_writes_both_digests_after_every_signature_checked_out(self):
        gh = Gh()
        self.run_script(Registry({COMMIT: record(COMMIT)}), gh, COMMIT)
        written = json.loads(self.out.read_text(encoding="utf-8"))
        self.assertEqual(written, {"dagster_service_image": f"{self.images['repository']}@{digest(f'{COMMIT}-dagster')}",
                                   "dagster_image": f"{self.images['repository']}@{digest(f'{COMMIT}-code-location')}",
                                   "runtime_image": f"{self.images['repository']}@{digest(f'{COMMIT}-runtime')}"})
        targets = [call[3] for call in gh.calls]
        self.assertTrue(targets[0].endswith("aws-images.json"), "the record is verified first")
        self.assertEqual(len(targets), 5, "the record and its four images")
        for call in gh.calls:
            self.assertEqual(call[call.index("--source-digest") + 1], COMMIT)
            self.assertEqual(call[call.index("--source-ref") + 1], self.images["source_ref"])
            self.assertIn("--deny-self-hosted-runners", call)

    def test_one_signature_that_does_not_check_out_writes_nothing(self):
        gh = Gh(refuse=[digest(f"{COMMIT}-runtime")])
        with self.assertRaises(SystemExit):
            self.run_script(Registry({COMMIT: record(COMMIT)}), gh, COMMIT)
        self.assertFalse(self.out.exists())

    def test_a_record_of_another_commit_is_refused(self):
        with self.assertRaises(SystemExit):
            self.run_script(Registry({COMMIT: record(OLDER)}), Gh(), COMMIT)
        self.assertFalse(self.out.exists())

    def test_a_layer_that_is_not_its_digest_is_refused(self):
        with self.assertRaises(SystemExit):
            self.run_script(Registry({COMMIT: record(COMMIT)}, corrupt=[COMMIT]), Gh(), COMMIT)
        self.assertFalse(self.out.exists())

    def test_an_image_an_earlier_commit_built_is_verified_against_that_commit(self):
        reused = record(COMMIT)
        reused["images"]["dagster"]["source_commit"] = OLDER
        gh = Gh()
        self.run_script(Registry({COMMIT: reused}), gh, COMMIT)
        sources = {call[3]: call[call.index("--source-digest") + 1] for call in gh.calls}
        self.assertEqual(sources[f"oci://{self.images['repository']}@{digest(f'{COMMIT}-dagster')}"], OLDER)
        self.assertEqual(sources[f"oci://{self.images['repository']}@{digest(f'{COMMIT}-runtime')}"], COMMIT)

    def test_a_rollback_keeps_the_newer_runtime(self):
        self.run_script(Registry({OLDER: record(OLDER), COMMIT: record(COMMIT)}), Gh(), OLDER, "--runtime-commit", COMMIT)
        written = json.loads(self.out.read_text(encoding="utf-8"))
        self.assertTrue(written["dagster_image"].endswith(digest(f"{OLDER}-code-location")))
        self.assertTrue(written["runtime_image"].endswith(digest(f"{COMMIT}-runtime")))


if __name__ == "__main__":
    unittest.main()
