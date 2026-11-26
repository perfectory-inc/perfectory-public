#!/usr/bin/env python3
"""The AWS images of one release commit, verified, as OpenTofu's variables (root ADR-0616 §2-4, ADR-0620).

    aws-release-images.py <commit> --out <file.tfvars.json> [--runtime-commit <commit>]

An apply takes one commit. Its record, <commit>-aws-images in the release package (written, attested and
published by the release workflow's aws-images job), names every image of that commit by digest. This:

1. reads the record from the registry, anonymously (the package is public);
2. verifies the record, then every image it names, with `gh attestation verify`: signed by the contract's
   workflow of the contract's repository, from the contract's ref, built from that very commit
   (`--source-digest`), on a GitHub-hosted runner. Any check that does not pass stops it;
3. writes `dagster_image` (the code location) and `runtime_image` (the migrator) by digest.

`--runtime-commit`: the runtime image from another release, verified the same way. A rollback moves the
code location back and keeps the newer runtime: migrations only go forward (docs/runbooks/aws-platform.md).
Nothing here reads a secret; `gh` needs the applier's GitHub login to read attestations.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "platforms" / "foundation-platform" / "config" / "aws-platform.contract.json"
SCHEMA = "foundation-platform.aws_images.v1"
RECORD = "aws-images.json"
COMMIT = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
# The record's image whose digest each variable takes.
VARIABLES = {"dagster_image": "code-location", "runtime_image": "runtime"}


def images_contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))["images"]


def registry_get(images: dict, path: str, accept: str) -> bytes:
    """GET <registry>/v2/<package>/<path> with an anonymous pull token."""
    host, package = images["repository"].split("/", 1)
    scope = urllib.parse.quote(f"repository:{package}:pull", safe=":/")
    with urllib.request.urlopen(f"https://{host}/token?scope={scope}", timeout=30) as response:
        token = json.load(response)["token"]
    request = urllib.request.Request(f"https://{host}/v2/{package}/{path}",
                                     headers={"Authorization": f"Bearer {token}", "Accept": accept})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def read_record(images: dict, commit: str, directory: Path) -> Path:
    """The commit's record, written to `directory`, its layer checked against the digest its manifest names."""
    tag = images["record_tag"].replace("<commit>", commit)
    manifest = json.loads(registry_get(images, f"manifests/{tag}", "application/vnd.oci.image.manifest.v1+json"))
    [layer] = manifest["layers"]
    data = registry_get(images, f"blobs/{layer['digest']}", "application/octet-stream")
    if "sha256:" + hashlib.sha256(data).hexdigest() != layer["digest"]:
        raise SystemExit(f"FAIL aws-release-images: the layer of {tag} is not the bytes its manifest names")
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(data))) as archive:
        member = archive.extractfile(RECORD)
        if member is None:
            raise SystemExit(f"FAIL aws-release-images: {tag} holds no {RECORD}")
        path = directory / commit / RECORD
        path.parent.mkdir(parents=True)
        path.write_bytes(member.read())
    return path


def verify(images: dict, commit: str, target: str) -> None:
    """`gh attestation verify` for one target (a file, or oci://<image>@<digest>); exits on anything but a pass."""
    repository = images["source_repository"]
    result = subprocess.run(
        ["gh", "attestation", "verify", target, "--repo", repository,
         "--signer-workflow", f"{repository}/{images['signer_workflow']}", "--source-ref", images["source_ref"],
         "--source-digest", commit, "--deny-self-hosted-runners"],
        capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise SystemExit(f"FAIL aws-release-images: {target} is not attested by {images['signer_workflow']} of "
                         f"{repository} at {images['source_ref']} for {commit}:\n{(result.stderr or result.stdout).strip()[-800:]}")
    print(f"aws-release-images: verified {target}", flush=True)


def verified_digests(images: dict, commit: str, directory: Path) -> dict[str, str]:
    """{image name: digest} of the commit's record, after the record and each digest passed `verify`."""
    path = read_record(images, commit, directory)
    verify(images, commit, str(path))
    record = json.loads(path.read_text(encoding="utf-8"))
    if record.get("schema_version") != SCHEMA or record.get("source_commit") != commit:
        raise SystemExit(f"FAIL aws-release-images: the record of {commit} is not a {SCHEMA} record of that commit")
    digests = {}
    for name, image in record["images"].items():
        if not DIGEST.fullmatch(image.get("digest", "")):
            raise SystemExit(f"FAIL aws-release-images: {name} in the record of {commit} has no sha256 digest")
        verify(images, commit, f"oci://{images['repository']}@{image['digest']}")
        digests[name] = image["digest"]
    return digests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("commit", help="the release commit to apply (40 hex)")
    parser.add_argument("--out", required=True, type=Path, help="the .tfvars.json file to write")
    parser.add_argument("--runtime-commit", help="the release commit whose runtime image to apply (default: commit)")
    arguments = parser.parse_args()
    runtime_commit = arguments.runtime_commit or arguments.commit
    for value in (arguments.commit, runtime_commit):
        if not COMMIT.fullmatch(value):
            raise SystemExit(f"FAIL aws-release-images: {value} is not a 40-hex commit")
    images = images_contract()
    with tempfile.TemporaryDirectory(prefix="aws-release-images-") as temporary:
        directory = Path(temporary)
        code = verified_digests(images, arguments.commit, directory)
        runtime = code if runtime_commit == arguments.commit else verified_digests(images, runtime_commit, directory)
    chosen = {"dagster_image": code[VARIABLES["dagster_image"]], "runtime_image": runtime[VARIABLES["runtime_image"]]}
    variables = {name: f"{images['repository']}@{digest}" for name, digest in chosen.items()}
    arguments.out.write_text(json.dumps(variables, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"aws-release-images: wrote {arguments.out}: code location of {arguments.commit}, runtime of {runtime_commit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
