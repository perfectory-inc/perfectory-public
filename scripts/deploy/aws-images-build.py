#!/usr/bin/env python3
"""Builds, checks and publishes the AWS images of one commit (root ADR-0619 §7).

Runs in foundation-release-build.yml's aws-images job, on GitHub's arm64 runner, after the release job
of the same commit has pushed its release. AWS runs on Graviton, so:

- the publisher is built here for linux/arm64 with the release build's own build_image (same Dockerfile,
  same build arguments, its BuildKit cache under the release package's buildcache-publisher-arm64 tag),
  and joined with the release's own linux/amd64 publisher (built and signed by the release job, which
  the data host runs) into one index, <commit>-aws-publisher;
- the code location (orchestration/dagster/Dockerfile.aws) is built for linux/arm64 only, because only
  AWS runs it: <commit>-aws-code-location. It carries the arm64 publisher and the arm64 DuckDB extension
  files the contract pins, and before anything is pushed its publisher loads them with no network.
- the runtime image (the compose build of services/foundation-api/Dockerfile, the same entry and build
  arguments as the release's) is built for linux/arm64 too, because AWS runs its foundation-migrate before
  every rollout (root ADR-0616 A1b): <commit>-aws-runtime. Before anything is pushed its migrator starts
  with no network and asks for its database URL.

Every manifest is pushed by digest into the release package (public, so ECS pulls it anonymously) and
read back; subjects.sha256 names the three digests the job then attests.

    <out>/subjects.sha256    sha256 and name of each image, for actions/attest-build-provenance
    <out>/aws-images.json    foundation-platform.aws_images.v1: each image's tag, digest and platforms
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLATFORM_TREE = ROOT / "platforms" / "foundation-platform"
EXTENSIONS_CONTRACT = PLATFORM_TREE / "infra" / "lakehouse" / "contracts" / "duckdb-extensions.contract.json"
CODE_LOCATION_DOCKERFILE = "orchestration/dagster/Dockerfile.aws"
# The release's compose image that carries the migrator (foundation-release-build.py compose_builds).
RUNTIME_IMAGE = "foundation-platform-runtime"
TARGET = "linux/arm64"
# The contract's platform key for TARGET, as the publisher names its own (`<os>-<arch>` of the Rust target).
TARGET_KEY = "linux-aarch64"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
SCHEMA = "foundation-platform.aws_images.v1"


def release_build():
    """The release build script as a module: its build_image and admission are the ones used here."""
    spec = importlib.util.spec_from_file_location("foundation_release_build", ROOT / "scripts/deploy/foundation-release-build.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(*args: str, capture: bool = False) -> str:
    result = subprocess.run(args, cwd=PLATFORM_TREE, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else ""


def fetch_extensions(output: Path) -> list[str]:
    """Every extension file the contract pins for TARGET, through the one pinned fetcher (root ADR-0197)."""
    contract = json.loads(EXTENSIONS_CONTRACT.read_text(encoding="utf-8"))
    names = []
    for distribution in contract["load_order"]:
        pin = contract["distributions"][distribution]["platforms"][TARGET_KEY]["file"]
        path = output / pin["name"]
        subprocess.run([sys.executable, "-I", "-B", str(ROOT / "scripts/ci/fetch_pinned.py"), "distribution",
                        str(EXTENSIONS_CONTRACT), distribution, "--output", str(path)], check=True)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != pin["sha256"] or path.stat().st_size != pin["size"]:
            raise SystemExit(f"FAIL aws-images: {pin['name']} is not the file the contract pins for {TARGET_KEY}")
        names.append(pin["name"])
    return names


def loads_offline(archive: Path, config: str) -> None:
    """The code location's publisher loads its own extension files with no network, and Dagster imports."""
    loaded = run("docker", "load", "--input", str(archive), capture=True)
    match = re.search(r"Loaded image ID: (sha256:[0-9a-f]{64})", loaded)
    if (match.group(1) if match else None) != config:
        raise SystemExit(f"FAIL aws-images: the loaded code location is not {config}: {loaded.strip()}")
    run("docker", "run", "--rm", "--network", "none", "--entrypoint", "foundation-outbox-publisher", config,
        "verify-lakehouse-engine-extensions")
    run("docker", "run", "--rm", "--network", "none", "--entrypoint", "python", config, "-c",
        "import dagster, dagster_aws, dagster_postgres; print('aws-images: dagster', dagster.__version__)")
    instance_loads(config)


# The names infra/aws/dagster.tf gives the service, with stand-in values: the check reads the instance's
# own file and builds its run launcher with the library in the image, which reaches no network on build.
INSTANCE_ENVIRONMENT = {
    "AWS_DEFAULT_REGION": "ap-northeast-2",
    "DAGSTER_PG_HOST": "database.invalid", "DAGSTER_PG_PORT": "5432", "DAGSTER_PG_DB": "dagster",
    "DAGSTER_PG_USERNAME": "dagster", "DAGSTER_PG_PASSWORD": "stand-in",
    "DAGSTER_PG_PASSWORD_SECRET": "arn:aws:secretsmanager:ap-northeast-2:000000000000:secret:stand-in:password::",
    "DAGSTER_RUN_TASK_ROLE_ARN": "arn:aws:iam::000000000000:role/stand-in/run/default",
    "DAGSTER_RUN_EXECUTION_ROLE_ARN": "arn:aws:iam::000000000000:role/stand-in/run/execution",
    "DAGSTER_LOG_GROUP": "/stand-in/dagster",
}
INSTANCE_CHECK = """
import os
import yaml
from dagster._core.instance.config import dagster_instance_config
from dagster._core.launcher import RunLauncher
from dagster._serdes.config_class import ConfigurableClassData
config, _ = dagster_instance_config(os.environ["DAGSTER_HOME"])
launcher = config["run_launcher"]
built = ConfigurableClassData(launcher["module"], launcher["class"], yaml.dump(launcher.get("config") or {})).rehydrate(as_type=RunLauncher)
assert built.secrets_tags == [], "the launcher would list the account's secrets"
assert built.task_role_arn == os.environ["DAGSTER_RUN_TASK_ROLE_ARN"], "a run would carry the service's role"
print("aws-images: the instance loads and its run launcher builds:", type(built).__name__)
"""


def instance_loads(config: str) -> None:
    """The image's dagster.yaml is one Dagster accepts, and its EcsRunLauncher builds with the contract's run
    prefix (dagster-aws refuses a prefix over 16 characters only when it builds the launcher: 2026-11-26 the
    live 23-character prefix would have stopped the first run on AWS)."""
    prefix = json.loads((PLATFORM_TREE / "config" / "aws-platform.contract.json").read_text(encoding="utf-8"))["dagster"]["run_task_definition_prefix"]
    environment = {**INSTANCE_ENVIRONMENT, "DAGSTER_RUN_TASK_DEFINITION_PREFIX": prefix}
    run("docker", "run", "--rm", "--network", "none", *[f"--env={name}={value}" for name, value in environment.items()],
        "--entrypoint", "python", config, "-c", INSTANCE_CHECK)


def migrator_starts(archive: Path, config: str) -> None:
    """The runtime image's migrator runs on this platform: with no database URL it refuses, by name."""
    loaded = run("docker", "load", "--input", str(archive), capture=True)
    match = re.search(r"Loaded image ID: (sha256:[0-9a-f]{64})", loaded)
    if (match.group(1) if match else None) != config:
        raise SystemExit(f"FAIL aws-images: the loaded runtime is not {config}: {loaded.strip()}")
    result = subprocess.run(["docker", "run", "--rm", "--network", "none", "--entrypoint",
                             "/usr/local/bin/foundation-migrate", config], cwd=PLATFORM_TREE, text=True,
                            capture_output=True, check=False)
    if result.returncode == 0 or "FOUNDATION_MIGRATOR_DATABASE_URL is required" not in result.stderr + result.stdout:
        raise SystemExit(f"FAIL aws-images: the runtime's migrator did not start as expected "
                         f"(exit {result.returncode}): {(result.stderr + result.stdout).strip()[-400:]}")
    print("aws-images: runtime foundation-migrate starts on linux/arm64", flush=True)


def push(layout: Path, name: str, digest: str, reference: str) -> None:
    """One manifest of the layout, byte for byte, read back by digest."""
    run("skopeo", "copy", "--quiet", "--retry-times", "3", "--preserve-digests", f"oci:{layout}:{name}", f"docker://{reference}")
    stored = "sha256:" + hashlib.sha256(run("skopeo", "inspect", "--raw", f"docker://{reference}", capture=True).encode()).hexdigest()
    if stored != digest:
        raise SystemExit(f"FAIL aws-images: {reference} is {stored} in the registry, not {digest}")


def index_digest(reference: str) -> str:
    raw = subprocess.run(["skopeo", "inspect", "--raw", f"docker://{reference}"], check=True, capture_output=True).stdout
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--repository", required=True, help="ghcr.io/<owner>/<release package>")
    parser.add_argument("--release-publisher", required=True,
                        help="the release's linux/amd64 publisher manifest digest (the release job's output)")
    arguments = parser.parse_args()
    if platform.machine().lower() not in ("aarch64", "arm64"):
        raise SystemExit("FAIL aws-images: build on the arm64 runner; the publisher is never emulated")
    if not DIGEST.fullmatch(arguments.release_publisher):
        raise SystemExit("FAIL aws-images: --release-publisher is not a sha256 digest")
    if not re.fullmatch(r"ghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+", arguments.repository):
        raise SystemExit(f"FAIL aws-images: {arguments.repository} is not ghcr.io/<owner>/<package>")
    out = arguments.out
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    blobs = out / "blobs" / "sha256"
    blobs.mkdir(parents=True)
    build = release_build()
    release = build.admission()
    commit = run("git", "rev-parse", "HEAD", capture=True).strip()
    release.require_sha(commit)
    repository = arguments.repository
    builds = build.image_builds(release)
    publisher = next(entry for entry in builds if entry[0] == "publisher")
    runtime = next(entry for entry in builds if entry[0] == RUNTIME_IMAGE)
    with tempfile.TemporaryDirectory(prefix="aws-images-") as temporary:
        work = Path(temporary)
        extensions = work / "engine-extensions"
        extensions.mkdir()
        fetch_extensions(extensions)
        images, measured = {}, {}
        _, dockerfile, context, args = publisher
        images["publisher-arm64"], measured["publisher-arm64"] = build.build_image(
            release, "publisher-arm64", dockerfile, context, args, work, blobs, repository, platform=TARGET)
        images["code-location-arm64"], measured["code-location-arm64"] = build.build_image(
            release, "code-location-arm64", CODE_LOCATION_DOCKERFILE, ".", {}, work, blobs, platform=TARGET,
            contexts={"foundation-outbox-publisher:local":
                      f"oci-layout://{work / 'publisher-arm64.oci'}@{images['publisher-arm64']['oci_manifest']}",
                      "foundation-engine-extensions:local": str(extensions)})
        _, dockerfile, context, args = runtime
        images["runtime-arm64"], measured["runtime-arm64"] = build.build_image(
            release, "runtime-arm64", dockerfile, context, args, work, blobs, repository, platform=TARGET)
        for name, entry in measured.items():
            print(f"aws-images {name}: {entry['seconds']}s peak_memory={entry['peak_memory_bytes']} "
                  f"blob_bytes={entry['blob_bytes']}", flush=True)
        loads_offline(work / "code-location-arm64.oci.tar", images["code-location-arm64"]["config"])
        migrator_starts(work / "runtime-arm64.oci.tar", images["runtime-arm64"]["config"])
    # One OCI layout over the kept blobs, each manifest named, as the release's own (release-push.sh).
    (out / "oci-layout").write_text('{"imageLayoutVersion": "1.0.0"}\n', encoding="utf-8")
    (out / "index.json").write_text(json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{"mediaType": build.layout_media_type(blobs, image["oci_manifest"]), "digest": image["oci_manifest"],
                       "size": (blobs / image["oci_manifest"].split(":")[1]).stat().st_size,
                       "annotations": {"org.opencontainers.image.ref.name": name}} for name, image in images.items()]}) + "\n",
        encoding="utf-8")
    tags = {name: f"{repository}:{commit}-aws-{name}" for name in images}
    for name, image in images.items():
        push(out, name, image["oci_manifest"], tags[name])
    # The publisher both platforms run: the release's amd64 manifest (the same repository) and this arm64 one.
    joined = f"{repository}:{commit}-aws-publisher"
    run("docker", "buildx", "imagetools", "create", "--tag", joined,
        f"{repository}@{arguments.release_publisher}", f"{repository}@{images['publisher-arm64']['oci_manifest']}")
    code_location = f"{repository}:{commit}-aws-code-location"
    run("docker", "buildx", "imagetools", "create", "--tag", code_location,
        f"{repository}@{images['code-location-arm64']['oci_manifest']}")
    runtime_tag = f"{repository}:{commit}-aws-runtime"
    run("docker", "buildx", "imagetools", "create", "--tag", runtime_tag,
        f"{repository}@{images['runtime-arm64']['oci_manifest']}")
    record = {"schema_version": SCHEMA, "source_commit": commit, "images": {
        "publisher": {"tag": joined, "digest": index_digest(joined), "platforms": ["linux/amd64", TARGET],
                      "linux/amd64": arguments.release_publisher, TARGET: images["publisher-arm64"]["oci_manifest"]},
        "code-location": {"tag": code_location, "digest": index_digest(code_location), "platforms": [TARGET],
                          TARGET: images["code-location-arm64"]["oci_manifest"]},
        "runtime": {"tag": runtime_tag, "digest": index_digest(runtime_tag), "platforms": [TARGET],
                    TARGET: images["runtime-arm64"]["oci_manifest"]}},
        "measured": measured}
    (out / "aws-images.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / "subjects.sha256").write_text("".join(
        f"{image['digest'].split(':')[1]}  {image['tag']}\n" for image in record["images"].values()), encoding="utf-8")
    print(json.dumps({name: image["digest"] for name, image in record["images"].items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
