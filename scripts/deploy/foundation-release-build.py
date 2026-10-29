#!/usr/bin/env python3
"""Build one Foundation release in CI, as content-addressed blobs and one manifest (root ADR-0276 §2, stage R1).

    foundation-release-build.py --out <dir> [--cache-ref <registry repository>]

Runs on a GitHub-hosted runner from a checkout of the commit it builds; nothing here holds a
credential. It builds what the server's release admission builds today, from the same definitions,
read from foundation-release-admission.py rather than repeated (its images, the Spark image and the
engine's packages for the jars), and also every image a compose file builds (foundation-api and the
recovery Postgres, which the server builds outside admission today). It writes:

    <dir>/manifest.json            foundation-platform.release_manifest.v1
    <dir>/blobs/sha256/<digest>    every file, jar and OCI blob the manifest names, once
    <dir>/subjects.sha256          the manifest and every blob, for the provenance attestation
    <dir>/measurements.json        seconds, peak memory and bytes per build (not signed)

The workflow (.github/workflows/foundation-release-build.yml) signs subjects.sha256 with a
Sigstore keyless SLSA provenance attestation; the server will verify it before it installs anything
(stage R2). Until stage R3 the server keeps building for itself and nothing reads these.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PLATFORM = ROOT / "platforms" / "foundation-platform"
SCHEMA = "foundation-platform.release_manifest.v1"
# Compose files whose `build:` entries are images the runtime starts; an image two files build from
# one Dockerfile (the publisher) is built once.
COMPOSE_FILES = ("docker-compose.yml", "compose.recovery.yml", "compose.lakehouse.yml", "compose.orchestration.yml")
FILE_MODES = {"foundation-outbox-publisher": "0555"}
JAR_MODE = "0444"


def admission():
    """The server's admission module: the one place the release's builds are defined."""
    path = ROOT / "scripts" / "deploy" / "foundation-release-admission.py"
    spec = importlib.util.spec_from_file_location("foundation_release_admission", path)
    module = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True
    spec.loader.exec_module(module)
    return module


def run(*args: str, cwd: Path = PLATFORM, capture: bool = False) -> str:
    result = subprocess.run(list(args), cwd=cwd, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else ""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compose_builds() -> list[tuple[str, str, str]]:
    """(name, dockerfile, context) of every image a runtime compose file builds, relative to the platform."""
    # Read as YAML, not through `docker compose config`: Compose versions differ in what they parse
    # without the runtime's environment (the runner's refused the Spark volume's interpolation).
    import yaml

    found = []
    for compose in COMPOSE_FILES:
        services = yaml.safe_load((PLATFORM / compose).read_text(encoding="utf-8")).get("services", {})
        for service in services.values():
            build = service.get("build")
            if not build:
                continue
            if isinstance(build, str):
                build = {"context": build}
            context = (PLATFORM / build.get("context", ".")).resolve().relative_to(PLATFORM.resolve()).as_posix()
            dockerfile = (Path(context) / build.get("dockerfile", "Dockerfile")).as_posix()
            name = service["image"].split(":")[0].split("/")[-1]
            found.append((name, dockerfile, context))
    return found


def image_builds(release) -> list[tuple[str, str, str, dict]]:
    """Admission's images first (with their contract's build arguments), then compose's, one per Dockerfile."""
    limits = release.build_limits()
    builds, dockerfiles = [], set()
    for name, _tag, dockerfile, context in release.RELEASE_IMAGES:
        builds.append((name, dockerfile, context, limits[name].get("build_args", {})))
        dockerfiles.add(dockerfile)
    for name, dockerfile, context in compose_builds():
        if dockerfile not in dockerfiles:
            builds.append((name, dockerfile, context, {}))
            dockerfiles.add(dockerfile)
    return builds


def base_images(dockerfile: Path) -> list[str]:
    return sorted({match for match in re.findall(r"(?im)^FROM\s+(?:--\S+\s+)*(\S+@sha256:[0-9a-f]{64})",
                                                    dockerfile.read_text(encoding="utf-8"))})


def peak_memory(builder: str) -> int | None:
    """The build container's peak memory (cgroup v2), when the host exposes it."""
    try:
        container = run("docker", "inspect", "--format", "{{.Id}}", f"buildx_buildkit_{builder}0", capture=True).strip()
    except subprocess.CalledProcessError:
        return None
    for path in (f"/sys/fs/cgroup/system.slice/docker-{container}.scope/memory.peak",
                 f"/sys/fs/cgroup/docker/{container}/memory.peak"):
        try:
            return int(Path(path).read_text())
        except (OSError, ValueError):
            continue
    return None


def keep_blob(source: Path, blobs: Path) -> tuple[str, int]:
    digest = sha256_of(source)
    target = blobs / digest
    if not target.exists():
        shutil.copyfile(source, target)
    return f"sha256:{digest}", source.stat().st_size


def build_image(release, name: str, dockerfile: str, context: str, args: dict, work: Path, blobs: Path,
                cache_ref: str | None = None) -> tuple[dict, dict]:
    """One image as an archive: its OCI blobs kept, its manifest and config digests returned."""
    builder = f"foundation-release-{name}"
    archive = work / f"{name}.oci.tar"
    run("docker", "buildx", "create", "--name", builder, "--driver", "docker-container",
        "--driver-opt", f"image={release.BUILDKIT_IMAGE}")
    started = time.monotonic()
    try:
        run("docker", "buildx", "build", "--builder", builder, "--pull", "--provenance=false", "--sbom=false",
            *(arg for key, value in args.items() for arg in ("--build-arg", f"{key}={value}")),
            # Every layer of every stage (mode=max), so cargo-chef's dependency layer survives the run;
            # a cache that cannot be read or written only costs a full build (root ADR-0276 §14).
            *(("--cache-from", f"type=registry,ref={cache_ref}:buildcache-{name}",
               "--cache-to", f"type=registry,ref={cache_ref}:buildcache-{name},mode=max,image-manifest=true,"
               "oci-mediatypes=true,ignore-error=true") if cache_ref else ()),
            # The docker exporter writes the OCI layout (index.json, blobs/sha256) and Docker's
            # manifest.json in one archive: `docker load` reads it, and the blobs are the OCI ones.
            "--output", f"type=docker,dest={archive}", "-f", dockerfile, context)
        seconds = round(time.monotonic() - started)
        memory = peak_memory(builder)
    finally:
        run("docker", "buildx", "rm", builder)
    layout = work / f"{name}.oci"
    with tarfile.open(archive) as tar:
        tar.extractall(layout, filter="data")
    index = json.loads((layout / "index.json").read_text(encoding="utf-8"))
    if len(index["manifests"]) != 1:
        raise ValueError(f"{name}: expected one image manifest, the archive has {len(index['manifests'])}")
    manifest_digest = index["manifests"][0]["digest"]
    manifest = json.loads((layout / "blobs" / "sha256" / manifest_digest.split(":")[1]).read_text(encoding="utf-8"))
    total = 0
    for blob in (layout / "blobs" / "sha256").iterdir():
        digest, size = keep_blob(blob, blobs)
        if digest != f"sha256:{blob.name}":
            raise ValueError(f"{name}: OCI blob {blob.name} is not named by its sha256")
        total += size
    record = {"oci_manifest": manifest_digest, "config": manifest["config"]["digest"]}
    measured = {"seconds": seconds, "peak_memory_bytes": memory, "blob_bytes": total,
                "base_images": base_images(PLATFORM / dockerfile)}
    return record, measured


def publisher_binary(archive: Path, config_digest: str, out: Path) -> None:
    """The executable from the publisher image, through docker so the image ID is checked too."""
    loaded = run("docker", "load", "-i", str(archive), capture=True)
    match = re.search(r"Loaded image ID: (sha256:[0-9a-f]{64})", loaded)
    image = match.group(1) if match else run("docker", "image", "inspect", "--format", "{{.Id}}",
                                             loaded.strip().rsplit(" ", 1)[-1], capture=True).strip()
    if image != config_digest:
        raise ValueError(f"the loaded publisher image {image} is not its config {config_digest}")
    container = run("docker", "create", image, capture=True).strip()
    try:
        run("docker", "cp", f"{container}:/usr/local/bin/foundation-outbox-publisher", str(out))
    finally:
        run("docker", "rm", container, capture=True)


def resolve_jars(release, work: Path) -> list[Path]:
    """The engine's packages as the server resolves them: spark-submit in the pinned Spark image."""
    spark_image = release.spark_image_from_compose(run(*release.SPARK_COMPOSE_CONFIG, capture=True).encode())
    packages = release.engine_packages(PLATFORM)
    cache = work / "ivy"
    cache.mkdir(mode=0o777)
    cache.chmod(0o777)
    resolver = work / "resolve.py"
    resolver.write_text("print('release dependency resolution complete')\n")
    run("docker", "run", "--rm", "--mount", f"type=bind,src={cache},dst=/resolve",
        "--mount", f"type=bind,src={resolver},dst=/resolve.py,readonly",
        "--entrypoint", "/opt/spark/bin/spark-submit", spark_image,
        "--conf", "spark.jars.ivy=/resolve", "--packages", packages, "/resolve.py")
    # Spark (uid 185) wrote the cache owner-only; the runner's account reads the jars and removes
    # the directory afterwards.
    run("docker", "run", "--rm", "--user", "0", "--mount", f"type=bind,src={cache},dst=/resolve",
        "--entrypoint", "chmod", spark_image, "-R", "a+rwX", "/resolve")
    jars = sorted((cache / "jars").glob("*.jar"))
    if not jars or any(jar.is_symlink() or not jar.is_file() for jar in jars):
        raise ValueError("the dependency resolver produced no jars or a non-regular one")
    return jars


def layout_media_type(blobs: Path, digest: str) -> str:
    return json.loads((blobs / digest.split(":")[1]).read_text(encoding="utf-8"))["mediaType"]


def toolchain() -> str:
    match = re.search(r'(?m)^channel\s*=\s*"([^"]+)"', (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))
    if not match:
        raise ValueError("rust-toolchain.toml names no channel")
    return match.group(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--cache-ref", help="registry repository whose buildcache-<image> tags hold the build cache")
    arguments = parser.parse_args()
    out = arguments.out
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    blobs = out / "blobs" / "sha256"
    blobs.mkdir(parents=True)
    release = admission()
    commit = run("git", "rev-parse", "HEAD", cwd=ROOT, capture=True).strip()
    release.require_sha(commit)
    images, measured, files = {}, {}, {}
    with tempfile.TemporaryDirectory(prefix="release-build-") as temporary:
        work = Path(temporary)
        for name, dockerfile, context, args in image_builds(release):
            images[name], measured[name] = build_image(release, name, dockerfile, context, args, work, blobs,
                                                         arguments.cache_ref)
            print(f"release-build {name}: {measured[name]['seconds']}s peak_memory={measured[name]['peak_memory_bytes']} "
                  f"blob_bytes={measured[name]['blob_bytes']}", flush=True)
        binary = work / "foundation-outbox-publisher"
        publisher_binary(work / "publisher.oci.tar", images["publisher"]["config"], binary)
        started = time.monotonic()
        jars = resolve_jars(release, work)
        measured["jars"] = {"seconds": round(time.monotonic() - started)}
        # The executable and the jars travel as one image of files (`FROM scratch`), so the registry
        # holds them as it holds the images; the manifest names each file's own digest as well.
        tree = work / "files"
        (tree / "jars").mkdir(parents=True)
        for path, name, mode in [(binary, "foundation-outbox-publisher", FILE_MODES["foundation-outbox-publisher"]),
                                 *((jar, f"jars/{jar.name}", JAR_MODE) for jar in jars)]:
            shutil.copyfile(path, tree / name)
            (tree / name).chmod(int(mode, 8))
            files[name] = {"sha256": f"sha256:{sha256_of(path)}", "size": path.stat().st_size, "mode": mode}
        (work / "files.Dockerfile").write_text("FROM scratch\nCOPY . /\n", encoding="utf-8")
        images["files"], measured["files"] = build_image(release, "files", str(work / "files.Dockerfile"), str(tree), {},
                                                         work, blobs)
    manifest = {
        "schema_version": SCHEMA,
        "source_commit": commit,
        "workflow": ".github/workflows/foundation-release-build.yml",
        "run_url": (f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/"
                    f"{os.environ['GITHUB_RUN_ID']}" if os.environ.get("GITHUB_RUN_ID") else None),
        "toolchain": {"rust": toolchain(), "buildkit": release.BUILDKIT_IMAGE,
                      "base_images": sorted({image for entry in measured.values() for image in entry.get("base_images", [])})},
        "files": dict(sorted(files.items())),
        "images": images,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    named = {f"sha256:{blob.name}" for blob in blobs.iterdir()}
    wanted = {d for image in images.values() for d in image.values()}
    if not wanted <= named:
        raise ValueError(f"the manifest names blobs that were not kept: {sorted(wanted - named)}")
    # One OCI layout over the blobs, each image named, so the exact bytes built here are what the
    # registry receives (scripts/ci/release-push.sh, skopeo --preserve-digests).
    (out / "oci-layout").write_text('{"imageLayoutVersion": "1.0.0"}\n', encoding="utf-8")
    index = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [
        {"mediaType": layout_media_type(blobs, image["oci_manifest"]), "digest": image["oci_manifest"],
         "size": (blobs / image["oci_manifest"].split(":")[1]).stat().st_size,
         "annotations": {"org.opencontainers.image.ref.name": name}} for name, image in images.items()]}
    (out / "index.json").write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
    subjects = [f"{sha256_of(out / 'manifest.json')}  manifest.json",
                *(f"{blob.name}  blobs/sha256/{blob.name}" for blob in sorted(blobs.iterdir())),
                *(f"{entry['sha256'].split(':')[1]}  files/{name}" for name, entry in sorted(files.items()))]
    (out / "subjects.sha256").write_text("\n".join(subjects) + "\n", encoding="utf-8")
    measured["total"] = {"blobs": len(named), "blob_bytes": sum(blob.stat().st_size for blob in blobs.iterdir())}
    (out / "measurements.json").write_text(json.dumps(measured, indent=2) + "\n", encoding="utf-8")
    print(f"release-build {commit}: {len(named)} blobs, {measured['total']['blob_bytes']} bytes", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
