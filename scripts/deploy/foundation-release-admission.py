#!/usr/bin/python3 -I
"""Admit Foundation source from canonical main; recheck installed bytes before execution.

Installed in the independently administered, root-owned monorepo control checkout at
/opt/perfectory-control/current. Never run a candidate archive's verifier with production
authority. Repository identity and Git transport remain the root publication SSOT (ADR-0134).

Host preconditions (root ADR-0134 §5, ADR-0136) fail here with their names, not as a generic
error: root must read the public repository identity from api.github.com without credentials,
root's Git must fetch canonical main over HTTPS without a credential helper, and Docker must
provide Buildx's docker-container driver.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile


CONTROL_ROOT = Path(__file__).resolve().parents[2]
SOURCE_CACHE = Path("/var/lib/perfectory/foundation-release-source.git")
RELEASE_ROOT = Path("/opt/foundation-platform")
ARTIFACT_ROOT = RELEASE_ROOT / "artifacts"
BUILD_CONTRACT = CONTROL_ROOT / "tools/release-build.contract.json"
JOB_SPECS = CONTROL_ROOT / "platforms/foundation-platform/orchestration/jobs.v1.json"
# Held exclusively for the whole build; every registered job's ExecStartPre (verify-current) refuses
# while it is held (ADR-0137). /run is root-owned tmpfs, so a reboot cannot leave it stale.
BUILD_LOCK = Path("/run/foundation-platform-release-build.lock")
# A oneshot job is "activating" for its whole run and never "active"; any state but these is running.
STOPPED_STATES = {"inactive", "failed"}
BUILDKIT_IMAGE = "moby/buildkit:v0.33.0@sha256:6c2fa84a6b61ccd72899dde4239f8d5717f05f9a8ca6f3cad185fb1a95a94de3"
# The publisher's dependency layer (`cargo chef cook`, keyed by Cargo.lock and the manifests) is kept
# between releases in BuildKit's local cache: an OCI layout whose blobs are named by their sha256.
# Every blob is re-hashed before a build may import it, and only the administrator writes it
# (ADR-0155). Tippecanoe builds in two minutes and stays uncached.
BUILD_CACHE_ROOT = Path("/var/lib/perfectory/foundation-release-build-cache")
CACHED_IMAGES = {"publisher"}
SUBTREE = "platforms/foundation-platform"
ID_FILE = ".foundation-release-id"
ARCHIVE_FILE = ".foundation-release-archive-sha256"
MARKERS = {ID_FILE, ARCHIVE_FILE}


def require_sha(value: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("release id must be a lowercase 40-character Git SHA")


def archive_files(raw: bytes) -> dict[str, tuple[int, bytes]]:
    """Read regular files only; no tar extraction, links, devices or traversal."""
    result = {}
    directories = set()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:*") as archive:
        for member in archive:
            name = member.name
            while name.startswith("./"):
                name = name[2:]
            if name in ("", ".") and member.isdir():
                continue
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or "\\" in name or not path.parts:
                raise ValueError("archive contains an unsafe path")
            name = path.as_posix()
            if name in MARKERS:
                raise ValueError("archive contains reserved release markers")
            if member.isdir():
                directories.add(name)
                continue
            if not member.isfile() or name in result:
                raise ValueError("archive contains a link, special file or duplicate file")
            if member.mode & 0o7000:
                raise ValueError("archive contains privileged file permissions")
            result[name] = (0o555 if member.mode & 0o111 else 0o444, archive.extractfile(member).read())
    if not result:
        raise ValueError("archive has no source files")
    parents = {p.as_posix() for name in result for p in PurePosixPath(name).parents if p != PurePosixPath(".")}
    if directories - parents or set(result) & parents:
        raise ValueError("archive contains unexpected or conflicting directories")
    return result


def release_files(git, release_id: str) -> dict[str, tuple[int, bytes]]:
    require_sha(release_id)
    try:
        git("merge-base", "--is-ancestor", release_id, "refs/heads/main")
    except subprocess.CalledProcessError as error:
        raise ValueError("release commit is not in independently fetched canonical main") from error
    return archive_files(git("archive", "--format=tar", f"{release_id}:{SUBTREE}"))


def protected_path(path: Path, owner: int = 0) -> None:
    """Only the administrator may replace any component, including symlink parents.

    Root-owned sticky temporary parents are safe for root/owner-owned fixture directories;
    non-sticky shared directories and foreign-owned path components are not.
    """
    path = path.absolute()
    for entry in (path, *path.parents):
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"protected path contains a symbolic link: {entry}")
        if info.st_uid not in (0, owner):
            raise ValueError(f"protected path has an untrusted owner: {entry}")
        sticky_root = stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and info.st_mode & stat.S_ISVTX
        if info.st_mode & 0o022 and not sticky_root:
            raise ValueError(f"protected path is writable by another principal: {entry}")


def prepare_release(expected, release_id: str, archive: Path, target: Path) -> None:
    raw = archive.read_bytes()
    if archive_files(raw) != expected:
        raise ValueError("archive contents do not match the canonical release commit")
    if any(target.iterdir()):
        raise ValueError("release staging directory must be empty")
    # Write the independently fetched Git bytes, never extract the supplied tar archive.
    for name, (mode, contents) in expected.items():
        file = target / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(contents)
        file.chmod(mode)
    (target / ID_FILE).write_text(release_id + "\n", encoding="ascii")
    (target / ARCHIVE_FILE).write_text(hashlib.sha256(raw).hexdigest() + "\n", encoding="ascii")
    for marker in MARKERS:
        (target / marker).chmod(0o444)
    for directory, _, _ in os.walk(target, topdown=False):
        Path(directory).chmod(0o555)


def verify_release(expected, release_id: str, target: Path, owner: int = 0) -> None:
    protected_path(target, owner)
    actual = set()
    for directory, dirs, files in os.walk(target, followlinks=False):
        for entry in [Path(directory), *(Path(directory) / name for name in dirs + files)]:
            info = entry.lstat()
            if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise ValueError("release contains a link or special file")
            if info.st_uid != owner:
                raise ValueError("release has an untrusted owner")
            if info.st_mode & 0o222:
                raise ValueError("release is writable; install an immutable canonical release")
            if stat.S_ISREG(info.st_mode):
                if info.st_nlink != 1:
                    raise ValueError("release contains a hard-linked file")
                actual.add(entry.relative_to(target).as_posix())
    if actual != set(expected) | MARKERS:
        raise ValueError("installed release file set differs from canonical source")
    for name, (mode, contents) in expected.items():
        file = target / name
        if stat.S_IMODE(file.stat().st_mode) != mode or file.read_bytes() != contents:
            raise ValueError(f"installed release contents differ from canonical source: {name}")
    if (target / ID_FILE).read_text(encoding="ascii").strip() != release_id:
        raise ValueError("installed release identity is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}\n", (target / ARCHIVE_FILE).read_text(encoding="ascii")):
        raise ValueError("installed release archive digest is invalid")


def control_environment() -> dict[str, str]:
    # The canonical repository is public: identity and main are read anonymously (ADR-0136).
    # No token, caller PATH, Python import path, Git override or dynamic-loader injection survives.
    result = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "HOME": "/root"}
    return result


def artifact_files(target: Path) -> dict[str, str]:
    result = {}
    for path in target.rglob("*"):
        if path.is_symlink() or not (path.is_dir() or path.is_file()):
            raise ValueError("artifact contains a link or special file")
        if path.is_file() and path != target / "build.json":
            result[path.relative_to(target).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def seal_artifacts(release_id: str, target: Path, images: dict[str, str],
                   caches: dict[str, str | None] | None = None) -> None:
    """Only the trusted builder calls this, in an administrator-owned staging directory.

    `caches` names, per cached image, the sha256 of the build-cache index the build imported
    (None for a clean build), so a release records which kept layers it was built from (ADR-0155).
    """
    files = artifact_files(target)
    jars = [name for name in files if re.fullmatch(r"jars/[A-Za-z0-9_.-]+\.jar", name)]
    if not jars or set(files) != {"foundation-outbox-publisher", *jars}:
        raise ValueError("unexpected build output; a publisher and resolved jars are required")
    if set(images) != {name for name, _, _, _ in RELEASE_IMAGES} or not all(
            re.fullmatch(r"sha256:[0-9a-f]{64}", image) for image in images.values()):
        raise ValueError("every release image needs an immutable image ID")
    manifest = {"source": release_id, "files": files}
    for name, tag_of, _, _ in RELEASE_IMAGES:
        manifest[f"{name}_image"] = images[name]
        manifest[f"{name}_tag"] = tag_of(release_id)
    for name, index in (caches or {}).items():
        if index is not None and not re.fullmatch(r"[0-9a-f]{64}", index):
            raise ValueError(f"{name} build cache must be named by the sha256 of its index")
        manifest[f"{name}_build_cache_index"] = index
    (target / "build.json").write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
    for path in target.rglob("*"):
        if path.is_file():
            if os.geteuid() == 0:
                os.chown(path, 0, 0)
            path.chmod(0o555 if path.name == "foundation-outbox-publisher" else 0o444)
    for directory, _, _ in os.walk(target, topdown=False):
        if os.geteuid() == 0:
            os.chown(directory, 0, 0)
        Path(directory).chmod(0o555)


def verify_artifacts(release_id: str, target: Path, owner: int = 0) -> None:
    protected_path(target, owner)
    for path in (target, *target.rglob("*")):
        info = path.lstat()
        if info.st_uid != owner or info.st_mode & 0o222:
            raise ValueError("artifact is writable or has an untrusted owner")
        if path.is_symlink() or not (path.is_dir() or path.is_file()) or (path.is_file() and info.st_nlink != 1):
            raise ValueError("artifact contains a link or special file")
    manifest = json.loads((target / "build.json").read_text(encoding="utf-8"))
    if manifest.get("source") != release_id:
        raise ValueError("artifact source differs from the admitted release")
    actual = artifact_files(target)
    if set(actual) != set(manifest.get("files", {})):
        raise ValueError("artifact file set differs from trusted build output")
    if actual != manifest["files"]:
        raise ValueError("artifact contents differ from trusted build output")
    if not actual.get("foundation-outbox-publisher") or not any(name.startswith("jars/") for name in actual):
        raise ValueError("artifact build output is incomplete")
    if stat.S_IMODE((target / "foundation-outbox-publisher").stat().st_mode) != 0o555:
        raise ValueError("publisher artifact must be executable and immutable")


def engine_packages(target: Path) -> str:
    # Root can write through source 0555 permissions. -B is essential: an import-created pyc
    # would change the admitted file set and make the following activation refuse the release.
    return subprocess.check_output(
        ["/usr/bin/python3", "-I", "-B", "-c",
         "import sys;sys.path.insert(0,'infra/lakehouse/spark/jobs');"
         "from lakehouse_engine import iceberg_packages;print(iceberg_packages())"],
        cwd=target, env=control_environment(), stderr=subprocess.PIPE,
    ).decode().strip()


# Spark is in the lakehouse-batch profile; without it Compose leaves the service out of `config`.
SPARK_COMPOSE_CONFIG = ("/usr/bin/docker", "compose", "--env-file", "/dev/null", "-f", "compose.lakehouse.yml",
                        "--profile", "lakehouse-batch", "config", "--format", "json")


def spark_image_from_compose(raw: bytes) -> str:
    try:
        image = json.loads(raw)["services"]["spark"]["image"]
    except (ValueError, KeyError, TypeError) as error:
        raise ValueError("compose.lakehouse.yml config does not name the spark service image") from error
    if not isinstance(image, str) or not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Spark base image must be pinned by digest")
    return image


def publisher_tag(release_id: str) -> str:
    # A tagged image survives `docker image prune`; an untagged --load result does not.
    return f"foundation-outbox-publisher:{release_id}"


def tippecanoe_tag(release_id: str) -> str:
    return f"foundation-tippecanoe:{release_id}"


# The images a release builds, each recorded in build.json as <name>_image (ID) and <name>_tag.
# FLOOR runs the publisher image; the lakehouse tile bake runs tippecanoe from the repository
# Dockerfile, no longer a tag built by hand on the host (root ADR-0134 §3).
RELEASE_IMAGES = (
    ("publisher", publisher_tag, "services/foundation-outbox-publisher/Dockerfile.lakehouse-control", "."),
    ("tippecanoe", tippecanoe_tag, "infra/tiles/tippecanoe/Dockerfile", "infra/tiles/tippecanoe"),
)


def require_release_images(release_id: str, artifacts: Path) -> None:
    """Every image the release built must still exist, under its recorded tag, with its ID."""
    manifest = json.loads((artifacts / "build.json").read_text(encoding="utf-8"))
    for name, tag_of, _, _ in RELEASE_IMAGES:
        tag, image = manifest.get(f"{name}_tag"), manifest.get(f"{name}_image")
        if tag != tag_of(release_id) or not isinstance(image, str):
            raise ValueError(f"build output does not record this release's {name} image tag")
        try:
            actual = subprocess.check_output(
                ["/usr/bin/docker", "image", "inspect", "--format", "{{.Id}}", tag],
                env=control_environment(), stderr=subprocess.DEVNULL,
            ).decode().strip()
        except (OSError, subprocess.CalledProcessError) as error:
            raise ValueError(f"{name} image {tag} is missing; as root move {artifacts} aside and run "
                             "prepare again to rebuild it (runbook: release admission)") from error
        if actual != image:
            raise ValueError(f"{name} image {tag} is not the image the release build recorded")


CONTROL_COMMIT_FILE = ".perfectory-control-commit"


def check_control_commit(git, release_id: str, control_root: Path) -> str:
    """The control checkout must be canonical main at or before the release it installs.

    Otherwise the verifier and the release script that admit a release can come from a
    different line of history than the release itself (root ADR-0134 §1).
    """
    try:
        control = (control_root / CONTROL_COMMIT_FILE).read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as error:
        raise ValueError(f"control checkout has no {CONTROL_COMMIT_FILE}; reinstall it (runbook)") from error
    require_sha(control)
    try:
        git("merge-base", "--is-ancestor", control, release_id)
    except subprocess.CalledProcessError as error:
        raise ValueError(f"control checkout {control} is not an ancestor of release {release_id}; "
                         "update the control checkout to canonical main") from error
    return control


def require_buildx() -> None:
    # Without Buildx the only build left is an uncapped `docker build`; refuse instead.
    try:
        subprocess.run(["/usr/bin/docker", "buildx", "version"], check=True, env=control_environment(),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError("host precondition: Docker Buildx is not installed for root (ADR-0134 §5)") from error


def build_limits() -> dict[str, dict[str, object]]:
    """CPU and memory per build container, from the control checkout's contract (ADR-0137)."""
    try:
        builders = json.loads(BUILD_CONTRACT.read_text(encoding="utf-8"))["builders"]
        limits = {}
        # Each image builder's job count reaches its Dockerfile as the build argument it names.
        jobs_argument = {"publisher": "CARGO_BUILD_JOBS", "tippecanoe": "MAKE_JOBS"}
        for name, keys in (("publisher", ("cpus", "cargo_build_jobs")), ("tippecanoe", ("cpus", "make_jobs")),
                           ("dependency_resolver", ("cpus",))):
            entry = builders[name]
            for key in keys:
                if not isinstance(entry[key], int) or isinstance(entry[key], bool) or not 1 <= entry[key] <= 64:
                    raise ValueError(f"{name}.{key} must be an integer from 1 to 64")
            if not isinstance(entry["memory_limit"], str) or not re.fullmatch(r"[1-9][0-9]*[mg]", entry["memory_limit"]):
                raise ValueError(f"{name}.memory_limit must look like 512m or 22g")
            limits[name] = dict(entry)
            if name in jobs_argument:
                limits[name]["build_args"] = {jobs_argument[name]: entry[keys[1]]}
        return limits
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"release build contract {BUILD_CONTRACT} is unreadable: {error}") from error


def require_no_registered_job_running() -> None:
    """The build is sized as the host's one-shot job; it does not start beside another (ADR-0137)."""
    try:
        units = sorted({job["systemd_service"] for job in json.loads(JOB_SPECS.read_text(encoding="utf-8"))["jobs"]})
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"job specs {JOB_SPECS} are unreadable: {error}") from error
    try:
        states = {unit: subprocess.run(["/usr/bin/systemctl", "show", "--property=ActiveState", "--value", unit],
                                       env=control_environment(), check=True, capture_output=True,
                                       text=True).stdout.strip() for unit in units}
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError("host precondition: systemctl cannot report the registered jobs (ADR-0137)") from error
    running = [f"{unit} ({state or 'unknown'})" for unit, state in states.items() if state not in STOPPED_STATES]
    if running:
        raise ValueError("a registered job is running (" + ", ".join(running) + "); the release build is the host's "
                         "one-shot job, so pause the DAGs, wait for it to finish and run prepare again (ADR-0137)")


@contextlib.contextmanager
def release_build_lock():
    """Exclusive while a release builds; taken before the job check, so no job can slip in after it."""
    with open(BUILD_LOCK, "a", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("another release build is running (ADR-0137)") from error
        yield


def require_no_release_build() -> None:
    """A registered job does not start while a release builds (ADR-0137); Airflow retries it later."""
    if not BUILD_LOCK.exists():
        return
    with open(BUILD_LOCK, encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("a release build is running; this job starts after it finishes (ADR-0137)") from error


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified_build_cache(name: str) -> tuple[Path, str] | None:
    """The kept cache for an image and the sha256 of its index, if every byte still matches its name.

    A cache that fails any check is deleted and the build starts clean: a cache can only save time,
    so a damaged one costs a cold build, never a release built from bytes nobody can name.
    """
    cache = BUILD_CACHE_ROOT / name
    if not cache.exists():
        return None
    try:
        protected_path(cache)
        blobs = cache / "blobs" / "sha256"
        present = set()
        owner = os.geteuid()  # root in production; the test runner's own account in fixtures
        for entry in [cache, *cache.rglob("*")]:
            info = entry.lstat()
            if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise ValueError(f"cache contains a link or special file: {entry}")
            if info.st_uid != owner or info.st_mode & 0o022:
                raise ValueError(f"cache entry is owned or writable by another principal: {entry}")
            if stat.S_ISREG(info.st_mode) and entry.parent == blobs:
                if not re.fullmatch(r"[0-9a-f]{64}", entry.name) or file_sha256(entry) != entry.name:
                    raise ValueError(f"cache blob does not hash to its name: {entry.name}")
                present.add(entry.name)
        index = json.loads((cache / "index.json").read_text(encoding="utf-8"))
        referenced = {manifest["digest"].removeprefix("sha256:") for manifest in index["manifests"]}
        if not referenced or not referenced <= present:
            raise ValueError("cache index names a blob the cache does not hold")
        return cache, file_sha256(cache / "index.json")
    except (OSError, KeyError, TypeError, AttributeError, ValueError, json.JSONDecodeError) as error:
        print(f"release build cache for {name} discarded, building clean: {error}", file=sys.stderr)
        shutil.rmtree(cache, ignore_errors=True)
        return None


def keep_build_cache(name: str, written: Path) -> None:
    """Replace the kept cache with the one this build exported; the old one is removed after the swap."""
    cache = BUILD_CACHE_ROOT / name
    retired = BUILD_CACHE_ROOT / (".retired" + written.name.removeprefix(".written"))
    had_cache = cache.exists()
    if had_cache:
        os.rename(cache, retired)
    os.rename(written, cache)
    if had_cache:
        shutil.rmtree(retired)


def clear_abandoned_build_caches() -> None:
    """An interrupted build can leave an exported or retired cache; only the kept one survives a build."""
    for entry in BUILD_CACHE_ROOT.iterdir():
        if entry.name.startswith((".written-", ".retired-")):
            shutil.rmtree(entry)


def build_artifacts(target: Path) -> None:
    """Build admitted source without runtime env/credentials; never accept caller binaries."""
    # Older merged releases ran an external publisher and honored source/cache overrides.
    # They cannot be promoted merely by building a new artifact they never execute.
    if not (target / "scripts/ops/admitted-writer-runtime.sh").is_file():
        raise ValueError("release predates the admitted writer runtime; install a supported merged release")
    release_id = target.name
    limits = build_limits()
    require_buildx()
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    protected_path(ARTIFACT_ROOT)
    destination = ARTIFACT_ROOT / release_id
    if destination.exists():
        verify_artifacts(release_id, destination)
        return
    with release_build_lock():
        require_no_registered_job_running()
        build_locked(target, release_id, destination, limits)


def build_locked(target: Path, release_id: str, destination: Path, limits) -> None:
    resolution = limits["dependency_resolver"]
    build_environment = control_environment()
    def run(*args):
        return subprocess.check_output(list(args), cwd=target, env=build_environment, stderr=subprocess.PIPE)
    with tempfile.TemporaryDirectory(prefix=".build-", dir=ARTIFACT_ROOT) as temporary:
        work = Path(temporary)
        # Keep the tempfile parent 0700. Docker's daemon binds the child directly, so the
        # container can resolve jars without letting host users poison its writable cache.
        docker_config = work / "docker"
        docker_config.mkdir(mode=0o700)
        build_environment["DOCKER_CONFIG"] = str(docker_config)
        output = work / "output"
        output.mkdir()
        images = {}
        caches_used = {}
        BUILD_CACHE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        protected_path(BUILD_CACHE_ROOT)
        clear_abandoned_build_caches()
        # One temporary builder per image, each held to its own contract entry: a builder caps
        # every build it runs, so sharing one would give tippecanoe the publisher's 22g (ADR-0137).
        for name, tag_of, dockerfile, context in RELEASE_IMAGES:
            cache_arguments: tuple[str, ...] = ("--no-cache",)
            written = None
            if name in CACHED_IMAGES:
                kept = verified_build_cache(name)
                caches_used[name] = kept[1] if kept else None
                written = BUILD_CACHE_ROOT / f".written-{name}-{work.name.removeprefix('.')}"
                cache_arguments = (
                    *(("--cache-from", f"type=local,src={kept[0]}") if kept else ()),
                    "--cache-to", f"type=local,dest={written},mode=max",
                )
            limit = limits[name]
            builder = f"foundation-release-{name}-" + work.name.removeprefix(".")
            configuration = work / f"buildkitd-{name}.toml"
            configuration.write_text("")
            run("/usr/bin/docker", "buildx", "create", "--name", builder, "--driver", "docker-container",
                "--driver-opt", f"image={BUILDKIT_IMAGE},memory={limit['memory_limit']},memory-swap={limit['memory_limit']},"
                f"cpu-period=100000,cpu-quota={limit['cpus'] * 100000},restart-policy=no",
                "--buildkitd-config", str(configuration))
            try:
                iid = work / f"{name}-image"
                run("/usr/bin/docker", "buildx", "build", "--builder", builder, "--load", "--pull",
                    *cache_arguments, "--iidfile", str(iid), "--tag", tag_of(release_id),
                    *(arg for key, value in limit["build_args"].items() for arg in ("--build-arg", f"{key}={value}")),
                    "-f", dockerfile, context)
                images[name] = iid.read_text().strip()
                if not re.fullmatch(r"sha256:[0-9a-f]{64}", images[name]):
                    raise ValueError(f"{name} build did not return an immutable image ID")
                if written is not None:
                    keep_build_cache(name, written)
            finally:
                if written is not None and written.exists():
                    shutil.rmtree(written, ignore_errors=True)
                try:
                    run("/usr/bin/docker", "buildx", "rm", builder)
                except subprocess.CalledProcessError as error:
                    raise ValueError(f"temporary builder cleanup failed; administrator must remove {builder}") from error
        publisher_image = images["publisher"]
        container = run("/usr/bin/docker", "create", publisher_image).decode().strip()
        try:
            run("/usr/bin/docker", "cp", container + ":/usr/local/bin/foundation-outbox-publisher",
                str(output / "foundation-outbox-publisher"))
        finally:
            run("/usr/bin/docker", "rm", container)
        # The existing image builds a native binary. Refuse promotion on a host whose loader
        # or libraries cannot run it; a successful image build alone does not prove that.
        libraries = run("/usr/bin/ldd", str(output / "foundation-outbox-publisher"))
        if b"not found" in libraries:
            raise ValueError("publisher requires unavailable host runtime libraries")
        # Compose and the engine contract remain the image/package SSOT. The clean process
        # environment and /dev/null env-file exclude operator overrides and production tokens.
        spark_image = spark_image_from_compose(run(*SPARK_COMPOSE_CONFIG))
        packages = engine_packages(target)
        resolver = work / "resolve.py"
        resolver.write_text("print('release dependency resolution complete')\n")
        cache = work / "ivy"
        cache.mkdir(mode=0o777)
        cache.chmod(0o777)  # Only the disposable credential-free Spark container can reach it.
        run("/usr/bin/docker", "run", "--rm", "--cpus", str(resolution["cpus"]), "--memory", resolution["memory_limit"],
            "--memory-swap", resolution["memory_limit"],
            "--mount", f"type=bind,src={cache},dst=/resolve",
            "--mount", f"type=bind,src={resolver},dst=/resolve.py,readonly",
            "--entrypoint", "/opt/spark/bin/spark-submit", spark_image,
            "--conf", "spark.jars.ivy=/resolve", "--packages", packages, "/resolve.py")
        (output / "jars").mkdir()
        for jar in (cache / "jars").glob("*.jar"):
            if jar.is_symlink() or not jar.is_file():
                raise ValueError("dependency resolver produced a non-regular jar")
            shutil.copyfile(jar, output / "jars" / jar.name)
        seal_artifacts(release_id, output, images, caches_used)
        output.rename(destination)
    verify_artifacts(release_id, destination)


class CanonicalSource:
    def __init__(self):
        self.transport = CONTROL_ROOT / "scripts/github/safe-git-transport.sh"
        self.identity_reader = CONTROL_ROOT / "scripts/github/show-public-repository-identity.sh"
        self.identity = CONTROL_ROOT / "tools/github/repository-identity.json"
        for path in (Path(__file__), self.transport, self.identity_reader, self.identity,
                     CONTROL_ROOT / "scripts/github/github-policy-json.py"):
            protected_path(path.resolve())

    def git(self, *args: str) -> bytes:
        return subprocess.check_output(
            # --anonymous: no credential helper; a public HTTPS fetch must not depend on gh.
            ["/bin/bash", str(self.transport), "--anonymous", "--repository", str(SOURCE_CACHE), *args],
            env=control_environment(), stderr=subprocess.PIPE,
        )

    def refresh(self) -> None:
        expected = json.loads(self.identity.read_text(encoding="utf-8"))
        try:
            actual = json.loads(subprocess.check_output(
                ["/bin/bash", str(self.identity_reader)], env=control_environment(), stderr=subprocess.PIPE,
            ))
        except subprocess.CalledProcessError as error:
            raise ValueError("host precondition: root cannot read the public repository identity from "
                             "https://api.github.com without credentials (ADR-0136)") from error
        if actual != expected:
            raise ValueError("live canonical repository identity differs from the root identity policy")
        SOURCE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        protected_path(SOURCE_CACHE.parent)
        if not SOURCE_CACHE.exists():
            subprocess.run(
                ["/bin/bash", str(self.transport), "--anonymous", "--no-repository", "init", "--bare",
                 str(SOURCE_CACHE)],
                check=True, env=control_environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        self.check_cache()
        # Literal canonical identity, not origin or a caller-supplied source repository/ref.
        try:
            self.git("fetch", "--no-tags", "https://" + expected["hostname"] + "/" + expected["full_name"] + ".git",
                     "+refs/heads/main:refs/heads/main")
        except subprocess.CalledProcessError as error:
            raise ValueError("host precondition: root cannot fetch canonical main over HTTPS without "
                             "credentials (ADR-0134 §5, ADR-0136)") from error

    def check_cache(self) -> None:
        protected_path(SOURCE_CACHE)
        for directory, dirs, files in os.walk(SOURCE_CACHE):
            for name in dirs + files:
                protected_path(Path(directory) / name)
        if (SOURCE_CACHE / "objects/info/alternates").exists():
            raise ValueError("canonical source cache cannot use object alternates")


def current_release() -> Path:
    protected_path(RELEASE_ROOT)
    current = RELEASE_ROOT / "current"
    if not current.is_symlink() or current.lstat().st_uid != 0:
        raise ValueError("current must be an administrator-owned release symlink")
    target = current.resolve(strict=True)
    if target.parent != RELEASE_ROOT / "releases":
        raise ValueError("current must select an installed release")
    require_sha(target.name)
    return target


def main() -> int:
    os.umask(0o022)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("release_id")
    prepare.add_argument("archive", type=Path)
    prepare.add_argument("target", type=Path)
    verify = sub.add_parser("verify")
    verify.add_argument("target", type=Path)
    build = sub.add_parser("build")
    build.add_argument("target", type=Path)
    sub.add_parser("verify-current")
    args = parser.parse_args()
    try:
        if os.geteuid() != 0:
            raise ValueError("release admission must run as the host administrator")
        source = CanonicalSource()
        if args.command == "prepare":
            protected_path(args.target)
            source.refresh()
            check_control_commit(source.git, args.release_id, CONTROL_ROOT)
            prepare_release(release_files(source.git, args.release_id), args.release_id, args.archive, args.target)
        else:
            source.check_cache()
            if args.command == "verify-current":
                require_no_release_build()
            target = current_release() if args.command == "verify-current" else args.target
            verify_release(release_files(source.git, target.name), target.name, target)
            if args.command == "build":
                build_artifacts(target)
            else:
                verify_artifacts(target.name, ARTIFACT_ROOT / target.name)
            require_release_images(target.name, ARTIFACT_ROOT / target.name)
        print("foundation-release-admission=ok")
        return 0
    except (OSError, ValueError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        # Subprocess output can include private transport details. Do not echo it. Any other
        # failure still ends here as a refusal, never as a traceback that reads like success.
        if isinstance(error, subprocess.CalledProcessError):
            message = "host command failed: " + os.path.basename(str(error.cmd[0] if error.cmd else ""))
        elif isinstance(error, KeyError):
            message = f"required field missing: {error}"
        else:
            message = str(error)
        print(f"foundation-release-admission: refused: {message}", file=sys.stderr)
        return 65


if __name__ == "__main__":
    raise SystemExit(main())
