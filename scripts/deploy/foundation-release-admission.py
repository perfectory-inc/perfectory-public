#!/usr/bin/python3 -I
"""Admit Foundation source from canonical main; recheck installed bytes before execution.

Installed in the independently administered, root-owned monorepo control checkout at
/opt/perfectory-control/current. Never run a candidate archive's verifier with production
authority. Repository identity and Git transport remain the root publication SSOT (ADR-0134).

Host preconditions (root ADR-0134 §5) fail here with their names, not as a generic error:
root's `gh` must answer the public repository identity, root's Git must fetch canonical main
over HTTPS, and Docker must provide Buildx's docker-container driver.
"""

from __future__ import annotations

import argparse
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
BUILD_CPUS = 2
BUILD_MEMORY = "4g"
BUILDKIT_IMAGE = "moby/buildkit:v0.33.0@sha256:6c2fa84a6b61ccd72899dde4239f8d5717f05f9a8ca6f3cad185fb1a95a94de3"
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
    # The host administrator supplies GitHub's ordinary read credentials if needed. No runtime
    # token, caller PATH, Python import path, Git override or dynamic-loader injection survives.
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


def seal_artifacts(release_id: str, target: Path, publisher_image: str) -> None:
    """Only the trusted builder calls this, in an administrator-owned staging directory."""
    files = artifact_files(target)
    jars = [name for name in files if re.fullmatch(r"jars/[A-Za-z0-9_.-]+\.jar", name)]
    if not jars or set(files) != {"foundation-outbox-publisher", *jars}:
        raise ValueError("unexpected build output; a publisher and resolved jars are required")
    (target / "build.json").write_text(json.dumps({
        "source": release_id, "publisher_image": publisher_image, "publisher_tag": publisher_tag(release_id),
        "files": files,
    }, sort_keys=True) + "\n", encoding="utf-8")
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


def require_publisher_image(release_id: str, artifacts: Path) -> None:
    """The image FLOOR runs must still exist, under the tag the build recorded, with its ID."""
    manifest = json.loads((artifacts / "build.json").read_text(encoding="utf-8"))
    tag, image = manifest.get("publisher_tag"), manifest.get("publisher_image")
    if tag != publisher_tag(release_id) or not isinstance(image, str):
        raise ValueError("build output does not record this release's publisher image tag")
    try:
        actual = subprocess.check_output(
            ["/usr/bin/docker", "image", "inspect", "--format", "{{.Id}}", tag],
            env=control_environment(), stderr=subprocess.DEVNULL,
        ).decode().strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError(f"publisher image {tag} is missing; as root move {artifacts} aside and run "
                         "prepare again to rebuild it (runbook: release admission)") from error
    if actual != image:
        raise ValueError(f"publisher image {tag} is not the image the release build recorded")


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


def build_artifacts(target: Path) -> None:
    """Build admitted source without runtime env/credentials; never accept caller binaries."""
    # Older merged releases ran an external publisher and honored source/cache overrides.
    # They cannot be promoted merely by building a new artifact they never execute.
    if not (target / "scripts/ops/admitted-writer-runtime.sh").is_file():
        raise ValueError("release predates the admitted writer runtime; install a supported merged release")
    release_id = target.name
    require_buildx()
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    protected_path(ARTIFACT_ROOT)
    destination = ARTIFACT_ROOT / release_id
    if destination.exists():
        verify_artifacts(release_id, destination)
        return
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
        iid = work / "publisher-image"
        builder = "foundation-release-" + work.name.removeprefix(".")
        configuration = work / "buildkitd.toml"
        configuration.write_text("")
        run("/usr/bin/docker", "buildx", "create", "--name", builder, "--driver", "docker-container",
            "--driver-opt", f"image={BUILDKIT_IMAGE},memory={BUILD_MEMORY},memory-swap={BUILD_MEMORY},cpu-period=100000,cpu-quota={BUILD_CPUS * 100000},restart-policy=no",
            "--buildkitd-config", str(configuration))
        try:
            run("/usr/bin/docker", "buildx", "build", "--builder", builder, "--load", "--pull",
                "--no-cache", "--iidfile", str(iid), "--tag", publisher_tag(release_id),
                "--build-arg", f"CARGO_BUILD_JOBS={BUILD_CPUS}",
                "-f", "services/foundation-outbox-publisher/Dockerfile.lakehouse-control", ".")
        finally:
            try:
                run("/usr/bin/docker", "buildx", "rm", builder)
            except subprocess.CalledProcessError as error:
                raise ValueError(f"temporary builder cleanup failed; administrator must remove {builder}") from error
        publisher_image = iid.read_text().strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", publisher_image):
            raise ValueError("publisher build did not return an immutable image ID")
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
        run("/usr/bin/docker", "run", "--rm", "--cpus", str(BUILD_CPUS), "--memory", BUILD_MEMORY,
            "--memory-swap", BUILD_MEMORY,
            "--mount", f"type=bind,src={cache},dst=/resolve",
            "--mount", f"type=bind,src={resolver},dst=/resolve.py,readonly",
            "--entrypoint", "/opt/spark/bin/spark-submit", spark_image,
            "--conf", "spark.jars.ivy=/resolve", "--packages", packages, "/resolve.py")
        (output / "jars").mkdir()
        for jar in (cache / "jars").glob("*.jar"):
            if jar.is_symlink() or not jar.is_file():
                raise ValueError("dependency resolver produced a non-regular jar")
            shutil.copyfile(jar, output / "jars" / jar.name)
        seal_artifacts(release_id, output, publisher_image)
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
            ["/bin/bash", str(self.transport), "--repository", str(SOURCE_CACHE), *args],
            env=control_environment(), stderr=subprocess.PIPE,
        )

    def refresh(self) -> None:
        expected = json.loads(self.identity.read_text(encoding="utf-8"))
        try:
            actual = json.loads(subprocess.check_output(
                ["/bin/bash", str(self.identity_reader)], env=control_environment(), stderr=subprocess.PIPE,
            ))
        except subprocess.CalledProcessError as error:
            raise ValueError("host precondition: root's gh cannot read the public repository identity "
                             "(gh auth login as root, ADR-0134 §5)") from error
        if actual != expected:
            raise ValueError("live canonical repository identity differs from the root identity policy")
        SOURCE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        protected_path(SOURCE_CACHE.parent)
        if not SOURCE_CACHE.exists():
            subprocess.run(
                ["/bin/bash", str(self.transport), "--no-repository", "init", "--bare", str(SOURCE_CACHE)],
                check=True, env=control_environment(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        self.check_cache()
        # Literal canonical identity, not origin or a caller-supplied source repository/ref.
        try:
            self.git("fetch", "--no-tags", "https://" + expected["hostname"] + "/" + expected["full_name"] + ".git",
                     "+refs/heads/main:refs/heads/main")
        except subprocess.CalledProcessError as error:
            raise ValueError("host precondition: root cannot fetch canonical main over HTTPS (ADR-0134 §5)") from error

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
            target = current_release() if args.command == "verify-current" else args.target
            verify_release(release_files(source.git, target.name), target.name, target)
            if args.command == "build":
                build_artifacts(target)
            else:
                verify_artifacts(target.name, ARTIFACT_ROOT / target.name)
            require_publisher_image(target.name, ARTIFACT_ROOT / target.name)
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
