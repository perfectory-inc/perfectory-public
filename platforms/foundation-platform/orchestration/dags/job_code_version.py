"""A job's code version: the digest of the release files its run reaches, inferred, never listed (root ADR-0499).

    python3 job_code_version.py <job id>            the job's code version
    python3 job_code_version.py <job id> --files    the files it is the digest of, one per line

Dagster gives every asset a `code_version` and AutomationCondition.code_version_changed() starts it
again when a deployment changes it; the job list has no such version, and a job whose run failed
waited for its next slot under a release that had fixed it (2026-11-10: data_go_kr_file_data failed
at 11:42Z on a CSV escape, the fix deployed at 13:42Z, the next try was 11-11 11:40Z). The version is
computed from the tree the way Pants infers a target's dependencies, so nothing is kept by hand:

- the job's unit files (its `systemd_service` and `check_service`, a template for `name@x`);
- every file of the release a reached unit, shell script or Python file names in its code: a path under
  the Foundation Platform tree, however it is prefixed (`/opt/foundation-platform/current/`,
  `${RELEASE_ROOT}/`, the Spark container's `/workspace/`), a file beside the one naming it, or a file
  name only one file of the tree has. A shell script's comments and a Python file's comments and
  docstrings are not code. A contract or other data file (JSON, YAML, TOML, SQL) is reached but not
  followed: what its prose names is not what the run executes (the job list names every script);
- the local modules a reached Python file imports, beside it;
- for a template unit's instance (`foundation-silver-refresh@<lane>`, whose script takes the lane as
  its argument): the contracts (under infra/lakehouse/contracts and config) that hold the instance as
  a key or a value, and the producer the Gold rebuild contract gives a Gold of that unit
  (gold_rebuild.load_contract, as gold-panel-rebuild.sh picks it), followed as code;
- for a workspace binary a reached file names (`foundation-outbox-publisher`), its crate's manifest,
  build script and sources, and those of every workspace crate it depends on by path.

The digest is SHA-256 over each file's path and the SHA-256 of its bytes, in path order. A file that
cannot be read is a part of its own ("unreadable"), so a version is always made. The scheduler's
runner, which runs jobs from this very tree, the deploy and the docs are not followed: a file is
reached only from the job's own unit.
"""

from __future__ import annotations

import ast
import functools
import hashlib
import json
import os
import pathlib
import re
import sys
import tomllib

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
JOBS = PLATFORM / "orchestration" / "jobs.v1.json"
SYSTEMD = PLATFORM / "infra" / "systemd"
# What a reached file may name and this follows: code, contracts and the configuration they read.
FOLLOWED = (".sh", ".py", ".json", ".jsonc", ".sql", ".toml", ".yml", ".yaml", ".mjs", ".js", ".ts", ".service")
PATH_LIKE = re.compile(r"[A-Za-z0-9_.@%-]+(?:/[A-Za-z0-9_.@%-]+)*")
BINARY_LIKE = re.compile(r"\bfoundation-[a-z0-9-]+\b")
# Not followed: generated or vendored trees, and what no run reads.
SKIPPED_DIRS = {".git", "node_modules", "target", ".terraform", "__pycache__", "docs", "tests", "test"}
# Where a template instance's own settings live: contracts, never the scheduler's job list or graph.
INSTANCE_CONTRACTS = ("infra/lakehouse/contracts", "config")
SPARK_JOBS = "infra/lakehouse/spark/jobs"
# The files whose names in code are followed; every other reached file is a leaf.
CODE = (".service", ".sh", ".py")
SERVICE = re.compile(r"(foundation-[a-z0-9-]+)(?:@([^.]+))?\.service")


@functools.cache
def _tree(platform: pathlib.Path) -> tuple[frozenset, dict]:
    """(every followed file of the tree, platform-relative; file name -> the files of that name)."""
    files, names = set(), {}
    for directory, subdirectories, entries in os.walk(platform):
        subdirectories[:] = sorted(name for name in subdirectories if name not in SKIPPED_DIRS)
        for entry in entries:
            if entry.endswith(FOLLOWED):
                relative = (pathlib.Path(directory) / entry).relative_to(platform).as_posix()
                files.add(relative)
                names.setdefault(entry, []).append(relative)
    return frozenset(files), names


@functools.cache
def _crates(platform: pathlib.Path) -> tuple[dict, dict]:
    """(binary or package name -> its crate directory, crate directory -> its path dependencies)."""
    root = tomllib.loads((platform / "Cargo.toml").read_text(encoding="utf-8"))
    shared = {name: spec["path"] for name, spec in root.get("workspace", {}).get("dependencies", {}).items()
              if isinstance(spec, dict) and "path" in spec}
    binaries, depends = {}, {}
    for member in root.get("workspace", {}).get("members", []):
        for crate in sorted(platform.glob(member)):
            manifest = crate / "Cargo.toml"
            if not manifest.is_file():
                continue
            parsed = tomllib.loads(manifest.read_text(encoding="utf-8"))
            directory = crate.relative_to(platform).as_posix()
            package = parsed.get("package", {}).get("name")
            if package:
                binaries.setdefault(package, directory)
            for binary in parsed.get("bin", []):
                if binary.get("name"):
                    binaries.setdefault(binary["name"], directory)
            found = set()
            for table in ("dependencies", "build-dependencies"):
                for name, spec in parsed.get(table, {}).items():
                    if not isinstance(spec, dict):
                        continue
                    if "path" in spec:
                        found.add((crate / spec["path"]).resolve().relative_to(platform.resolve()).as_posix())
                    elif spec.get("workspace") and name in shared:
                        found.add(pathlib.PurePosixPath(shared[name]).as_posix())
            depends[directory] = found
    return binaries, depends


def _crate_files(platform: pathlib.Path, directory: str) -> list[str]:
    crate = platform / directory
    found = [path for name in ("Cargo.toml", "build.rs") if (path := crate / name).is_file()]
    # A test module (`tests.rs`, `*_tests.rs`, the crate's convention) is not in the binary.
    found += sorted(path for path in (crate / "src").rglob("*") if path.is_file()
                    and path.name != "tests.rs" and not path.name.endswith("_tests.rs"))
    return [path.relative_to(platform).as_posix() for path in found]


def _named(platform: pathlib.Path, source: str, text: str) -> set[str]:
    """The tree's files `text` (the file `source`) names."""
    files, names = _tree(platform)
    here = pathlib.PurePosixPath(source).parent
    found = set()
    for token in PATH_LIKE.findall(text):
        if not token.endswith(FOLLOWED):
            continue
        parts = token.split("/")
        # The longest tail of the token that is a path of the tree: any prefix (a mount, a variable) drops.
        for start in range(len(parts)):
            tail = "/".join(parts[start:])
            if tail in files:
                found.add(tail)
                break
            beside = (here / tail).as_posix()
            if beside in files:
                found.add(beside)
                break
        else:
            unique = names.get(parts[-1], [])
            if len(unique) == 1:
                found.add(unique[0])
    return found


def code_text(source: str, text: str) -> str:
    """What of `text` is code: a shell script without its comments, a Python file's string literals
    without its docstrings (its imports are _imported's), a unit file as it is."""
    if source.endswith(".sh"):
        return "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in text.splitlines())
    if not source.endswith(".py"):
        return text
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return text
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docstrings.add(id(first.value))
    return "\n".join(node.value for node in ast.walk(tree)
                     if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings)


def _imported(platform: pathlib.Path, source: str, text: str) -> set[str]:
    """The modules beside `source` (a Python file) that it imports."""
    files, _ = _tree(platform)
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return set()
    here = pathlib.PurePosixPath(source).parent
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.add(node.module)
    return {path for module in modules if (path := (here / (module.replace(".", "/") + ".py")).as_posix()) in files}


def _strings(value) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {text for item in value.values() for text in _strings(item)}
    if isinstance(value, list):
        return {text for item in value for text in _strings(item)}
    return {value} if isinstance(value, str) else set()


@functools.cache
def _contract_strings(platform: pathlib.Path) -> dict[str, frozenset]:
    """Each JSON contract of INSTANCE_CONTRACTS -> every key and string value it holds."""
    found = {}
    for directory in INSTANCE_CONTRACTS:
        for path in sorted((platform / directory).rglob("*.json")):
            try:
                found[path.relative_to(platform).as_posix()] = frozenset(
                    _strings(json.loads(path.read_text(encoding="utf-8"))))
            except (OSError, ValueError):
                continue
    return found


@functools.cache
def _gold_producers(platform: pathlib.Path) -> dict[str, frozenset]:
    """Unit -> the producer scripts the Gold rebuild contract gives its Gold tables."""
    jobs_dir = platform / SPARK_JOBS
    if str(jobs_dir) not in sys.path:
        sys.path.insert(0, str(jobs_dir))
    try:
        import gold_rebuild  # noqa: PLC0415 - only for a template instance, and only on a release tree

        tables = gold_rebuild.load_contract()["tables"]
    except (ImportError, OSError, ValueError, KeyError):
        return {}
    found: dict[str, set] = {}
    for entry in tables.values():
        if isinstance(entry, dict) and entry.get("unit") and entry.get("producer"):
            found.setdefault(entry["unit"], set()).add(f"{SPARK_JOBS}/{entry['producer']}.py")
    return {unit: frozenset(scripts) for unit, scripts in found.items()}


def instances(job: dict) -> list[str]:
    """The template instances of `job`'s services (`lane` of `foundation-x@lane.service`)."""
    return [match.group(2) for service in filter(None, (job.get("systemd_service"), job.get("check_service")))
            if (match := SERVICE.fullmatch(service)) and match.group(2) is not None]


def unit_files(job: dict, platform: pathlib.Path = PLATFORM) -> list[str]:
    """The unit files of `job`'s services, platform-relative (a template for `name@instance`)."""
    found = []
    for service in filter(None, (job.get("systemd_service"), job.get("check_service"))):
        match = SERVICE.fullmatch(service)
        if not match:
            raise ValueError(f"{job['id']}: {service!r} is not a foundation-*.service name")
        name = f"{match.group(1)}@.service" if match.group(2) is not None else service
        found.append((platform / "infra" / "systemd" / name).relative_to(platform).as_posix())
    return found


@functools.cache
def _reached(platform: pathlib.Path, source: str) -> tuple[frozenset, frozenset]:
    """(the files `source` names in its code, the workspace binaries it names); empty for a leaf."""
    if not source.endswith(CODE):
        return frozenset(), frozenset()
    try:
        text = (platform / source).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return frozenset(), frozenset()
    code = code_text(source, text)
    reached = _named(platform, source, code)
    if source.endswith(".py"):
        reached |= _imported(platform, source, text)
    return frozenset(reached), frozenset(BINARY_LIKE.findall(code))


@functools.cache
def _digest(platform: pathlib.Path, relative: str) -> str:
    try:
        return hashlib.sha256((platform / relative).read_bytes()).hexdigest()
    except OSError:
        return "unreadable"


def files(job: dict, platform: pathlib.Path = PLATFORM) -> list[str]:
    """Every file of the release `job`'s run reaches, platform-relative, sorted."""
    binaries, depends = _crates(platform)
    seen, crates, todo = set(), set(), list(unit_files(job, platform))
    for instance in instances(job):
        todo += [path for path, strings in _contract_strings(platform).items() if instance in strings]
        todo += sorted(_gold_producers(platform).get(instance, ()))
    while todo:
        source = todo.pop()
        if source in seen:
            continue
        seen.add(source)
        reached, named_binaries = _reached(platform, source)
        todo.extend(sorted(reached - seen))
        for binary in named_binaries:
            if binary in binaries:
                crates.add(binaries[binary])
    walk = list(crates)
    while walk:
        for dependency in depends.get(walk.pop(), ()):
            if dependency not in crates:
                crates.add(dependency)
                walk.append(dependency)
    for crate in crates:
        seen.update(_crate_files(platform, crate))
    return sorted(seen)


def version(job: dict, platform: pathlib.Path = PLATFORM) -> str:
    """`job`'s code version: SHA-256 over each reached file's path and the SHA-256 of its bytes."""
    digest = hashlib.sha256()
    for relative in files(job, platform):
        digest.update(f"{relative}\0{_digest(platform, relative)}\n".encode())
    return digest.hexdigest()


def job_of(job_id: str, jobs_path: pathlib.Path = JOBS) -> dict:
    listing = {job["id"]: job for job in json.loads(jobs_path.read_text(encoding="utf-8"))["jobs"]}
    if job_id not in listing:
        raise KeyError(f"{job_id} is not a job of {jobs_path}")
    return listing[job_id]


def main(argv: list[str]) -> int:
    if len(argv) not in (1, 2) or (len(argv) == 2 and argv[1] != "--files"):
        print("usage: job_code_version.py <job id> [--files]", file=sys.stderr)
        return 64
    try:
        job = job_of(argv[0])
    except (OSError, ValueError, KeyError) as error:
        print(f"job_code_version: {error}", file=sys.stderr)
        return 64
    print("\n".join(files(job)) if len(argv) == 2 else version(job))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
