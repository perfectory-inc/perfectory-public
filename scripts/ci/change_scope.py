#!/usr/bin/env python3
"""Decide whether a pull-request workflow does its work, from the changed paths (root ADR-0202).

`tools/ci/change-scope.json` is the one place an area's inputs are written. Every
scoped workflow starts on every pull request and reports its required context; its
`scope` job runs `select`, and the other jobs run only when the answer is `true`.
`check` refuses a workflow that is in neither list, a scoped workflow that keeps a
`paths:` filter or skips the shared scope job, and an input glob that names nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "tools/ci/change-scope.json"
SELECTOR = "scripts/ci/change_scope.py"
WORKFLOWS = ".github/workflows"
# A change to how scope is decided can change every decision.
RUN_ALL = frozenset({CONTRACT, SELECTOR})
# A manual run and the nightly schedule run everything: the safety net under every narrowed run. The
# merge queue narrows like a pull request, against its base, the main tip it builds on (root
# ADR-0313): its head is that base plus every queued change ahead of it and this one, so the
# comparison holds every change not yet on main.
ALWAYS_RUN_EVENTS = frozenset({"workflow_dispatch", "schedule"})
# The events that narrow; a scoped workflow that takes one also keeps a schedule (checked).
NARROWED_EVENTS = frozenset({"pull_request", "merge_group"})
# The verification tool every area CI runs lives outside every area (ADR-0004, ADR-0010).
XTASK_RUN = re.compile(r"cargo\s+.*xtask\s+(verify|integration)")
XTASK_INPUTS = ("tools/xtask/**", ".cargo/config.toml", "rust-toolchain.toml")
# Repository files a workflow names in its steps are its inputs without being listed.
NAMED_FILE = re.compile(r"(?<![\w./-])((?:[\w.-]+/)+[\w.-]+\.(?:sh|py|json|toml|yml|yaml))\b")
SCOPE_IF = "${{ needs.scope.outputs.affected == 'true' }}"
SCOPE_ENV = "CHANGE_SCOPE_RUN: ${{ needs.scope.outputs.affected }}"
# Root ADR-0324: an untouched area's required job is skipped (GitHub counts that as passing); a
# failed or cancelled scope job still runs it, and it fails.
REQUIRED_IF = "${{ always() && (needs.scope.result != 'success' || needs.scope.outputs.affected == 'true') }}"
# A scope job that outputs `skipped` (the jobs it found with no work) gates each job on it too.
SKIPPED_OUTPUT = "skipped: ${{ steps.jobs.outputs.skipped }}"
SKIPPED_ENV = "CHANGE_SCOPE_SKIPPED: ${{ needs.scope.outputs.skipped }}"


def job_if(job: str) -> str:
    return f"${{{{ needs.scope.outputs.affected == 'true' && !contains(fromJSON(needs.scope.outputs.skipped), '{job}') }}}}"


def glob_regex(glob: str) -> re.Pattern[str]:
    """GitHub `paths` glob semantics for the forms in use: `**`, `*`, literal."""
    out, i = [], 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif glob[i] == "*":
            out.append("[^/]*")
            i += 1
        else:
            out.append(re.escape(glob[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def load_contract(root: Path = ROOT) -> dict:
    return json.loads((root / CONTRACT).read_text(encoding="utf-8"))


def named_files(root: Path, text: str, working_directory: str) -> set[str]:
    found = set()
    for literal in NAMED_FILE.findall(text):
        for candidate in (literal, f"{working_directory}/{literal}" if working_directory else literal):
            path = PurePosixPath(candidate).as_posix()
            if ".." not in PurePosixPath(path).parts and (root / path).is_file():
                found.add(path)
    return found


def default_working_directory(text: str) -> str:
    match = re.search(r"^defaults:\n  run:\n    working-directory: (\S+)$", text, re.MULTILINE)
    return match.group(1) if match else ""


def workflow_inputs(root: Path, workflow: str, globs: Iterable[str]) -> tuple[list[re.Pattern[str]], set[str]]:
    path = f"{WORKFLOWS}/{workflow}"
    text = (root / path).read_text(encoding="utf-8")
    exact = {path} | named_files(root, text, default_working_directory(text))
    return [glob_regex(g) for g in globs], exact


def matches(path: str, inputs: tuple[list[re.Pattern[str]], set[str]]) -> bool:
    patterns, exact = inputs
    return path in exact or any(p.match(path) for p in patterns)


def claimed(contract: dict, path: str, root: Path = ROOT) -> bool:
    """Whether some workflow is known to answer for this path: a scoped workflow
    takes it as an input, or an unscoped workflow (which always runs) owns it."""
    for workflow, entry in contract["workflows"].items():
        if matches(path, workflow_inputs(root, workflow, entry["paths"])):
            return True
    for workflow, entry in contract["unscoped"].items():
        if path == f"{WORKFLOWS}/{workflow}" or any(glob_regex(g).match(path) for g in entry["owns"]):
            return True
    return False


def decide(
    contract: dict, workflow: str, event: str, changed: set[str] | None, root: Path = ROOT
) -> tuple[bool, str]:
    entry = contract["workflows"].get(workflow)
    if entry is None:
        raise ValueError(f"{workflow} is not a scoped workflow in {CONTRACT}")
    if event in ALWAYS_RUN_EVENTS:
        return True, f"{event} runs everything"
    if changed is None:
        return True, "changed paths unavailable"
    if changed & RUN_ALL:
        return True, "the scope definition changed"
    inputs = workflow_inputs(root, workflow, entry["paths"])
    for path in sorted(changed):
        if matches(path, inputs):
            return True, f"input changed: {path}"
    # A path no workflow answers for may be read by anything: run everything.
    for path in sorted(changed):
        if not claimed(contract, path, root):
            return True, f"no area claims {path}"
    return False, f"none of {len(changed)} changed paths is an input"


def tree_of(root: Path, commit: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "rev-parse", f"{commit}^{{tree}}"], check=True, capture_output=True, text=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return out or None


def queue_runs(repository: str, workflow: str, token: str, api: str = "https://api.github.com") -> list[dict]:
    """The latest successful merge-queue runs of one workflow, newest first."""
    request = urllib.request.Request(
        f"{api}/repos/{repository}/actions/workflows/{workflow}/runs?event=merge_group&status=success&per_page=50",
        headers={"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}", "User-Agent": "change-scope"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["workflow_runs"]


def queue_verified(event: str, tree: str | None, runs) -> str | None:
    """On a push to main, the merge-queue run that passed on this exact tree, if any (root ADR-0313).
    The queue tests the commit main is then moved to; running again on the push only repeated a
    verdict already given (Foundation CI's median was 21 minutes after every merge). Keyed by the
    tree, not the commit: the same tree is the same input to every job. Unreadable runs: None."""
    if event != "push" or not tree:
        return None
    try:
        found = next((run for run in runs() if (run.get("head_commit") or {}).get("tree_id") == tree), None)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"change-scope: merge-queue runs unreadable, running everything: {error}", file=sys.stderr)
        return None
    return str(found.get("html_url") or found.get("id")) if found else None


def push_verified_by_queue(workflow: str) -> str | None:
    """`queue_verified` for this run, from the GitHub environment; None outside Actions."""
    repository, token = os.environ.get("GITHUB_REPOSITORY", ""), os.environ.get("GITHUB_TOKEN", "")
    event, head = os.environ.get("CHANGE_SCOPE_EVENT", ""), os.environ.get("CHANGE_SCOPE_HEAD", "")
    if event != "push" or not repository or not token or not head:
        return None
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    return queue_verified(event, tree_of(ROOT, head), lambda: queue_runs(repository, workflow, token, api))


def git_changed_paths(root: Path, base: str, head: str) -> set[str] | None:
    if not base or not head or set(base) == {"0"}:
        return None
    # Three dots: what the head adds since it left the base, not what the base
    # gained meanwhile. Equal to two dots when the base is an ancestor (push, queue).
    command = ["git", "-C", str(root), "diff", "--name-only", "-z", "--no-renames", f"{base}...{head}"]
    try:
        out = subprocess.run(command, check=True, capture_output=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"change-scope: git comparison unavailable, running everything: {error}", file=sys.stderr)
        return None
    return {os.fsdecode(p) for p in out.split(b"\0") if p}


# --- check: the contract and the workflows agree -------------------------------------------------


def _jobs(text: str) -> dict[str, str]:
    """Job id -> its block, for the canonical two-space `jobs:` dialect the policy guard admits."""
    body = text.split("\njobs:\n", 1)[1] if "\njobs:\n" in text else ""
    jobs, current = {}, None
    for line in body.splitlines():
        if line and not line.startswith(" "):
            break
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            current = match.group(1)
            jobs[current] = ""
        elif current:
            jobs[current] += line + "\n"
    return jobs


def _triggers_pull_request(text: str) -> bool:
    return re.search(r"^  pull_request:\s*$", text, re.MULTILINE) is not None


def check_workflow(workflow: str, text: str) -> list[str]:
    errors = []
    on_block = text.split("\njobs:\n", 1)[0]
    # The merge queue narrows (root ADR-0313); the nightly full run is what still sees an input no
    # contract declares.
    if re.search(r"^  merge_group:", on_block, re.MULTILINE) and not re.search(r"^  schedule:", on_block, re.MULTILINE):
        errors.append(f"{workflow}: a scoped workflow the merge queue narrows keeps a `schedule:` full run")
    if re.search(r"^\s+paths(-ignore)?:", on_block, re.MULTILINE):
        errors.append(f"{workflow}: a scoped workflow keeps no `paths:` filter; its inputs live in {CONTRACT}")
    jobs = _jobs(text)
    scope = jobs.get("scope", "")
    if f"run: python3 {SELECTOR} select --workflow {workflow}\n" not in scope or "affected: ${{ steps.select.outputs.affected }}" not in scope:
        errors.append(f"{workflow}: job `scope` must run `python3 {SELECTOR} select --workflow {workflow}` and output `affected`")
    lists_skipped = SKIPPED_OUTPUT in scope
    for job, block in jobs.items():
        if job == "scope":
            continue
        if re.search(r"^    name: required/", block, re.MULTILINE):
            if SCOPE_ENV not in block:
                errors.append(f"{workflow}: the required job must pass `{SCOPE_ENV}` to its result check")
            if lists_skipped and SKIPPED_ENV not in block:
                errors.append(f"{workflow}: the required job must pass `{SKIPPED_ENV}` to its result check")
            if f"    if: {REQUIRED_IF}\n" not in block:
                errors.append(f"{workflow}: the required job must run `if: {REQUIRED_IF}`")
            continue
        needs = re.search(r"^    needs: \[([^\]]*)\]", block, re.MULTILINE)
        if not needs or "scope" not in [n.strip() for n in needs.group(1).split(",")]:
            errors.append(f"{workflow}: job `{job}` must need `scope`")
        expected = job_if(job) if lists_skipped else SCOPE_IF
        if f"    if: {expected}\n" not in block:
            errors.append(f"{workflow}: job `{job}` must run only `if: {expected}`")
    return errors


def check(root: Path = ROOT, tracked: Iterable[str] | None = None) -> list[str]:
    contract = load_contract(root)
    errors: list[str] = []
    scoped, unscoped = contract.get("workflows", {}), contract.get("unscoped", {})
    if tracked is None:
        tracked = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True
        ).stdout.decode().split("\0")
    tracked = [t for t in tracked if t]
    for workflow in sorted(set(scoped) & set(unscoped)):
        errors.append(f"{workflow}: both scoped and unscoped")
    for workflow, entry in sorted(unscoped.items()):
        if not str(entry.get("reason", "")).strip():
            errors.append(f"{workflow}: an unscoped workflow states why")
        for glob in entry.get("owns", []):
            if not any(glob_regex(glob).match(t) for t in tracked):
                errors.append(f"{workflow}: owned glob `{glob}` names no tracked file (stale)")
    crates = contract.get("crates", {})
    for key, prefix in (("run_all", ""), ("run_all_in_workspace", "**/")):
        globs = crates.get(key)
        if not isinstance(globs, list) or not globs:
            errors.append(f"crates.{key} must be a non-empty list")
            continue
        for glob in globs:
            if not any(glob_regex(prefix + glob).match(t) for t in tracked):
                errors.append(f"crates.{key}: `{glob}` names no tracked file (stale)")
    for name in sorted(set(scoped) | set(unscoped)):
        if not (root / WORKFLOWS / name).is_file():
            errors.append(f"{name}: listed in {CONTRACT} but no such workflow")
    for path in sorted((root / WORKFLOWS).glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        if _triggers_pull_request(text) and path.name not in scoped and path.name not in unscoped:
            errors.append(
                f"{path.name}: a pull-request workflow must be scoped in {CONTRACT} (or listed as unscoped with a reason)"
            )
    for workflow, entry in sorted(scoped.items()):
        globs = entry.get("paths") or []
        if not globs or len(globs) != len(set(globs)):
            errors.append(f"{workflow}: paths must be non-empty and duplicate-free")
        for glob in globs:
            pattern = glob_regex(glob)
            if not any(pattern.match(t) for t in tracked):
                errors.append(f"{workflow}: input `{glob}` names no tracked file (stale)")
        if not (root / WORKFLOWS / workflow).is_file():
            continue
        text = (root / WORKFLOWS / workflow).read_text(encoding="utf-8")
        errors += check_workflow(workflow, text)
        if XTASK_RUN.search(text):
            for required in XTASK_INPUTS:
                if required not in globs:
                    errors.append(
                        f"{workflow}: runs `cargo xtask verify|integration` but does not take `{required}` as an input"
                    )
        errors += unlisted_reads(root, workflow, globs, tracked)
    return errors


# --- what an area's sources read must be an input ------------------------------------------------

SOURCE_SUFFIXES = (".rs", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".py", ".sh")
# A quoted relative path: what include_str!/read_to_string/readFileSync/open receive.
QUOTED_PATH = re.compile(r"""["'`]((?:\.\.?/)*[\w@.-]+(?:/[\w@.-]+)+/?)["'`]""")
# A quoted bare file name, joined onto a directory the same source names (`resolve(docs, "a.json")`).
QUOTED_NAME = re.compile(r"""["'`]([\w@.-]+\.[A-Za-z0-9]+)["'`]""")
COMMENT = re.compile(r"^\s*(//|/\*|\*|#)")
# Areas are the scanned trees. tools/xtask names every area because it dispatches
# them; a change to it already reruns every workflow that runs it.
AREA_PREFIXES = ("platforms/", "products/", "consoles/")


def _resolve(base: PurePosixPath, literal: str) -> str | None:
    """The repository-relative path `literal` names from `base`, if it stays inside the repository."""
    parts: list[str] = list(base.parts)
    for part in PurePosixPath(literal).parts:
        if part == "..":
            if not parts:
                return None
            parts.pop()
        elif part != ".":
            parts.append(part)
    return "/".join(parts) if parts else None


def source_reads(
    source: str, text: str, tracked: set[str], tracked_dirs: set[str], package_dir: str | None = None
) -> set[str]:
    """Repository paths (files, or directories nothing narrower explains) a source names in quotes.

    A quoted path is resolved from the repository root, the area root and the source's
    own directory (and, given one, its package directory: `migrate!` and `CARGO_MANIFEST_DIR` paths
    start there); the first resolution that is tracked counts. A directory is
    narrowed by the bare file names and relative paths the same source joins onto it.
    Comment lines are ignored.
    """
    code = "\n".join(line for line in text.splitlines() if not COMMENT.match(line))
    area = PurePosixPath(*PurePosixPath(source).parts[:2])
    bases = (PurePosixPath(), area, PurePosixPath(source).parent)
    if package_dir is not None:
        bases += (PurePosixPath(package_dir),)
    files: set[str] = set()
    dirs: set[str] = set()
    for literal in set(QUOTED_PATH.findall(code)):
        for base in bases:
            path = _resolve(base, literal.rstrip("/"))
            if path in tracked:
                files.add(path)
                break
            if path in tracked_dirs:
                dirs.add(path)
                break
    names = set(QUOTED_NAME.findall(code)) | set(QUOTED_PATH.findall(code))
    reads = set(files)
    for directory in dirs:
        joined = {_resolve(PurePosixPath(directory), name) for name in names} & tracked
        reads |= joined if joined else {directory}
    return reads


def unlisted_reads(root: Path, workflow: str, globs: list[str], tracked: list[str]) -> list[str]:
    """Every repository path an area's sources read must be one of the workflow's inputs."""
    inputs = workflow_inputs(root, workflow, globs)
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    roots = [
        g[: -len("/**")]
        for g in globs
        if g.endswith("/**") and "*" not in g[: -len("/**")] and g.startswith(AREA_PREFIXES)
    ]
    errors: set[str] = set()
    for source in tracked:
        if not source.endswith(SOURCE_SUFFIXES) or not any(source.startswith(r + "/") for r in roots):
            continue
        try:
            text = (root / source).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for path in source_reads(source, text, tracked_set, tracked_dirs):
            probe = path if path in tracked_set else f"{path}/x"
            if not matches(probe, inputs):
                errors.add(f"{workflow}: {source} reads `{path}`, which is not an input; add it to `paths`")
    return sorted(errors)


# --- crates: which workspace crates a pull request affects (root ADR-0269) ------------------------

# A pull request and the merge queue narrow to crates (root ADR-0313). A push to main the queue did
# not verify, the nightly run and a manual run verify every crate.
CRATE_SELECT_EVENTS = NARROWED_EVENTS


def workspace_members(root: Path, workspace: str) -> dict[str, tuple[str, set[str]]]:
    """Member name -> (its directory from the repository root, the members it depends on).

    `--no-deps` reads the manifests only: no lockfile resolution, no network. Every kind of
    dependency counts (normal, build, dev): a dev-dependency's change can fail the dependent's tests.
    """
    manifest = root / workspace / "Cargo.toml"
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--manifest-path", str(manifest)],
        check=True,
        capture_output=True,
    ).stdout
    packages = json.loads(out.decode("utf-8"))["packages"]
    names = {package["name"] for package in packages}
    members: dict[str, tuple[str, set[str]]] = {}
    for package in packages:
        directory = Path(package["manifest_path"]).resolve().parent.relative_to(root.resolve()).as_posix()
        deps = {dep["name"] for dep in package["dependencies"] if dep.get("path") and dep["name"] in names}
        members[package["name"]] = (directory, deps)
    return members


def closure(members: dict[str, tuple[str, set[str]]], changed: set[str]) -> set[str]:
    """`changed` and every member that depends on one of them, directly or not: nextest's `rdeps()`."""
    dependents: dict[str, set[str]] = {name: set() for name in members}
    for name, (_, deps) in members.items():
        for dep in deps:
            dependents[dep].add(name)
    selected, stack = set(changed), list(changed)
    while stack:
        for dependent in dependents[stack.pop()]:
            if dependent not in selected:
                selected.add(dependent)
                stack.append(dependent)
    return selected


def owning_member(members: dict[str, tuple[str, set[str]]], path: str) -> str | None:
    """The member whose directory holds `path` (the deepest, when members nest)."""
    owners = [(len(directory), name) for name, (directory, _) in members.items() if path.startswith(directory + "/")]
    return max(owners)[1] if owners else None


# A quoted bare word passed on as a value: `for root in ["crates", "services"]`, `join("crates")`
# walk every member below those folders. A JSON key (`"services": …`) or an index
# (`value["services"]`) names no folder.
QUOTED_WORD = re.compile(r"""(?<![\w)\]]\[)["']([\w-]+)["'](?=\s*[,\])])""")


def member_reads(
    root: Path, workspace: str, members: dict[str, tuple[str, set[str]]], tracked: list[str]
) -> dict[str, set[str]]:
    """Member name -> the repository paths its Rust sources name in quotes (ADR-0202 §6's reader).

    A bare quoted word that is a top folder of the workspace holding members (`"crates"`) reads that
    whole folder: a test that walks every crate's sources is a reader of every crate.
    """
    member_roots = {
        f"{workspace}/{PurePosixPath(directory).relative_to(workspace).parts[0]}"
        for directory, _ in members.values()
        if directory.startswith(workspace + "/")
    }
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    reads: dict[str, set[str]] = {name: set() for name in members}
    for source in tracked:
        if not source.endswith(".rs"):
            continue
        owner = owning_member(members, source)
        if owner is None:
            continue
        try:
            text = (root / source).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        reads[owner] |= source_reads(source, text, tracked_set, tracked_dirs, members[owner][0])
        code = "\n".join(line for line in text.splitlines() if not COMMENT.match(line))
        reads[owner] |= {f"{workspace}/{word}" for word in QUOTED_WORD.findall(code)} & member_roots
    return reads


def decide_crates(
    contract: dict,
    workspace: str,
    event: str,
    changed: set[str] | None,
    members: dict[str, tuple[str, set[str]]],
    reads: dict[str, set[str]],
) -> tuple[set[str] | None, set[str], set[str], str]:
    """(the members to verify, or None for all of them; the members whose dependents run too; the
    members that only read a changed crate's sources; why).

    A changed path selects the member whose directory holds it and every member whose sources
    name it (or a directory above it) in quotes, then every dependent of those. A path that no
    member holds or reads is not a Rust input. A run-all path, or Rust outside every member, runs
    every member.
    """
    if event not in CRATE_SELECT_EVENTS:
        return None, set(), set(), f"{event or 'an unnamed event'} verifies every crate"
    if changed is None:
        return None, set(), set(), "changed paths unavailable"
    if changed & RUN_ALL:
        return None, set(), set(), "the scope definition changed"
    rules = contract["crates"]
    run_all = [glob_regex(g) for g in rules["run_all"]]
    run_all += [glob_regex(f"{workspace}/{g}") for g in rules["run_all_in_workspace"]]
    direct: set[str] = set()
    readers: set[str] = set()
    # The top folders that hold members: a quoted bare word naming one is a test that walks every
    # crate's sources (member_reads), which reads them as text and compiles none of them in.
    walked_roots = {
        f"{workspace}/{PurePosixPath(directory).relative_to(workspace).parts[0]}"
        for directory, _ in members.values()
        if directory.startswith(workspace + "/")
    }
    for path in sorted(changed):
        if any(pattern.match(path) for pattern in run_all):
            return None, set(), set(), f"run-all input changed: {path}"
        owner = owning_member(members, path)
        if owner is not None:
            direct.add(owner)
        elif path.startswith(workspace + "/") and path.endswith(".rs"):
            return None, set(), set(), f"Rust source outside every crate: {path}"
        ancestors = {path} | {str(p) for p in PurePosixPath(path).parents}
        named_by = {name for name, paths in reads.items() if (paths & ancestors) - walked_roots}
        walkers = {name for name, paths in reads.items() if paths & ancestors & walked_roots}
        # Another crate's source or manifest is text to the test that reads it: the reader's
        # build does not change, so its dependents do not run. Any other file it names may be
        # compiled in (`include_str!`), and its dependents run as for a change in it. A walk over
        # every crate is text whatever the file (2026-10-19: a JSON fixture inside foundation-api
        # ran two walkers' 14 dependents, the Postgres job's 8 minutes).
        if owner is not None and (path.endswith(".rs") or path.endswith("/Cargo.toml")):
            readers |= named_by | walkers
        else:
            direct |= named_by
            readers |= walkers
    selected = closure(members, direct)
    readers -= selected
    return (
        selected | readers,
        direct,
        readers,
        f"{len(direct)} crate(s) changed, {len(selected)} with their dependents, {len(readers)} reading their sources",
    )


def suite_reads(root: Path, workspace: str, tracked: list[str]) -> set[str]:
    """Repository paths the workspace's non-Rust sources (the Python and Node suites and what they
    run) name in quotes, with ADR-0202 §6's reader."""
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    reads: set[str] = set()
    for source in tracked:
        if not source.startswith(workspace + "/") or source.endswith(".rs") or not source.endswith(SOURCE_SUFFIXES):
            continue
        try:
            text = (root / source).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        reads |= source_reads(source, text, tracked_set, tracked_dirs)
    return reads


def decide_suites(
    contract: dict,
    workspace: str,
    event: str,
    changed: set[str] | None,
    members: dict[str, tuple[str, set[str]]],
    reads: set[str],
) -> tuple[bool, str]:
    """(whether the workspace's Python and Node suites run, why).

    They are skipped only on a pull request whose every changed path sits inside a Rust crate, or
    outside the workspace, and is named by none of the workspace's non-Rust sources. Anything else
    in the workspace, a run-all input, or an unknown comparison runs them.
    """
    selected, _, _, reason = decide_crates(contract, workspace, event, changed, members, {})
    if selected is None or changed is None:
        return True, reason
    for path in sorted(changed):
        ancestors = {path} | {str(p) for p in PurePosixPath(path).parents}
        if reads & ancestors:
            return True, f"a suite reads {path}"
        if path.startswith(workspace + "/") and owning_member(members, path) is None:
            return True, f"changed outside every crate: {path}"
    return False, f"none of {len(changed)} changed paths reaches a suite: only Rust crates or other areas changed"


# A Python module a source imports: `import a.b`, `from a.b import c, d`.
PY_IMPORT = re.compile(r"^\s*(?:from\s+([\w.]+)\s+import\s+([\w, ()]+)|import\s+([\w., ]+))", re.M)
TEST_FILE = re.compile(r"(^|/)test_[^/]*\.py$")
# Sources whose quoted names are dependencies; only Python also has imports.
GRAPH_SUFFIXES = (".py", ".sh", ".ts", ".tsx", ".js", ".mjs", ".cjs")


def dependency_graph(
    root: Path, tracked: list[str], scope: tuple[str, ...] = ("",)
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """(imports, quotes): file -> the tracked Python modules it imports (matched by module or package
    name), and file -> every tracked file it names in quotes (a path, or a bare file name). Only
    sources under `scope` are read; names resolve against the whole tree."""
    tracked_set = set(tracked)
    tracked_dirs = {p for t in tracked for p in map(str, PurePosixPath(t).parents) if p != "."}
    by_name: dict[str, set[str]] = {}
    by_basename: dict[str, set[str]] = {}
    for path in tracked:
        pure = PurePosixPath(path)
        by_basename.setdefault(pure.name, set()).add(path)
        if pure.suffix == ".py":
            by_name.setdefault(pure.parent.name if pure.name == "__init__.py" else pure.stem, set()).add(path)
    imports: dict[str, set[str]] = {}
    quotes: dict[str, set[str]] = {}
    for source in tracked:
        if not source.endswith(GRAPH_SUFFIXES) or not source.startswith(scope):
            continue
        try:
            text = (root / source).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        code = "\n".join(line for line in text.splitlines() if not COMMENT.match(line))
        imported: set[str] = set()
        if source.endswith(".py"):
            for module, names, plain in PY_IMPORT.findall(code):
                for word in filter(None, re.split(r"[\s,().]+", f"{module} {names} {plain}".strip())):
                    imported |= by_name.get(word, set())
        quoted: set[str] = set()
        for path in source_reads(source, text, tracked_set, tracked_dirs):
            quoted |= {t for t in tracked_set if t == path or t.startswith(path + "/")}
        for name in QUOTED_NAME.findall(code):
            quoted |= by_basename.get(name, set())
        imports[source] = imported - {source}
        quotes[source] = quoted - {source}
    return imports, quotes


def select_suite_modules(
    workspace: str, changed: set[str], tracked: list[str], graph: tuple[dict[str, set[str]], dict[str, set[str]]]
) -> tuple[set[str] | None, set[str], str]:
    """(the test files to run, or None for every suite; the Node package directories to run; why).

    A test file runs when it changed, or depends on a changed file at any depth: it imports a module
    that does, or names in quotes a file (a script it starts, a contract it reads) that does. Code
    never depends on a test, so the walk stops at tests: a module that merely mentions a test's file
    name pulls in nothing else. A Node package runs when a file inside it is reached. A changed file
    of the workspace that nothing imports or names, and that is neither documentation, Rust nor a
    Python or shell source, cannot be placed: every suite runs.
    """
    dependents: dict[str, set[str]] = {}
    for edges in graph:
        for source, deps in edges.items():
            for dep in deps:
                dependents.setdefault(dep, set()).add(source)
    reached, stack = set(changed), [path for path in changed if not TEST_FILE.search(path)]
    while stack:
        for source in dependents.get(stack.pop(), ()):
            if source not in reached:
                reached.add(source)
                if not TEST_FILE.search(source):
                    stack.append(source)
    tests = {path for path in reached if path.startswith(workspace + "/") and TEST_FILE.search(path)}
    packages = {
        str(PurePosixPath(path).parent)
        for path in tracked
        if path.startswith(workspace + "/") and path.endswith("/package.json") and "node_modules" not in path
    }
    nodes = {package for package in packages if any(path.startswith(package + "/") for path in reached)}
    for path in sorted(changed):
        if not path.startswith(workspace + "/") or path.endswith((".md", ".rs")) or path.endswith("/Cargo.toml"):
            continue
        if path.endswith(GRAPH_SUFFIXES) or dependents.get(path):
            continue
        if any(path.startswith(package + "/") for package in packages):
            continue
        return None, set(), f"no test, package or source names {path}"
    return tests, nodes, f"{len(tests)} test module(s) and {len(nodes)} Node package(s) reach the change"


def suite_answer(
    contract: dict,
    workspace: str,
    event: str,
    changed: set[str] | None,
    members: dict[str, tuple[str, set[str]]],
    reads: set[str],
    tracked: list[str],
    graph,
) -> tuple[str, set[str], set[str], str]:
    """("all" or "some", test files, Node package directories, why). "some" with nothing selected
    runs nothing. Every event but a pull request, an unknown comparison, and a run-all input (the
    toolchain, a workflow, xtask, this selector) answer "all"."""
    if decide_crates(contract, workspace, event, changed, members, {})[0] is None:
        return "all", set(), set(), decide_crates(contract, workspace, event, changed, members, {})[3]
    run, reason = decide_suites(contract, workspace, event, changed, members, reads)
    if not run:
        return "some", set(), set(), reason
    selected, nodes, why = select_suite_modules(workspace, changed or set(), tracked, graph())
    if selected is None:
        return "all", set(), set(), why
    return "some", selected, nodes, why


def suites_main(args: argparse.Namespace) -> int:
    """Print the suite selection for `cargo xtask verify --part suites --affected`, tab-separated:
    `all <why>` or `some <why>`, then `test <path>` and `node <directory>` lines (none: nothing runs)."""
    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    changed = git_changed_paths(ROOT, os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", ""))
    workspace = args.workspace.strip("/")
    members = workspace_members(ROOT, workspace)
    reads: set[str] = set()
    tracked: list[str] = []
    if event in CRATE_SELECT_EVENTS and changed is not None:
        listed = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z"], check=True, capture_output=True).stdout
        tracked = sorted({t for t in listed.decode().split("\0") if t} | changed)
        reads = suite_reads(ROOT, workspace, tracked)
    kind, selected, nodes, why = suite_answer(
        load_contract(),
        workspace,
        event,
        changed,
        members,
        reads,
        tracked,
        # The workspace, and the repository scripts and tools its tests start, are the only
        # sources on a path to its tests; reading every other area's sources only costs time.
        lambda: dependency_graph(ROOT, tracked, (workspace + "/", "scripts/", "tools/")),
    )
    print(f"{kind}\t{why}")
    if kind == "all":
        return 0
    for path in sorted(selected):
        print(f"test\t{path}")
    for path in sorted(nodes):
        print(f"node\t{path}")
    return 0


def crates_main(args: argparse.Namespace) -> int:
    """Print the selection for xtask, tab-separated: `all <why>` or `some <why>`, then one line per
    member: `changed` (its tests and its dependents' run: nextest `rdeps()`), `reader` (only its own
    tests run: `package()`), `affected` (every member built, linted and tested). With `--includes
    NAME` print nothing and exit 0 when that member is verified, 3 when it is not."""
    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    changed = git_changed_paths(ROOT, os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", ""))
    workspace = args.workspace.strip("/")
    members = workspace_members(ROOT, workspace)
    if args.includes is not None and args.includes not in members:
        print(f"change-scope: {args.includes} is not a member of {workspace}", file=sys.stderr)
        return 2
    reads: dict[str, set[str]] = {}
    if event in CRATE_SELECT_EVENTS and changed is not None:
        listed = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z"], check=True, capture_output=True).stdout
        # A deleted file is still found by the sources that name it: those are what it breaks.
        reads = member_reads(ROOT, workspace, members, sorted({t for t in listed.decode().split("\0") if t} | changed))
    selected, direct, readers, reason = decide_crates(load_contract(), workspace, event, changed, members, reads)
    if args.includes is not None:
        verified = selected is None or args.includes in selected
        print(f"change-scope: {args.includes} {'is' if verified else 'is not'} verified ({reason})", file=sys.stderr)
        return 0 if verified else 3
    print(f"{'all' if selected is None else 'some'}\t{reason}")
    for name in sorted(direct):
        print(f"changed\t{name}")
    for name in sorted(readers):
        print(f"reader\t{name}")
    for name in sorted(selected or ()):
        print(f"affected\t{name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    crates = sub.add_parser("crates", help="the workspace crates a pull request affects, for xtask --affected")
    crates.add_argument("--workspace", required=True, help="the Cargo workspace directory, from the repository root")
    crates.add_argument("--includes", help="exit 0 when this member is verified, 3 when it is not")
    suites = sub.add_parser("suites", help="whether a pull request runs a workspace's Python and Node suites")
    suites.add_argument("--workspace", required=True, help="the Cargo workspace directory, from the repository root")
    select = sub.add_parser("select", help="write affected=true|false for one workflow to $GITHUB_OUTPUT")
    select.add_argument("--workflow", required=True)
    paths = sub.add_parser("paths", help="print one scoped workflow's input globs")
    paths.add_argument("--workflow", required=True)
    paths.add_argument("--root", type=Path, default=ROOT)
    sub.add_parser("check", help="the contract and the workflows agree")
    args = parser.parse_args(argv)

    if args.command == "crates":
        return crates_main(args)
    if args.command == "suites":
        return suites_main(args)
    if args.command == "paths":
        entry = load_contract(args.root)["workflows"].get(args.workflow)
        if entry is not None:
            print("\n".join(entry["paths"]))
        return 0
    if args.command == "check":
        errors = check()
        for error in errors:
            print(f"FAIL change-scope: {error}", file=sys.stderr)
        if not errors:
            print("OK change-scope")
        return 1 if errors else 0

    event = os.environ.get("CHANGE_SCOPE_EVENT", "")
    verified = push_verified_by_queue(args.workflow)
    if verified:
        run, reason = False, f"the merge queue passed this tree: {verified}"
    else:
        changed = git_changed_paths(ROOT, os.environ.get("CHANGE_SCOPE_BASE", ""), os.environ.get("CHANGE_SCOPE_HEAD", ""))
        run, reason = decide(load_contract(), args.workflow, event, changed)
    print(json.dumps({"workflow": args.workflow, "event": event, "run": run, "reason": reason}, ensure_ascii=False))
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8", newline="\n") as handle:
            handle.write(f"affected={str(run).lower()}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
