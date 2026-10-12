"""Keep workspace-crate build artifacts across CI runs (root ADR-0201).

Swatinem/rust-cache caches dependencies only: before it saves it deletes every
workspace crate's artifacts, and even if it kept them a fresh checkout stamps
every source file with the checkout time, which cargo reads as "changed". So
every run recompiled every workspace crate, the 70k-line publisher binary and
its test harness included, about four minutes of the Rust quality job. A
content cache such as sccache cannot help there: it does not cache binaries or
test harnesses, the crates that cost the most.

This script adds the missing half on top of rust-cache:

* ``snapshot`` (after rust-cache restores) lists the dependency artifacts it put
  in place, so that everything else in the target directories is this
  workspace's own output.
* ``stash`` (on main, after the build) records every tracked source file's blob
  id and mtime, then moves the workspace's own artifacts into a directory that
  actions/cache saves. Moving is a rename, so it costs no disk.
* ``restore`` moves a stashed set back into the target directories and gives
  each tracked file whose blob is unchanged the mtime it had when that set was
  built. A changed, added or locally modified file keeps its checkout time, so
  cargo rebuilds exactly the crates a run would have rebuilt on main and keeps
  the rest fresh. Cargo stays the judge of freshness; the script only restores
  the state cargo last saw.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

MANIFEST = "source-mtimes.tsv"
ARTIFACTS = "artifacts"
DIRECTORY = "directory"  # the blob column of a directory line
# The profile subdirectories that hold compiled units and their fingerprints.
# Uplifted copies in the profile root are relinked by cargo from these.
UNIT_DIRS = ("deps", ".fingerprint", "build")


def artifact_files(repo: Path, targets: list[str]) -> list[str]:
    files = []
    for target in targets:
        root = repo / target
        if not root.is_dir():
            continue
        for profile in sorted(p for p in root.iterdir() if p.is_dir()):
            for unit_dir in UNIT_DIRS:
                base = profile / unit_dir
                if not base.is_dir():
                    continue
                for dirpath, _dirs, names in os.walk(base):
                    for name in names:
                        files.append((Path(dirpath) / name).relative_to(repo).as_posix())
    return sorted(files)


def git_lines(repo: Path, *args: str) -> list[str]:
    output = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True
    ).stdout
    return [line for line in output.decode("utf-8").split("\0") if line]


def tracked_blobs(repo: Path) -> dict[str, str]:
    """Tracked path -> blob id of the checked-out content, minus local edits."""
    blobs = {}
    for entry in git_lines(repo, "ls-files", "--stage", "-z"):
        meta, path = entry.split("\t", 1)
        blobs[path] = meta.split()[1]
    # A file the job edited no longer has the indexed content; leave it out so
    # the next run treats it as changed.
    for path in git_lines(repo, "diff", "--name-only", "-z"):
        blobs.pop(path, None)
    return blobs


def tracked_children(paths: list[str]) -> dict[str, set[str]]:
    """Directory -> names directly inside it, for every directory above `paths`."""
    children: dict[str, set[str]] = {}
    for path in paths:
        parts = path.split("/")
        for depth in range(1, len(parts)):
            children.setdefault("/".join(parts[:depth]), set()).add(parts[depth])
    return children


def snapshot(repo: Path, targets: list[str], out: Path) -> int:
    files = artifact_files(repo, targets)
    out.write_text("".join(f"{path}\n" for path in files), encoding="utf-8")
    print(f"workspace-build-cache: {len(files)} dependency artifacts came from rust-cache")
    return 0


def stash(repo: Path, targets: list[str], snapshot_file: Path, stash_dir: Path) -> int:
    from_dependencies = set(snapshot_file.read_text(encoding="utf-8").splitlines())
    if stash_dir.exists():
        shutil.rmtree(stash_dir)
    (stash_dir / ARTIFACTS).mkdir(parents=True)

    lines = []
    recorded = []
    for path, blob in sorted(tracked_blobs(repo).items()):
        source = repo / path
        if source.is_file() and not source.is_symlink():
            lines.append(f"{blob}\t{source.stat().st_mtime_ns}\t{path}\n")
            recorded.append(path)
    for directory in sorted(tracked_children(recorded)):
        lines.append(f"{DIRECTORY}\t{(repo / directory).stat().st_mtime_ns}\t{directory}\n")
    (stash_dir / MANIFEST).write_text("".join(lines), encoding="utf-8")

    moved = 0
    for path in artifact_files(repo, targets):
        if path in from_dependencies:
            continue
        destination = stash_dir / ARTIFACTS / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(repo / path, destination)
        moved += 1
    print(
        f"workspace-build-cache: stashed {moved} workspace artifacts "
        f"and {len(lines)} source mtimes"
    )
    if moved == 0:
        print("::error::workspace-build-cache: nothing to stash; the build produced no workspace artifacts")
        return 1
    return 0


def restore(repo: Path, stash_dir: Path) -> dict[str, int]:
    manifest = stash_dir / MANIFEST
    artifacts = stash_dir / ARTIFACTS
    stats = {"artifacts": 0, "unchanged": 0, "changed": 0}
    if not manifest.is_file() or not artifacts.is_dir():
        return stats
    current = tracked_blobs(repo)

    for dirpath, _dirs, names in os.walk(artifacts):
        for name in names:
            source = Path(dirpath) / name
            destination = repo / source.relative_to(artifacts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, destination)
            stats["artifacts"] += 1

    entries = [line.split("\t", 2) for line in manifest.read_text(encoding="utf-8").splitlines()]
    for blob, mtime_ns, path in entries:
        if blob != DIRECTORY and current.get(path) == blob and (repo / path).is_file():
            os.utime(repo / path, ns=(int(mtime_ns), int(mtime_ns)))
            stats["unchanged"] += 1
    # Cargo also reads a directory's own mtime (`rerun-if-changed=<dir>`, the
    # migration set). A directory gets its recorded mtime back only when its
    # tracked entries are the same names, so an added or deleted file shows.
    then = tracked_children([path for blob, _mtime, path in entries if blob != DIRECTORY])
    now = tracked_children(list(current))
    for blob, mtime_ns, path in entries:
        if blob == DIRECTORY and then.get(path) == now.get(path) and (repo / path).is_dir():
            os.utime(repo / path, ns=(int(mtime_ns), int(mtime_ns)))
    # Changed and added files keep the checkout time, newer than any artifact.
    stats["changed"] = len(current) - stats["unchanged"]
    return stats


CACHE_CAP_BYTES = 10 * 1000**3  # tools/github/actions-cache-policy.json


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    token = os.environ.get("GH_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def cache_pressure(fetch, repository: str, prefix: str) -> list[str]:
    """Lines that say how full the repository cache is and what goes next.

    GitHub evicts the least recently used entries once the repository passes
    its cap, silently; the workflow token may only read, so this reports
    rather than prunes.
    """
    api = f"https://api.github.com/repos/{repository}/actions/caches"
    used = fetch(f"https://api.github.com/repos/{repository}/actions/cache/usage")
    oldest = fetch(f"{api}?per_page=3&sort=last_accessed_at&direction=asc")["actions_caches"]
    ours = fetch(f"{api}?per_page=100&key={urllib.parse.quote(prefix)}")["actions_caches"]
    size = used["active_caches_size_in_bytes"]
    percent = size * 100 // CACHE_CAP_BYTES
    lines = [
        f"Actions cache: {size // 10**6} MB of {CACHE_CAP_BYTES // 10**6} MB ({percent}%) "
        f"in {used['active_caches_count']} entries; {len(ours)} stashed workspace builds "
        f"under `{prefix}` hold {sum(c['size_in_bytes'] for c in ours) // 10**6} MB",
        "next to be evicted: "
        + ", ".join(f"`{c['key']}` (last used {c['last_accessed_at'][:16]})" for c in oldest),
    ]
    if percent >= 90:
        lines.append(
            f"::warning::Actions cache is at {percent}% of its cap; GitHub evicts "
            "the least recently used entries above next"
        )
    return lines


def report_restore(stats: dict[str, int], matched_key: str) -> None:
    if not matched_key:
        message = (
            "workspace-build-cache: no stashed workspace build for this job "
            "(main has not saved one for this toolchain and lockfile, or GitHub "
            "evicted it after 7 idle days or under the 10 GB cap); every "
            "workspace crate compiles from scratch"
        )
        print(f"::warning::{message}")
    else:
        message = (
            f"workspace-build-cache: restored {stats['artifacts']} workspace "
            f"artifacts from `{matched_key}`; {stats['unchanged']} tracked files "
            f"unchanged since that build, {stats['changed']} changed"
        )
        print(message)
    lines = [message]
    repository = os.environ.get("GITHUB_REPOSITORY")
    if repository:
        try:
            pressure = cache_pressure(fetch_json, repository, os.environ.get("CACHE_KEY_PREFIX", ""))
        except (OSError, KeyError, ValueError) as error:
            pressure = [f"::notice::Actions cache usage unavailable: {error}"]
        for line in pressure:
            print(line)
        lines += pressure
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n\n" for line in lines))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)

    snap = commands.add_parser("snapshot")
    snap.add_argument("--target", action="append", required=True)
    snap.add_argument("--out", type=Path, required=True)

    put = commands.add_parser("stash")
    put.add_argument("--target", action="append", required=True)
    put.add_argument("--snapshot", type=Path, required=True)
    put.add_argument("--stash", type=Path, required=True)

    get = commands.add_parser("restore")
    get.add_argument("--stash", type=Path, required=True)
    get.add_argument("--matched-key", default="")

    args = parser.parse_args(argv)
    repo = args.repository.resolve()
    if args.command == "snapshot":
        return snapshot(repo, args.target, args.out)
    if args.command == "stash":
        return stash(repo, args.target, args.snapshot, args.stash)
    report_restore(restore(repo, args.stash), args.matched_key)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
