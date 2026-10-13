#!/usr/bin/env python3
"""Removes the Docker images nothing on the host still needs (root ADR-0198).

Docker lives on the root disk. Measured 2026-10-13 on the host: images 43.5 GB (16.7 GB of
them unused by any container), build cache 57.9 GB but only 0.5 GB of it reclaimable — under the
containerd image store the build cache and the images share their layers, so pruning the cache
frees almost nothing and pruning images is what returns space. Nothing removed either before.

An image is removed only when all of these hold:

* no container uses it, running or stopped;
* it was created more than `--min-age-days` ago, so a build or pull in progress is never raced;
* the release prune does not own it: the publisher and tippecanoe images follow the releases
  they were built for, including the emergency release, and `foundation-release.sh prune` decides;
* the repository checkout the host deploys from (`--reference-root`) names neither its tag nor its
  digest anywhere, so every image a compose file, a contract or a job script pins stays.

It never prunes volumes, containers or networks. Build-cache records unused for the same age are
pruned too; that is the 0.5 GB Docker itself calls reclaimable, not the 58 GB it shares.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

# Image repositories the release prune keeps or removes with their releases.
RELEASE_OWNED = frozenset({"foundation-outbox-publisher", "foundation-tippecanoe"})
DEFAULT_REFERENCE_ROOT = pathlib.Path("/opt/perfectory-control/current")
REFERENCE_SUFFIXES = (".env", ".json", ".yml", ".yaml", ".sh", ".py", ".toml", "Dockerfile")
IMAGE_REFERENCE = re.compile(
    r"(?P<repo>[a-z0-9][a-z0-9./_-]*)(?::(?P<tag>[A-Za-z0-9._-]+))?(?:@sha256:(?P<digest>[0-9a-f]{64}))?"
)
DIGEST = re.compile(r"sha256:([0-9a-f]{64})")
DAY = 86400

Runner = Callable[[Sequence[str]], str]


@dataclass(frozen=True)
class Image:
    image_id: str
    tags: tuple[str, ...]
    digests: tuple[str, ...]
    created: float
    size: int

    @property
    def repositories(self) -> set[str]:
        names = {tag.rsplit(":", 1)[0] for tag in self.tags}
        return names | {digest.split("@", 1)[0] for digest in self.digests}


def run(command: Sequence[str]) -> str:
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def references(root: pathlib.Path) -> tuple[set[str], set[str]]:
    """Every `repo:tag` and every digest the deployed checkout names."""
    tags: set[str] = set()
    digests: set[str] = set()
    for path in root.rglob("*"):
        if not path.is_file() or not path.name.endswith(REFERENCE_SUFFIXES):
            continue
        if any(part in {"node_modules", "target", ".git"} for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        digests.update(DIGEST.findall(text))
        for match in IMAGE_REFERENCE.finditer(text):
            if match.group("tag"):
                tags.add(f"{match.group('repo')}:{match.group('tag')}")
    return tags, digests


def parse_created(value: str) -> float:
    # Docker prints RFC 3339 with nanoseconds; seconds are enough to age an image by days.
    return time.mktime(time.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone


def images(runner: Runner) -> list[Image]:
    ids = sorted(set(runner(["docker", "image", "ls", "--all", "--quiet", "--no-trunc"]).split()))
    if not ids:
        return []
    found = []
    for item in json.loads(runner(["docker", "image", "inspect", *ids])):
        found.append(
            Image(
                image_id=item["Id"],
                tags=tuple(item.get("RepoTags") or ()),
                digests=tuple(item.get("RepoDigests") or ()),
                created=parse_created(item["Created"]),
                size=int(item.get("Size") or 0),
            )
        )
    return found


def used_image_ids(runner: Runner) -> set[str]:
    containers = runner(["docker", "ps", "--all", "--quiet", "--no-trunc"]).split()
    if not containers:
        return set()
    out = runner(["docker", "container", "inspect", "--format", "{{.Image}}", *containers])
    return {line.strip() for line in out.splitlines() if line.strip()}


def removable(
    candidates: Iterable[Image],
    used: set[str],
    referenced_tags: set[str],
    referenced_digests: set[str],
    now: float,
    min_age_days: float,
) -> list[Image]:
    chosen = []
    for image in candidates:
        if image.image_id in used:
            continue
        if now - image.created < min_age_days * DAY:
            continue
        if image.repositories & RELEASE_OWNED:
            continue
        if any(tag in referenced_tags for tag in image.tags):
            continue
        if any(DIGEST.search(digest) and DIGEST.search(digest).group(1) in referenced_digests
               for digest in image.digests):
            continue
        chosen.append(image)
    return chosen


def root_free_bytes() -> int:
    return shutil.disk_usage("/").free


def main(argv: Sequence[str] | None = None, runner: Runner = run) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reference-root", type=pathlib.Path, default=DEFAULT_REFERENCE_ROOT)
    parser.add_argument("--min-age-days", type=float, default=7)
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args(argv)
    if not arguments.reference_root.is_dir():
        # Without the list of pinned images every unused image would look removable.
        print(f"host-retention-refused: reference root {arguments.reference_root} is missing",
              file=sys.stderr)
        return 65

    referenced_tags, referenced_digests = references(arguments.reference_root)
    before = root_free_bytes()
    chosen = removable(images(runner), used_image_ids(runner), referenced_tags,
                       referenced_digests, time.time(), arguments.min_age_days)
    planned = sum(image.size for image in chosen)
    for image in chosen:
        name = ",".join(image.tags) or image.image_id[:19]
        print(f"{'would-remove' if arguments.dry_run else 'remove'} {name} size_bytes={image.size}")
        if arguments.dry_run:
            continue
        # By tag, so an image tagged twice goes with both names; an untagged one by ID.
        for reference in image.tags or (image.image_id,):
            runner(["docker", "image", "rm", reference])
    if not arguments.dry_run:
        runner(["docker", "builder", "prune", "--force", "--filter",
                f"until={int(arguments.min_age_days * 24)}h"])
    after = root_free_bytes()
    print(
        f"host-retention-{'dry-run' if arguments.dry_run else 'ok'} images={len(chosen)} "
        f"image_bytes={planned} root_free_before_bytes={before} root_free_after_bytes={after} "
        f"freed_gib={(after - before) / 2**30:.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
