#!/usr/bin/env python3
"""`foundation-release.sh floor-config`: carry an older release's FLOOR file across a declared key change.

The deploy hands floor-config the running release's file (foundation-deploy.sh step 3, which the
release being replaced runs), and floor-config refuses any key set but this release's example. So a
release that retires a key declares it in CHANGES with the key that replaces it, oldest first; without
this a renamed key refused every later commit (2026-11-22: #1599's Ivy cache -> DuckDB root stopped
every deploy at step 3 from 09:12Z). The replacement takes this release's example value, not the
retired key's: the old value meant something else. Only a declared change is carried; any other key
the example does not hold is still refused by the validation that follows.

Prints `carried` after writing the carried file, nothing when no change applies (the file is used as
it is). Run from the control checkout, never from a release.
"""
import re
import sys
from pathlib import Path

CHANGES = (
    # Spark left FLOOR; its DuckDB stage works on the system disk (root ADR-0599).
    ("FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE", "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_DUCKDB_ROOT"),
)
source, release, carried = map(Path, sys.argv[1:4])


def key(line):
    match = re.match(rb"([A-Z][A-Z0-9_]*)=", line)
    return match[1].decode() if match else None


example = {}
for line in (release / "infra/systemd/building-register-floor.env.example").read_bytes().split(b"\n"):
    if key(line):
        example[key(line)] = line.split(b"=", 1)[1]
# The validation reads no more than this either, and refuses a longer file.
with source.open("rb") as handle:
    lines = handle.read(65537).split(b"\n")
held = [key(line) for line in lines]
changed = False
for retired, replacement in CHANGES:
    if retired not in held or retired in example or replacement in held or not example.get(replacement):
        continue
    index = held.index(retired)
    lines[index] = replacement.encode() + b"=" + example[replacement]
    held[index] = replacement
    print(f"FLOOR config: {retired} is retired; {replacement} takes this release's value", file=sys.stderr)
    changed = True
if changed:
    carried.write_bytes(b"\n".join(lines))
    print("carried")
