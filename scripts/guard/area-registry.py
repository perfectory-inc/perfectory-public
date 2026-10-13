#!/usr/bin/env python3
"""The area list has one home: tools/areas.contract.json.

The list was copied into xtask, the workflow policy, the document inventory and the public-root
audit, and the copies drifted (the inventory never learned `consoles/dawneer`). Readers now load the
registry. This guard refuses the next copy: a tracked non-prose file that names all areas but one or
more (the drift case is exactly a list missing one) is a hand-written area list. An area counts as
named by its path, or by its last segment in quotes, the form the inventory's copy used. Markdown is
prose and exempt (root ADR-0009 rewrites it freely).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REGISTRY = "tools/areas.contract.json"
CHANGE_SCOPE = "tools/ci/change-scope.json"
# A file may name every area when its subject is not the area list. Each entry says why.
NOT_AN_AREA_LIST = {
    # Per-workflow change inputs; checked below to cover every registry area instead.
    CHANGE_SCOPE,
    # Attaches verification config to each area; its test `areas_match_the_registry` (std-only,
    # no JSON parser to load the registry at run time) refuses a table that disagrees.
    "tools/xtask/src/main.rs",
    # Service identities that call Foundation, not the area set.
    "platforms/foundation-platform/docs/architecture/traffic-auth-policy-registry.v1.json",
}


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).resolve().parents[2])
    failures: list[str] = []

    areas = json.loads((root / REGISTRY).read_text(encoding="utf-8"))["areas"]
    dirs = [area["dir"] for area in areas]
    for key in ("slug", "dir", "title"):
        values = [area[key] for area in areas]
        if len(set(values)) != len(values) or not all(values):
            failures.append(f"{REGISTRY}: every area needs a distinct non-empty {key}")
    for directory in dirs:
        if not (root / directory).is_dir():
            failures.append(f"{REGISTRY}: area dir {directory} does not exist")

    named: dict[str, set[str]] = {}
    for directory in dirs:
        name = directory.rsplit("/", 1)[-1]
        patterns = [directory, f'"{name}"', f"'{name}'"]
        listed = subprocess.run(
            ["git", "-C", str(root), "grep", "-z", "-l", "-F"]
            + [arg for pattern in patterns for arg in ("-e", pattern)]
            + ["--", ":!*.md"],
            check=False,
            stdout=subprocess.PIPE,
        )
        if listed.returncode not in (0, 1):
            failures.append(f"git grep failed for {directory}")
            continue
        for raw in listed.stdout.split(b"\0"):
            if raw:
                named.setdefault(raw.decode("utf-8"), set()).add(directory)
    threshold = max(2, len(dirs) - 1)
    for path, found in sorted(named.items()):
        if len(found) >= threshold and path != REGISTRY and path not in NOT_AN_AREA_LIST:
            failures.append(
                f"{path} names {len(found)} of {len(dirs)} areas by hand; read {REGISTRY} instead"
            )

    scope = json.loads((root / CHANGE_SCOPE).read_text(encoding="utf-8"))
    claimed = {
        path
        for section, key in (("workflows", "paths"), ("unscoped", "owns"))
        for workflow in scope.get(section, {}).values()
        for path in workflow.get(key, [])
    }
    for directory in dirs:
        if f"{directory}/**" not in claimed:
            failures.append(f"{CHANGE_SCOPE}: no workflow watches or owns area {directory}/**")

    for failure in failures:
        print(f"FAIL area-registry: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"OK area-registry (areas={len(dirs)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
