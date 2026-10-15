#!/usr/bin/env bash
# Bounded guard: the data side gives parts, the web composes (root ADR-0193 §5).
#
# What failing this prevents: on 2026-10-14 an audit found the new per-kind Gold tables about to
# carry a screen's words — a Korean floor label assembled in Spark, Silver's floor display text,
# rooftop usages joined into one string, land rights cut to the old API's 200-row page with a count
# beside them — and the platform ADR holding a "first screen" column, size and latency target. Each
# came in one review at a time; nothing refused the next one.
#
# The shapes, where each is looked for and the bounded exceptions are data:
# platforms/foundation-platform/config/data-knows-no-screen.contract.json. An exception that no
# longer matches anything is refused too, so the list shrinks when the bundle panels retire.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL data-knows-no-screen: python3 is required" >&2
  exit 1
}

python3 - "$repo_root" <<'PY'
import glob
import json
import os
import re
import sys

root = sys.argv[1]
area = os.path.join(root, "platforms", "foundation-platform")
rules_path = os.path.join(area, "config", "data-knows-no-screen.contract.json")
try:
    rules = json.load(open(rules_path, encoding="utf-8"))
except (OSError, ValueError) as error:
    print(f"FAIL data-knows-no-screen: cannot read {rules_path}: {error}", file=sys.stderr)
    raise SystemExit(1)

shapes = {scope: [] for scope in rules["scopes"]}
for shape in rules["shapes"]:
    if not shape.get("reason"):
        print(f"FAIL data-knows-no-screen: shape {shape['id']} has no reason", file=sys.stderr)
        raise SystemExit(1)
    for scope in shape["applies_to"]:
        shapes[scope].append((shape["id"], re.compile(shape["pattern"])))

# (scope, where, line number, text the exception is compared with, shape id)
findings = []


def check(scope, where, number, text, subject=None):
    for shape_id, pattern in shapes[scope]:
        if pattern.search(subject if subject is not None else text):
            findings.append((scope, where, number, text, shape_id))


def relative(path):
    return os.path.relpath(path, area).replace(os.sep, "/")


artifact = json.load(open(os.path.join(area, "infra/lakehouse/contracts/industrial_complex_lakehouse_contracts.json"),
                          encoding="utf-8"))
for table, contract in sorted(artifact["contracts"].items()):
    if table.startswith("gold."):
        for column in contract["columns"]:
            check("gold_columns", table, 0, column["name"])

lanes = json.load(open(os.path.join(area, "config/r2-connections.contract.json"), encoding="utf-8")).get("by_pnu_lanes", {})
for lane, line in sorted(lanes.items()):
    if "host" not in line:
        continue
    for spec in line.get("columns", []):
        check("kind_line_columns", f"by_pnu_lanes.{lane}", 0, spec.split(":", 1)[0])
    for key, value in line.items():
        if key == "decision" or key.endswith("_reason"):
            continue
        check("kind_line_fields", f"by_pnu_lanes.{lane}", 0, key)
        if isinstance(value, str):
            check("kind_line_fields", f"by_pnu_lanes.{lane}", 0, value)

producers = sorted({path for pattern in rules["gold_producers"] for path in glob.glob(os.path.join(area, pattern))})
for path in producers:
    where = relative(path)
    for number, text in enumerate(open(path, encoding="utf-8"), 1):
        for name in re.findall(r"[\"']([a-z0-9_]+)[\"']", text):
            check("gold_producer_names", where, number, name)
        check("gold_producer_source", where, number, text.rstrip("\n"))

contracts = glob.glob(os.path.join(area, "config", "*.json")) + glob.glob(os.path.join(area, "contracts", "data", "*.yaml"))
for path in sorted(contracts):
    if os.path.samefile(path, rules_path):
        continue
    for number, text in enumerate(open(path, encoding="utf-8"), 1):
        check("platform_contracts", relative(path), number, text.rstrip("\n"))

for path in sorted(glob.glob(os.path.join(root, "docs", "adr", "*.md")) + glob.glob(os.path.join(area, "docs", "adr", "*.md"))):
    for number, text in enumerate(open(path, encoding="utf-8"), 1):
        if text.startswith("|"):
            check("adr_target_rows", os.path.relpath(path, root).replace(os.sep, "/"), number, text.rstrip("\n"))

exceptions = rules.get("exceptions", [])
used = set()
problems = []
for scope, where, number, text, shape_id in findings:
    excused = [i for i, e in enumerate(exceptions) if e["where"] == where and
               (e["match"] == text if number == 0 or scope == "gold_producer_names" else e["match"] in text)]
    if excused:
        used.update(excused)
        continue
    place = f"{where}:{number}" if number else where
    reason = next(s["reason"] for s in rules["shapes"] if s["id"] == shape_id)
    problems.append(f"{place}: {shape_id} ({text.strip()[:120]}) — {reason}")
for index, exception in enumerate(exceptions):
    if not exception.get("reason") or not exception.get("until"):
        problems.append(f"exception {exception.get('where')} {exception.get('match')!r} needs a reason and an until")
    elif index not in used:
        problems.append(f"exception {exception['where']} {exception['match']!r} matches nothing any more; remove it")

if problems:
    print("FAIL data-knows-no-screen: the data side gives parts, the web composes "
          "(platforms/foundation-platform/config/data-knows-no-screen.contract.json):", file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    raise SystemExit(1)
print(f"OK data-knows-no-screen ({len(producers)} Gold producers, {len(exceptions)} bounded exceptions)")
PY
