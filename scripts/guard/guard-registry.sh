#!/usr/bin/env bash
# Prevents: a guard nobody runs, and a second guard (or a second run) for an invariant that is
# already checked.
#
# On 2026-10-13 the 131 files under scripts/guard/ were inventoried by hand. One guard and its
# self-test (intelligence-no-vector-provider) had been merged with its area AGENTS.md saying it
# "enforces" the rule, but nothing ran it. check-workflow-policy.sh ran twice per guard run (from
# public-github-policy.sh and public-repository-safety.sh, same arguments), and four self-tests ran
# twice (from the runner list and again from public-github-policy.sh). Each was invisible because
# the runner's `for g in` list is a hand copy of the directory and nothing compared the two.
#
# This guard reads the directory and refuses:
#   (i)   a scripts/guard/*.sh that is neither in the monorepo-guard.sh `for g in` list nor in
#         RUN_ELSEWHERE below -- and a RUN_ELSEWHERE entry whose named caller no longer invokes it;
#   (ii)  two guard files that are duplicates: the same body once comments and blank lines are
#         removed, the same `# Prevents:` line, or the same name apart from a -check/-checker/
#         -vN/-N/-new/-old suffix;
#   (iii) a guard run from two places (the runner list and/or other non-self-test guards), so
#         one guard run executes it twice -- unless RUN_TWICE names it with the reason.
#
# Threat model: honest-mistake detection. A guard that is invoked through a variable built at
# run time is invisible to the invocation scan; name it in RUN_ELSEWHERE.
set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"

if command -v python3 >/dev/null 2>&1; then py=python3; else py=python; fi
exec "$py" - "$root" <<'PY'
from __future__ import annotations

import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path

# Guards run by something other than the runner's `for g in` list: guard -> the file that runs it.
# Each claim is checked: the caller must still invoke the guard, and must itself be run.
RUN_ELSEWHERE = {
    "repository-identity-ci": "scripts/guard/monorepo-guard.sh",
    "legal-publication-ci": "scripts/guard/monorepo-guard.sh",
    "check-container-runtime-policy": "scripts/guard/container-runtime-policy.sh",
    "check-tracked-blob-sizes": "scripts/guard/public-repository-safety.sh",
    "public-fixture-safety": "scripts/guard/public-repository-safety.sh",
    "public-doc-boundary": "scripts/guard/public-repository-safety.sh",
    "check-workflow-policy": "scripts/guard/public-github-policy.sh",
    "cargo-verify-isolation-self-test": "scripts/guard/public-github-policy.sh",
    "public-tree-snapshot-self-test": "scripts/guard/public-github-policy.sh",
    "frontend-test-isolation-self-test": "scripts/guard/public-github-policy.sh",
    "safe-git-transport-self-test": "scripts/guard/public-github-policy.sh",
    "repository-identity-capture-self-test": "scripts/guard/public-github-policy.sh",
    "foundation-r2-lakehouse-live-self-test": ".github/workflows/foundation-ci.yml",
}

# Runner-listed guards another guard also runs, on purpose. public-github-policy.sh asserts that
# both it and monorepo-guard.sh name these three (its `runtime_policy` / `monorepo_guard` checks).
RUN_TWICE = {
    "repository-identity-policy-self-test",
    "legal-publication-self-test",
    "third-party-artifact-policy-self-test",
}

RUNNER = "monorepo-guard"
SUFFIX = re.compile(r"-(?:check|checker|v\d+|\d+|new|old)$")

root = Path(sys.argv[1])
guard_dir = root / "scripts" / "guard"
errors: list[str] = []


def code_lines(text: str) -> list[str]:
    """The script without comments or blank lines, each line stripped."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def invoked(text: str) -> set[str]:
    """Guard names a script runs: a line that starts `bash .../scripts/guard/X.sh` (or `exec bash`,
    `run: bash`), or `var=".../scripts/guard/X.sh"`. Anchored at the line start so a quoted
    mention inside an embedded checker's string is not read as a run."""
    names = set()
    for line in code_lines(text):
        match = re.match(
            r'(?:(?:-\s*)?run:\s*|exec\s+)?(?:bash\s+|[A-Za-z_][A-Za-z0-9_]*=)'
            r'"?[^\s"]*scripts/guard/([A-Za-z0-9_.-]+)\.sh\b',
            line,
        )
        if match:
            names.add(match.group(1))
    return names


guards = {p.stem: p for p in sorted(guard_dir.glob("*.sh"))}
texts = {name: path.read_text(encoding="utf-8") for name, path in guards.items()}

runner_text = texts.get(RUNNER)
if runner_text is None:
    print(f"FAIL guard-registry: missing scripts/guard/{RUNNER}.sh", file=sys.stderr)
    sys.exit(1)
loop = re.search(r"^for g in (.*?); do$", runner_text, re.MULTILINE | re.DOTALL)
if loop is None:
    print(f"FAIL guard-registry: no `for g in ...; do` list in {RUNNER}.sh", file=sys.stderr)
    sys.exit(1)
listed = set(loop.group(1).replace("\\", " ").split())

# (i) every guard file is run.
for name in guards:
    if name == RUNNER or name in listed or name in RUN_ELSEWHERE:
        continue
    errors.append(
        f"scripts/guard/{name}.sh is run by nothing: add it to the `for g in` list of "
        f"{RUNNER}.sh, or name its caller in RUN_ELSEWHERE of guard-registry.sh"
    )
for name, caller in RUN_ELSEWHERE.items():
    if name not in guards:
        errors.append(f"RUN_ELSEWHERE names {name}, but scripts/guard/{name}.sh does not exist")
        continue
    if name in listed:
        errors.append(f"{name} is in the {RUNNER}.sh list and in RUN_ELSEWHERE; keep one")
    caller_path = root / caller
    if not caller_path.is_file():
        errors.append(f"RUN_ELSEWHERE says {caller} runs {name}, but {caller} does not exist")
        continue
    if name not in invoked(caller_path.read_text(encoding="utf-8")):
        errors.append(f"RUN_ELSEWHERE says {caller} runs {name}, but it no longer invokes it")
    caller_stem = Path(caller).stem
    if caller.startswith("scripts/guard/") and caller_stem != RUNNER and caller_stem not in listed:
        errors.append(f"{name} is run by {caller}, which is not in the {RUNNER}.sh list itself")

# (ii) no duplicate guard files.
by_body: dict[str, list[str]] = defaultdict(list)
by_prevents: dict[str, list[str]] = defaultdict(list)
by_stem: dict[str, list[str]] = defaultdict(list)
for name, text in texts.items():
    body = "\n".join(code_lines(text))
    by_body[hashlib.sha256(body.encode("utf-8")).hexdigest()].append(name)
    prevents = re.search(r"^#\s*Prevents:\s*(.+)$", text, re.MULTILINE)
    if prevents:
        by_prevents[" ".join(prevents.group(1).split())].append(name)
    base, self_test = (name[: -len("-self-test")], "-self-test") if name.endswith("-self-test") else (name, "")
    by_stem[SUFFIX.sub("", base) + self_test].append(name)
for kind, groups in (("the same body", by_body), ("the same `# Prevents:` line", by_prevents),
                     ("the same name apart from a suffix", by_stem)):
    for names in groups.values():
        if len(names) > 1:
            errors.append(f"duplicate guards with {kind}: {', '.join(sorted(names))}; merge them into one")

# (iii) every guard runs once.
callers: dict[str, set[str]] = defaultdict(set)
for name in listed:
    callers[name].add(f"the {RUNNER}.sh list")
for name, text in texts.items():
    if name.endswith("-self-test"):
        continue  # self-tests run guards against their own fixtures
    for target in invoked(text) - {name}:
        callers[target].add(f"scripts/guard/{name}.sh")
for target, by in sorted(callers.items()):
    if len(by) > 1 and target not in RUN_TWICE:
        errors.append(f"{target} runs {len(by)} times: from {', '.join(sorted(by))}; drop all but one")
for name in sorted(RUN_TWICE - listed):
    errors.append(f"RUN_TWICE names {name}, which is not in the {RUNNER}.sh list")

if errors:
    for error in errors:
        print(f"FAIL guard-registry: {error}", file=sys.stderr)
    sys.exit(1)
print(f"OK guard-registry: {len(guards)} guard scripts, each run, none duplicated")
PY
