#!/usr/bin/env bash
# Prevents: a pull request that silently undoes a commit merged to main before it. 2026-11-27 #1780 (a
# data.go.kr download fix of four files) deleted all 13 files #1777 (the Lakekeeper dev rig) had merged an
# hour earlier: its branch began before #1777, and the author squashed it with `git fetch origin main &&
# git reset --soft origin/main && git commit`, which keeps the old branch's tree on the new base. Every
# check passed and review saw only the PR's own subject; W109c found it and restored it in #1784.
# Re-merging such a head shows nothing wrong: the undo is inside the head commit itself.
#
# The rule: against the pull request's base (CHANGE_SCOPE_BASE), no commit reachable from the fork point and
# committed within LOOKBACK_DAYS before it may have UNDONE_SHARE or more of the lines it added deleted by the
# pull request and not added back in any file (counted when it added at least MIN_LINES to the files the pull
# request changes; a line added back was moved, as #1786 moved #1775's rig into one stack). A deliberate
# revert says so: a commit message in the pull request, or the merge queue's squash message (the pull
# request's title and body), names the commit as `Reverts <sha>` (7+ hex characters), `Reverts #<pull
# request>` or git's own `This reverts commit <sha>`. Without a base there is nothing to compare and it passes.
set -euo pipefail
cd "${1:-$(dirname "$0")/../..}"
base="${CHANGE_SCOPE_BASE:-}"
head="${CHANGE_SCOPE_HEAD:-HEAD}"
if [ -z "$base" ] || [ -z "${base//0/}" ]; then
  echo "OK a-pull-request-undoes-no-merged-commit: no pull request base to compare with"
  exit 0
fi
if ! fork="$(git merge-base "$base" "$head")"; then
  echo "FAIL a-pull-request-undoes-no-merged-commit: cannot compare $head with base $base" >&2
  exit 1
fi
python3 -I -B - "$fork" "$head" <<'PY'
import datetime
import re
import subprocess
import sys

fork, head = sys.argv[1:3]
LOOKBACK_DAYS = 14
UNDONE_SHARE = 0.9
MIN_LINES = 20
NAMED = re.compile(r"(?:Reverts|This reverts commit)\s+#?([0-9a-fA-F]{7,40}|\d+)")
PULL = re.compile(r"\(#(\d+)\)\s*$")


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, check=True).stdout.decode("utf-8", "replace")


def lines(diff):
    """{path: (added lines, deleted lines)} of a unified diff, blank lines left out."""
    found, path = {}, None
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            path = line.split(" b/", 1)[1] if " b/" in line else None
            found.setdefault(path, (set(), set()))
        elif path is None or line.startswith(("+++", "---")):
            continue
        elif line.startswith("+") and line[1:].strip():
            found[path][0].add(line[1:])
        elif line.startswith("-") and line[1:].strip():
            found[path][1].add(line[1:])
    return found


def is_named(sha, subject, named):
    """Whether a message of the pull request names this commit as reverted: by its sha (7+ characters) or by
    the pull request number its squash subject ends with, `(#N)`."""
    pull = PULL.search(subject)
    return any((len(name) >= 7 and sha.lower().startswith(name.lower())) or (pull is not None and pull.group(1) == name)
               for name in named)


changed = lines(git("diff", "--no-renames", "-U0", fork, head))
if not changed:
    print("OK a-pull-request-undoes-no-merged-commit: the pull request changes no line")
    sys.exit(0)
added_anywhere = set().union(*(add for add, _ in changed.values()))
named = set(NAMED.findall(git("log", "--format=%B", f"{fork}..{head}")))
forked = datetime.datetime.fromtimestamp(int(git("show", "-s", "--format=%ct", fork).strip()), datetime.timezone.utc)
since = (forked - datetime.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
# One process for every recent commit's patch: a CI checkout is blobless, and one log fetches its blobs in batches.
recent = git("log", f"--since={since}", "--patch", "--no-renames", "-U0", "--format=%x00%H%x09%s", fork, "--",
             *sorted(changed)).split("\x00")[1:]
undone = []
for entry in recent:
    header, _, patch = entry.partition("\n")
    sha, _, subject = header.partition("\t")
    if is_named(sha, subject, named):
        continue
    theirs = lines(patch)
    added = sum(len(add) for add, _ in theirs.values())
    if added < MIN_LINES:
        continue
    # A line the pull request adds back anywhere was moved or kept, not undone (2026-11-27 #1786 moved #1775's rig).
    gone = sum(len((add & changed.get(path, (set(), set()))[1]) - added_anywhere) for path, (add, _) in theirs.items())
    if gone / added >= UNDONE_SHARE:
        undone.append(f"{sha[:12]} ({gone} of the {added} lines it added): {subject}")
if undone:
    sys.exit("FAIL a-pull-request-undoes-no-merged-commit: this pull request deletes what earlier commits added:\n  "
             + "\n  ".join(undone)
             + "\nIf the branch began before them, rebase it on origin/main (never `reset --soft` onto a newer "
             "base). If the revert is meant, name each commit as `Reverts <sha>` or `Reverts #<pull request>` in "
             "the reverting commit's message and in the pull request's title or body.")
print(f"OK a-pull-request-undoes-no-merged-commit ({len(recent)} recent commits on the files it changes)")
PY
