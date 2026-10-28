#!/usr/bin/env bash
# Under `set -o pipefail` no pipeline may end in a reader that stops before its input does.
#
# What real incident does failing this prevent? On 2026-10-28 CI refused a valid unit: in
# platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh
#
#     printf '%s\n' "${table_units[@]}" | grep -qxF -- "$1" || refuse ...
#
# rejected "parcel", which is in the list. `grep -q` exits at its first match and closes the pipe;
# printf, still writing, takes SIGPIPE (exit 141); pipefail reports the pipeline as failed, so the
# match read as a miss. The same shape made run-postgres-backup.sh take a full backup instead of a
# diff and host-docker-data-root.sh fail its verify. It is a race, more likely the more the writer
# prints, so it passes locally and fails on a loaded runner — and in a guard's own
# `if cmd | grep -q bad; then fail` it fails *open*.
#
# Early-exit readers: `grep` with -q/--quiet/--silent, -m/--max-count or -l/--files-with-matches,
# and `head`. Write `grep -q PATTERN < <(writer)`, `grep -q PATTERN <<<"$var"`, or `sed -n 1p`
# in place of `head -n 1` (sed reads to the end, so the writer's own failure still fails the
# pipeline). A reviewed exception carries `# early-exit-reader: <why it is safe>` on the line.
#
# Scope: every tracked *.sh, not only those that set pipefail themselves — a sourced file runs
# under its caller's options (runtime-up-in-order.sh, sourced by foundation-release.sh). POSIX sh
# scripts that never name pipefail are skipped: they cannot set it and have no `< <(...)`.
set -euo pipefail
cd "${1:-$(dirname "$0")/../..}"
exec python3 - <<'PY'
import re
import subprocess
import sys

NAME = "a-pipefail-pipe-has-no-early-exit-reader"
EXEMPT = re.compile(r"#\s*early-exit-reader:\s*\S")
# A pipe (not `||`) into grep or head; `|&` pipes stderr too.
READER = r"(" + "|".join(("grep", "head")) + r")(?![\w-])(.*)"
PIPE_INTO = re.compile(r"(?:^|[^|])\|&?\s*" + READER)
STARTS_WITH_READER = re.compile(r"\s*" + READER)
TOKEN = re.compile(r"""'[^']*'|"(?:[^"\\]|\\.)*"|\$'[^']*'|[^\s'"]+""")
EARLY_LONG = ("--quiet", "--silent", "--max-count", "--files-with-matches")


def stops_early(reader: str, rest: str) -> bool:
    if reader == "head":
        return True
    for token in TOKEN.findall(rest):
        if token[0] in "'\"$":
            continue
        word = re.split(r"[|;&)<>]", token, maxsplit=1)[0]
        if word.startswith("--"):
            if word.split("=", 1)[0] in EARLY_LONG:
                return True
        elif re.fullmatch(r"-[A-Za-z]*[qml][A-Za-z0-9]*", word):
            return True
        if word != token:  # the command ended inside this token
            return False
    return False


def posix_sh_without_pipefail(text: str) -> bool:
    first = text.split("\n", 1)[0]
    return bool(re.match(r"#!\s*(/usr/bin/env\s+)?(/bin/)?(sh|dash)\s*$", first)) and "pipefail" not in text


files = subprocess.run(["git", "ls-files", "-z", "--", "*.sh"], check=True, capture_output=True).stdout
found = []
for path in filter(None, files.decode().split("\0")):
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        continue
    if posix_sh_without_pipefail(text):
        continue
    previous = ""
    for number, line in enumerate(text.split("\n"), 1):
        code = line.strip()
        if not code or code.startswith("#"):
            continue
        hit = PIPE_INTO.search(line)
        if not hit and previous.endswith("|") and not previous.endswith("||"):
            hit = STARTS_WITH_READER.match(line)
        previous = code
        if hit and stops_early(hit.group(1), hit.group(2)) and not EXEMPT.search(line):
            found.append(f"  {path}:{number}: {code}")
if found:
    print("\n".join(found), file=sys.stderr)
    print(
        f"FAIL {NAME}: a pipe into grep -q/-m/-l or head; under pipefail the writer's SIGPIPE turns a"
        " match into a miss (2026-10-28). Use `grep -q P < <(writer)`, `grep -q P <<<\"$v\"`, or"
        " `sed -n 1p`; a reviewed exception says `# early-exit-reader: <why>` on the line.",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"OK {NAME}")
PY
