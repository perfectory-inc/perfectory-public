#!/usr/bin/env bash
# Bounded guard: a command that opens an input names that input itself.
#
# What failing this prevents: on 2026-08-31 the same archive was recorded under two names — one
# caller passed the bucket key, an earlier one passed only the file name — and the re-run guard,
# which compares those names as text, read them as two archives and appended 1,164,467 rows that
# were already in `silver.parcel_boundaries`. Removing the input is the fix (root ADR-0068); this
# keeps it removed, because the next caller to add it back would look entirely reasonable.
#
# Bare names are not merely less precise. Sampling the bucket found one file name under twelve
# datasets, so a bare name can also mark eleven unloaded datasets as loaded — the same defect
# pointing at silence instead of duplication.
#
# **Scope, and why it is this and not "the name is forbidden".** Three publication commands take
# a variable with the same ending whose value is a `catalog.publication_revision` row id, not an
# object name; `industrial_complex_boundary_runtime_promote.rs` says so in a comment. Forbidding
# the spelling would refuse a correct use, so the rule is a property instead: a command that is
# *given an input to open* must not also be *told what that input is called*. Those two can
# disagree, and that disagreement is the incident. Commands that open nothing are not in scope.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
cd "$repo_root"

for command in git grep; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "FAIL lineage-is-derived-not-passed: $command is required" >&2
    exit 1
  }
done

# Assembled rather than written out, so this file and its self-test are not themselves matches.
# A guard that reads its own prose as a violation is a guard nobody can describe (root ADR-0001).
token="SOURCE""_RECORD_ID"
names_the_input="[A-Z0-9_]*(INPUT_OBJECT_KEY|INPUT_PATH)"
names_the_record="[A-Z0-9_]*${token}"
reads_from_env="(required_env|optional_env)\([[:space:]]*\"${names_the_record}\""

failed=0
report() {
  echo "FAIL lineage-is-derived-not-passed: $1" >&2
  printf '    %s\n' "$2" >&2
  failed=1
}

# Shell callers. `git ls-files` rather than `find`: an ignored or vendored script cannot hide a
# caller here, and a new one is caught before its first commit.
while IFS= read -r candidate; do
  case "$candidate" in
    scripts/guard/*) continue ;;
  esac
  grep -Eq "${names_the_input}=" "$candidate" 2>/dev/null || continue
  hits=$(grep -nE "${names_the_record}[[:space:]]*=" "$candidate" 2>/dev/null || true)
  [ -z "$hits" ] && continue
  report "a script hands the command an input and also names it: $candidate" \
    "$(printf '%s' "$hits" | head -3)"
done < <(git ls-files '*.sh' 2>/dev/null | sort -u)

# Rust commands. Reading it from the environment is the same defect one layer down: the value
# still arrives from outside, and the command still cannot tell a wrong one from a right one.
while IFS= read -r candidate; do
  grep -Eq "\"${names_the_input}\"" "$candidate" 2>/dev/null || continue
  hits=$(grep -nE "$reads_from_env" "$candidate" 2>/dev/null || true)
  [ -z "$hits" ] && continue
  report "a command opens an input and takes its name from the environment: $candidate" \
    "$(printf '%s' "$hits" | head -3)"
done < <(git ls-files '*.rs' 2>/dev/null | sort -u)

if [ "$failed" -ne 0 ]; then
  echo "    The command already knows what it opened. Derive the value there." >&2
  echo "    root ADR-0068" >&2
  exit 1
fi

echo "OK lineage-is-derived-not-passed"
