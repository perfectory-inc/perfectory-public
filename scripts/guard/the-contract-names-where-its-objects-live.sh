#!/usr/bin/env bash
# Bounded guard: the handoff prefix is declared once, in the contract that names the objects.
#
# What failing this prevents: measured 2026-09-02, the string "silver-handoff/vworldkr__parcel"
# was a default in three separate callers — the exporter script, the batch loader, and the
# projection load command — and the contract that lists the objects did not mention it at all.
# Moving the objects would have meant editing three files, and any one missed would have read
# from a prefix nothing was written to: an empty listing, which every one of those callers
# reports as "no work to do" rather than as a failure.
#
# Held as a property: the contract must declare the prefix, and no production file may restate
# its value. Test fixtures may — a test's own input is not a second source of truth for a
# deployment — so `.rs` lines from the test module onward are exempt.
set -uo pipefail

name="the-contract-names-where-its-objects-live"
root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
contract="$root/platforms/foundation-platform/infra/lakehouse/contracts/vworld-parcel-source-objects.json"
failed=0

report() {
  printf 'FAIL %s: %s\n' "$name" "$1" >&2
  failed=1
}

[ -f "$contract" ] || { report "the source contract is missing: $contract"; exit 1; }

# Fail when it cannot look. A guard that finds no value to search for and reports OK is the
# defect it exists to catch, one level up.
prefix="$(
  grep -oE '"handoff_prefix"[[:space:]]*:[[:space:]]*"[^"]+"' "$contract" \
    | head -1 | sed -E 's/.*:[[:space:]]*"([^"]+)"/\1/'
)"
if [ -z "$prefix" ]; then
  report "the contract does not declare handoff_prefix, so the loaders have nowhere to read it from"
  exit 1
fi

# Every occurrence outside the contract, with the test modules dropped. `grep -r` walks the tree
# rather than a list of readers held here: a fourth caller must be caught the day it is written,
# and a guard naming three files would not see it.
while IFS= read -r hit; do
  file="${hit%%:*}"
  rest="${hit#*:}"
  line="${rest%%:*}"

  case "$file" in
    "$contract") continue ;;
    *"/target/"*|*"/node_modules/"*|*"/.git/"*) continue ;;
  esac

  case "$file" in
    *.rs)
      tests_at="$(grep -n '^mod tests\|^[[:space:]]*mod tests' "$file" | head -1 | cut -d: -f1)"
      if [ -n "$tests_at" ] && [ "$line" -ge "$tests_at" ]; then
        continue
      fi
      ;;
  esac

  report "$file:$line restates the handoff prefix the contract declares; read it from $contract instead"
done < <(
  grep -rnF --binary-files=without-match \
    --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git \
    -- "$prefix" "$root/platforms/foundation-platform" 2>/dev/null
)

if [ "$failed" -eq 0 ]; then
  printf 'OK %s: handoff_prefix declared once and restated nowhere\n' "$name"
fi
exit "$failed"
