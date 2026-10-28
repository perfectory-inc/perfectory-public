#!/usr/bin/env bash
# Proves a-pipefail-pipe-has-no-early-exit-reader refuses a pipe into grep -q / -m / -l or head —
# on one line, across a line break, and in a sourced bash file that does not set pipefail itself —
# and accepts process substitution, a here-string, `||`, a reader that reads to the end, a reviewed
# exception that says why, and a POSIX sh script that never names pipefail.
set -euo pipefail
. "$(dirname "$0")/lib/fixture-repo.sh"
guard="$(cd "$(dirname "$0")" && pwd)/a-pipefail-pipe-has-no-early-exit-reader.sh"
name="a-pipefail-pipe-has-no-early-exit-reader-self-test"
fixture_root
repo="$FIXTURE_ROOT/repo"
trap 'rm -rf "$FIXTURE_ROOT"' EXIT
mkdir -p "$repo"
fixture_git "$repo" init -q
# Built from parts so this file does not itself hold the shapes the guard refuses.
bar='|'
pipefail_header=$'#!/usr/bin/env bash\nset -euo pipefail\n'

# Writes "$3" (after the header "$4", default the pipefail one) to x.sh, and runs the guard.
expect() {
  local want=$1 label=$2 body=$3 header=${4-$pipefail_header}
  printf '%s%s\n' "$header" "$body" >"$repo/x.sh"
  fixture_git "$repo" add -A
  if bash "$guard" "$repo" >"$FIXTURE_ROOT/out" 2>&1; then got=pass; else got=fail; fi
  if [ "$got" != "$want" ]; then
    cat "$FIXTURE_ROOT/out" >&2
    echo "FAIL $name: $label: expected $want, got $got" >&2
    exit 1
  fi
}

expect fail "the 2026-10-28 shape" "printf '%s\n' a b $bar grep -qxF -- b || exit 1"
expect fail "grep -Eq after a continued line" "if ! printf x \\"$'\n'"  $bar grep -Eq x; then exit 1; fi"
expect fail "a line that ends in a pipe" "printf x $bar"$'\n'"  grep -Fq x"
expect fail "grep --quiet" "printf x $bar grep --quiet x"
expect fail "grep -m1" "x=\"\$(printf x $bar grep -m1 x)\""
expect fail "grep -l" "printf x $bar grep -l x"
expect fail "head -n 1" "x=\"\$(printf 'a\nb\n' $bar head -n 1)\""
expect fail "a sourced bash file without its own pipefail" "printf x $bar grep -q x" $'#!/usr/bin/env bash\n'
expect fail "an exception that gives no reason" "printf x $bar grep -q x # early-exit-reader:"

expect pass "process substitution" "grep -q x < <(printf x)"
expect pass "a here-string" "v=x; grep -q x <<<\"\$v\""
expect pass "an or-list, not a pipe" "test -f y $bar$bar grep -q x y"
expect pass "a filter that reads to the end" "printf x $bar grep -v y $bar sed -n 1p"
expect pass "grep -c before a test operator" "[ \"\$(printf x $bar grep -c .)\" -eq 1 ]"
expect pass "a comment line" "# printf x $bar grep -q x"
expect pass "a reviewed exception" "printf x $bar grep -q x # early-exit-reader: fixture text, never run"
expect pass "a POSIX sh script that never names pipefail" "printf x $bar grep -q x" $'#!/bin/sh\nset -eu\n'
echo "OK $name"
