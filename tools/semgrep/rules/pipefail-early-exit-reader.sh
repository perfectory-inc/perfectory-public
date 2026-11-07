#!/usr/bin/env bash
# Fixture for tools/semgrep/rules/pipefail-early-exit-reader.yml (`opengrep scan --test`). Not run.
set -euo pipefail

# The 2026-10-28 shape.
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf '%s\n' a b | grep -qxF -- b || exit 1 # verification-judgment: reviewed-pipeline (fixture text)
# grep -Eq after a continued line.
if ! printf x \
  # ruleid: pipefail-pipe-has-no-early-exit-reader
  | grep -Eq x; then exit 1; fi
# A line that ends in a pipe.
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf x |
  grep -Fq x
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf x | grep --quiet x
# ruleid: pipefail-pipe-has-no-early-exit-reader
x="$(printf x | grep -m1 x)"
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf x | grep -l x
# ruleid: pipefail-pipe-has-no-early-exit-reader
x="$(printf 'a\nb\n' | head -n 1)"
# A flag after a quoted pattern.
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf x | grep "$x" -q
# An exception that gives no reason.
# ruleid: pipefail-pipe-has-no-early-exit-reader
printf x | grep -q x # early-exit-reader:

# ok: pipefail-pipe-has-no-early-exit-reader
grep -q x < <(printf x)
# ok: pipefail-pipe-has-no-early-exit-reader
v=x; grep -q x <<<"$v"
# An or-list, not a pipe.
# ok: pipefail-pipe-has-no-early-exit-reader
test -f y || grep -q x y
# A filter that reads to the end.
# ok: pipefail-pipe-has-no-early-exit-reader
printf x | grep -v y | sed -n 1p
# ok: pipefail-pipe-has-no-early-exit-reader
[ "$(printf x | grep -c .)" -eq 1 ]
# ok: pipefail-pipe-has-no-early-exit-reader
printf x | grep -e '-q' --line-buffered y | header
# A comment line.
# ok: pipefail-pipe-has-no-early-exit-reader
#   printf x | grep -q x
# ok: pipefail-pipe-has-no-early-exit-reader
printf x | grep -q x # early-exit-reader: fixture text, never run
