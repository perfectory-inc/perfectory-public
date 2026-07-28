#!/usr/bin/env bash
# Prevents: a test that never ran being reported as a test that passed.
#
# ADR-0004 unified the *verification command*, but explicitly deferred live-backend
# tests to a Phase 2 that never landed. In that gap each area invented its own way
# to handle "the database/broker isn't here", and one of them green-washes: the
# test reads a connection env var, prints a notice, and `return`s. cargo reports
# PASSED. An unrun contract test is then indistinguishable from a verified one, so
# a whole transport can rot untested behind a green check.
#
# Only two gating patterns keep "did not run" distinguishable from "passed":
#   * `#[ignore]`   — excluded by default, listed as ignored, run via `--ignored`
#                     (foundation; `cargo xtask integration` refuses without a URL)
#   * fail-loud     — `#![cfg(feature = "...")]` + panic on the missing resource
#                     (gongzzang), so a misconfigured run is a failure, not a pass
#
# Non-test source may still log a skip: a runtime deciding to drop a stale event
# is not a test claiming to have verified something. Only test files are scanned.
set -euo pipefail
cd "$(dirname "$0")/../.."

scan_root="${1:-.}"

if [ ! -d "$scan_root" ]; then
  echo "FAIL no-silent-test-skip: scan root '$scan_root' is not a directory" >&2
  exit 1
fi

# Test code lives either under a `tests/` directory (integration tests) or in a
# `tests.rs` / `*_tests.rs` module beside the code it covers (the convention this
# repository uses for in-crate suites).
mapfile -t test_files < <(
  find "$scan_root" \
    -type d \( -name target -o -name node_modules -o -name .git \) -prune -o \
    -type f -name '*.rs' -print 2>/dev/null \
    | grep -E '(/tests/|/tests\.rs$|_tests\.rs$)' \
    | sort
)

rc=0
report() {
  local file="$1" hit="$2"
  echo "FAIL no-silent-test-skip: $file:$hit" >&2
  echo "    A missing backend must not read as a pass. Use #[ignore] (run it via" >&2
  echo "    'cargo xtask integration <area>'), or panic on the missing resource." >&2
  rc=1
}

for f in "${test_files[@]}"; do
  [ -n "$f" ] || continue

  # (1) A print whose message announces a skip is the loud form of the shape: the
  # accompanying early `return` is what makes cargo report a pass anyway.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(grep -nEi '(eprintln|println)!\(.*skip' "$f" 2>/dev/null || true)

  # (2) The quiet form leaves no trace at all: `.ok()` demotes a missing required
  # resource to `None`, the caller returns early, and nothing is printed. A test
  # that needs a backend must state that need, not silently absorb its absence.
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    report "$f" "$hit"
  done < <(grep -nE 'env::var\([^)]*\)[[:space:]]*\.ok\(\)' "$f" 2>/dev/null || true)
done

if [ "$rc" -eq 0 ]; then
  echo "OK no-silent-test-skip"
fi
exit "$rc"
