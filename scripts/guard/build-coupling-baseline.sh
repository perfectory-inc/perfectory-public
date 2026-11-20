#!/usr/bin/env bash
# Prevents: build-system coupling growing without anyone deciding to grow it; and (2026-10-29) an
# image that compiles a crate without a file the crate compiles in — the publisher image lacked
# contracts/, and no release was built for 11 hours (build-coupling-baseline.py, root ADR-0345).
#
# Two things decide what a move to a graph-based build system (Bazel, Buck2) would
# cost, and both grow silently one commit at a time:
#
#   1. Build scripts. `build.rs` runs arbitrary code at compile time. A sandboxed
#      build system has to be told exactly what each one reads and writes, and a
#      script that reaches the network or the wider filesystem cannot be made
#      hermetic at all. This repository has one.
#
#   2. Compile-time file reads. `include_str!`, `sqlx::migrate!` and friends make
#      the compiler depend on files the build system cannot infer from the source
#      graph. Each site needs an explicit data dependency declared later.
#
# The roadmap (docs/roadmap/production-readiness.md,
# 우선순위 4) records the decision NOT to migrate now and the three triggers that
# would reopen it. That decision rests on these numbers being small. Nothing was
# watching them.
#
# This is a ratchet, not a ban. Growing either is allowed — it just has to be a
# visible edit rather than a side effect nobody priced: the build-script number in
# this file, and for the reads one line per source file in LEDGER (root ADR-0556).
# The guard fails in BOTH directions: an unnoticed increase, and a declaration left
# after the real count dropped. A ceiling that only ever rises is not a ratchet.
#
#   build-coupling-baseline.sh [<repo root> [<build scripts> [<total reads>]]]
#   build-coupling-baseline.sh --write     rewrite LEDGER from the tree, to commit
#
# Until 2026-11-12 the reads were one number in this file. Every pull request that
# added an `include_str!` edited that one line and appended its reason below it,
# so any two of them conflicted and had to merge one after the other: the number
# was raised 87 times, and on 2026-11-12 eight open pull requests each edited it.
# A line per source file, sorted by path, is touched only by the pull request
# that touches that file; the reason is the commit's.
set -euo pipefail

# The thresholds are parameters, not constants, for one reason: a guard whose thresholds cannot be
# varied cannot have its own failure paths tested. Only the self-test passes them.
write=0
if [ "${1:-}" = --write ]; then
  write=1
  shift
fi
script_dir="$(cd "$(dirname "$0")" && pwd -P)"
repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
# Build scripts 1 -> 6: root ADR-0201 has CI keep the workspace crates main built,
# so a package is rebuilt only when cargo sees one of its inputs change. The five
# other packages that embed the migration set get the same one-line
# `rerun-if-changed` the API already had; without it an added migration would
# leave a reused binary with the old set. Each reads nothing and writes nothing.
# 6 -> 10: root ADR-0269 gives each multi-member workspace a hakari workspace-hack crate; cargo
# only builds a package's [build-dependencies] when it has a build script, so each carries an empty
# `fn main() {}` that hakari writes. It reads nothing and writes nothing.
BUILD_SCRIPT_BASELINE="${2:-10}"
# The self-test's total; without it the reads are held to LEDGER, file by file.
COMPILE_TIME_READ_TOTAL="${3:-}"
LEDGER=tools/build-coupling/compile-time-reads.txt

cd "$repo_root"

# `git ls-files` rather than `find`: it already knows what is ignored, so a
# vendored or generated `build.rs` under an ignored path cannot inflate the count,
# and a new one is caught before its first commit.
build_scripts="$(git ls-files '*/build.rs' 'build.rs' 2>/dev/null || true)"
build_script_count="$(printf '%s' "$build_scripts" | grep -c . || true)"

# Counted as occurrences, not files. One file gaining a second `include_str!` is
# one more data dependency to declare, and a file-level count would hide it.
compile_time_reads="$(
  git grep -c -E 'include_str!|include_bytes!|sqlx::migrate!|env!\("CARGO_MANIFEST_DIR"\)' \
    -- '*.rs' 2>/dev/null || true
)"
compile_time_read_count="$(
  printf '%s\n' "$compile_time_reads" | awk -F: '{ total += $2 } END { print total + 0 }'
)"

if [ "$write" -eq 1 ]; then
  mkdir -p "$(dirname "$LEDGER")"
  {
    echo "# Every Rust source file that reads a file at compile time, and how many times (root ADR-0556)."
    echo "# Checked by scripts/guard/build-coupling-baseline.sh; rewrite with its --write and commit."
    [ -z "$compile_time_reads" ] || printf '%s\n' "$compile_time_reads"
  } >"$LEDGER"
  echo "build-coupling-baseline: wrote $LEDGER ($compile_time_read_count compile-time reads)"
  exit 0
fi

fail=0

report() {
  local label="$1" actual="$2" baseline="$3" why="$4"
  if [ "$actual" -gt "$baseline" ]; then
    echo "FAIL build-coupling-baseline: $label rose to $actual (baseline $baseline)." >&2
    echo "    $why" >&2
    echo "    Raise the baseline in scripts/guard/build-coupling-baseline.sh in the" >&2
    echo "    same commit, and say why." >&2
    fail=1
  elif [ "$actual" -lt "$baseline" ]; then
    echo "FAIL build-coupling-baseline: $label fell to $actual but the baseline still says $baseline." >&2
    echo "    Lower it, or the guard silently permits growing back to the old number." >&2
    fail=1
  fi
}

report "build scripts" "$build_script_count" "$BUILD_SCRIPT_BASELINE" \
  "Each one must be made hermetic by hand before any sandboxed build can run it."
if [ -n "$COMPILE_TIME_READ_TOTAL" ]; then
  report "compile-time file reads" "$compile_time_read_count" "$COMPILE_TIME_READ_TOTAL" \
    "Each site becomes an explicit data dependency the build system cannot infer."
else
  declared="$(grep -v '^#' "$LEDGER" 2>/dev/null || true)"
  if [ "$declared" != "$compile_time_reads" ]; then
    echo "FAIL build-coupling-baseline: the compile-time file reads differ from $LEDGER" >&2
    echo "    (< declared, > in the tree; each site is a data dependency the build system cannot infer):" >&2
    diff <(printf '%s\n' "$declared") <(printf '%s\n' "$compile_time_reads") | grep '^[<>]' | sed 's/^/    /' >&2 || true
    echo "    Run: bash scripts/guard/build-coupling-baseline.sh --write, commit $LEDGER," >&2
    echo "    and say in the commit why the read is needed." >&2
    fail=1
  fi
fi

# The same reads, followed into each image that compiles them (build-coupling-baseline.py): every one
# a Dockerfile's build compiles must be a file that image holds. 2026-10-29: the publisher image did
# not copy contracts/, so every release build on main failed for 11 hours while pull requests, which
# compile from the checkout, passed (root ADR-0345).
if ! python3 "$script_dir/build-coupling-baseline.py" "$repo_root"; then
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo "OK build-coupling-baseline (build scripts=$build_script_count, compile-time reads=$compile_time_read_count, every image holds what it compiles in)"
fi
exit "$fail"
