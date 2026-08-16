#!/usr/bin/env bash
# Prevents: build-system coupling growing without anyone deciding to grow it.
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
# This is a ratchet, not a ban. Growing either number is allowed — it just has to
# be a visible edit to this file rather than a side effect nobody priced. The
# guard fails in BOTH directions: an unnoticed increase, and a baseline left high
# after the real count dropped. A ceiling that only ever rises is not a ratchet.
set -euo pipefail

# Measured 2026-07-29, raised to 80 on 2026-07-30. Change deliberately, with the
# reason in the commit message. They are parameters, not constants, for one reason:
# a guard whose thresholds cannot be varied cannot have its own failure paths
# tested. The defaults are the real repository's numbers; only the self-test passes
# anything else.
#
# 79 -> 80: `catalog-infrastructure/tests/spatial_tile_publication.rs` declares a
# second `sqlx::migrate!` in that crate. The v2 publication transaction has to be
# proved against a migrated database, and the promotion gate counts publication
# units globally, so the suite needs its own disposable database rather than the
# shared harness one — which is what the migrator site is for.
#
# 80 -> 81: `foundation-outbox-publisher/tests/administrative_boundary_postgis_publish.rs`
# declares the first `sqlx::migrate!` in that crate, for the same reason: the only
# production writer of a projection load creates the `admin` publication unit, so
# it cannot run against the shared harness database without failing every
# promotion test. This cost one site, not two — the fixture's scratch directory
# comes from `env::temp_dir()` at runtime rather than from the crate manifest
# directory at compile time, which the first draft used and this guard priced.
#
# 81 -> 82: `catalog-infrastructure/tests/parcel_complex_membership.rs` declares a
# third migrator site in that crate. ADR-0019's backfill is only observable on a
# database that held parcels *before* the membership migration ran, so that test
# applies the migrations in two passes and needs the migration set as a value it
# can filter. Applying everything and inserting afterwards leaves the backfill
# nothing to find, and such a test passes unchanged against a migration whose
# `INSERT` was deleted — so the alternative is not a cheaper site but no assertion.
#
# 82 -> 83: `foundation-outbox-publisher/tests/parcel_boundary_publication.rs` declares
# a second migrator site in that crate, for the same reason as the first: the only
# production writer of the parcel serving projection creates the `parcels`
# publication unit, and the promotion gate counts publication units globally, so a
# unit left in the shared harness database fails every promotion test elsewhere while
# reading as a promotion bug. One site, not two — the fixture keeps nothing on disk,
# so it prices no crate-manifest-directory read.
#
# 83 -> 84: `foundation-outbox-publisher/tests/parcel_publication_source_evidence.rs`
# declares the third migrator site in that crate. The sealed-evidence constraints and
# append-only guards are PostgreSQL contracts, and the suite must prove them in a
# disposable migrated database without leaving publication state in the shared
# harness database.
#
# 84 -> 85: `catalog-infrastructure/tests/vector_tile_runtime_manifest_promote.rs`
# owns one migrator site after terminal projection loads became immutable. The old shared-database
# cleanup had to DELETE succeeded loads, which is now the forbidden behavior under test; one
# migrated disposable database per test preserves both the invariant and isolation.
#
# 85 -> 86: `foundation-outbox-publisher/src/postgis_parcel_boundary_mirror_national_rebuild.rs`
# owns one test-only migrator site. The provenance regression must call the production rebuild's
# private staging and mirror-insert functions, then inspect both the run and every loaded row in one
# disposable migrated database. An integration-test binary cannot reach those functions, while a
# CLI test would replace the database proof with R2 and process-fixture coupling.
#
# 86 -> 87: `foundation-outbox-publisher/src/industrial_complex_address_source_collect/tests.rs`
# reads the public source-endpoint catalog from the crate's own directory to prove every ILIS
# dataset the collector declares is registered there. The catalog is the human-facing SSOT for what
# we collect and from where; restating its three entries in the test would recreate exactly the
# mirrored list the check exists to prevent, so the test reads the document instead of copying it.
#
# That count is a text search, so a comment naming one of these macros is counted
# like a call site. It is not a bug to fix here: these guards deliberately do not
# parse Rust, because a second analyzer of the language is a larger liability than
# an occasional reworded comment. Write about the macros without spelling them.
repo_root="${1:-$(cd "$(dirname "$0")/../.." && pwd -P)}"
BUILD_SCRIPT_BASELINE="${2:-1}"
COMPILE_TIME_READ_BASELINE="${3:-87}"

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
report "compile-time file reads" "$compile_time_read_count" "$COMPILE_TIME_READ_BASELINE" \
  "Each site becomes an explicit data dependency the build system cannot infer."

if [ "$fail" -eq 0 ]; then
  echo "OK build-coupling-baseline (build scripts=$build_script_count, compile-time reads=$compile_time_read_count)"
fi
exit "$fail"
