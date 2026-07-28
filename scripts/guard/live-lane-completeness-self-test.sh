#!/usr/bin/env bash
# Synthetic fixtures prove the completeness guard accepts a fully-declared lane
# table, and rejects a live test that belongs to no lane — whether it is gated by
# `#[ignore]` or by a cargo feature. Both gating styles exist in this repository
# (foundation/identity use `#[ignore]`, gongzzang uses `#![cfg(feature = ...)]`),
# so a guard that understood only one would report completeness it had not checked.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/live-lane-completeness.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL live-lane-completeness-self-test: missing $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

# One lane table declaring exactly one target.
xtask="$test_root/xtask.rs"
cat >"$xtask" <<'RUST'
const AREAS: &[Area] = &[Area {
    live_lanes: &[LiveLane {
        name: "postgres",
        required_env: &["DATABASE_URL"],
        targets: &[LaneTarget {
            package: "example-persistence",
            test: "declared_live_reads",
        }],
    }],
}];
RUST

make_crate() {
  local root="$1" pkg="$2"
  mkdir -p "$root/crates/$pkg/tests"
  cat >"$root/crates/$pkg/Cargo.toml" <<TOML
[package]
name = "$pkg"
version = "0.1.0"
TOML
}

expect_accepted() {
  local label="$1" root="$2"
  if ! bash "$checker" "$xtask" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1" root="$2"
  if bash "$checker" "$xtask" "$root" >/dev/null 2>&1; then
    echo "FAIL live-lane-completeness-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

# --- accepted: the declared target, and a test needing no backend at all -------
ok="$test_root/ok"
make_crate "$ok" example-persistence
cat >"$ok/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$ok/crates/example-persistence/tests/pure_unit.rs" <<'RUST'
#[test]
fn parses_without_any_backend() {}
RUST
expect_accepted "declared #[ignore] target plus a backend-free test" "$ok"

# --- rejected: an #[ignore] target missing from the table ---------------------
missing_ignore="$test_root/missing-ignore"
make_crate "$missing_ignore" example-persistence
cat >"$missing_ignore/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$missing_ignore/crates/example-persistence/tests/undeclared_live_writes.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn writes_rows() {}
RUST
expect_rejected "#[ignore] target absent from every lane" "$missing_ignore"

# --- rejected: a feature-gated target missing from the table ------------------
# This is the shape gongzzang uses; an #[ignore]-only guard would pass it.
missing_feature="$test_root/missing-feature"
make_crate "$missing_feature" example-persistence
cat >"$missing_feature/crates/example-persistence/tests/declared_live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live database"]
async fn reads_rows() {}
RUST
cat >"$missing_feature/crates/example-persistence/tests/undeclared_feature_gated.rs" <<'RUST'
#![cfg(feature = "integration")]

#[tokio::test]
async fn writes_rows() {}
RUST
expect_rejected "feature-gated target absent from every lane" "$missing_feature"

echo "OK live-lane-completeness-self-test"
