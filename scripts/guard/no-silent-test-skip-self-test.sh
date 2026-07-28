#!/usr/bin/env bash
# Synthetic fixtures prove that the silent-test-skip guard rejects a test which
# turns "resource unavailable" into a pass, while accepting the two sanctioned
# gating patterns (`#[ignore]`, and fail-loud on a missing resource) and leaving
# non-test source logging alone.
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/no-silent-test-skip.sh"
if [ ! -f "$checker" ]; then
  echo "FAIL no-silent-test-skip-self-test: missing $checker" >&2
  exit 1
fi

test_root="$(mktemp -d)"
cleanup() {
  if [ -n "${test_root:-}" ] && [ -d "$test_root" ]; then
    rm -rf -- "$test_root"
  fi
}
trap cleanup EXIT

expect_accepted() {
  local label="$1"
  local root="$2"
  if ! bash "$checker" "$root" >/dev/null 2>&1; then
    echo "FAIL no-silent-test-skip-self-test: $label should have been accepted" >&2
    exit 1
  fi
}

expect_rejected() {
  local label="$1"
  local root="$2"
  if bash "$checker" "$root" >/dev/null 2>&1; then
    echo "FAIL no-silent-test-skip-self-test: $label should have been rejected" >&2
    exit 1
  fi
}

# --- rejected: the green-washing pattern -------------------------------------
# A missing connection URL makes the test print a notice and return, so the run
# reports PASSED for a test that never exercised anything.
silent="$test_root/silent"
mkdir -p "$silent/crates/example/tests"
cat >"$silent/crates/example/tests/adapter_contract.rs" <<'RUST'
#[tokio::test]
async fn postgres_adapter_passes_outbox_contract() {
    let url = match std::env::var("EXAMPLE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("skipping postgres_adapter_passes_outbox_contract: EXAMPLE_TEST_DATABASE_URL not set");
            return;
        }
    };
    let _ = url;
}
RUST
expect_rejected "silent env-var skip in a test file" "$silent"

# The same green-washing shape via `println!` must not slip through either.
stdout_skip="$test_root/stdout-skip"
mkdir -p "$stdout_skip/crates/example/tests"
cat >"$stdout_skip/crates/example/tests/live_kafka.rs" <<'RUST'
#[test]
fn live_kafka_round_trip() {
    if std::env::var("EXAMPLE_TEST_KAFKA_BOOTSTRAP_SERVERS").is_err() {
        println!("skipping live Kafka test");
        return;
    }
}
RUST
expect_rejected "silent env-var skip printed to stdout" "$stdout_skip"

# The quietest and worst variant prints nothing at all: a missing env var is
# turned into `None` by `.ok()`, and the caller returns early. There is no trace
# in the output that anything was skipped, so the print-based rule alone misses it.
no_trace="$test_root/no-trace"
mkdir -p "$no_trace/crates/example/tests"
cat >"$no_trace/crates/example/tests/live_reads.rs" <<'RUST'
async fn pool() -> Option<Pool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    Pool::connect(&url).await.ok()
}

#[tokio::test]
async fn reads_rows() -> TestResult {
    let Some(pool) = pool().await else {
        return Ok(());
    };
    let _ = pool;
    Ok(())
}
RUST
expect_rejected "env var swallowed by .ok() in a test file" "$no_trace"

# --- accepted: `#[ignore]`, the foundation pattern ---------------------------
ignored="$test_root/ignored"
mkdir -p "$ignored/crates/example/tests"
cat >"$ignored/crates/example/tests/live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a migrated PostgreSQL database in DATABASE_URL"]
async fn catalog_round_trip() {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let _ = url;
}
RUST
expect_accepted "#[ignore]-gated live test" "$ignored"

# --- accepted: fail-loud on a missing resource, the gongzzang pattern --------
failloud="$test_root/fail-loud"
mkdir -p "$failloud/crates/example/tests"
cat >"$failloud/crates/example/tests/common.rs" <<'RUST'
#![cfg(feature = "integration")]

pub async fn setup_test_pool() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests")
}
RUST
expect_accepted "feature-gated fail-loud helper" "$failloud"

# --- accepted: non-test source may legitimately log a skip -------------------
# Production code skipping a stale event is a runtime decision, not a test that
# claims to have verified something.
src_log="$test_root/src-log"
mkdir -p "$src_log/crates/example/src"
cat >"$src_log/crates/example/src/vector_tile_manifest.rs" <<'RUST'
pub fn handle(stale: bool) {
    if stale {
        tracing::info!("skipping stale vector tile manifest pointer event");
        return;
    }
}
RUST
expect_accepted "skip logging in non-test source" "$src_log"

echo "OK no-silent-test-skip-self-test"
