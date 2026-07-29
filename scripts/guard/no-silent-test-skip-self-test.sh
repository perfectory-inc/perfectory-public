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

# The let-else variant evades both earlier rules: nothing is printed and `.ok()`
# never appears. `Ok(_)` is refutable, so this is always a let-else, and its
# diverging block is where "no backend" quietly becomes a pass. This is the exact
# shape foundation-outbox's publish_roundtrip carried for six contract tests.
let_else="$test_root/let-else"
mkdir -p "$let_else/crates/example/tests"
cat >"$let_else/crates/example/tests/publish_roundtrip.rs" <<'RUST'
async fn pool() -> TestResult<Option<PgPool>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(None);
    };
    PgPool::connect(&url).await.map(Some)
}
RUST
expect_rejected "let-else on env::var in a test file" "$let_else"

# The conditional variant reads like an opt-in switch and behaves like a pass:
# the probe guards an early SUCCESS return, so a harness that forgot the switch
# gets a green test that reached no broker.
opt_in="$test_root/opt-in-switch"
mkdir -p "$opt_in/crates/example/tests"
cat >"$opt_in/crates/example/tests/live_outage.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live broker"]
async fn outage_retries_then_quarantines() -> TestResult {
    if std::env::var("EXAMPLE_TEST_KAFKA_REQUIRED").as_deref() != Ok("1") {
        return Ok(());
    }
    Ok(())
}
RUST
expect_rejected "env probe guarding an early success return" "$opt_in"

# --- accepted: the same probe returning Err ----------------------------------
# Only a SUCCESS return is the defect. Refusing to run is the correct behaviour
# and must not be flagged, or the rule would push authors back toward silence.
fail_loud_probe="$test_root/fail-loud-probe"
mkdir -p "$fail_loud_probe/crates/example/tests"
cat >"$fail_loud_probe/crates/example/tests/live_karapace.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a live broker"]
async fn karapace_round_trip() -> TestResult {
    if std::env::var("EXAMPLE_TEST_KAFKA_REQUIRED").as_deref() != Ok("1") {
        return Err("EXAMPLE_TEST_KAFKA_REQUIRED=1 is required for the live test".into());
    }
    Ok(())
}
RUST
expect_accepted "env probe that refuses to run instead of passing" "$fail_loud_probe"

# --- accepted: an unrelated early return far below an env read ---------------
# The window must not attribute a later `return Ok(())` to an earlier condition,
# or every live test would be flagged for its final success path.
distant_return="$test_root/distant-return"
mkdir -p "$distant_return/crates/example/tests"
cat >"$distant_return/crates/example/tests/live_reads.rs" <<'RUST'
#[tokio::test]
#[ignore = "requires a migrated PostgreSQL database"]
async fn reads_rows() -> TestResult {
    if std::env::var("EXAMPLE_MODE").as_deref() == Ok("strict") {
        assert!(strict_invariants_hold());
    }
    let pool = connect().await?;
    let _ = pool;
    return Ok(());
}
RUST
expect_accepted "later success return not attributed to an earlier probe" "$distant_return"

# rustfmt is enough to defeat a line-oriented rule. Both halves of the shape get
# pushed onto their own lines, and neither line matches on its own. This is not
# hypothetical: intelligence's knowledge_source_registry_contract carried exactly
# this, wrapped exactly this way, and both rules walked past it.
wrapped_print="$test_root/wrapped-print"
mkdir -p "$wrapped_print/crates/example/tests"
cat >"$wrapped_print/crates/example/tests/registry_contract.rs" <<'RUST'
#[tokio::test]
async fn postgres_registry_upserts_sources() {
    let Some(registry) = pg_registry_or_skip().await else {
        eprintln!(
            "skipping postgres_registry_upserts_sources: EXAMPLE_TEST_DATABASE_URL not set"
        );
        return;
    };
    let _ = registry;
}
RUST
expect_rejected "rustfmt-wrapped eprintln! skip" "$wrapped_print"

wrapped_ok="$test_root/wrapped-ok"
mkdir -p "$wrapped_ok/crates/example/tests"
cat >"$wrapped_ok/crates/example/tests/registry_helper.rs" <<'RUST'
async fn pg_registry_or_skip() -> Option<Registry> {
    let url = std::env::var("EXAMPLE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())?;
    Registry::connect(url).await.ok()
}
RUST
expect_rejected "rustfmt-wrapped .ok() chain ending in ?" "$wrapped_ok"

# --- accepted: the same wrapped chain terminated by a panic ------------------
# `.ok().filter(..).expect(..)` reads the variable, rejects an empty value, and
# panics when it is absent. Flagging it would push authors back toward silence,
# which is the opposite of what this guard is for.
wrapped_expect="$test_root/wrapped-expect"
mkdir -p "$wrapped_expect/crates/example/tests"
cat >"$wrapped_expect/crates/example/tests/state_contract.rs" <<'RUST'
async fn pg_state() -> Arc<PostgresWorkflowState> {
    let url = std::env::var("EXAMPLE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
        .expect("EXAMPLE_TEST_DATABASE_URL must be set and non-empty for the Postgres lane");
    Arc::new(PostgresWorkflowState::connect(url).await.unwrap())
}
RUST
expect_accepted "wrapped chain that panics on the missing variable" "$wrapped_expect"

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
