// Fixture for tools/semgrep/rules/no-silent-test-skip.yml (`opengrep scan --test`). Not compiled.
// Every case of the former scripts/guard/no-silent-test-skip-self-test.sh, one function each.

#[tokio::test]
async fn match_arm_prints_a_skip_and_returns() {
    let url = match std::env::var("EXAMPLE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => {
            // ruleid: a-test-does-not-print-a-skip
            eprintln!("skipping postgres_adapter_passes_outbox_contract: EXAMPLE_TEST_DATABASE_URL not set");
            return;
        }
    };
    let _ = url;
}

#[test]
fn stdout_skip() {
    // ruleid: a-test-does-not-pass-when-a-variable-is-unset
    if std::env::var("EXAMPLE_TEST_KAFKA_BOOTSTRAP_SERVERS").is_err() {
        // ruleid: a-test-does-not-print-a-skip
        println!("skipping live Kafka test");
        return;
    }
}

async fn no_trace_pool() -> Option<Pool> {
    // ruleid: a-test-does-not-swallow-a-missing-variable
    let url = std::env::var("DATABASE_URL").ok()?;
    Pool::connect(&url).await.ok()
}

async fn let_else_pool() -> TestResult<Option<PgPool>> {
    // ruleid: a-test-does-not-let-else-a-missing-variable
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(None);
    };
    PgPool::connect(&url).await.map(Some)
}

#[tokio::test]
#[ignore = "requires a live broker"]
async fn opt_in_switch() -> TestResult {
    // ruleid: a-test-does-not-pass-when-a-variable-is-unset
    if std::env::var("EXAMPLE_TEST_KAFKA_REQUIRED").as_deref() != Ok("1") {
        return Ok(());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a live broker"]
async fn fail_loud_probe() -> TestResult {
    // ok: a-test-does-not-pass-when-a-variable-is-unset
    if std::env::var("EXAMPLE_TEST_KAFKA_REQUIRED").as_deref() != Ok("1") {
        return Err("EXAMPLE_TEST_KAFKA_REQUIRED=1 is required for the live test".into());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a migrated PostgreSQL database"]
async fn distant_return() -> TestResult {
    // ok: a-test-does-not-pass-when-a-variable-is-unset
    if std::env::var("EXAMPLE_MODE").as_deref() == Ok("strict") {
        assert!(strict_invariants_hold());
    }
    let pool = connect().await?;
    let _ = pool;
    return Ok(());
}

#[tokio::test]
async fn wrapped_print() {
    let Some(registry) = pg_registry_or_skip().await else {
        // ruleid: a-test-does-not-print-a-skip
        eprintln!(
            "skipping postgres_registry_upserts_sources: EXAMPLE_TEST_DATABASE_URL not set"
        );
        return;
    };
    let _ = registry;
}

async fn wrapped_ok() -> Option<Registry> {
    // ruleid: a-test-does-not-swallow-a-missing-variable
    let url = std::env::var("EXAMPLE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())?;
    Registry::connect(url).await.ok()
}

async fn wrapped_expect() -> Arc<PostgresWorkflowState> {
    // ok: a-test-does-not-swallow-a-missing-variable
    let url = std::env::var("EXAMPLE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
        .expect("EXAMPLE_TEST_DATABASE_URL must be set and non-empty for the Postgres lane");
    Arc::new(PostgresWorkflowState::connect(url).await.unwrap())
}

#[tokio::test]
#[ignore = "requires a migrated PostgreSQL database in DATABASE_URL"]
async fn ignored() {
    // ok: a-test-does-not-swallow-a-missing-variable
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let _ = url;
}

pub async fn fail_loud_helper() -> String {
    // ok: a-test-does-not-swallow-a-missing-variable
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests")
}

fn ran(url: &str) {
    // ok: a-test-does-not-print-a-skip
    println!("ran against {url}");
}
