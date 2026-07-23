//! Contract tests for [`InMemoryWorkflowState`] and [`PostgresWorkflowState`].
//!
//! The generic outbox contract suite lives in `tests/common/mod.rs` so that
//! both adapters can be exercised against the same pinned scenarios without
//! duplication.
//!
//! # Postgres tests
//!
//! Postgres tests are gated on the `INTELLIGENCE_TEST_DATABASE_URL` environment
//! variable.  When the variable is absent or empty, the tests self-skip with an
//! `eprintln!` notice — they are never silently ignored.

// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use intelligence_normalization_application::{NormalizationAuditEvent, NormalizationAuditPort};
use intelligence_normalization_infrastructure::{
    InMemoryWorkflowState, PostgresWorkflowState, PostgresWorkflowStateConfig,
};
use sqlx::postgres::PgPoolOptions;

fn postgres_test_mutex() -> &'static tokio::sync::Mutex<()> {
    static MUTEX: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    MUTEX.get_or_init(|| tokio::sync::Mutex::new(()))
}

// ---------------------------------------------------------------------------
// Memory adapter — outbox contract
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_adapter_passes_outbox_contract() {
    common::outbox_contract_suite(InMemoryWorkflowState::default()).await;
}

// ---------------------------------------------------------------------------
// Audit port test (memory-adapter specific; uses audit_events() helper)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_adapter_audit_port_appends_events_in_order() {
    let state = InMemoryWorkflowState::default();
    let tc = common::make_trace_context();

    let event1 = NormalizationAuditEvent::new("event-type-1", tc.clone(), BTreeMap::new());
    let event2 = NormalizationAuditEvent::new("event-type-2", tc.clone(), BTreeMap::new());

    state.append(event1).await.unwrap();
    state.append(event2).await.unwrap();

    let events = state.audit_events();
    assert_eq!(events.len(), 2, "audit log must contain both events");
    assert_eq!(
        events[0].event_type, "event-type-1",
        "first event must be event-type-1"
    );
    assert_eq!(
        events[1].event_type, "event-type-2",
        "second event must be event-type-2"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a [`PostgresWorkflowState`] connected to the test database, or
/// `None` when `INTELLIGENCE_TEST_DATABASE_URL` is not set/empty (triggers
/// self-skip in the caller).
async fn pg_state_or_skip() -> Option<Arc<PostgresWorkflowState>> {
    let url = std::env::var("INTELLIGENCE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())?;

    let config = PostgresWorkflowStateConfig::new(url, 10)
        .expect("INTELLIGENCE_TEST_DATABASE_URL produced an invalid config");

    let pg = PostgresWorkflowState::connect(config)
        .await
        .expect("failed to connect to test database");

    Some(Arc::new(pg))
}

// ---------------------------------------------------------------------------
// Postgres adapter — outbox contract
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_adapter_passes_outbox_contract() {
    // Both Postgres tests intentionally truncate the same dedicated test
    // tables. Serialize them so the default parallel harness cannot erase a
    // record while the other contract scenario is still using it.
    let _guard = postgres_test_mutex().lock().await;

    let Some(pg) = pg_state_or_skip().await else {
        eprintln!("skipping postgres_adapter_passes_outbox_contract: INTELLIGENCE_TEST_DATABASE_URL not set");
        return;
    };

    // Truncate the outbox and audit tables so the suite's fixed idempotency
    // keys succeed on every run against the same database container.
    pg.truncate_for_tests()
        .await
        .expect("truncate_for_tests must succeed before outbox contract suite");

    // The contract suite takes ownership of the port. Because Arc<T> implements
    // the port traits when T does, we unwrap the Arc here so the suite gets an
    // owned value. The pool inside remains shared-reference-counted.
    //
    // Unwrap is safe: we just created this Arc with strong_count == 1.
    let owned = Arc::try_unwrap(pg).expect("Arc must be uniquely owned at this point");
    common::outbox_contract_suite(owned).await;
}

// ---------------------------------------------------------------------------
// Postgres adapter — audit port
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_adapter_audit_port_appends_event() {
    let _guard = postgres_test_mutex().lock().await;

    let url = match std::env::var("INTELLIGENCE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
    {
        Some(u) => u,
        None => {
            eprintln!("skipping postgres_adapter_audit_port_appends_event: INTELLIGENCE_TEST_DATABASE_URL not set");
            return;
        }
    };

    let config = PostgresWorkflowStateConfig::new(&url, 10)
        .expect("INTELLIGENCE_TEST_DATABASE_URL produced an invalid config");
    let pg = PostgresWorkflowState::connect(config)
        .await
        .expect("failed to connect to test database");

    // Truncate so the count assertion holds on every run against the same
    // database container (event_id is a fresh UUID, but audit rows from prior
    // runs would accumulate and could confuse future count-based assertions).
    pg.truncate_for_tests()
        .await
        .expect("truncate_for_tests must succeed before audit append test");

    let tc = common::make_trace_context();
    let event = NormalizationAuditEvent::new(
        "test-audit-event",
        tc,
        BTreeMap::from([("source".to_string(), "contract-test".to_string())]),
    );
    let event_id = event.event_id.clone();

    pg.append(event).await.expect("audit append must succeed");

    // Verify the row landed using a direct sqlx pool opened on the same URL.
    let verify_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("verification pool connect must succeed");

    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM ip_normalization_audit_events WHERE event_id = $1::uuid",
    )
    .bind(&event_id)
    .fetch_one(&verify_pool)
    .await
    .expect("audit count query must succeed");

    assert_eq!(
        row.0, 1,
        "exactly one audit row must be present for event_id {event_id}"
    );

    verify_pool.close().await;
}
