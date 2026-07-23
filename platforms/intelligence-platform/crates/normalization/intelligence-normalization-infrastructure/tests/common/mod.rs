//! Shared test fixtures and the generic outbox contract suite.
//!
//! This module is compiled as a helper (not a standalone test binary) when
//! included via `mod common;` from a test file.  Any integration test binary
//! that needs the contract suite should declare:
//!
//! ```text
//! mod common;
//! ```
//!
//! at the top of its own file and then call `common::outbox_contract_suite`.

// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationSubmissionResult, FoundationSubmissionStatus, NormalizationOutboxPort,
    NormalizationOutboxRecord, NormalizationOutboxStatus, NormalizationProposalSubmission,
    OutboxAcquireResult, OutboxTransitionError,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};

// ---------------------------------------------------------------------------
// Generic contract suite (Task 9 must pass this unchanged)
// ---------------------------------------------------------------------------

/// Exercises the full pinned outbox contract against any [`NormalizationOutboxPort`]
/// implementation.  Run against a fresh, empty port each call.
///
/// # Task 9 usage
///
/// Add `mod common;` at the top of the Postgres test file, then:
///
/// ```ignore
/// #[tokio::test]
/// async fn postgres_adapter_passes_outbox_contract() {
///     let url = match std::env::var("DATABASE_URL") {
///         Ok(u) => u,
///         Err(_) => { eprintln!("DATABASE_URL not set — skipping"); return; }
///     };
///     let adapter = PostgresWorkflowState::connect(&url).await.unwrap();
///     common::outbox_contract_suite(adapter).await;
/// }
/// ```
///
/// # Clock-skew warning
///
/// Postgres adapters must set **and** compare lease expiry on the DB clock
/// (`now()`).  Mixing `Utc::now()` writes with `SQL now()` comparisons makes
/// scenarios 8, 9, and 12 flake under clock skew between the application host
/// and the database server.
pub async fn outbox_contract_suite<P: NormalizationOutboxPort>(port: P) {
    let standard_lease = Duration::from_secs(30);
    let tiny_lease = Duration::from_millis(10);

    // -----------------------------------------------------------------------
    // Scenarios 1-6: single "main" key lifecycle through to Sent
    // -----------------------------------------------------------------------

    let key = "scenario-main";
    let sub_a = make_submission("raw-a");

    // 1. enqueue new => Acquired; get_sent => None (not Sent yet).
    let record_a = NormalizationOutboxRecord::new(key.to_string(), sub_a.clone());
    let fingerprint_a = record_a.payload_fingerprint.clone();
    let result = port.enqueue(record_a, standard_lease).await.unwrap();
    assert_eq!(
        result,
        OutboxAcquireResult::Acquired,
        "first enqueue must be Acquired"
    );
    let sent = port.get_sent(key).await.unwrap();
    assert!(sent.is_none(), "get_sent must return None while InFlight");

    // 2. enqueue same key+fingerprint while InFlight => AlreadyInFlight.
    let record_a2 = NormalizationOutboxRecord::new(key.to_string(), sub_a.clone());
    assert_eq!(
        record_a2.payload_fingerprint, fingerprint_a,
        "same submission must produce same fingerprint"
    );
    let result = port.enqueue(record_a2, standard_lease).await.unwrap();
    assert_eq!(
        result,
        OutboxAcquireResult::AlreadyInFlight,
        "re-enqueue of InFlight with same fingerprint must be AlreadyInFlight"
    );

    // 3. enqueue same key, DIFFERENT fingerprint => PayloadMismatch (checked first).
    let sub_b = make_submission("raw-b-different");
    let record_b_diff = NormalizationOutboxRecord::new(key.to_string(), sub_b);
    assert_ne!(
        record_b_diff.payload_fingerprint, fingerprint_a,
        "different submission must produce different fingerprint"
    );
    let result = port.enqueue(record_b_diff, standard_lease).await.unwrap();
    assert_eq!(
        result,
        OutboxAcquireResult::PayloadMismatch,
        "different fingerprint must return PayloadMismatch regardless of status"
    );

    // 4. mark_sent from InFlight => Ok, status Sent, attempts == 1,
    //    last_error None, submission_result Some; get_sent => Some.
    let sent_record = port.mark_sent(key, make_submission_result()).await.unwrap();
    assert_eq!(
        sent_record.status,
        NormalizationOutboxStatus::Sent,
        "mark_sent must transition to Sent"
    );
    assert_eq!(sent_record.attempts, 1, "mark_sent must increment attempts");
    assert!(
        sent_record.last_error.is_none(),
        "mark_sent must clear last_error"
    );
    assert!(
        sent_record.submission_result.is_some(),
        "mark_sent must set submission_result"
    );
    let sent_via_get = port.get_sent(key).await.unwrap();
    assert!(
        sent_via_get.is_some(),
        "get_sent must return Some after mark_sent"
    );

    // 5. enqueue same key+fingerprint after Sent => AlreadySent.
    let record_a3 = NormalizationOutboxRecord::new(key.to_string(), sub_a.clone());
    let result = port.enqueue(record_a3, standard_lease).await.unwrap();
    assert_eq!(
        result,
        OutboxAcquireResult::AlreadySent,
        "re-enqueue with same fingerprint when Sent must return AlreadySent"
    );

    // 6. mark_retryable_failure on the Sent record => Err(Rejected{current: Sent}).
    let err = port
        .mark_retryable_failure(key, "should fail".to_string())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            OutboxTransitionError::Rejected {
                current: NormalizationOutboxStatus::Sent,
                ..
            }
        ),
        "mark_retryable_failure on Sent must be Rejected with current=Sent; got {err:?}"
    );

    // -----------------------------------------------------------------------
    // Scenario 7: retryable failure → reclaim → dead letter → terminal enqueue
    // -----------------------------------------------------------------------

    let key7 = "scenario-retryable";
    let sub7 = make_submission("raw-7");
    let record7 = NormalizationOutboxRecord::new(key7.to_string(), sub7.clone());
    let fingerprint7 = record7.payload_fingerprint.clone();

    port.enqueue(record7, standard_lease).await.unwrap();

    // mark_retryable_failure => FailedRetryable, attempts 1, last_error Some.
    let r7 = port
        .mark_retryable_failure(key7, "first failure".to_string())
        .await
        .unwrap();
    assert_eq!(r7.status, NormalizationOutboxStatus::FailedRetryable);
    assert_eq!(r7.attempts, 1);
    assert_eq!(
        r7.last_error.as_deref(),
        Some("first failure"),
        "mark_retryable_failure must record error"
    );

    // claim_next_pending claims it => InFlight, fresh claimed_until.
    let claimed = port.claim_next_pending(10, standard_lease).await.unwrap();
    let claimed7 = claimed
        .iter()
        .find(|r| r.idempotency_key == key7)
        .expect("claim_next_pending must return the FailedRetryable record");
    assert_eq!(
        claimed7.status,
        NormalizationOutboxStatus::InFlight,
        "claimed record must be InFlight"
    );
    assert!(
        claimed7.claimed_until.is_some(),
        "claimed record must have claimed_until set"
    );

    // mark_dead_letter => DeadLetter, attempts 2.
    let r7_dead = port
        .mark_dead_letter(key7, "too many retries".to_string())
        .await
        .unwrap();
    assert_eq!(r7_dead.status, NormalizationOutboxStatus::DeadLetter);
    assert_eq!(r7_dead.attempts, 2);

    // enqueue same key+fingerprint after DeadLetter => Err(Rejected{current: DeadLetter}).
    let record7_again = NormalizationOutboxRecord::new(key7.to_string(), sub7);
    assert_eq!(record7_again.payload_fingerprint, fingerprint7);
    let err7 = port
        .enqueue(record7_again, standard_lease)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err7,
            OutboxTransitionError::Rejected {
                current: NormalizationOutboxStatus::DeadLetter,
                ..
            }
        ),
        "enqueue after DeadLetter must be Rejected with current=DeadLetter; got {err7:?}"
    );

    // -----------------------------------------------------------------------
    // Scenario 8: lease expiry reclaim
    // -----------------------------------------------------------------------

    let key8 = "scenario-lease-expiry";
    let record8 = NormalizationOutboxRecord::new(key8.to_string(), make_submission("raw-8"));

    port.enqueue(record8, tiny_lease).await.unwrap();

    // Wait past the tiny lease.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let reclaimed = port.claim_next_pending(10, standard_lease).await.unwrap();
    assert!(
        reclaimed.iter().any(|r| r.idempotency_key == key8),
        "InFlight record with expired lease must be reclaimable"
    );

    // Claim again immediately: fresh lease must not be reclaimable.
    let reclaimed2 = port.claim_next_pending(10, standard_lease).await.unwrap();
    assert!(
        !reclaimed2.iter().any(|r| r.idempotency_key == key8),
        "freshly reclaimed record must not be immediately reclaimable again"
    );

    // -----------------------------------------------------------------------
    // Scenario 9: oldest-first ordering
    //
    // Anti-vacuity design: key9b ("scenario-order-b") is enqueued FIRST so
    // it has the older updated_at, but it sorts LATER than key9a
    // ("scenario-order-a") in BTreeMap key order. A buggy implementation that
    // iterates the BTreeMap directly without sorting by updated_at would claim
    // key9a (lexicographically first) and fail the assertion below. Only a
    // correct implementation that sorts eligible records by updated_at
    // ascending will claim key9b (the truly older record).
    // -----------------------------------------------------------------------

    let key9a = "scenario-order-a";
    let key9b = "scenario-order-b";

    // Enqueue the lexicographically LATER key first so key order opposes
    // updated_at order.
    port.enqueue(
        NormalizationOutboxRecord::new(key9b.to_string(), make_submission("raw-9b")),
        tiny_lease,
    )
    .await
    .unwrap();

    // Sleep between enqueues to ensure distinct updated_at timestamps.
    tokio::time::sleep(Duration::from_millis(10)).await;

    port.enqueue(
        NormalizationOutboxRecord::new(key9a.to_string(), make_submission("raw-9a")),
        tiny_lease,
    )
    .await
    .unwrap();

    // Wait for both tiny leases to expire.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let ordered = port.claim_next_pending(1, standard_lease).await.unwrap();
    assert_eq!(ordered.len(), 1, "claim(1) must return exactly 1 record");
    assert_eq!(
        ordered[0].idempotency_key, key9b,
        "oldest record (key9b, enqueued first despite being lexicographically later) must be claimed first"
    );

    // -----------------------------------------------------------------------
    // Scenario 10: mark_* on unknown key => NotFound
    // -----------------------------------------------------------------------

    let missing = "nonexistent-key-xyz";
    assert!(matches!(
        port.mark_sent(missing, make_submission_result())
            .await
            .unwrap_err(),
        OutboxTransitionError::NotFound
    ));
    assert!(matches!(
        port.mark_retryable_failure(missing, "e".to_string())
            .await
            .unwrap_err(),
        OutboxTransitionError::NotFound
    ));
    assert!(matches!(
        port.mark_dead_letter(missing, "e".to_string())
            .await
            .unwrap_err(),
        OutboxTransitionError::NotFound
    ));
    assert!(matches!(
        port.mark_reconcile_required(missing, "e".to_string())
            .await
            .unwrap_err(),
        OutboxTransitionError::NotFound
    ));

    // -----------------------------------------------------------------------
    // Scenario 11: mark_reconcile_required from InFlight => ReconcileRequired,
    //              attempts incremented.
    // -----------------------------------------------------------------------

    let key11 = "scenario-reconcile";
    let record11 = NormalizationOutboxRecord::new(key11.to_string(), make_submission("raw-11"));

    port.enqueue(record11, standard_lease).await.unwrap();

    let r11 = port
        .mark_reconcile_required(key11, "ambiguous timeout".to_string())
        .await
        .unwrap();
    assert_eq!(r11.status, NormalizationOutboxStatus::ReconcileRequired);
    assert_eq!(
        r11.attempts, 1,
        "mark_reconcile_required must increment attempts"
    );

    // -----------------------------------------------------------------------
    // Scenario 12: limit respected — 4 claimable (key12a/b/c plus the
    // scenario-order-a leftover from Scenario 9 whose tiny lease has long
    // expired and which was not claimed by Scenario 9's claim(1)), claim(2)
    // => 2 records.
    // -----------------------------------------------------------------------

    let key12a = "scenario-limit-a";
    let key12b = "scenario-limit-b";
    let key12c = "scenario-limit-c";

    for key in &[key12a, key12b, key12c] {
        port.enqueue(
            NormalizationOutboxRecord::new(key.to_string(), make_submission(key)),
            tiny_lease,
        )
        .await
        .unwrap();
    }

    // Wait for all three tiny leases to expire.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let limited = port.claim_next_pending(2, standard_lease).await.unwrap();
    assert_eq!(
        limited.len(),
        2,
        "claim(limit=2) must return exactly 2 records when 4 are claimable"
    );
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

pub fn make_submission(raw_record_id: &str) -> NormalizationProposalSubmission {
    NormalizationProposalSubmission {
        request: NormalizationRequest {
            tenant_id: "tenant-test".to_string(),
            source_system: "test-system".to_string(),
            raw_record_id: raw_record_id.to_string(),
            raw_record: serde_json::json!({"name": raw_record_id}),
            trace_context: TraceContext {
                trace_id: format!("trace-{raw_record_id}"),
                tenant_id: "tenant-test".to_string(),
                human_user_id: "user-test".to_string(),
                product_id: "foundation-platform".to_string(),
            },
            target_schema: serde_json::json!({"required": ["normalized_name"]}),
            target_schema_version: "v1".to_string(),
            raw_object_key: None,
            raw_checksum_sha256: None,
            target_kind: "test_kind".to_string(),
            target_identity: serde_json::json!({"id": raw_record_id}),
            dictionaries: BTreeMap::new(),
        },
        proposal: NormalizationProposal {
            raw_record_id: raw_record_id.to_string(),
            proposed_record: serde_json::json!({"normalized_name": raw_record_id}),
            confidence: 0.92,
            reasons: vec!["test".to_string()],
            schema_version: "v1".to_string(),
            policy_id: "normalization-proposal-policy".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: None,
            model_id: None,
            prompt_id: None,
            prompt_version: None,
        },
        validation: NormalizationValidationResult {
            accepted: true,
            raw_record_id: raw_record_id.to_string(),
            confidence: 0.92,
            errors: vec![],
        },
        trace_context: TraceContext {
            trace_id: format!("trace-{raw_record_id}"),
            tenant_id: "tenant-test".to_string(),
            human_user_id: "user-test".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}

pub fn make_submission_result() -> FoundationSubmissionResult {
    FoundationSubmissionResult {
        submission_id: "sub-test-1".to_string(),
        status: FoundationSubmissionStatus::Queued,
        review_required: true,
        platform: "foundation-platform".to_string(),
        metadata: BTreeMap::new(),
    }
}

pub fn make_trace_context() -> TraceContext {
    TraceContext {
        trace_id: "trace-audit-test".to_string(),
        tenant_id: "tenant-test".to_string(),
        human_user_id: "user-test".to_string(),
        product_id: "foundation-platform".to_string(),
    }
}
