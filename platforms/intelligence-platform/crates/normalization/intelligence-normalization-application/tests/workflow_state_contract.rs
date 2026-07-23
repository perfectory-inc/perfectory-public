// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;

use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationSubmissionResult, NormalizationOutboxRecord, NormalizationOutboxStatus,
    NormalizationProposalSubmission, OutboxAcquireResult,
};
use intelligence_normalization_domain::normalization::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};

#[test]
fn foundation_submission_result_defaults_to_foundation_platform_wire_name() {
    let result = serde_json::from_value::<FoundationSubmissionResult>(serde_json::json!({
        "submission_id": "018f7c6a-0000-7000-8000-000000000001",
        "status": "queued",
        "review_required": true,
        "metadata": {}
    }))
    .unwrap();

    assert_eq!(result.platform, "foundation-platform");
}

#[test]
fn durable_outbox_statuses_are_explicit() {
    assert_eq!(
        serde_json::to_string(&NormalizationOutboxStatus::InFlight).unwrap(),
        "\"in_flight\""
    );
    assert_eq!(
        serde_json::to_string(&NormalizationOutboxStatus::ReconcileRequired).unwrap(),
        "\"reconcile_required\""
    );
}

#[test]
fn acquire_result_distinguishes_new_existing_sent_and_mismatch() {
    assert!(OutboxAcquireResult::Acquired.is_acquired());
    assert!(!OutboxAcquireResult::AlreadyInFlight.is_acquired());
    assert!(!OutboxAcquireResult::AlreadySent.is_acquired());
    assert!(!OutboxAcquireResult::PayloadMismatch.is_acquired());
}

#[test]
fn outbox_record_new_computes_64_char_lowercase_hex_fingerprint() {
    let submission = minimal_submission();
    let record = NormalizationOutboxRecord::new("key-1".to_string(), submission.clone());

    assert_eq!(
        record.payload_fingerprint.len(),
        64,
        "fingerprint must be 64 hex chars (SHA-256)"
    );
    assert!(
        record
            .payload_fingerprint
            .chars()
            .all(|c| c.is_ascii_hexdigit()),
        "fingerprint must be hex digits only"
    );
    assert_eq!(
        record.payload_fingerprint,
        record.payload_fingerprint.to_lowercase(),
        "fingerprint must be lowercase"
    );

    // Two records with identical submissions share the fingerprint.
    let record2 = NormalizationOutboxRecord::new("key-2".to_string(), submission);
    assert_eq!(
        record.payload_fingerprint, record2.payload_fingerprint,
        "identical submissions must produce identical fingerprints"
    );
}

#[test]
fn mutated_submission_changes_fingerprint() {
    let submission = minimal_submission();
    let record = NormalizationOutboxRecord::new("key-1".to_string(), submission);

    let mut mutated = minimal_submission();
    mutated.request.raw_record_id = "different-raw-record".to_string();
    // Also mutate the proposal to keep it consistent (raw_record_id must match).
    mutated.proposal.raw_record_id = "different-raw-record".to_string();
    mutated.validation.raw_record_id = "different-raw-record".to_string();
    let record2 = NormalizationOutboxRecord::new("key-1".to_string(), mutated);

    assert_ne!(
        record.payload_fingerprint, record2.payload_fingerprint,
        "different submissions must produce different fingerprints"
    );
}

/// Key-order tripwire: serde_json serialises JSON objects via BTreeMap
/// (preserve_order OFF), so `{"b":1,"a":2}` and `{"a":2,"b":1}` produce
/// identical bytes and therefore identical fingerprints.
#[test]
fn fingerprint_is_stable_under_json_key_order_variation() {
    let mut sub_a = minimal_submission();
    sub_a.request.raw_record = serde_json::json!({"b": 1, "a": 2});

    let mut sub_b = minimal_submission();
    sub_b.request.raw_record = serde_json::json!({"a": 2, "b": 1});

    let record_a = NormalizationOutboxRecord::new("key-order-a".to_string(), sub_a);
    let record_b = NormalizationOutboxRecord::new("key-order-b".to_string(), sub_b);

    assert_eq!(
        record_a.payload_fingerprint, record_b.payload_fingerprint,
        "key-order variant submissions must produce identical fingerprints (BTreeMap serialisation)"
    );
}

/// Trace-variance pin: the fingerprint covers trace_context in full (strict
/// Stripe-style body fingerprint). Two submissions that differ only in
/// trace_id must produce different fingerprints — a retry that regenerates
/// trace_id is a distinct payload by design.
#[test]
fn fingerprint_differs_when_trace_id_differs() {
    let mut sub_a = minimal_submission();
    sub_a.trace_context.trace_id = "trace-alpha".to_string();

    let mut sub_b = minimal_submission();
    sub_b.trace_context.trace_id = "trace-beta".to_string();

    let record_a = NormalizationOutboxRecord::new("trace-var-a".to_string(), sub_a);
    let record_b = NormalizationOutboxRecord::new("trace-var-b".to_string(), sub_b);

    assert_ne!(
        record_a.payload_fingerprint, record_b.payload_fingerprint,
        "differing trace_id must produce different fingerprints (strict body fingerprint)"
    );
}

// ---- fixture helpers -------------------------------------------------------

fn minimal_submission() -> NormalizationProposalSubmission {
    NormalizationProposalSubmission {
        request: NormalizationRequest {
            tenant_id: "tenant-1".to_string(),
            source_system: "test-system".to_string(),
            raw_record_id: "raw-1".to_string(),
            raw_record: serde_json::json!({"name": "Test"}),
            trace_context: TraceContext {
                trace_id: "trace-1".to_string(),
                tenant_id: "tenant-1".to_string(),
                human_user_id: "user-1".to_string(),
                product_id: "foundation-platform".to_string(),
            },
            target_schema: serde_json::json!({"required": ["normalized_name"]}),
            target_schema_version: "v1".to_string(),
            raw_object_key: None,
            raw_checksum_sha256: None,
            target_kind: "industrial_complex".to_string(),
            target_identity: serde_json::json!({"id": "1"}),
            dictionaries: BTreeMap::new(),
        },
        proposal: NormalizationProposal {
            raw_record_id: "raw-1".to_string(),
            proposed_record: serde_json::json!({"normalized_name": "Test"}),
            confidence: 0.91,
            reasons: vec!["source name field maps to normalized_name".to_string()],
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
            raw_record_id: "raw-1".to_string(),
            confidence: 0.91,
            errors: vec![],
        },
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            human_user_id: "user-1".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}
