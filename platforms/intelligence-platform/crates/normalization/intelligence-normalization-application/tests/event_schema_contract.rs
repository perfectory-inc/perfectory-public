// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Contract tests that pin the Avro schema
//! (`schemas/intelligence.normalization-proposal.submission-requested.v1.avsc`)
//! to the Rust code.
//!
//! These tests fail immediately when:
//!   - the .avsc file is syntactically invalid
//!   - a required field is missing from the schema
//!   - a round-trip produces a different value (serialization regression)
//!   - a schema evolution violates additive-only discipline (no default on a new field)

use std::collections::BTreeMap;

use apache_avro::{types::Value as AvroValue, Reader, Schema, Writer};
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    NormalizationOutboxRecord, NormalizationProposalSubmission,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};

// The .avsc file, embedded at compile time.
// Path is relative to this test file: tests/ -> normalization-application/ -> normalization/
// -> crates/ -> repo root -> schemas/.
const SCHEMA_STR: &str = include_str!(
    "../../../../schemas/intelligence.normalization-proposal.submission-requested.v1.avsc"
);

// Fields that carry semantic meaning and must exist in the schema (no default required).
// When this set changes, the schema and this list must be updated together.
const REQUIRED_FIELDS: &[&str] = &[
    "event_id",
    "aggregate_id",
    "idempotency_key",
    "payload_fingerprint",
    "tenant_id",
    "product_id",
    "trace_id",
    "raw_record_id",
    "target_kind",
    "target_schema_version",
    "proposal_schema_version",
    "policy_id",
    "policy_version",
    "confidence",
    "submission_json",
    "occurred_at",
];

// ---------------------------------------------------------------------------
// Test 1: schema parses and a round-trip through Writer/Reader is lossless
// ---------------------------------------------------------------------------

#[test]
fn schema_parses_and_round_trips() {
    // 1. Parse the schema.
    let schema = Schema::parse_str(SCHEMA_STR).expect("Avro schema must parse without errors");

    // 2. Build a realistic NormalizationOutboxRecord from a fixture submission.
    let submission = realistic_submission();
    let record = NormalizationOutboxRecord::new(
        "tenant-1:building_register_floor:floor-raw-1:building_register_floor.normalized.v1"
            .to_string(),
        submission.clone(),
    );

    // 3. Map the outbox record to the Avro event value.
    let occurred_at_ms = record.created_at.timestamp_millis();
    let submission_json =
        serde_json::to_string(&record.submission).expect("submission must be serializable");

    let event_value = AvroValue::Record(vec![
        (
            "event_id".to_string(),
            AvroValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
        ),
        (
            "event_type".to_string(),
            AvroValue::String("normalization-proposal.submission-requested".to_string()),
        ),
        (
            "source".to_string(),
            AvroValue::String("/intelligence-platform/normalization".to_string()),
        ),
        (
            "specversion".to_string(),
            AvroValue::String("1.0".to_string()),
        ),
        (
            "aggregate_type".to_string(),
            AvroValue::String("normalization-proposal".to_string()),
        ),
        (
            "aggregate_id".to_string(),
            AvroValue::String(record.idempotency_key.clone()),
        ),
        (
            "idempotency_key".to_string(),
            AvroValue::String(record.idempotency_key.clone()),
        ),
        (
            "payload_fingerprint".to_string(),
            AvroValue::String(record.payload_fingerprint.clone()),
        ),
        (
            "tenant_id".to_string(),
            AvroValue::String(submission.request.tenant_id.clone()),
        ),
        (
            "product_id".to_string(),
            AvroValue::String(submission.trace_context.product_id.clone()),
        ),
        (
            "trace_id".to_string(),
            AvroValue::String(submission.trace_context.trace_id.clone()),
        ),
        (
            "raw_record_id".to_string(),
            AvroValue::String(submission.request.raw_record_id.clone()),
        ),
        (
            "target_kind".to_string(),
            AvroValue::String(submission.request.target_kind.clone()),
        ),
        (
            "target_schema_version".to_string(),
            AvroValue::String(submission.request.target_schema_version.clone()),
        ),
        (
            "proposal_schema_version".to_string(),
            AvroValue::String(submission.proposal.schema_version.clone()),
        ),
        (
            "policy_id".to_string(),
            AvroValue::String(submission.proposal.policy_id.clone()),
        ),
        (
            "policy_version".to_string(),
            AvroValue::String(submission.proposal.policy_version.clone()),
        ),
        (
            "confidence".to_string(),
            AvroValue::Double(submission.proposal.confidence),
        ),
        (
            "submission_json".to_string(),
            AvroValue::String(submission_json),
        ),
        (
            "occurred_at".to_string(),
            AvroValue::TimestampMillis(occurred_at_ms),
        ),
    ]);

    // 4. Write → encoded bytes.
    let mut writer = Writer::new(&schema, Vec::new());
    writer
        .append_value_ref(&event_value)
        .expect("Avro value must be valid against the schema");
    let encoded = writer
        .into_inner()
        .expect("Writer::into_inner must succeed");

    assert!(!encoded.is_empty(), "encoded bytes must not be empty");

    // 5. Read back and verify load-bearing fields.
    let reader = Reader::new(&encoded[..]).expect("Reader must parse the encoded bytes");
    let mut read_count = 0;
    for result in reader {
        let decoded = result.expect("record must decode without error");

        // Unwrap the Record variant.
        let fields = match decoded {
            AvroValue::Record(fields) => fields,
            other => panic!("expected AvroValue::Record, got {other:?}"),
        };

        let get = |name: &str| -> &AvroValue {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("field '{name}' missing from decoded record"))
        };

        // aggregate_id == idempotency_key (Kafka message key contract)
        assert_eq!(
            get("aggregate_id"),
            get("idempotency_key"),
            "aggregate_id must equal idempotency_key"
        );

        // aggregate_id is the key we provided
        assert_eq!(
            get("aggregate_id"),
            &AvroValue::String(record.idempotency_key.clone()),
            "aggregate_id round-trip mismatch"
        );

        // payload_fingerprint is 64 lowercase hex chars
        match get("payload_fingerprint") {
            AvroValue::String(fp) => {
                assert_eq!(fp.len(), 64, "fingerprint must be 64 hex chars");
                assert!(
                    fp.chars().all(|c| c.is_ascii_hexdigit()),
                    "fingerprint must be hex"
                );
                assert_eq!(*fp, fp.to_lowercase(), "fingerprint must be lowercase");
            }
            other => panic!("payload_fingerprint must be String, got {other:?}"),
        }

        // tenant_id
        assert_eq!(
            get("tenant_id"),
            &AvroValue::String("tenant-floor".to_string()),
            "tenant_id round-trip mismatch"
        );

        // product_id
        assert_eq!(
            get("product_id"),
            &AvroValue::String("foundation-platform".to_string()),
            "product_id round-trip mismatch"
        );

        // confidence
        assert_eq!(
            get("confidence"),
            &AvroValue::Double(0.93),
            "confidence round-trip mismatch"
        );

        // occurred_at is a timestamp-millis (TimestampMillis or Long depending on avro reader)
        match get("occurred_at") {
            AvroValue::TimestampMillis(ms) => {
                assert!(*ms > 0, "occurred_at must be a positive timestamp")
            }
            AvroValue::Long(ms) => assert!(*ms > 0, "occurred_at must be a positive timestamp"),
            other => panic!("occurred_at must be a timestamp value, got {other:?}"),
        }

        // event_type default survived the round-trip
        assert_eq!(
            get("event_type"),
            &AvroValue::String("normalization-proposal.submission-requested".to_string()),
            "event_type round-trip mismatch"
        );

        read_count += 1;
    }

    assert_eq!(
        read_count, 1,
        "expected exactly one record in the Avro container"
    );
}

// ---------------------------------------------------------------------------
// Test 2: every schema field either is required-by-design or carries a default
// ---------------------------------------------------------------------------

#[test]
fn schema_defaults_cover_evolvable_fields() {
    let schema = Schema::parse_str(SCHEMA_STR).expect("schema must parse");

    let record_fields = match &schema {
        Schema::Record(record_schema) => &record_schema.fields,
        other => panic!("schema must be a Record, got {other:?}"),
    };

    let required: std::collections::HashSet<&str> = REQUIRED_FIELDS.iter().copied().collect();

    let mut violations: Vec<String> = Vec::new();
    for field in record_fields {
        if required.contains(field.name.as_str()) {
            // Required-by-design: no default needed.
            continue;
        }
        if field.default.is_none() {
            violations.push(format!(
                "field '{}' is neither in the REQUIRED set nor has an Avro default — \
                 adding a default or listing it in REQUIRED_FIELDS is mandatory for \
                 BACKWARD_TRANSITIVE evolution",
                field.name
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "schema evolution tripwire triggered:\n{}",
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn realistic_submission() -> NormalizationProposalSubmission {
    let trace = TraceContext {
        trace_id: "trace-floor-contract-1".to_string(),
        tenant_id: "tenant-floor".to_string(),
        human_user_id: "service:intelligence-platform".to_string(),
        product_id: "foundation-platform".to_string(),
    };

    let request = NormalizationRequest {
        tenant_id: "tenant-floor".to_string(),
        source_system: "foundation-platform.silver.building_register_floors".to_string(),
        raw_record_id: "floor-raw-1".to_string(),
        raw_record: serde_json::json!({
            "target_raw_floor": {
                "floor_type_code_raw": "10",
                "floor_type_name_raw": "\u{C9C0}\u{D558}",
                "floor_number_raw": "1",
                "floor_label_raw": "\u{C9C0}1\u{CE35}"
            },
            "current_deterministic_normalization": {
                "status": "proposal_required"
            }
        }),
        trace_context: trace.clone(),
        target_schema: serde_json::json!({
            "required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"]
        }),
        target_schema_version: "building_register_floor.normalized.v1".to_string(),
        raw_object_key: Some(
            "bronze/source=datagokr__building_register_floor_overview/page-000001.json".to_string(),
        ),
        raw_checksum_sha256: Some(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ),
        target_kind: "building_register_floor".to_string(),
        target_identity: serde_json::json!({"mgm_bldrgst_pk": "11680-floor-raw-1"}),
        dictionaries: BTreeMap::new(),
    };

    let proposal = NormalizationProposal {
        raw_record_id: "floor-raw-1".to_string(),
        proposed_record: serde_json::json!({
            "floor_kind": "basement",
            "floor_number": 1,
            "floor_index": -1,
            "floor_display_ko": "\u{C9C0}\u{D558} 1\u{CE35}"
        }),
        confidence: 0.93,
        reasons: vec!["\u{C9C0}\u{D558} 표기를 상위 규칙에서 정규화".to_string()],
        schema_version: "building_register_floor.normalized.v1".to_string(),
        policy_id: "normalization-proposal-policy".to_string(),
        policy_version: "v1".to_string(),
        model_profile_id: Some("ollama/qwen3:8b".to_string()),
        model_id: Some("qwen3:8b".to_string()),
        prompt_id: Some("building-floor-normalization-v1".to_string()),
        prompt_version: Some("v1".to_string()),
    };

    let validation = NormalizationValidationResult {
        accepted: true,
        raw_record_id: "floor-raw-1".to_string(),
        confidence: 0.93,
        errors: vec![],
    };

    NormalizationProposalSubmission {
        request,
        proposal,
        validation,
        trace_context: trace,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}
