// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationProposalError, NormalizationProposalGenerator,
    NormalizationProposalSubmission,
};
use intelligence_normalization_domain::{NormalizationProposal, NormalizationRequest};
use intelligence_worker::unit_proposal_job::{
    run_building_register_unit_proposal_dry_run, run_building_register_unit_proposal_job,
    BuildingRegisterUnitProposalJobConfig,
};
use serde_json::json;

#[tokio::test]
async fn unit_proposal_job_generates_validates_and_submits_review_only_proposals() {
    let input_path = write_temp_jsonl(sample_foundation_context_pack().to_string());
    let generator = Arc::new(FakeGenerator::default());
    let submitter = Arc::new(FakeSubmitter::default());

    let summary = run_building_register_unit_proposal_job(
        BuildingRegisterUnitProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator.clone(),
        submitter.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.input_row_count, 1);
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.model_request_count, 1);
    assert_eq!(summary.submitted_count, 1);
    assert_eq!(summary.skipped_manual_review_count, 0);
    assert_eq!(generator.call_count(), 1);

    let submissions = submitter.submissions.lock().unwrap();
    let submission = submissions.first().unwrap();
    assert!(!submission.commit_allowed);
    assert!(submission.requires_human_review);
    assert_eq!(submission.request.target_kind, "building_register_unit");
    assert_eq!(
        submission.proposal.proposed_record,
        json!({
            "unit_number": 301,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong",
            "normalization_status": "accepted",
            "normalization_reason": "numeric_unit_name_with_context"
        })
    );
}

#[tokio::test]
async fn unit_proposal_dry_run_skips_manual_review_rows_without_model_call() {
    let mut row = sample_foundation_context_pack();
    row["second_pass_decision"] = json!({
        "status": "manual_review_required",
        "reason": "no_scope_sequence",
        "ai_required": false
    });
    let input_path = write_temp_jsonl(row.to_string());
    let generator = Arc::new(FakeGenerator::default());

    let summary = run_building_register_unit_proposal_dry_run(
        BuildingRegisterUnitProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.input_row_count, 1);
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.model_request_count, 0);
    assert_eq!(summary.skipped_manual_review_count, 1);
    assert_eq!(summary.accepted_count, 0);
    assert_eq!(summary.rejected_count, 0);
    assert_eq!(generator.call_count(), 0);
}

#[tokio::test]
async fn unit_proposal_job_skips_malformed_input_rows_without_aborting_batch() {
    let input_path = write_temp_jsonl(format!(
        "{}\n{{not-valid-json}}\n{}",
        sample_foundation_context_pack(),
        sample_foundation_context_pack_with_silver_row_id("building-register-unit:line-102")
    ));
    let generator = Arc::new(FakeGenerator::default());
    let submitter = Arc::new(FakeSubmitter::default());

    let summary = run_building_register_unit_proposal_job(
        BuildingRegisterUnitProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-unit-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator,
        submitter.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.input_row_count, 3);
    assert_eq!(summary.request_count, 2);
    assert_eq!(summary.invalid_input_count, 1);
    assert_eq!(summary.submitted_count, 2);
    assert_eq!(submitter.submissions.lock().unwrap().len(), 2);
    assert_eq!(summary.input_errors[0].line, 2);
}

#[derive(Default)]
struct FakeGenerator {
    calls: Mutex<u64>,
}

impl FakeGenerator {
    fn call_count(&self) -> u64 {
        *self.calls.lock().unwrap()
    }
}

#[async_trait]
impl NormalizationProposalGenerator for FakeGenerator {
    async fn propose(
        &self,
        request: &NormalizationRequest,
    ) -> Result<NormalizationProposal, NormalizationProposalError> {
        *self.calls.lock().unwrap() += 1;
        Ok(NormalizationProposal {
            raw_record_id: request.raw_record_id.clone(),
            proposed_record: json!({
                "unit_number": 301,
                "building_mgm_bldrgst_pk": "building-pk-1",
                "building_link_method": "canonical_dong",
                "normalization_status": "accepted",
                "normalization_reason": "numeric_unit_name_with_context"
            }),
            confidence: 0.93,
            reasons: vec!["같은 범위의 호실 순번과 동/층 맥락을 근거로 판단했습니다.".to_string()],
            schema_version: request.target_schema_version.clone(),
            policy_id: "building-register-unit-normalization".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: Some("qwen3.6-building-register-unit".to_string()),
            model_id: Some("qwen3.6".to_string()),
            prompt_id: Some("building-register-unit-normalization-v1".to_string()),
            prompt_version: Some("v1".to_string()),
        })
    }
}

#[derive(Default)]
struct FakeSubmitter {
    submissions: Mutex<Vec<NormalizationProposalSubmission>>,
}

#[async_trait]
impl FoundationNormalizationSubmitter for FakeSubmitter {
    async fn submit(
        &self,
        submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        self.submissions.lock().unwrap().push(submission.clone());
        Ok(FoundationSubmissionResult {
            submission_id: "proposal-1".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: Default::default(),
        })
    }
}

fn write_temp_jsonl(line: String) -> PathBuf {
    static TEMP_JSONL_COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_JSONL_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.push(format!(
        "building-register-unit-proposal-job-{}-{unique}-{counter}.jsonl",
        std::process::id(),
    ));
    std::fs::write(&path, format!("{line}\n")).unwrap();
    path
}

fn sample_foundation_context_pack_with_silver_row_id(silver_row_id: &str) -> serde_json::Value {
    let mut value = sample_foundation_context_pack();
    value["target"]["silver_row_id"] = json!(silver_row_id);
    value
}

fn sample_foundation_context_pack() -> serde_json::Value {
    json!({
        "schema_version": "foundation-platform.unit_entity_context_pack.v1",
        "context_pack_id": "unit-context-pack:pack-1",
        "source_system": "foundation-platform.silver.building_register_units",
        "target": {
            "target_kind": "building_register_unit",
            "silver_row_id": "building-register-unit:line-101",
            "bronze_object_key": "bronze/source=hubgokr__building_register_exclusive_unit/OPN.zip",
            "row_checksum_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_snapshot_id": "snapshot-1",
            "source_line_number": 101
        },
        "unit_identity_candidate": {
            "mgm_bldrgst_pk": "unit-pk-101",
            "pnu": "9999900601100010000",
            "dong_join_name": "101동",
            "dong_name_raw": "101동",
            "unit_name_raw": "301호",
            "unit_number": null,
            "floor_kind": "above_ground",
            "floor_index": 3,
            "floor_number": 3,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong"
        },
        "current_deterministic_normalization": {
            "status": "proposal_required",
            "reason": "numeric_unit_name_with_context",
            "unit_number": null,
            "building_mgm_bldrgst_pk": "building-pk-1",
            "building_link_method": "canonical_dong"
        },
        "same_scope_unit_summary": {
            "scope_key": "9999900601100010000|building-pk-1|101동|3",
            "accepted_unit_count": 2,
            "min_unit_number": 301,
            "max_unit_number": 302,
            "distinct_unit_number_count": 2
        },
        "entity_context": {
            "entity_context_key": "9999900601100010000|building-pk-1|101동|3",
            "same_scope_accepted_unit_count": 2,
            "same_building_accepted_unit_count": 20,
            "neighbor_unit_examples": [
                {"unit_name_raw": "301호", "unit_number": 301},
                {"unit_name_raw": "302호", "unit_number": 302}
            ],
            "conflict_flags": []
        },
        "second_pass_decision": {
            "status": "ai_required",
            "reason": "numeric_unit_name_with_context",
            "ai_required": true
        },
        "policy_context": {
            "policy_id": "foundation-platform.unit-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US",
            "ai_role": "proposal_only",
            "decision_owner": "foundation-platform",
            "canonical_write_path": "proposal_inbox_human_review_then_command"
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "unit_number",
                "building_mgm_bldrgst_pk",
                "building_link_method",
                "normalization_status",
                "normalization_reason"
            ],
            "localized_fields": ["review_message_ko"]
        },
        "trace": {
            "valid_from_utc": "2026-07-01T00:00:00Z",
            "ingested_at_utc": "2026-07-01T01:00:00Z"
        }
    })
}
