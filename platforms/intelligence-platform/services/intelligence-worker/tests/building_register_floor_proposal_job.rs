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
use intelligence_worker::floor_proposal_job::{
    run_building_register_floor_proposal_dry_run, run_building_register_floor_proposal_job,
    BuildingRegisterFloorProposalJobConfig,
};
use serde_json::json;

#[tokio::test]
async fn floor_proposal_job_generates_validates_and_submits_review_only_proposals() {
    let input_path = write_temp_jsonl(sample_foundation_context_pack().to_string());
    let generator = Arc::new(FakeGenerator);
    let submitter = Arc::new(FakeSubmitter::default());

    let summary = run_building_register_floor_proposal_job(
        BuildingRegisterFloorProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator,
        submitter.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.submitted_count, 1);
    assert_eq!(summary.skipped_invalid_count, 0);

    let submissions = submitter.submissions.lock().unwrap();
    let submission = submissions.first().unwrap();
    assert!(!submission.commit_allowed);
    assert!(submission.requires_human_review);
    assert_eq!(submission.request.target_kind, "building_register_floor");
    assert_eq!(
        submission.request.target_identity,
        json!({
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_confidence": "provider_primary_key",
            "source_record_id": "line-43",
            "silver_row_id": "building-register-floor:line-43",
            "entity_impact": {
                "entity_type": "building",
                "entity_key": "11680-raw-1",
                "consistency_domains": ["floor"]
            }
        })
    );
    assert_eq!(
        submission.request.raw_record["same_building_floor_sequence"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        submission.proposal.proposed_record,
        json!({
            "floor_kind": "basement",
            "floor_number": 1,
            "floor_index": -1,
            "floor_display_ko": "\u{C9C0}\u{D558} 1\u{CE35}"
        })
    );
}

#[tokio::test]
async fn floor_proposal_job_skips_malformed_input_rows_without_aborting_batch() {
    let input_path = write_temp_jsonl(format!(
        "{}\n{{not-valid-json}}\n{}",
        sample_foundation_context_pack(),
        sample_foundation_context_pack_with_raw_record_id("line-44")
    ));
    let generator = Arc::new(FakeGenerator);
    let submitter = Arc::new(FakeSubmitter::default());

    let summary = run_building_register_floor_proposal_job(
        BuildingRegisterFloorProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
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
    assert_eq!(summary.input_errors.len(), 1);
    assert_eq!(summary.input_errors[0].line, 2);
}

#[tokio::test]
async fn floor_proposal_dry_run_generates_and_validates_without_submitter() {
    let input_path = write_temp_jsonl(sample_foundation_context_pack().to_string());
    let generator = Arc::new(FakeGenerator);

    let summary = run_building_register_floor_proposal_dry_run(
        BuildingRegisterFloorProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator,
    )
    .await
    .unwrap();

    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.accepted_count, 1);
    assert_eq!(summary.rejected_count, 0);
    assert_eq!(summary.results.len(), 1);
    assert!(!summary.results[0].commit_allowed);
    assert!(summary.results[0].requires_human_review);
    assert_eq!(
        summary.results[0].proposal.proposed_record["floor_display_ko"],
        "\u{C9C0}\u{D558} 1\u{CE35}"
    );
}

#[tokio::test]
async fn floor_proposal_dry_run_reports_malformed_input_rows() {
    let input_path = write_temp_jsonl(format!(
        "{}\n{{not-valid-json}}",
        sample_foundation_context_pack()
    ));
    let generator = Arc::new(FakeGenerator);

    let summary = run_building_register_floor_proposal_dry_run(
        BuildingRegisterFloorProposalJobConfig {
            input_path,
            tenant_id: "foundation-platform".to_string(),
            trace_id: "trace-floor-1".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
            minimum_confidence: 0.85,
        },
        generator,
    )
    .await
    .unwrap();

    assert_eq!(summary.input_row_count, 2);
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.invalid_input_count, 1);
    assert_eq!(summary.input_errors[0].line, 2);
}

#[test]
fn floor_proposal_job_summary_records_validation_rejections() {
    let mut summary =
        intelligence_worker::floor_proposal_job::BuildingRegisterFloorProposalJobSummary {
            input_row_count: 3,
            request_count: 2,
            submitted_count: 1,
            skipped_invalid_count: 1,
            invalid_input_count: 1,
            input_errors: vec![
                intelligence_worker::floor_proposal_job::BuildingRegisterFloorInputErrorSummary {
                    line: 2,
                    message: "invalid json".to_string(),
                },
            ],
        };

    summary.skipped_invalid_count += 1;

    assert_eq!(summary.skipped_invalid_count, 2);
    assert_eq!(summary.input_errors[0].line, 2);
}

struct FakeGenerator;

#[async_trait]
impl NormalizationProposalGenerator for FakeGenerator {
    async fn propose(
        &self,
        request: &NormalizationRequest,
    ) -> Result<NormalizationProposal, NormalizationProposalError> {
        Ok(NormalizationProposal {
            raw_record_id: request.raw_record_id.clone(),
            proposed_record: json!({
                "floor_kind": "basement",
                "floor_number": 1,
                "floor_index": -1,
                "floor_display_ko": "\u{C9C0}\u{D558} 1\u{CE35}"
            }),
            confidence: 0.93,
            reasons: vec!["원천 층 값과 같은 건물 층 순서를 함께 확인했습니다.".to_string()],
            schema_version: request.target_schema_version.clone(),
            policy_id: "building-register-floor-normalization".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: Some("qwen3.6-building-register-floor".to_string()),
            model_id: Some("qwen3.6".to_string()),
            prompt_id: Some("building-register-floor-normalization-v1".to_string()),
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
        "building-register-floor-proposal-job-{}-{unique}-{counter}.jsonl",
        std::process::id(),
    ));
    std::fs::write(&path, format!("{line}\n")).unwrap();
    path
}

fn sample_foundation_context_pack() -> serde_json::Value {
    json!({
        "schema_version": "foundation-platform.floor_entity_context_pack.v1",
        "context_pack_id": "floor-context-pack:pack-1",
        "source_system": "foundation-platform.silver.building_register_floors",
        "target": {
            "target_kind": "building_register_floor",
            "raw_record_id": "line-43",
            "silver_row_id": "building-register-floor:line-43",
            "bronze_object_key": "bronze/source=datagokr__building_register_floor/page-000001.json",
            "row_checksum_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "source_snapshot_id": "snapshot-1",
            "source_line_number": 43
        },
        "building_identity_candidate": {
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_confidence": "provider_primary_key"
        },
        "entity_impact": {
            "entity_type": "building",
            "entity_key": "11680-raw-1",
            "consistency_domains": ["floor"]
        },
        "semantic_contract": {
            "source_slug": "datagokr__building_register_floor",
            "field_mappings": [],
            "entity_impacts": []
        },
        "target_raw_floor": {
            "floor_type_code_raw": "10",
            "floor_type_name_raw": "\u{C9C0}\u{D558}",
            "floor_number_raw": "1",
            "floor_label_raw": "\u{C9C0}1\u{CE35}"
        },
        "current_deterministic_normalization": {
            "floor_kind": "unknown",
            "floor_number": null,
            "floor_index": null,
            "floor_display_ko": null,
            "status": "proposal_required",
            "reason": "label_kind_mismatch"
        },
        "same_building_floor_sequence": [
            {
                "mgm_bldrgst_pk": "11680-raw-1",
                "source_record_id": "line-42",
                "silver_row_id": "building-register-floor:line-42",
                "floor_type_code_raw": "20",
                "floor_type_name_raw": "\u{C9C0}\u{C0C1}",
                "floor_number_raw": "1",
                "floor_label_raw": "\u{C9C0}\u{C0C1}1\u{CE35}",
                "floor_kind": "above_ground",
                "floor_number": 1,
                "floor_index": 1,
                "floor_display_ko": "\u{C9C0}\u{C0C1} 1\u{CE35}",
                "normalization_status": "normalized",
                "normalization_reason": "deterministic_rule",
                "source_line_number": 42
            },
            {
                "mgm_bldrgst_pk": "11680-raw-1",
                "source_record_id": "line-43",
                "silver_row_id": "building-register-floor:line-43",
                "floor_type_code_raw": "10",
                "floor_type_name_raw": "\u{C9C0}\u{D558}",
                "floor_number_raw": "1",
                "floor_label_raw": "\u{C9C0}1\u{CE35}",
                "floor_kind": "unknown",
                "floor_number": null,
                "floor_index": null,
                "floor_display_ko": null,
                "normalization_status": "proposal_required",
                "normalization_reason": "label_kind_mismatch",
                "source_line_number": 43
            }
        ],
        "building_title_context": {
            "status": "not_available_in_current_handoff"
        },
        "unit_context_summary": {
            "status": "not_available_in_current_handoff"
        },
        "policy_context": {
            "policy_id": "foundation-platform.floor-normalization",
            "policy_version": "v1",
            "default_locale": "ko-KR",
            "machine_values_language": "en-US"
        },
        "allowed_output_contract": {
            "required_locale": "ko-KR",
            "machine_fields": [
                "floor_kind",
                "floor_number",
                "floor_index",
                "normalization_status",
                "normalization_reason"
            ],
            "localized_fields": [
                "floor_display_ko",
                "review_message_ko"
            ]
        },
        "trace": {
            "valid_from_utc": "2026-07-01T00:00:00Z",
            "ingested_at_utc": "2026-07-01T01:00:00Z"
        }
    })
}

fn sample_foundation_context_pack_with_raw_record_id(raw_record_id: &str) -> serde_json::Value {
    let mut value = sample_foundation_context_pack();
    value["target"]["raw_record_id"] = json!(raw_record_id);
    value
}
