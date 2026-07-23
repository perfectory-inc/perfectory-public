#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    run_floor_proposal, run_floor_proposal_dry_run, run_unit_proposal, run_unit_proposal_dry_run,
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationProposalError, NormalizationProposalGenerator,
    NormalizationProposalSubmission,
};
use intelligence_normalization_domain::normalization::{
    NormalizationProposal, NormalizationRequest,
};

struct Generator {
    confidence: f64,
    calls: Mutex<u32>,
}

#[async_trait]
impl NormalizationProposalGenerator for Generator {
    async fn propose(
        &self,
        request: &NormalizationRequest,
    ) -> Result<NormalizationProposal, NormalizationProposalError> {
        *self.calls.lock().unwrap() += 1;
        Ok(NormalizationProposal {
            raw_record_id: request.raw_record_id.clone(),
            proposed_record: serde_json::json!({"name": "normalized"}),
            confidence: self.confidence,
            reasons: vec!["normalized from prepared input".to_string()],
            schema_version: request.target_schema_version.clone(),
            policy_id: "policy-1".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: Some("profile-1".to_string()),
            model_id: Some("model-1".to_string()),
            prompt_id: Some("prompt-1".to_string()),
            prompt_version: Some("v1".to_string()),
        })
    }
}

#[derive(Default)]
struct Submitter {
    submissions: Mutex<Vec<NormalizationProposalSubmission>>,
}

#[async_trait]
impl FoundationNormalizationSubmitter for Submitter {
    async fn submit(
        &self,
        submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        self.submissions.lock().unwrap().push(submission.clone());
        Ok(FoundationSubmissionResult {
            submission_id: "submission-1".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: BTreeMap::new(),
        })
    }
}

#[tokio::test]
async fn floor_use_case_submits_valid_review_only_proposal_with_metadata() {
    let generator = Arc::new(Generator {
        confidence: 0.9,
        calls: Mutex::new(0),
    });
    let submitter = Arc::new(Submitter::default());

    let summary = run_floor_proposal(
        1,
        vec![request("floor-1", true)],
        vec![],
        0.85,
        generator,
        submitter.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.submitted_count, 1);
    let submission = submitter.submissions.lock().unwrap().remove(0);
    assert!(!submission.commit_allowed);
    assert!(submission.requires_human_review);
    assert_eq!(submission.submission_metadata["policy_id"], "policy-1");
    assert_eq!(submission.submission_metadata["model_id"], "model-1");
}

#[tokio::test]
async fn floor_use_case_validation_gate_skips_submission_and_dry_run_keeps_metadata() {
    let rejected = Arc::new(Generator {
        confidence: 0.5,
        calls: Mutex::new(0),
    });
    let submitter = Arc::new(Submitter::default());
    let summary = run_floor_proposal(
        1,
        vec![request("floor-rejected", true)],
        vec![],
        0.85,
        rejected,
        submitter.clone(),
    )
    .await
    .unwrap();
    assert_eq!(summary.skipped_invalid_count, 1);
    assert!(submitter.submissions.lock().unwrap().is_empty());

    let accepted = Arc::new(Generator {
        confidence: 0.9,
        calls: Mutex::new(0),
    });
    let dry_run =
        run_floor_proposal_dry_run(1, vec![request("floor-dry", true)], vec![], 0.85, accepted)
            .await
            .unwrap();
    assert_eq!(dry_run.accepted_count, 1);
    assert!(!dry_run.results[0].commit_allowed);
    assert_eq!(dry_run.results[0].metadata["prompt_id"], "prompt-1");
}

#[tokio::test]
async fn unit_use_case_isolates_manual_review_requests_without_model_calls() {
    let generator = Arc::new(Generator {
        confidence: 0.9,
        calls: Mutex::new(0),
    });
    let summary = run_unit_proposal_dry_run(
        1,
        vec![request("unit-manual", false)],
        vec![],
        0.85,
        generator.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.skipped_manual_review_count, 1);
    assert_eq!(summary.model_request_count, 0);
    assert_eq!(*generator.calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn unit_use_case_validates_and_submits_review_only_proposals() {
    let generator = Arc::new(Generator {
        confidence: 0.9,
        calls: Mutex::new(0),
    });
    let submitter = Arc::new(Submitter::default());
    let summary = run_unit_proposal(
        1,
        vec![request("unit-1", true)],
        vec![],
        0.85,
        generator,
        submitter.clone(),
    )
    .await
    .unwrap();

    assert_eq!(summary.model_request_count, 1);
    assert_eq!(summary.submitted_count, 1);
    let submission = submitter.submissions.lock().unwrap().remove(0);
    assert!(!submission.commit_allowed);
    assert!(submission.requires_human_review);
    assert_eq!(submission.submission_metadata["prompt_version"], "v1");
}

fn request(raw_record_id: &str, ai_required: bool) -> NormalizationRequest {
    let trace_context = TraceContext {
        trace_id: format!("trace-{raw_record_id}"),
        tenant_id: "tenant-1".to_string(),
        human_user_id: "user-1".to_string(),
        product_id: "foundation-platform".to_string(),
    };
    NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "test".to_string(),
        raw_record_id: raw_record_id.to_string(),
        raw_record: serde_json::json!({
            "second_pass_decision": { "ai_required": ai_required }
        }),
        trace_context,
        target_schema: serde_json::json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        }),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "test".to_string(),
        target_identity: serde_json::json!({"id": raw_record_id}),
        dictionaries: BTreeMap::new(),
    }
}
