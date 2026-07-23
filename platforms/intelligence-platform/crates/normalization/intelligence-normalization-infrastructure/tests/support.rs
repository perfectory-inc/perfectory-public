use std::collections::BTreeMap;

use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationSubmissionStatus, NormalizationProposalSubmission,
};
use intelligence_normalization_domain::{
    NormalizationProposal, NormalizationRequest, NormalizationValidationResult,
};
use serde_json::json;

pub fn submission() -> NormalizationProposalSubmission {
    let trace_context = TraceContext {
        trace_id: "trace-1".to_string(),
        tenant_id: "tenant-1".to_string(),
        human_user_id: "user-1".to_string(),
        product_id: "foundation-platform".to_string(),
    };
    let request = NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "building-register".to_string(),
        raw_record_id: "raw-1".to_string(),
        raw_record: json!({"floor": "B1"}),
        trace_context: trace_context.clone(),
        target_schema: json!({"required": ["floor_index"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "building_register_floor".to_string(),
        target_identity: json!({"source_record_id": "raw-1"}),
        dictionaries: BTreeMap::new(),
    };
    NormalizationProposalSubmission {
        request,
        proposal: NormalizationProposal {
            raw_record_id: "raw-1".to_string(),
            schema_version: "v1".to_string(),
            proposed_record: json!({"floor_index": -1}),
            confidence: 0.91,
            reasons: vec!["normalized".to_string()],
            policy_id: "policy".to_string(),
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
        trace_context,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::from([(
            "status".to_string(),
            format!("{:?}", FoundationSubmissionStatus::Queued),
        )]),
    }
}
