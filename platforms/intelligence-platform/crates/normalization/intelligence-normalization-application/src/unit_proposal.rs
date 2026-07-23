use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use intelligence_normalization_domain::normalization::{
    validate_normalization_proposal_with_minimum_confidence, NormalizationProposal,
    NormalizationRequest,
};
use serde::Serialize;

use crate::{
    FoundationNormalizationSubmitter, NormalizationProposalGenerator,
    NormalizationProposalSubmission, NormalizationRunResult,
};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BuildingRegisterUnitInputErrorSummary {
    pub line: usize,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BuildingRegisterUnitProposalJobSummary {
    pub input_row_count: usize,
    pub request_count: usize,
    pub model_request_count: usize,
    pub submitted_count: usize,
    pub skipped_invalid_count: usize,
    pub skipped_manual_review_count: usize,
    pub invalid_input_count: usize,
    pub input_errors: Vec<BuildingRegisterUnitInputErrorSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct BuildingRegisterUnitProposalDryRunSummary {
    pub input_row_count: usize,
    pub request_count: usize,
    pub model_request_count: usize,
    pub skipped_manual_review_count: usize,
    pub invalid_input_count: usize,
    pub accepted_count: usize,
    pub rejected_count: usize,
    pub input_errors: Vec<BuildingRegisterUnitInputErrorSummary>,
    pub results: Vec<NormalizationRunResult>,
}

pub async fn run_unit_proposal<G, S>(
    input_row_count: usize,
    requests: Vec<NormalizationRequest>,
    input_errors: Vec<BuildingRegisterUnitInputErrorSummary>,
    minimum_confidence: f64,
    generator: Arc<G>,
    submitter: Arc<S>,
) -> Result<BuildingRegisterUnitProposalJobSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
    S: FoundationNormalizationSubmitter + ?Sized,
{
    let mut summary = BuildingRegisterUnitProposalJobSummary {
        input_row_count,
        request_count: requests.len(),
        invalid_input_count: input_errors.len(),
        input_errors,
        ..Default::default()
    };

    for request in requests {
        if !unit_ai_required(&request) {
            summary.skipped_manual_review_count += 1;
            continue;
        }

        summary.model_request_count += 1;
        let proposal = generator.propose(&request).await?;
        let validation = validate_normalization_proposal_with_minimum_confidence(
            &request,
            &proposal,
            minimum_confidence,
        );
        if !validation.accepted {
            summary.skipped_invalid_count += 1;
            continue;
        }

        let submission = NormalizationProposalSubmission {
            request: request.clone(),
            proposal: proposal.clone(),
            validation,
            trace_context: request.trace_context.clone(),
            commit_allowed: false,
            requires_human_review: true,
            submission_metadata: submission_metadata(&proposal),
        };
        submitter.submit(&submission).await?;
        summary.submitted_count += 1;
    }

    Ok(summary)
}

pub async fn run_unit_proposal_dry_run<G>(
    input_row_count: usize,
    requests: Vec<NormalizationRequest>,
    input_errors: Vec<BuildingRegisterUnitInputErrorSummary>,
    minimum_confidence: f64,
    generator: Arc<G>,
) -> Result<BuildingRegisterUnitProposalDryRunSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
{
    let mut summary = BuildingRegisterUnitProposalDryRunSummary {
        input_row_count,
        request_count: requests.len(),
        invalid_input_count: input_errors.len(),
        input_errors,
        ..Default::default()
    };

    for request in requests {
        if !unit_ai_required(&request) {
            summary.skipped_manual_review_count += 1;
            continue;
        }

        summary.model_request_count += 1;
        let proposal = generator.propose(&request).await?;
        let validation = validate_normalization_proposal_with_minimum_confidence(
            &request,
            &proposal,
            minimum_confidence,
        );
        if validation.accepted {
            summary.accepted_count += 1;
        } else {
            summary.rejected_count += 1;
        }
        summary.results.push(NormalizationRunResult {
            proposal: proposal.clone(),
            validation,
            commit_allowed: false,
            requires_human_review: true,
            metadata: generation_metadata(&request, &proposal),
        });
    }

    Ok(summary)
}

fn unit_ai_required(request: &NormalizationRequest) -> bool {
    request
        .raw_record
        .pointer("/second_pass_decision/ai_required")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn submission_metadata(proposal: &NormalizationProposal) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::from([
        ("policy_id".to_string(), proposal.policy_id.clone()),
        (
            "policy_version".to_string(),
            proposal.policy_version.clone(),
        ),
    ]);
    insert_optional(
        &mut metadata,
        "model_profile_id",
        proposal.model_profile_id.as_deref(),
    );
    insert_optional(&mut metadata, "model_id", proposal.model_id.as_deref());
    insert_optional(&mut metadata, "prompt_id", proposal.prompt_id.as_deref());
    insert_optional(
        &mut metadata,
        "prompt_version",
        proposal.prompt_version.as_deref(),
    );
    metadata
}

fn generation_metadata(
    request: &NormalizationRequest,
    proposal: &NormalizationProposal,
) -> BTreeMap<String, String> {
    let mut metadata = submission_metadata(proposal);
    metadata.insert("source_system".to_string(), request.source_system.clone());
    metadata.insert("raw_record_id".to_string(), request.raw_record_id.clone());
    metadata.insert(
        "target_schema_version".to_string(),
        request.target_schema_version.clone(),
    );
    metadata
}

fn insert_optional(map: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        map.insert(key.to_string(), value.to_string());
    }
}
