use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use intelligence_normalization_application::{
    run_unit_proposal, run_unit_proposal_dry_run, FoundationNormalizationSubmitter,
    NormalizationProposalGenerator,
};
pub use intelligence_normalization_application::{
    BuildingRegisterUnitInputErrorSummary, BuildingRegisterUnitProposalDryRunSummary,
    BuildingRegisterUnitProposalJobSummary,
};
use intelligence_normalization_domain::{
    building_register_unit_requests_from_jsonl, BuildingRegisterUnitProposalInputContext,
    NormalizationRequest,
};

#[derive(Clone, Debug, PartialEq)]
pub struct BuildingRegisterUnitProposalJobConfig {
    pub input_path: PathBuf,
    pub tenant_id: String,
    pub trace_id: String,
    pub human_user_id: String,
    pub product_id: String,
    pub minimum_confidence: f64,
}

struct BuildingRegisterUnitRequestBatch {
    input_row_count: usize,
    requests: Vec<NormalizationRequest>,
    input_errors: Vec<BuildingRegisterUnitInputErrorSummary>,
}

impl BuildingRegisterUnitProposalJobConfig {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let input_path = lookup("BUILDING_REGISTER_UNIT_PROPOSAL_INPUT_PATH")
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "BUILDING_REGISTER_UNIT_PROPOSAL_INPUT_PATH is required".to_string())?;
        let trace_id = lookup("NORMALIZATION_TRACE_ID")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "building-register-unit-normalization-{}",
                    Utc::now().timestamp_millis()
                )
            });
        let minimum_confidence = lookup("NORMALIZATION_MINIMUM_CONFIDENCE")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0.85);

        Ok(Self {
            input_path: PathBuf::from(input_path),
            tenant_id: lookup("NORMALIZATION_TENANT_ID")
                .unwrap_or_else(|| "foundation-platform".to_string()),
            trace_id,
            human_user_id: lookup("NORMALIZATION_HUMAN_USER_ID")
                .unwrap_or_else(|| "service:intelligence-platform".to_string()),
            product_id: lookup("NORMALIZATION_PRODUCT_ID")
                .unwrap_or_else(|| "foundation-platform".to_string()),
            minimum_confidence,
        })
    }
}

pub fn building_register_unit_proposal_dry_run_enabled_from_env() -> Result<bool, String> {
    building_register_unit_proposal_dry_run_enabled_from_lookup(|key| std::env::var(key).ok())
}

fn building_register_unit_proposal_dry_run_enabled_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<bool, String> {
    let Some(value) = lookup("BUILDING_REGISTER_UNIT_PROPOSAL_DRY_RUN") else {
        return Ok(false);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" => Ok(false),
        "1" | "true" | "yes" => Ok(true),
        _ => Err(
            "BUILDING_REGISTER_UNIT_PROPOSAL_DRY_RUN must be one of 1, true, yes, 0, false, no"
                .to_string(),
        ),
    }
}

pub async fn run_building_register_unit_proposal_job<G, S>(
    config: BuildingRegisterUnitProposalJobConfig,
    generator: Arc<G>,
    submitter: Arc<S>,
) -> Result<BuildingRegisterUnitProposalJobSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
    S: FoundationNormalizationSubmitter + ?Sized,
{
    let batch = requests_from_config(&config)?;
    run_unit_proposal(
        batch.input_row_count,
        batch.requests,
        batch.input_errors,
        config.minimum_confidence,
        generator,
        submitter,
    )
    .await
}

pub async fn run_building_register_unit_proposal_dry_run<G>(
    config: BuildingRegisterUnitProposalJobConfig,
    generator: Arc<G>,
) -> Result<BuildingRegisterUnitProposalDryRunSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
{
    let batch = requests_from_config(&config)?;
    run_unit_proposal_dry_run(
        batch.input_row_count,
        batch.requests,
        batch.input_errors,
        config.minimum_confidence,
        generator,
    )
    .await
}

fn requests_from_config(
    config: &BuildingRegisterUnitProposalJobConfig,
) -> Result<BuildingRegisterUnitRequestBatch, Box<dyn Error + Send + Sync>> {
    let jsonl = std::fs::read_to_string(&config.input_path)?;
    let context = BuildingRegisterUnitProposalInputContext {
        tenant_id: config.tenant_id.clone(),
        trace_id: config.trace_id.clone(),
        human_user_id: config.human_user_id.clone(),
        product_id: config.product_id.clone(),
    };
    let mut input_row_count = 0;
    let mut requests = Vec::new();
    let mut input_errors = Vec::new();

    for (index, line) in jsonl.lines().enumerate() {
        let line_number = index + 1;
        let row = line.trim().trim_start_matches('\u{feff}');
        if row.is_empty() {
            continue;
        }
        input_row_count += 1;
        match building_register_unit_requests_from_jsonl(row, &context) {
            Ok(mut parsed) => requests.append(&mut parsed),
            Err(error) => input_errors.push(BuildingRegisterUnitInputErrorSummary {
                line: line_number,
                message: error.to_string(),
            }),
        }
    }

    Ok(BuildingRegisterUnitRequestBatch {
        input_row_count,
        requests,
        input_errors,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{
        building_register_unit_proposal_dry_run_enabled_from_lookup,
        BuildingRegisterUnitProposalJobConfig,
    };

    #[test]
    fn config_from_lookup_requires_input_path() {
        let error = BuildingRegisterUnitProposalJobConfig::from_lookup(|_| None).unwrap_err();
        assert!(error.contains("BUILDING_REGISTER_UNIT_PROPOSAL_INPUT_PATH"));
    }

    #[test]
    fn config_from_lookup_defaults_to_foundation_platform_identity() {
        let values = BTreeMap::from([(
            "BUILDING_REGISTER_UNIT_PROPOSAL_INPUT_PATH",
            "target/unit-input.jsonl",
        )]);
        let config = BuildingRegisterUnitProposalJobConfig::from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap();
        assert_eq!(config.input_path, PathBuf::from("target/unit-input.jsonl"));
        assert_eq!(config.tenant_id, "foundation-platform");
        assert_eq!(config.product_id, "foundation-platform");
        assert_eq!(config.human_user_id, "service:intelligence-platform");
    }

    #[test]
    fn dry_run_flag_from_lookup_accepts_explicit_boolean_values() {
        assert!(
            building_register_unit_proposal_dry_run_enabled_from_lookup(|key| {
                (key == "BUILDING_REGISTER_UNIT_PROPOSAL_DRY_RUN").then(|| "1".to_string())
            })
            .unwrap()
        );
        assert!(
            !building_register_unit_proposal_dry_run_enabled_from_lookup(|key| {
                (key == "BUILDING_REGISTER_UNIT_PROPOSAL_DRY_RUN").then(|| "false".to_string())
            })
            .unwrap()
        );
        assert!(!building_register_unit_proposal_dry_run_enabled_from_lookup(|_| None).unwrap());
    }
}
