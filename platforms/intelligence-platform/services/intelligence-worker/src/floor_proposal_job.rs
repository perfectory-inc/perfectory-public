use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use intelligence_normalization_application::{
    run_floor_proposal, run_floor_proposal_dry_run, FoundationNormalizationSubmitter,
    FoundationSubmissionError, ModelGateway, ModelReasoningEffort, NormalizationProposalGenerator,
};
pub use intelligence_normalization_application::{
    BuildingRegisterFloorInputErrorSummary, BuildingRegisterFloorProposalDryRunSummary,
    BuildingRegisterFloorProposalJobSummary,
};
use intelligence_normalization_domain::{
    building_register_floor_requests_from_jsonl, BuildingRegisterFloorProposalInputContext,
    NormalizationRequest,
};
use intelligence_normalization_infrastructure::{
    ModelBackedNormalizationProposalGenerator, NormalizationGeneratorConfig,
    OllamaNativeModelGateway, OllamaNativeModelGatewayConfig, OpenAiCompatibleModelGateway,
    OpenAiCompatibleModelGatewayConfig,
};

#[derive(Clone, Debug, PartialEq)]
pub struct BuildingRegisterFloorProposalJobConfig {
    pub input_path: PathBuf,
    pub tenant_id: String,
    pub trace_id: String,
    pub human_user_id: String,
    pub product_id: String,
    pub minimum_confidence: f64,
}

struct BuildingRegisterFloorRequestBatch {
    input_row_count: usize,
    requests: Vec<NormalizationRequest>,
    input_errors: Vec<BuildingRegisterFloorInputErrorSummary>,
}

impl BuildingRegisterFloorProposalJobConfig {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let input_path = lookup("BUILDING_REGISTER_FLOOR_PROPOSAL_INPUT_PATH")
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "BUILDING_REGISTER_FLOOR_PROPOSAL_INPUT_PATH is required".to_string())?;
        let trace_id = lookup("NORMALIZATION_TRACE_ID")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "building-register-floor-normalization-{}",
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

pub fn proposal_generator_from_env(
) -> Result<Option<Arc<dyn NormalizationProposalGenerator>>, FoundationSubmissionError> {
    let Some(config) = model_runtime_config_from_lookup(|key| std::env::var(key).ok())? else {
        return Ok(None);
    };

    let gateway: Arc<dyn ModelGateway> = if config.chat_path.trim() == "/api/chat" {
        Arc::new(
            OllamaNativeModelGateway::new(OllamaNativeModelGatewayConfig {
                base_url: config.base_url,
                chat_path: config.chat_path,
                api_key: config.api_key,
                default_model: config.default_model.clone(),
                timeout_seconds: config.timeout_seconds,
            })
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?,
        )
    } else {
        Arc::new(
            OpenAiCompatibleModelGateway::new(OpenAiCompatibleModelGatewayConfig {
                base_url: config.base_url,
                chat_path: config.chat_path,
                api_key: config.api_key,
                default_model: config.default_model.clone(),
                timeout_seconds: config.timeout_seconds,
            })
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })?,
        )
    };

    Ok(Some(Arc::new(
        ModelBackedNormalizationProposalGenerator::new_dyn(
            gateway,
            NormalizationGeneratorConfig {
                profile_id: config.profile_id,
                model_id: Some(config.default_model),
                prompt_id: "normalization-proposal-v1".to_string(),
                prompt_version: "v1".to_string(),
                temperature: 0.1,
                max_output_tokens: 1024,
                reasoning_effort: config.reasoning_effort,
            },
        ),
    )))
}

struct ModelRuntimeEnvConfig {
    base_url: String,
    chat_path: String,
    api_key: Option<String>,
    default_model: String,
    profile_id: String,
    timeout_seconds: u64,
    reasoning_effort: Option<ModelReasoningEffort>,
}

fn model_runtime_config_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<ModelRuntimeEnvConfig>, FoundationSubmissionError> {
    let Some(base_url) = env_value(&lookup, "MODEL_RUNTIME_BASE_URL", "MODEL_GATEWAY_BASE_URL")
    else {
        return Ok(None);
    };
    let Some(default_model) = env_value(
        &lookup,
        "MODEL_RUNTIME_DEFAULT_MODEL",
        "MODEL_GATEWAY_DEFAULT_MODEL",
    ) else {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "MODEL_RUNTIME_DEFAULT_MODEL is required when MODEL_RUNTIME_BASE_URL is set"
                .to_string(),
        });
    };
    let chat_path = env_value(
        &lookup,
        "MODEL_RUNTIME_CHAT_PATH",
        "MODEL_GATEWAY_CHAT_PATH",
    )
    .unwrap_or_else(|| "/v1/chat/completions".to_string());
    let api_key = env_value(&lookup, "MODEL_RUNTIME_API_KEY", "MODEL_GATEWAY_API_KEY")
        .filter(|value| !value.is_empty());
    let timeout_seconds = env_value(
        &lookup,
        "MODEL_RUNTIME_TIMEOUT_SECONDS",
        "MODEL_GATEWAY_TIMEOUT_SECONDS",
    )
    .and_then(|value| value.parse().ok())
    .unwrap_or(30);
    let profile_id = env_value(
        &lookup,
        "MODEL_RUNTIME_PROFILE_ID",
        "MODEL_GATEWAY_PROFILE_ID",
    )
    .unwrap_or_else(|| "normalization-default".to_string());
    let reasoning_effort = env_value(
        &lookup,
        "MODEL_RUNTIME_REASONING_EFFORT",
        "MODEL_GATEWAY_REASONING_EFFORT",
    )
    .map(|value| parse_reasoning_effort(&value))
    .transpose()?;

    Ok(Some(ModelRuntimeEnvConfig {
        base_url,
        chat_path,
        api_key,
        default_model,
        profile_id,
        timeout_seconds,
        reasoning_effort,
    }))
}

fn parse_reasoning_effort(value: &str) -> Result<ModelReasoningEffort, FoundationSubmissionError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(ModelReasoningEffort::None),
        "low" => Ok(ModelReasoningEffort::Low),
        "medium" => Ok(ModelReasoningEffort::Medium),
        "high" => Ok(ModelReasoningEffort::High),
        _ => Err(FoundationSubmissionError::InvalidResponse {
            message: "MODEL_RUNTIME_REASONING_EFFORT must be one of none, low, medium, high"
                .to_string(),
        }),
    }
}

fn env_value(
    lookup: &impl Fn(&str) -> Option<String>,
    primary_key: &str,
    fallback_key: &str,
) -> Option<String> {
    lookup(primary_key).or_else(|| lookup(fallback_key))
}

pub fn building_register_floor_proposal_dry_run_enabled_from_env() -> Result<bool, String> {
    building_register_floor_proposal_dry_run_enabled_from_lookup(|key| std::env::var(key).ok())
}

fn building_register_floor_proposal_dry_run_enabled_from_lookup(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<bool, String> {
    let Some(value) = lookup("BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN") else {
        return Ok(false);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" => Ok(false),
        "1" | "true" | "yes" => Ok(true),
        _ => Err(
            "BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN must be one of 1, true, yes, 0, false, no"
                .to_string(),
        ),
    }
}

pub async fn run_building_register_floor_proposal_job<G, S>(
    config: BuildingRegisterFloorProposalJobConfig,
    generator: Arc<G>,
    submitter: Arc<S>,
) -> Result<BuildingRegisterFloorProposalJobSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
    S: FoundationNormalizationSubmitter + ?Sized,
{
    let batch = requests_from_config(&config)?;
    run_floor_proposal(
        batch.input_row_count,
        batch.requests,
        batch.input_errors,
        config.minimum_confidence,
        generator,
        submitter,
    )
    .await
}

pub async fn run_building_register_floor_proposal_dry_run<G>(
    config: BuildingRegisterFloorProposalJobConfig,
    generator: Arc<G>,
) -> Result<BuildingRegisterFloorProposalDryRunSummary, Box<dyn Error + Send + Sync>>
where
    G: NormalizationProposalGenerator + ?Sized,
{
    let batch = requests_from_config(&config)?;
    run_floor_proposal_dry_run(
        batch.input_row_count,
        batch.requests,
        batch.input_errors,
        config.minimum_confidence,
        generator,
    )
    .await
}

fn requests_from_config(
    config: &BuildingRegisterFloorProposalJobConfig,
) -> Result<BuildingRegisterFloorRequestBatch, Box<dyn Error + Send + Sync>> {
    let jsonl = std::fs::read_to_string(&config.input_path)?;
    let context = BuildingRegisterFloorProposalInputContext {
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
        match building_register_floor_requests_from_jsonl(row, &context) {
            Ok(mut parsed) => requests.append(&mut parsed),
            Err(error) => input_errors.push(BuildingRegisterFloorInputErrorSummary {
                line: line_number,
                message: error.to_string(),
            }),
        }
    }

    Ok(BuildingRegisterFloorRequestBatch {
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
        building_register_floor_proposal_dry_run_enabled_from_lookup,
        BuildingRegisterFloorProposalJobConfig,
    };

    #[test]
    fn config_from_lookup_requires_input_path() {
        let error = BuildingRegisterFloorProposalJobConfig::from_lookup(|_| None).unwrap_err();
        assert!(error.contains("BUILDING_REGISTER_FLOOR_PROPOSAL_INPUT_PATH"));
    }

    #[test]
    fn config_from_lookup_reads_explicit_job_settings() {
        let values = BTreeMap::from([
            (
                "BUILDING_REGISTER_FLOOR_PROPOSAL_INPUT_PATH",
                "target/floor-input.jsonl",
            ),
            ("NORMALIZATION_TRACE_ID", "trace-1"),
            ("NORMALIZATION_TENANT_ID", "tenant-1"),
            ("NORMALIZATION_HUMAN_USER_ID", "staff-1"),
            ("NORMALIZATION_PRODUCT_ID", "foundation-platform"),
            ("NORMALIZATION_MINIMUM_CONFIDENCE", "0.91"),
        ]);
        let config = BuildingRegisterFloorProposalJobConfig::from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap();
        assert_eq!(config.input_path, PathBuf::from("target/floor-input.jsonl"));
        assert_eq!(config.trace_id, "trace-1");
        assert_eq!(config.tenant_id, "tenant-1");
        assert_eq!(config.human_user_id, "staff-1");
        assert_eq!(config.product_id, "foundation-platform");
        assert_eq!(config.minimum_confidence, 0.91);
    }

    #[test]
    fn config_from_lookup_defaults_to_foundation_platform_identity() {
        let values = BTreeMap::from([(
            "BUILDING_REGISTER_FLOOR_PROPOSAL_INPUT_PATH",
            "target/floor-input.jsonl",
        )]);
        let config = BuildingRegisterFloorProposalJobConfig::from_lookup(|key| {
            values.get(key).map(|value| value.to_string())
        })
        .unwrap();
        assert_eq!(config.tenant_id, "foundation-platform");
        assert_eq!(config.product_id, "foundation-platform");
    }

    #[test]
    fn dry_run_flag_from_lookup_accepts_explicit_boolean_values() {
        assert!(
            building_register_floor_proposal_dry_run_enabled_from_lookup(|key| {
                (key == "BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN").then(|| "1".to_string())
            })
            .unwrap()
        );
        assert!(
            !building_register_floor_proposal_dry_run_enabled_from_lookup(|key| {
                (key == "BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN").then(|| "false".to_string())
            })
            .unwrap()
        );
        assert!(!building_register_floor_proposal_dry_run_enabled_from_lookup(|_| None).unwrap());
    }

    #[test]
    fn dry_run_flag_from_lookup_rejects_ambiguous_values() {
        let error = building_register_floor_proposal_dry_run_enabled_from_lookup(|key| {
            (key == "BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN").then(|| "maybe".to_string())
        })
        .unwrap_err();
        assert!(error.contains("BUILDING_REGISTER_FLOOR_PROPOSAL_DRY_RUN"));
    }
}
