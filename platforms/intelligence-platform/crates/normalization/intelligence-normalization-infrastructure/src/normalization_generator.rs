use std::sync::Arc;

use async_trait::async_trait;
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayMessage, ModelGatewayRequest, ModelMessageRole, ModelReasoningEffort,
    NormalizationProposalError, NormalizationProposalGenerator,
};
use intelligence_normalization_domain::{NormalizationProposal, NormalizationRequest};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq)]
pub struct NormalizationGeneratorConfig {
    pub profile_id: String,
    pub model_id: Option<String>,
    pub prompt_id: String,
    pub prompt_version: String,
    pub temperature: f32,
    pub max_output_tokens: u32,
    pub reasoning_effort: Option<ModelReasoningEffort>,
}

impl NormalizationGeneratorConfig {
    pub fn default_for_profile(profile_id: impl Into<String>) -> Self {
        Self {
            profile_id: profile_id.into(),
            model_id: None,
            prompt_id: "normalization-proposal-v1".to_string(),
            prompt_version: "v1".to_string(),
            temperature: 0.1,
            max_output_tokens: 1024,
            reasoning_effort: None,
        }
    }
}

pub struct ModelBackedNormalizationProposalGenerator {
    gateway: Arc<dyn ModelGateway>,
    config: NormalizationGeneratorConfig,
}

impl ModelBackedNormalizationProposalGenerator {
    pub fn new<T>(gateway: Arc<T>, config: NormalizationGeneratorConfig) -> Self
    where
        T: ModelGateway + 'static,
    {
        Self { gateway, config }
    }

    pub fn new_dyn(gateway: Arc<dyn ModelGateway>, config: NormalizationGeneratorConfig) -> Self {
        Self { gateway, config }
    }
}

#[async_trait]
impl NormalizationProposalGenerator for ModelBackedNormalizationProposalGenerator {
    async fn propose(
        &self,
        request: &NormalizationRequest,
    ) -> Result<NormalizationProposal, NormalizationProposalError> {
        let gateway_request = ModelGatewayRequest {
            profile_id: self.config.profile_id.clone(),
            model_id: self.config.model_id.clone(),
            messages: vec![
                ModelGatewayMessage {
                    role: ModelMessageRole::System,
                    content: system_prompt(),
                },
                ModelGatewayMessage {
                    role: ModelMessageRole::User,
                    content: user_prompt(request),
                },
            ],
            temperature: Some(self.config.temperature),
            max_output_tokens: Some(self.config.max_output_tokens),
            response_format: Some(json!({"type": "json_object"})),
            reasoning_effort: self.config.reasoning_effort.clone(),
            metadata: Default::default(),
        };

        let response = self.gateway.chat(gateway_request).await.map_err(|error| {
            NormalizationProposalError::GenerationFailed {
                message: error.to_string(),
            }
        })?;
        let generated: GeneratedNormalizationProposal =
            serde_json::from_str(json_content(&response.content)).map_err(|error| {
                NormalizationProposalError::InvalidResponse {
                    message: error.to_string(),
                }
            })?;

        Ok(NormalizationProposal {
            raw_record_id: request.raw_record_id.clone(),
            proposed_record: generated.proposed_record,
            confidence: generated.confidence,
            reasons: generated.reasons,
            schema_version: request.target_schema_version.clone(),
            policy_id: "normalization-proposal-policy".to_string(),
            policy_version: "v1".to_string(),
            model_profile_id: Some(self.config.profile_id.clone()),
            model_id: Some(response.model_id),
            prompt_id: Some(self.config.prompt_id.clone()),
            prompt_version: Some(self.config.prompt_version.clone()),
        })
    }
}

#[derive(Debug, Deserialize)]
struct GeneratedNormalizationProposal {
    proposed_record: Value,
    confidence: f64,
    reasons: Vec<String>,
}

fn system_prompt() -> String {
    [
        "You are an enterprise data-normalization proposal assistant.",
        "AI is a proposer, not the decision-maker.",
        "Do not claim canonical truth and do not write canonical data.",
        "Create only a normalization proposal for human/admin review.",
        "Output only a JSON object; do not output markdown, reasoning, or commentary.",
        "If raw_record.allowed_output_contract.required_locale is ko-KR, reasons must be Korean (ko-KR).",
        "Output schema: {\"proposed_record\": object, \"confidence\": number, \"reasons\": string[]}",
    ]
    .join("\n")
}

fn user_prompt(request: &NormalizationRequest) -> String {
    let mut prompt = format!(
        "tenant_id: {}\nsource_system: {}\nraw_record_id: {}\ntarget_kind: {}\ntarget_identity: {}\ntarget_schema_version: {}\ntarget_schema: {}\nraw_record: {}\ndictionaries: {}",
        request.tenant_id,
        request.source_system,
        request.raw_record_id,
        request.target_kind,
        request.target_identity,
        request.target_schema_version,
        request.target_schema,
        request.raw_record,
        serde_json::to_string(&request.dictionaries).unwrap_or_else(|_| "{}".to_string())
    );

    if request.target_kind == "building_register_floor" {
        prompt.push_str("\n\n");
        prompt.push_str(building_register_floor_instruction());
    }
    if request.target_kind == "building_register_unit" {
        prompt.push_str("\n\n");
        prompt.push_str(building_register_unit_instruction());
    }

    prompt
}

fn building_register_floor_instruction() -> &'static str {
    "For building_register_floor, use target_raw_floor, current_deterministic_normalization, semantic_contract, entity_impact, and same_building_floor_sequence together. Do not decide from floor_label_raw alone. If allowed_output_contract.required_locale is ko-KR, reasons must be Korean. If target_raw_floor.floor_type_name_raw is 지하 and target_raw_floor.floor_number_raw is 1, compact labels such as 지1층 should normally propose floor_kind=basement, floor_number=1, floor_index=-1, floor_display_ko=지하 1층 unless entity context contradicts it. If evidence conflicts, keep proposal_required=true with a Korean review reason."
}

fn building_register_unit_instruction() -> &'static str {
    "For building_register_unit, use unit_identity_candidate, current_deterministic_normalization, same_scope_unit_summary, entity_context, and second_pass_decision together. Do not decide from unit_name_raw alone. Only infer unit_number when second_pass_decision.ai_required is true and same-scope examples support the proposal. Preserve building_mgm_bldrgst_pk and building_link_method only when the provided entity context supports them; otherwise return normalization_status=proposal_required with null uncertain fields. If allowed_output_contract.required_locale is ko-KR, reasons must be Korean."
}

fn json_content(content: &str) -> &str {
    let trimmed = content.trim();
    if !trimmed.starts_with("```") {
        return trimmed;
    }

    let without_opening = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"))
        .unwrap_or(trimmed)
        .trim_start();

    without_opening
        .strip_suffix("```")
        .unwrap_or(without_opening)
        .trim()
}
