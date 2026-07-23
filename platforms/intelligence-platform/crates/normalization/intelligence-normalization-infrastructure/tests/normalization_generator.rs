// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    ModelGateway, ModelGatewayError, ModelGatewayRequest, ModelGatewayResponse,
    ModelReasoningEffort, NormalizationProposalGenerator,
};
use intelligence_normalization_domain::NormalizationRequest;
use intelligence_normalization_infrastructure::normalization_generator::{
    ModelBackedNormalizationProposalGenerator, NormalizationGeneratorConfig,
};
use serde_json::json;

#[tokio::test]
async fn builds_policy_json_prompt_and_parses_normalization_proposal() {
    let gateway = Arc::new(FakeGateway::new(
        r#"{
            "proposed_record": {"normalized_name": "Acme"},
            "confidence": 0.92,
            "reasons": ["source name field maps to normalized_name"]
        }"#,
    ));
    let generator = ModelBackedNormalizationProposalGenerator::new(
        gateway.clone(),
        NormalizationGeneratorConfig {
            profile_id: "normalization-ko".to_string(),
            model_id: Some("gemma-ko".to_string()),
            prompt_id: "normalization-proposal-v1".to_string(),
            prompt_version: "v1".to_string(),
            temperature: 0.1,
            max_output_tokens: 512,
            reasoning_effort: Some(ModelReasoningEffort::None),
        },
    );

    let proposal = generator.propose(&request()).await.unwrap();

    assert_eq!(proposal.raw_record_id, "raw-1");
    assert_eq!(proposal.proposed_record["normalized_name"], "Acme");
    assert_eq!(proposal.confidence, 0.92);
    assert_eq!(proposal.schema_version, "v1");
    assert_eq!(
        proposal.model_profile_id,
        Some("normalization-ko".to_string())
    );
    assert_eq!(proposal.model_id, Some("gemma-ko".to_string()));
    assert_eq!(
        proposal.prompt_id,
        Some("normalization-proposal-v1".to_string())
    );

    let sent = gateway.last_request();
    assert_eq!(sent.profile_id, "normalization-ko");
    assert_eq!(sent.model_id, Some("gemma-ko".to_string()));
    assert!(sent.messages[0]
        .content
        .contains("AI is a proposer, not the decision-maker"));
    assert!(sent.messages[0]
        .content
        .contains("Output only a JSON object"));
    assert!(
        !sent.messages[0].content.contains('?'),
        "system prompt must not contain mojibake/question-mark replacement artifacts: {}",
        sent.messages[0].content
    );
    assert!(sent.messages[0].content.contains("JSON"));
    assert!(sent.messages[1].content.contains("foundation-platform-r2"));
    assert_eq!(sent.response_format.unwrap()["type"], "json_object");
    assert_eq!(sent.reasoning_effort, Some(ModelReasoningEffort::None));
}

#[tokio::test]
async fn maps_invalid_model_json_to_generator_error() {
    let gateway = Arc::new(FakeGateway::new("not json"));
    let generator = ModelBackedNormalizationProposalGenerator::new(
        gateway,
        NormalizationGeneratorConfig::default_for_profile("normalization-ko"),
    );

    let error = generator.propose(&request()).await.unwrap_err();

    assert_eq!(
        error.safe_message(),
        "normalization proposal response was invalid"
    );
}

#[tokio::test]
async fn building_register_floor_prompt_uses_entity_context_and_korean_review_reasons() {
    let gateway = Arc::new(FakeGateway::new(
        r#"{
            "proposed_record": {
                "floor_kind": "basement",
                "floor_number": 1,
                "floor_index": -1,
                "floor_display_ko": "지하 1층"
            },
            "confidence": 0.92,
            "reasons": ["층구분명과 층번호가 지하 1층을 가리켜요."]
        }"#,
    ));
    let generator = ModelBackedNormalizationProposalGenerator::new(
        gateway.clone(),
        NormalizationGeneratorConfig::default_for_profile("normalization-ko"),
    );

    generator
        .propose(&building_register_floor_request())
        .await
        .unwrap();

    let sent = gateway.last_request();
    assert!(sent.messages[0].content.contains("ko-KR"));
    assert!(sent.messages[1]
        .content
        .contains("same_building_floor_sequence"));
    assert!(sent.messages[1]
        .content
        .contains("For building_register_floor"));
    assert!(sent.messages[1].content.contains("floor_type_name_raw"));
    assert!(sent.messages[1].content.contains("reasons must be Korean"));
}

#[tokio::test]
async fn building_register_unit_prompt_uses_entity_context_and_second_pass_decision() {
    let gateway = Arc::new(FakeGateway::new(
        r#"{
            "proposed_record": {
                "unit_number": 301,
                "building_mgm_bldrgst_pk": "building-pk-1",
                "building_link_method": "canonical_dong",
                "normalization_status": "accepted",
                "normalization_reason": "numeric_unit_name_with_context"
            },
            "confidence": 0.92,
            "reasons": ["같은 범위의 호실 순번과 동/층 맥락을 근거로 판단했습니다."]
        }"#,
    ));
    let generator = ModelBackedNormalizationProposalGenerator::new(
        gateway.clone(),
        NormalizationGeneratorConfig::default_for_profile("normalization-ko"),
    );

    generator
        .propose(&building_register_unit_request())
        .await
        .unwrap();

    let sent = gateway.last_request();
    assert!(sent.messages[1]
        .content
        .contains("For building_register_unit"));
    assert!(sent.messages[1].content.contains("entity_context"));
    assert!(sent.messages[1].content.contains("second_pass_decision"));
    assert!(sent.messages[1].content.contains("unit_identity_candidate"));
    assert!(sent.messages[1].content.contains("reasons must be Korean"));
}

#[tokio::test]
async fn accepts_json_wrapped_in_markdown_code_fence() {
    let gateway = Arc::new(FakeGateway::new(
        r#"```json
        {
            "proposed_record": {"normalized_name": "Acme"},
            "confidence": 0.91,
            "reasons": ["stripped JSON code fence before parsing"]
        }
        ```"#,
    ));
    let generator = ModelBackedNormalizationProposalGenerator::new(
        gateway,
        NormalizationGeneratorConfig::default_for_profile("normalization-ko"),
    );

    let proposal = generator.propose(&request()).await.unwrap();

    assert_eq!(proposal.proposed_record["normalized_name"], "Acme");
    assert_eq!(proposal.confidence, 0.91);
}

struct FakeGateway {
    content: String,
    last_request: Mutex<Option<ModelGatewayRequest>>,
}

impl FakeGateway {
    fn new(content: &str) -> Self {
        Self {
            content: content.to_string(),
            last_request: Mutex::new(None),
        }
    }

    fn last_request(&self) -> ModelGatewayRequest {
        self.last_request.lock().unwrap().clone().unwrap()
    }
}

#[async_trait]
impl ModelGateway for FakeGateway {
    async fn chat(
        &self,
        request: ModelGatewayRequest,
    ) -> Result<ModelGatewayResponse, ModelGatewayError> {
        *self.last_request.lock().unwrap() = Some(request);
        Ok(ModelGatewayResponse {
            content: self.content.clone(),
            model_id: "gemma-ko".to_string(),
            usage: None,
            metadata: Default::default(),
        })
    }
}

fn request() -> NormalizationRequest {
    NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "foundation-platform-r2".to_string(),
        raw_record_id: "raw-1".to_string(),
        raw_record: json!({"name": "Acme"}),
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            human_user_id: "user-1".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: json!({"required": ["normalized_name"]}),
        target_schema_version: "v1".to_string(),
        raw_object_key: None,
        raw_checksum_sha256: None,
        target_kind: "industrial_complex".to_string(),
        target_identity: json!({"industrial_complex_id": "complex-1"}),
        dictionaries: Default::default(),
    }
}

fn building_register_floor_request() -> NormalizationRequest {
    NormalizationRequest {
        tenant_id: "foundation-platform".to_string(),
        source_system: "foundation-platform.silver.building_register_floors".to_string(),
        raw_record_id: "line-43".to_string(),
        raw_record: json!({
            "target_raw_floor": {
                "floor_type_code_raw": "10",
                "floor_type_name_raw": "지하",
                "floor_number_raw": "1",
                "floor_label_raw": "지1층"
            },
            "current_deterministic_normalization": {
                "status": "proposal_required",
                "reason": "label_kind_mismatch"
            },
            "same_building_floor_sequence": [
                {
                    "floor_type_name_raw": "지상",
                    "floor_number_raw": "1",
                    "floor_display_ko": "지상 1층"
                },
                {
                    "floor_type_name_raw": "지하",
                    "floor_number_raw": "1",
                    "floor_display_ko": null
                }
            ],
            "allowed_output_contract": {
                "required_locale": "ko-KR"
            }
        }),
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "foundation-platform".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: json!({
            "required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"]
        }),
        target_schema_version: "building_register_floor.normalized.v1".to_string(),
        raw_object_key: Some(
            "bronze/source=datagokr__building_register_floor/page-000001.json".to_string(),
        ),
        raw_checksum_sha256: Some(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ),
        target_kind: "building_register_floor".to_string(),
        target_identity: json!({"mgm_bldrgst_pk": "11680-raw-1"}),
        dictionaries: Default::default(),
    }
}

fn building_register_unit_request() -> NormalizationRequest {
    NormalizationRequest {
        tenant_id: "foundation-platform".to_string(),
        source_system: "foundation-platform.silver.building_register_units".to_string(),
        raw_record_id: "building-register-unit:line-101".to_string(),
        raw_record: json!({
            "unit_identity_candidate": {
                "unit_name_raw": "301호",
                "floor_index": 3,
                "building_mgm_bldrgst_pk": "building-pk-1",
                "building_link_method": "canonical_dong"
            },
            "current_deterministic_normalization": {
                "status": "proposal_required",
                "reason": "numeric_unit_name_with_context"
            },
            "same_scope_unit_summary": {
                "accepted_unit_count": 2,
                "min_unit_number": 301,
                "max_unit_number": 302
            },
            "entity_context": {
                "entity_context_key": "9999900601100010000|building-pk-1|101동|3",
                "neighbor_unit_examples": [
                    {"unit_name_raw": "301호", "unit_number": 301},
                    {"unit_name_raw": "302호", "unit_number": 302}
                ]
            },
            "second_pass_decision": {
                "status": "ai_required",
                "reason": "numeric_unit_name_with_context",
                "ai_required": true
            },
            "allowed_output_contract": {
                "required_locale": "ko-KR"
            }
        }),
        trace_context: TraceContext {
            trace_id: "trace-1".to_string(),
            tenant_id: "foundation-platform".to_string(),
            human_user_id: "service:intelligence-platform".to_string(),
            product_id: "foundation-platform".to_string(),
        },
        target_schema: json!({
            "required": [
                "unit_number",
                "building_mgm_bldrgst_pk",
                "building_link_method",
                "normalization_status",
                "normalization_reason"
            ]
        }),
        target_schema_version: "building_register_unit.normalized.v1".to_string(),
        raw_object_key: Some(
            "bronze/source=hubgokr__building_register_exclusive_unit/OPN.zip".to_string(),
        ),
        raw_checksum_sha256: Some(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ),
        target_kind: "building_register_unit".to_string(),
        target_identity: json!({"silver_row_id": "building-register-unit:line-101"}),
        dictionaries: Default::default(),
    }
}
