use std::collections::BTreeMap;
use std::time::Duration;

use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use chrono::Utc;
use intelligence_normalization_application::{
    ModelGatewayMessage, ModelGatewayRequest, ModelMessageRole, ModelReasoningEffort, ModelUsage,
    NormalizationRunResult, NormalizationSubmissionRunResult, NormalizationSubmissionWorkflow,
    SubmitProposalError, SubmitProposalEvent,
};
use intelligence_normalization_domain::{
    korean_answer_system_prompt, korean_repair_instruction, validate_korean_answer,
    validate_normalization_proposal, NormalizationProposal, NormalizationRequest,
    NormalizationValidationResult, DEFAULT_TARGET_LANGUAGE, KOREAN_ANSWER_POLICY_ID,
    KOREAN_ANSWER_POLICY_VERSION, KOREAN_MODEL_PROFILE_ID, KOREAN_OUTPUT_VALIDATOR_ID,
    KOREAN_OUTPUT_VALIDATOR_VERSION, KOREAN_REPAIR_POLICY_ID, KOREAN_REPAIR_POLICY_VERSION,
};
use serde::{Deserialize, Serialize};

use crate::state::AppState;

/// Delivery lease for inline submit path.
///
/// Covers the round-trip to Foundation Platform before the route handler returns.
/// A crash between `enqueue` and `mark_*` leaves the record `InFlight`; the
/// drain worker reclaims it after this lease expires.
///
/// Kept as a constant; revisit if operators need to tune it.
pub const ROUTE_LEASE: Duration = Duration::from_secs(60);
pub const DEFAULT_CHAT_OUTPUT_TOKENS: u32 = 1024;
pub const MAX_CHAT_OUTPUT_TOKENS: u32 = 4096;

fn bounded_output_tokens(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(DEFAULT_CHAT_OUTPUT_TOKENS)
        .min(MAX_CHAT_OUTPUT_TOKENS)
}

/// Two route namespaces coexist per root ADR-0001 §6: the OpenAI-compatible
/// surface (`/v1/models`, `/v1/chat/completions`) keeps its ecosystem-mandated
/// paths (recorded exception), while platform-native routes mount under
/// `/intelligence/v1/...`.
pub(crate) fn protected_router() -> Router<AppState> {
    Router::new()
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route(
            "/intelligence/v1/normalization/validate-proposal",
            post(validate_proposal),
        )
        .route(
            "/intelligence/v1/normalization/generate-and-validate",
            post(generate_and_validate),
        )
        .route(
            "/intelligence/v1/normalization/generate-validate-submit",
            post(generate_validate_submit),
        )
        .route(
            "/intelligence/v1/normalization/submit-proposal",
            post(submit_proposal),
        )
}

#[derive(Debug, Serialize)]
struct ModelListResponse {
    object: &'static str,
    data: Vec<ModelListItem>,
}

#[derive(Debug, Serialize)]
struct ModelListItem {
    id: String,
    object: &'static str,
    created: u32,
    owned_by: &'static str,
}

async fn list_models(State(state): State<AppState>) -> Json<ModelListResponse> {
    Json(ModelListResponse {
        object: "list",
        data: state
            .chat_model_ids
            .iter()
            .map(|model_id| ModelListItem {
                id: model_id.clone(),
                object: "model",
                created: 0,
                owned_by: "intelligence-platform",
            })
            .collect(),
    })
}

#[derive(Debug, Deserialize)]
struct ChatCompletionPayload {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    messages: Vec<ChatMessagePayload>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    reasoning_effort: Option<ModelReasoningEffort>,
    #[serde(default)]
    stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ChatMessagePayload {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    #[serde(rename = "object")]
    object_type: &'static str,
    created: i64,
    model: String,
    choices: Vec<ChatCompletionChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ChatCompletionUsage>,
    metadata: ChatCompletionMetadata,
}

#[derive(Debug, Serialize)]
struct ChatCompletionChoice {
    index: u32,
    message: ChatCompletionMessage,
    finish_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct ChatCompletionMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

#[derive(Debug, Serialize)]
struct ChatCompletionMetadata {
    model_profile_id: &'static str,
    target_language: &'static str,
    language_policy_id: &'static str,
    language_policy_version: &'static str,
    validator_policy_id: &'static str,
    validator_policy_version: &'static str,
    repair_policy_id: &'static str,
    repair_policy_version: &'static str,
    language_policy_passed: bool,
    repair_attempted: bool,
}

async fn chat_completions(
    State(state): State<AppState>,
    Json(payload): Json<ChatCompletionPayload>,
) -> Result<Json<ChatCompletionResponse>, (StatusCode, Json<ApiError>)> {
    let Some(gateway) = state.model_gateway.as_ref().cloned() else {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "chat_model_gateway_not_configured",
            "chat model gateway is not configured",
        ));
    };

    if payload.stream.unwrap_or(false) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "chat_streaming_not_supported",
            "chat streaming is not supported by this gateway yet",
        ));
    }

    let messages = policy_wrapped_messages(payload.messages)?;
    let max_output_tokens = bounded_output_tokens(payload.max_tokens);
    let request = ModelGatewayRequest {
        profile_id: KOREAN_MODEL_PROFILE_ID.to_string(),
        model_id: payload.model.clone(),
        messages,
        temperature: payload.temperature,
        max_output_tokens: Some(max_output_tokens),
        response_format: None,
        reasoning_effort: payload.reasoning_effort.clone(),
        metadata: metadata([
            ("target_language", DEFAULT_TARGET_LANGUAGE),
            ("language_policy_id", KOREAN_ANSWER_POLICY_ID),
            ("language_policy_version", KOREAN_ANSWER_POLICY_VERSION),
        ]),
    };

    let initial_generation = gateway.chat(request).await.map_err(|error| {
        api_error(
            StatusCode::BAD_GATEWAY,
            "chat_generation_failed",
            error.safe_message(),
        )
    })?;
    let initial_validation = validate_korean_answer(&initial_generation.content);

    let mut final_generation = initial_generation;
    let mut final_validation = initial_validation.clone();
    let mut repair_attempted = false;

    if !initial_validation.passed {
        repair_attempted = true;
        let repair_request = ModelGatewayRequest {
            profile_id: KOREAN_MODEL_PROFILE_ID.to_string(),
            model_id: payload.model,
            messages: vec![
                ModelGatewayMessage {
                    role: ModelMessageRole::System,
                    content: korean_answer_system_prompt(),
                },
                ModelGatewayMessage {
                    role: ModelMessageRole::User,
                    content: korean_repair_instruction(&final_generation.content),
                },
            ],
            temperature: Some(0.1),
            max_output_tokens: Some(max_output_tokens),
            response_format: None,
            reasoning_effort: payload.reasoning_effort,
            metadata: metadata([
                ("target_language", DEFAULT_TARGET_LANGUAGE),
                ("language_policy_id", KOREAN_ANSWER_POLICY_ID),
                ("repair_policy_id", KOREAN_REPAIR_POLICY_ID),
            ]),
        };

        let repaired_generation = gateway.chat(repair_request).await.map_err(|error| {
            api_error(
                StatusCode::BAD_GATEWAY,
                "chat_repair_failed",
                error.safe_message(),
            )
        })?;
        final_validation = validate_korean_answer(&repaired_generation.content);
        final_generation = repaired_generation;
    }

    Ok(Json(ChatCompletionResponse {
        id: format!("chatcmpl-{}", Utc::now().timestamp_millis()),
        object_type: "chat.completion",
        created: Utc::now().timestamp(),
        model: final_generation.model_id.clone(),
        choices: vec![ChatCompletionChoice {
            index: 0,
            message: ChatCompletionMessage {
                role: "assistant",
                content: final_generation.content,
            },
            finish_reason: "stop",
        }],
        usage: final_generation.usage.map(Into::into),
        metadata: ChatCompletionMetadata {
            model_profile_id: KOREAN_MODEL_PROFILE_ID,
            target_language: DEFAULT_TARGET_LANGUAGE,
            language_policy_id: KOREAN_ANSWER_POLICY_ID,
            language_policy_version: KOREAN_ANSWER_POLICY_VERSION,
            validator_policy_id: KOREAN_OUTPUT_VALIDATOR_ID,
            validator_policy_version: KOREAN_OUTPUT_VALIDATOR_VERSION,
            repair_policy_id: KOREAN_REPAIR_POLICY_ID,
            repair_policy_version: KOREAN_REPAIR_POLICY_VERSION,
            language_policy_passed: final_validation.passed,
            repair_attempted,
        },
    }))
}

fn policy_wrapped_messages(
    messages: Vec<ChatMessagePayload>,
) -> Result<Vec<ModelGatewayMessage>, (StatusCode, Json<ApiError>)> {
    if messages.is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "chat_messages_required",
            "chat messages are required",
        ));
    }

    let mut gateway_messages = vec![ModelGatewayMessage {
        role: ModelMessageRole::System,
        content: korean_answer_system_prompt(),
    }];

    for message in messages {
        gateway_messages.push(ModelGatewayMessage {
            role: role_from_payload(&message.role)?,
            content: message.content,
        });
    }

    Ok(gateway_messages)
}

fn role_from_payload(role: &str) -> Result<ModelMessageRole, (StatusCode, Json<ApiError>)> {
    match role {
        "system" => Ok(ModelMessageRole::System),
        "user" => Ok(ModelMessageRole::User),
        "assistant" => Ok(ModelMessageRole::Assistant),
        "tool" => Ok(ModelMessageRole::Tool),
        _ => Err(api_error(
            StatusCode::BAD_REQUEST,
            "chat_message_role_unsupported",
            "chat message role is unsupported",
        )),
    }
}

impl From<ModelUsage> for ChatCompletionUsage {
    fn from(value: ModelUsage) -> Self {
        Self {
            prompt_tokens: value.input_tokens,
            completion_tokens: value.output_tokens,
            total_tokens: value.total_tokens,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ValidateProposalPayload {
    request: NormalizationRequest,
    proposal: NormalizationProposal,
}

async fn validate_proposal(
    Json(payload): Json<ValidateProposalPayload>,
) -> Json<NormalizationValidationResult> {
    Json(validate_normalization_proposal(
        &payload.request,
        &payload.proposal,
    ))
}

async fn generate_and_validate(
    State(state): State<AppState>,
    Json(request): Json<NormalizationRequest>,
) -> Result<Json<NormalizationRunResult>, (StatusCode, Json<ApiError>)> {
    let Some(generator) = state.proposal_generator.as_ref() else {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "normalization_generator_not_configured",
            "model generation adapter is not configured",
        ));
    };

    let proposal = generator.propose(&request).await.map_err(|error| {
        api_error(
            StatusCode::BAD_GATEWAY,
            "normalization_generation_failed",
            error.safe_message(),
        )
    })?;
    let validation = validate_normalization_proposal(&request, &proposal);

    Ok(Json(run_result(request, proposal, validation)))
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: &'static str,
    message: &'static str,
}

async fn generate_validate_submit(
    State(state): State<AppState>,
    Json(request): Json<NormalizationRequest>,
) -> Result<Json<NormalizationSubmissionRunResult>, (StatusCode, Json<ApiError>)> {
    let Some(generator) = state.proposal_generator.as_ref() else {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "normalization_generator_not_configured",
            "model generation adapter is not configured",
        ));
    };

    let proposal = generator.propose(&request).await.map_err(|error| {
        api_error(
            StatusCode::BAD_GATEWAY,
            "normalization_generation_failed",
            error.safe_message(),
        )
    })?;

    submit_validated_proposal(state, request, proposal).await
}

async fn submit_proposal(
    State(state): State<AppState>,
    Json(payload): Json<ValidateProposalPayload>,
) -> Result<Json<NormalizationSubmissionRunResult>, (StatusCode, Json<ApiError>)> {
    submit_validated_proposal(state, payload.request, payload.proposal).await
}

async fn submit_validated_proposal(
    state: AppState,
    request: NormalizationRequest,
    proposal: NormalizationProposal,
) -> Result<Json<NormalizationSubmissionRunResult>, (StatusCode, Json<ApiError>)> {
    let execution = NormalizationSubmissionWorkflow::new(
        state.normalization_outbox,
        state.normalization_audit_log,
        state.foundation_submitter,
        ROUTE_LEASE,
    )
    .submit(request, proposal)
    .await;
    for event in execution.events {
        emit_submit_proposal_event(event);
    }
    execution.outcome.map(Json).map_err(submit_proposal_error)
}

fn emit_submit_proposal_event(event: SubmitProposalEvent) {
    match event {
        SubmitProposalEvent::AlreadySentRecordMissing { idempotency_key } => tracing::warn!(
            idempotency_key = %idempotency_key,
            "outbox reported AlreadySent but get_sent returned none; store inconsistency"
        ),
        SubmitProposalEvent::MarkSentLeaseRace { idempotency_key } => tracing::debug!(
            idempotency_key = %idempotency_key,
            "mark_sent lease race: record already Sent"
        ),
        SubmitProposalEvent::DeliveredMarkSentFailed { idempotency_key } => tracing::error!(
            idempotency_key = %idempotency_key,
            "delivered to foundation-platform but mark_sent failed; record stranded InFlight until lease expiry; drain re-delivery relies on Foundation Platform dedups via the Idempotency-Key header"
        ),
        SubmitProposalEvent::SubmissionFailureRecordingFailed {
            idempotency_key,
            safe_diagnostic,
        } => tracing::warn!(
            idempotency_key = %idempotency_key,
            error = %safe_diagnostic,
            "failed to record submission failure"
        ),
        SubmitProposalEvent::ReconcileRequired { .. } => {
            metrics::counter!("outbox_reconcile_required_total").increment(1);
        }
    }
}

fn submit_proposal_error(error: SubmitProposalError) -> (StatusCode, Json<ApiError>) {
    match error {
        SubmitProposalError::SubmitterNotConfigured => api_error(
            StatusCode::NOT_IMPLEMENTED,
            "normalization_submitter_not_configured",
            "foundation-platform submitter adapter is not configured",
        ),
        SubmitProposalError::SubmissionNotRetryable => api_error(
            StatusCode::CONFLICT,
            "submission_not_retryable",
            "outbox transition was rejected",
        ),
        SubmitProposalError::PayloadMismatch => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "idempotency_payload_mismatch",
            "idempotency key was reused with a different payload",
        ),
        SubmitProposalError::SubmissionInProgress => api_error(
            StatusCode::CONFLICT,
            "submission_in_progress",
            "submission with this idempotency key is already queued or in flight; delivery is retried by the outbox worker",
        ),
        SubmitProposalError::AuditAppendFailed => api_error(
            StatusCode::BAD_GATEWAY,
            "normalization_audit_append_failed",
            "audit append failed",
        ),
        SubmitProposalError::OutboxStoreFailed { safe_message } => api_error(
            StatusCode::BAD_GATEWAY,
            "normalization_outbox_store_failed",
            safe_message,
        ),
        SubmitProposalError::FoundationSubmissionFailed { safe_message } => api_error(
            StatusCode::BAD_GATEWAY,
            "foundation_platform_submission_failed",
            safe_message,
        ),
    }
}

fn run_result(
    request: NormalizationRequest,
    proposal: NormalizationProposal,
    validation: NormalizationValidationResult,
) -> NormalizationRunResult {
    NormalizationRunResult {
        proposal,
        validation,
        commit_allowed: false,
        requires_human_review: true,
        metadata: metadata([
            ("source_system", request.source_system.as_str()),
            ("raw_record_id", request.raw_record_id.as_str()),
            (
                "target_schema_version",
                request.target_schema_version.as_str(),
            ),
        ]),
    }
}

fn api_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> (StatusCode, Json<ApiError>) {
    (status, Json(ApiError { code, message }))
}

fn metadata<const N: usize>(items: [(&str, &str); N]) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn health_endpoint_reports_ok() {
        let response = crate::app(AppState::default())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn validate_proposal_accepts_valid_payload() {
        let payload = json!({
            "request": {
                "tenant_id": "tenant-1",
                "source_system": "foundation-platform-r2",
                "raw_record_id": "raw-1",
                "raw_record": {"name": "Acme"},
                "trace_context": {
                    "trace_id": "trace-1",
                    "tenant_id": "tenant-1",
                    "human_user_id": "user-1",
                    "product_id": "foundation-platform"
                },
                "target_schema": {"required": ["normalized_name"]},
                "target_schema_version": "v1",
                "target_kind": "industrial_complex",
                "target_identity": {"industrial_complex_id": "complex-1"}
            },
            "proposal": {
                "raw_record_id": "raw-1",
                "proposed_record": {"normalized_name": "Acme"},
                "confidence": 0.91,
                "reasons": ["field matched source name"],
                "schema_version": "v1"
            }
        });

        let response = crate::app(AppState::default())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/intelligence/v1/normalization/validate-proposal")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["accepted"], true);
    }

    #[tokio::test]
    async fn generate_and_validate_is_closed_until_adapter_is_configured() {
        let response = crate::app(AppState::default())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/intelligence/v1/normalization/generate-and-validate")
                    .header("content-type", "application/json")
                    .body(Body::from(normalization_request_payload().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn generate_validate_submit_is_closed_until_adapters_are_configured() {
        let response = crate::app(AppState::default())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/intelligence/v1/normalization/generate-validate-submit")
                    .header("content-type", "application/json")
                    .body(Body::from(normalization_request_payload().to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn chat_output_tokens_are_bounded_by_service_policy() {
        assert_eq!(bounded_output_tokens(Some(256)), 256);
        assert_eq!(
            bounded_output_tokens(Some(u32::MAX)),
            MAX_CHAT_OUTPUT_TOKENS
        );
        assert_eq!(bounded_output_tokens(None), DEFAULT_CHAT_OUTPUT_TOKENS);
    }

    fn normalization_request_payload() -> Value {
        json!({
            "tenant_id": "tenant-1",
            "source_system": "foundation-platform-r2",
            "raw_record_id": "raw-1",
            "raw_record": {"name": "Acme"},
            "trace_context": {
                "trace_id": "trace-1",
                "tenant_id": "tenant-1",
                "human_user_id": "user-1",
                "product_id": "foundation-platform"
            },
            "target_schema": {"required": ["normalized_name"]},
            "target_schema_version": "v1",
            "target_kind": "industrial_complex",
            "target_identity": {"industrial_complex_id": "complex-1"}
        })
    }
}
