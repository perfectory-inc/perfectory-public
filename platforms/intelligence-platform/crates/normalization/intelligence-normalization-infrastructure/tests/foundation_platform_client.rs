// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    FoundationSubmissionStatus, NormalizationProposalSubmission,
};
use intelligence_normalization_domain::{
    normalization_idempotency_key, NormalizationProposal, NormalizationRequest,
    NormalizationValidationResult,
};
use intelligence_normalization_infrastructure::foundation_platform::{
    FoundationPlatformNormalizationClient, FoundationPlatformNormalizationConfig,
    WorkloadTokenProvider,
};
use serde_json::{json, Value};

#[tokio::test]
async fn posts_submission_with_authorization_and_parses_result() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let token_file = TestTokenFile::new("zitadel-workload-token");
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: Some(token_file.provider()),
            timeout_seconds: 5,
        })
        .unwrap();

    let result = client.submit(&submission()).await.unwrap();

    assert_eq!(result.submission_id, "018f7c6a-0000-7000-8000-000000000001");
    assert_eq!(result.status, FoundationSubmissionStatus::Queued);
    assert!(result.review_required);
    assert_eq!(result.platform, "foundation-platform");
    assert_eq!(result.metadata["storage"], "proposal_inbox");
    assert_eq!(result.metadata["mode"], "durable_review_gate");
    assert_eq!(
        result.metadata["proposal_key"],
        "tenant-1:building_register_floor:raw-1"
    );

    let captured = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.authorization,
        Some("Bearer zitadel-workload-token".to_string())
    );
    assert!(captured.nonstandard_authorization_headers.is_empty());
    assert_eq!(captured.body["request"]["raw_record_id"], "raw-1");
    assert_eq!(
        captured.body["request"]["target_kind"],
        "building_register_floor"
    );
    assert_eq!(
        captured.body["request"]["target_identity"],
        json!({
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_record_id": "raw-1"
        })
    );
    assert_eq!(
        captured.body["proposal"]["record"],
        json!({"floor_display_ko": "지하 1층", "floor_index": -1, "floor_kind": "basement", "floor_number": 1})
    );
    assert_eq!(
        captured.body["proposal"]["evidence"]["reasons"][0],
        "floor label normalized from source fields"
    );
    assert!(captured.body["proposal"].get("proposed_record").is_none());
    assert_eq!(captured.body["commit_allowed"], false);
    assert_eq!(captured.body["requires_human_review"], true);
    assert_eq!(captured.body["validation"]["accepted"], true);
    assert_eq!(captured.body["validation"]["issues"], json!({"errors": []}));
}

#[tokio::test]
async fn sends_only_workload_bearer_for_foundation_authorization() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let token_file = TestTokenFile::new("zitadel-workload-token");
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: Some(token_file.provider()),
            timeout_seconds: 5,
        })
        .unwrap();

    let sub = submission();
    client.submit(&sub).await.unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    assert!(captured.nonstandard_authorization_headers.is_empty());
    assert_eq!(
        captured.authorization.as_deref(),
        Some("Bearer zitadel-workload-token")
    );
    assert_eq!(
        captured.idempotency_key.as_deref(),
        Some(normalization_idempotency_key(&sub.request).as_str())
    );
}

#[tokio::test]
async fn maps_non_success_status_to_rejected_error() {
    let base_url = spawn_rejection_server().await;
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: None,
            timeout_seconds: 5,
        })
        .unwrap();

    let error = client.submit(&submission()).await.unwrap_err();

    assert!(matches!(
        error,
        FoundationSubmissionError::Rejected {
            status: 409,
            retryable: true,
            ..
        }
    ));
    assert_eq!(
        error.safe_message(),
        "foundation-platform rejected submission"
    );
}

#[tokio::test]
async fn maps_validation_rejection_to_terminal_error() {
    let base_url = spawn_status_server(StatusCode::UNPROCESSABLE_ENTITY, "invalid payload").await;
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: None,
            timeout_seconds: 5,
        })
        .unwrap();

    let error = client.submit(&submission()).await.unwrap_err();

    assert!(matches!(
        error,
        FoundationSubmissionError::Rejected {
            status: 422,
            retryable: false,
            ..
        }
    ));
}

#[tokio::test]
async fn maps_success_with_invalid_body_to_reconcile_error() {
    let base_url = spawn_invalid_json_server().await;
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: None,
            timeout_seconds: 5,
        })
        .unwrap();

    let error = client.submit(&submission()).await.unwrap_err();

    assert!(matches!(
        error,
        FoundationSubmissionError::InvalidResponse { .. }
    ));
    assert_eq!(
        error.failure_class(),
        intelligence_normalization_application::FoundationSubmissionFailureClass::ReconcileRequired
    );
}

#[tokio::test]
async fn posts_submission_with_idempotency_key_header() {
    let captured = Arc::new(Mutex::new(None));
    let base_url = spawn_success_server(captured.clone()).await;
    let token_file = TestTokenFile::new("zitadel-workload-token");
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: Some(token_file.provider()),
            timeout_seconds: 5,
        })
        .unwrap();

    let sub = submission();
    client.submit(&sub).await.unwrap();

    let captured = captured.lock().unwrap().clone().unwrap();
    let expected_key = normalization_idempotency_key(&sub.request);
    assert_eq!(
        captured.idempotency_key.as_deref(),
        Some(expected_key.as_str()),
        "Idempotency-Key header must equal normalization_idempotency_key(&submission.request)"
    );
}

#[tokio::test]
async fn maps_timeout_send_failures_to_ambiguous_outcome() {
    let base_url = spawn_timeout_server(Duration::from_secs(2)).await;
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: None,
            timeout_seconds: 1,
        })
        .unwrap();

    let error = client.submit(&submission()).await.unwrap_err();

    assert!(matches!(
        error,
        FoundationSubmissionError::AmbiguousOutcome { .. }
    ));
}

#[tokio::test]
async fn rereads_workload_token_file_for_each_submission() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let base_url = spawn_rotating_token_server(captured.clone()).await;
    let token_file = TestTokenFile::new("first-workload-token");
    let client =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url,
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: Some(token_file.provider()),
            timeout_seconds: 5,
        })
        .unwrap();

    client.submit(&submission()).await.unwrap();
    token_file.write("rotated-workload-token");
    client.submit(&submission()).await.unwrap();

    assert_eq!(
        *captured.lock().unwrap(),
        vec![
            Some("Bearer first-workload-token".to_string()),
            Some("Bearer rotated-workload-token".to_string())
        ]
    );
}

#[test]
fn config_debug_redacts_workload_credential_source() {
    let token_file = TestTokenFile::new("credential-must-not-appear");
    let config = FoundationPlatformNormalizationConfig {
        base_url: "https://foundation.example.com".to_string(),
        submission_path: "/internal/normalization/proposals".to_string(),
        workload_token_provider: Some(token_file.provider()),
        timeout_seconds: 5,
    };

    let debug = format!("{config:?}");

    assert!(!debug.contains("credential-must-not-appear"));
    assert!(!debug.contains(token_file.path().to_str().unwrap()));
    assert!(debug.contains("<redacted>"));
}

#[test]
fn rejects_insecure_non_loopback_foundation_url() {
    let result =
        FoundationPlatformNormalizationClient::new(FoundationPlatformNormalizationConfig {
            base_url: "http://foundation.example.com".to_string(),
            submission_path: "/internal/normalization/proposals".to_string(),
            workload_token_provider: None,
            timeout_seconds: 5,
        });
    let Err(error) = result else {
        panic!("insecure Foundation URL should be rejected");
    };

    assert!(matches!(
        error,
        FoundationSubmissionError::InvalidResponse { .. }
    ));
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    authorization: Option<String>,
    nonstandard_authorization_headers: Vec<String>,
    idempotency_key: Option<String>,
    body: Value,
}

async fn spawn_success_server(captured: Arc<Mutex<Option<CapturedRequest>>>) -> String {
    async fn handler(
        State(captured): State<Arc<Mutex<Option<CapturedRequest>>>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<FoundationSubmissionResult>) {
        *captured.lock().unwrap() = Some(CapturedRequest {
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            nonstandard_authorization_headers: headers
                .keys()
                .map(|name| name.as_str())
                .filter(|name| name.starts_with("x-gongzzang-"))
                .map(ToOwned::to_owned)
                .collect(),
            idempotency_key: header_string(&headers, "idempotency-key"),
            body,
        });

        (
            StatusCode::ACCEPTED,
            Json(FoundationSubmissionResult {
                submission_id: "018f7c6a-0000-7000-8000-000000000001".to_string(),
                status: FoundationSubmissionStatus::Queued,
                review_required: true,
                platform: "foundation-platform".to_string(),
                metadata: BTreeMap::from([
                    ("storage".to_string(), "proposal_inbox".to_string()),
                    ("mode".to_string(), "durable_review_gate".to_string()),
                    (
                        "proposal_key".to_string(),
                        "tenant-1:building_register_floor:raw-1".to_string(),
                    ),
                ]),
            }),
        )
    }

    spawn_server(
        Router::new()
            .route("/internal/normalization/proposals", post(handler))
            .with_state(captured),
    )
    .await
}

async fn spawn_rotating_token_server(captured: Arc<Mutex<Vec<Option<String>>>>) -> String {
    async fn handler(
        State(captured): State<Arc<Mutex<Vec<Option<String>>>>>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<FoundationSubmissionResult>) {
        captured
            .lock()
            .unwrap()
            .push(header_string(&headers, "authorization"));
        (
            StatusCode::ACCEPTED,
            Json(FoundationSubmissionResult {
                submission_id: "018f7c6a-0000-7000-8000-000000000001".to_string(),
                status: FoundationSubmissionStatus::Queued,
                review_required: true,
                platform: "foundation-platform".to_string(),
                metadata: BTreeMap::new(),
            }),
        )
    }

    spawn_server(
        Router::new()
            .route("/internal/normalization/proposals", post(handler))
            .with_state(captured),
    )
    .await
}

async fn spawn_rejection_server() -> String {
    async fn handler() -> (StatusCode, &'static str) {
        (StatusCode::CONFLICT, "duplicate proposal")
    }

    spawn_server(Router::new().route("/internal/normalization/proposals", post(handler))).await
}

async fn spawn_status_server(status: StatusCode, body: &'static str) -> String {
    async fn handler(
        State((status, body)): State<(StatusCode, &'static str)>,
    ) -> (StatusCode, &'static str) {
        (status, body)
    }

    spawn_server(
        Router::new()
            .route("/internal/normalization/proposals", post(handler))
            .with_state((status, body)),
    )
    .await
}

async fn spawn_invalid_json_server() -> String {
    async fn handler() -> &'static str {
        "not-json"
    }

    spawn_server(Router::new().route("/internal/normalization/proposals", post(handler))).await
}

async fn spawn_timeout_server(delay: Duration) -> String {
    async fn handler(State(delay): State<Duration>) -> Json<FoundationSubmissionResult> {
        tokio::time::sleep(delay).await;

        Json(FoundationSubmissionResult {
            submission_id: "018f7c6a-0000-7000-8000-000000000099".to_string(),
            status: FoundationSubmissionStatus::Queued,
            review_required: true,
            platform: "foundation-platform".to_string(),
            metadata: BTreeMap::new(),
        })
    }

    spawn_server(
        Router::new()
            .route("/internal/normalization/proposals", post(handler))
            .with_state(delay),
    )
    .await
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

async fn spawn_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{address}")
}

struct TestTokenFile {
    path: std::path::PathBuf,
}

impl TestTokenFile {
    fn new(token: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "intelligence-foundation-token-{}-{}.txt",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, token).unwrap();
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn provider(&self) -> WorkloadTokenProvider {
        WorkloadTokenProvider::from_file(&self.path).unwrap()
    }

    fn write(&self, token: &str) {
        std::fs::write(&self.path, token).unwrap();
    }
}

impl Drop for TestTokenFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn submission() -> NormalizationProposalSubmission {
    let trace_context = TraceContext {
        trace_id: "trace-1".to_string(),
        tenant_id: "tenant-1".to_string(),
        human_user_id: "user-1".to_string(),
        product_id: "foundation-platform".to_string(),
    };
    let request = NormalizationRequest {
        tenant_id: "tenant-1".to_string(),
        source_system: "foundation-platform-r2".to_string(),
        raw_record_id: "raw-1".to_string(),
        raw_record: json!({
            "floor_type_code_raw": "10",
            "floor_type_name_raw": "지하",
            "floor_number_raw": "1",
            "floor_label_raw": "지1층"
        }),
        trace_context: trace_context.clone(),
        target_schema: json!({"required": ["floor_kind", "floor_number", "floor_index", "floor_display_ko"]}),
        target_schema_version: "building_register_floor.normalized.v1".to_string(),
        raw_object_key: Some(
            "bronze/source=datagokr__building_register_floor/page-000001.json".to_string(),
        ),
        raw_checksum_sha256: Some(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        ),
        target_kind: "building_register_floor".to_string(),
        target_identity: json!({
            "mgm_bldrgst_pk": "11680-raw-1",
            "source_record_id": "raw-1"
        }),
        dictionaries: BTreeMap::new(),
    };
    let proposal = NormalizationProposal {
        raw_record_id: "raw-1".to_string(),
        proposed_record: json!({
            "floor_kind": "basement",
            "floor_number": 1,
            "floor_index": -1,
            "floor_display_ko": "지하 1층"
        }),
        confidence: 0.91,
        reasons: vec!["floor label normalized from source fields".to_string()],
        schema_version: "building_register_floor.normalized.v1".to_string(),
        policy_id: "normalization-proposal-policy".to_string(),
        policy_version: "v1".to_string(),
        model_profile_id: None,
        model_id: None,
        prompt_id: None,
        prompt_version: None,
    };
    let validation = NormalizationValidationResult {
        accepted: true,
        raw_record_id: "raw-1".to_string(),
        confidence: 0.91,
        errors: vec![],
    };

    NormalizationProposalSubmission {
        request,
        proposal,
        validation,
        trace_context,
        commit_allowed: false,
        requires_human_review: true,
        submission_metadata: BTreeMap::new(),
    }
}
