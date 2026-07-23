use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use intelligence_contracts::TraceContext;
use intelligence_normalization_application::{
    FoundationNormalizationSubmitter, FoundationSubmissionError, FoundationSubmissionResult,
    NormalizationProposalSubmission,
};
use intelligence_normalization_domain::{
    normalization_idempotency_key, NormalizationValidationResult,
};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoundationPlatformNormalizationConfig {
    pub base_url: String,
    pub submission_path: String,
    pub workload_token_provider: Option<WorkloadTokenProvider>,
    pub timeout_seconds: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct WorkloadTokenProvider {
    token_file: PathBuf,
}

impl WorkloadTokenProvider {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FoundationSubmissionError> {
        let token_file = path.as_ref().to_path_buf();
        if token_file.as_os_str().is_empty() {
            return Err(invalid_workload_token_source());
        }
        let provider = Self { token_file };
        provider
            .read_bearer()
            .map_err(|_| invalid_workload_token_source())?;
        Ok(provider)
    }

    fn read_bearer(&self) -> Result<String, ()> {
        let bearer = std::fs::read_to_string(&self.token_file).map_err(|_| ())?;
        let bearer = bearer.trim();
        if bearer.is_empty() {
            return Err(());
        }
        Ok(bearer.to_owned())
    }
}

impl fmt::Debug for WorkloadTokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkloadTokenProvider")
            .field("token_file", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct FoundationPlatformNormalizationClient {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    workload_token_provider: Option<WorkloadTokenProvider>,
}

impl FoundationPlatformNormalizationClient {
    pub fn new(
        config: FoundationPlatformNormalizationConfig,
    ) -> Result<Self, FoundationSubmissionError> {
        let base_url = parse_secure_foundation_url(&config.base_url)?;
        let endpoint = base_url.join(&config.submission_path).map_err(|_| {
            FoundationSubmissionError::InvalidResponse {
                message: "foundation-platform submission endpoint is invalid".to_string(),
            }
        })?;
        if !same_origin(&base_url, &endpoint) {
            return Err(FoundationSubmissionError::InvalidResponse {
                message:
                    "foundation-platform submission endpoint must remain on the configured origin"
                        .to_string(),
            });
        }
        if let Some(provider) = &config.workload_token_provider {
            provider
                .read_bearer()
                .map_err(|_| invalid_workload_token_source())?;
        }

        let timeout_seconds = config.timeout_seconds.max(1);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|error| FoundationSubmissionError::PreSendFailure {
                message: error.to_string(),
            })?;

        Ok(Self {
            client,
            endpoint,
            workload_token_provider: config.workload_token_provider,
        })
    }
}

#[async_trait]
impl FoundationNormalizationSubmitter for FoundationPlatformNormalizationClient {
    async fn submit(
        &self,
        submission: &NormalizationProposalSubmission,
    ) -> Result<FoundationSubmissionResult, FoundationSubmissionError> {
        let payload = FoundationPlatformNormalizationSubmission::from(submission);
        let idempotency_key = normalization_idempotency_key(&submission.request);
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .header("Idempotency-Key", idempotency_key)
            .json(&payload);

        if let Some(provider) = &self.workload_token_provider {
            let workload_bearer =
                provider
                    .read_bearer()
                    .map_err(|()| FoundationSubmissionError::PreSendFailure {
                        message: "Foundation Platform workload identity token is unavailable"
                            .to_string(),
                    })?;
            request = request.bearer_auth(workload_bearer);
        }

        let response = request.send().await.map_err(classify_send_error)?;
        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(FoundationSubmissionError::Rejected {
                status: status.as_u16(),
                retryable: is_retryable_rejection(status, &body),
                body,
            });
        }

        response
            .json()
            .await
            .map_err(|error| FoundationSubmissionError::InvalidResponse {
                message: error.to_string(),
            })
    }
}

fn invalid_workload_token_source() -> FoundationSubmissionError {
    FoundationSubmissionError::InvalidResponse {
        message: "Foundation Platform workload identity token file must be readable and non-empty"
            .to_string(),
    }
}

fn parse_secure_foundation_url(raw: &str) -> Result<reqwest::Url, FoundationSubmissionError> {
    let url = reqwest::Url::parse(raw).map_err(|_| FoundationSubmissionError::InvalidResponse {
        message: "foundation-platform base_url is invalid".to_string(),
    })?;
    let host = url
        .host_str()
        .ok_or_else(|| FoundationSubmissionError::InvalidResponse {
            message: "foundation-platform base_url must include a host".to_string(),
        })?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "foundation-platform base_url must use HTTPS except for loopback".to_string(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(FoundationSubmissionError::InvalidResponse {
            message: "foundation-platform base_url must not contain credentials".to_string(),
        });
    }
    Ok(url)
}

fn same_origin(left: &reqwest::Url, right: &reqwest::Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

#[derive(Serialize)]
struct FoundationPlatformNormalizationSubmission<'a> {
    request: FoundationPlatformNormalizationRequest<'a>,
    proposal: FoundationPlatformNormalizationProposal<'a>,
    validation: FoundationPlatformProposalValidation,
    trace_context: &'a TraceContext,
    commit_allowed: bool,
    requires_human_review: bool,
    submission_metadata: FoundationPlatformSubmissionMetadata<'a>,
}

impl<'a> From<&'a NormalizationProposalSubmission>
    for FoundationPlatformNormalizationSubmission<'a>
{
    fn from(submission: &'a NormalizationProposalSubmission) -> Self {
        Self {
            request: FoundationPlatformNormalizationRequest {
                source_system: &submission.request.source_system,
                raw_record_id: &submission.request.raw_record_id,
                raw_object_key: submission.request.raw_object_key.as_deref(),
                raw_checksum_sha256: submission.request.raw_checksum_sha256.as_deref(),
                target_kind: &submission.request.target_kind,
                target_identity: &submission.request.target_identity,
                target_schema_version: &submission.request.target_schema_version,
            },
            proposal: FoundationPlatformNormalizationProposal {
                raw_record_id: &submission.proposal.raw_record_id,
                schema_version: &submission.proposal.schema_version,
                record: &submission.proposal.proposed_record,
                confidence: submission.proposal.confidence,
                evidence: json!({ "reasons": submission.proposal.reasons }),
            },
            validation: FoundationPlatformProposalValidation::from(&submission.validation),
            trace_context: &submission.trace_context,
            commit_allowed: submission.commit_allowed,
            requires_human_review: submission.requires_human_review,
            submission_metadata: FoundationPlatformSubmissionMetadata {
                model_profile_id: submission.proposal.model_profile_id.as_deref(),
                model_id: submission.proposal.model_id.as_deref(),
                prompt_id: submission.proposal.prompt_id.as_deref(),
                prompt_version: submission.proposal.prompt_version.as_deref(),
                policy_id: &submission.proposal.policy_id,
                policy_version: &submission.proposal.policy_version,
            },
        }
    }
}

#[derive(Serialize)]
struct FoundationPlatformNormalizationRequest<'a> {
    source_system: &'a str,
    raw_record_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_object_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_checksum_sha256: Option<&'a str>,
    target_kind: &'a str,
    target_identity: &'a Value,
    target_schema_version: &'a str,
}

#[derive(Serialize)]
struct FoundationPlatformNormalizationProposal<'a> {
    raw_record_id: &'a str,
    schema_version: &'a str,
    record: &'a Value,
    confidence: f64,
    evidence: Value,
}

#[derive(Serialize)]
struct FoundationPlatformProposalValidation {
    accepted: bool,
    issues: Value,
}

impl From<&NormalizationValidationResult> for FoundationPlatformProposalValidation {
    fn from(validation: &NormalizationValidationResult) -> Self {
        Self {
            accepted: validation.accepted,
            issues: json!({ "errors": validation.errors }),
        }
    }
}

#[derive(Serialize)]
struct FoundationPlatformSubmissionMetadata<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    model_profile_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_version: Option<&'a str>,
    policy_id: &'a str,
    policy_version: &'a str,
}

fn is_retryable_rejection(status: reqwest::StatusCode, body: &str) -> bool {
    match status {
        reqwest::StatusCode::REQUEST_TIMEOUT
        | reqwest::StatusCode::TOO_MANY_REQUESTS
        | reqwest::StatusCode::CONFLICT => true,
        status if status.is_server_error() => true,
        status => match status.as_u16() {
            400 | 401 | 403 | 404 | 422 => false,
            _ => serde_json::from_str::<Value>(body)
                .ok()
                .and_then(|value| value.get("retryable").and_then(Value::as_bool))
                .unwrap_or(false),
        },
    }
}

fn classify_send_error(error: reqwest::Error) -> FoundationSubmissionError {
    classify_send_error_flags(
        error.is_timeout(),
        error.is_connect(),
        error.is_request(),
        error.to_string(),
    )
}

fn classify_send_error_flags(
    is_timeout: bool,
    is_connect: bool,
    is_request: bool,
    message: String,
) -> FoundationSubmissionError {
    if is_timeout {
        FoundationSubmissionError::AmbiguousOutcome { message }
    } else if is_connect || is_request {
        FoundationSubmissionError::PreSendFailure { message }
    } else {
        FoundationSubmissionError::AmbiguousOutcome { message }
    }
}

#[cfg(test)]
mod tests {
    use intelligence_normalization_application::FoundationSubmissionError;
    use reqwest::StatusCode;

    use super::{classify_send_error_flags, is_retryable_rejection};

    #[test]
    fn classifies_timeout_send_errors_as_ambiguous() {
        let error = classify_send_error_flags(true, false, false, "timed out".to_string());

        assert!(matches!(
            error,
            FoundationSubmissionError::AmbiguousOutcome { .. }
        ));
    }

    #[test]
    fn classifies_connect_send_errors_as_pre_send_failure() {
        let error = classify_send_error_flags(false, true, false, "connection refused".to_string());

        assert!(matches!(
            error,
            FoundationSubmissionError::PreSendFailure { .. }
        ));
    }

    #[test]
    fn classifies_request_send_errors_as_pre_send_failure() {
        let error = classify_send_error_flags(false, false, true, "request failed".to_string());

        assert!(matches!(
            error,
            FoundationSubmissionError::PreSendFailure { .. }
        ));
    }

    #[test]
    fn classifies_other_send_errors_as_ambiguous() {
        let error = classify_send_error_flags(false, false, false, "unknown transport".to_string());

        assert!(matches!(
            error,
            FoundationSubmissionError::AmbiguousOutcome { .. }
        ));
    }

    #[test]
    fn classifies_conflict_rejections_as_retryable() {
        assert!(is_retryable_rejection(
            StatusCode::CONFLICT,
            "duplicate proposal"
        ));
    }

    #[test]
    fn classifies_validation_rejections_as_terminal() {
        assert!(!is_retryable_rejection(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid payload"
        ));
    }

    #[test]
    fn treats_timeout_and_server_statuses_as_retryable() {
        assert!(is_retryable_rejection(StatusCode::REQUEST_TIMEOUT, ""));
        assert!(is_retryable_rejection(
            StatusCode::INTERNAL_SERVER_ERROR,
            ""
        ));
        assert!(is_retryable_rejection(StatusCode::BAD_GATEWAY, ""));
    }

    #[test]
    fn classifies_machine_readable_retryable_hint_as_retryable() {
        assert!(is_retryable_rejection(
            StatusCode::IM_A_TEAPOT,
            "{\"retryable\":true}"
        ));
    }

    #[test]
    fn does_not_treat_plain_text_retryable_token_as_retryable() {
        assert!(!is_retryable_rejection(
            StatusCode::IM_A_TEAPOT,
            "plain text with {\"retryable\":true} inside"
        ));
    }
}
