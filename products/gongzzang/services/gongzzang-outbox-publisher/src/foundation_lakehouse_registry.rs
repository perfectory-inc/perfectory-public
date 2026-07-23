//! Foundation Platform Lakehouse Registry HTTP adapter.

#![allow(clippy::disallowed_types, clippy::module_name_repetitions)]

use circuit_breaker::{execute, Breaker, Policy};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use foundation_platform_client::{
    parse_foundation_endpoint_url, FoundationEndpointUrlError, FoundationServiceAuth,
    FoundationServiceAuthError,
};

/// Foundation Platform HTTP-backed Lakehouse Registry client.
pub struct FoundationPlatformLakehouseRegistryClient {
    base_url: reqwest::Url,
    client: reqwest::Client,
    auth: FoundationServiceAuth,
    breaker: Breaker,
    policy: Policy,
}

impl FoundationPlatformLakehouseRegistryClient {
    /// Build a Foundation Platform Lakehouse Registry client from an API base URL.
    ///
    /// # Errors
    ///
    /// Returns a config error when the URL is empty, invalid, or the HTTP
    /// client cannot be constructed.
    pub fn new(
        base_url: &str,
        auth: FoundationServiceAuth,
    ) -> Result<Self, FoundationPlatformLakehouseRegistryConfigError> {
        let base_url = parse_foundation_endpoint_url(base_url)?;
        let client = reqwest::Client::builder().build().map_err(|source| {
            FoundationPlatformLakehouseRegistryConfigError::HttpClient { source }
        })?;
        Ok(Self {
            base_url,
            client,
            auth,
            breaker: Breaker::new(),
            policy: Policy::foundation_platform_default(),
        })
    }

    /// Register one lakehouse object artifact with Foundation Platform.
    ///
    /// # Errors
    ///
    /// Returns an error when local artifact invariants fail, the Foundation Platform
    /// request fails, or the registry returns a non-success status.
    pub async fn register_object_artifact_http(
        &self,
        artifact: LakehouseObjectArtifactRegistration,
    ) -> Result<LakehouseArtifactRegistrationReceipt, FoundationPlatformLakehouseRegistryError>
    {
        artifact.validate()?;
        let url = self
            .base_url
            .join("internal/lakehouse/artifacts")
            .map_err(|source| FoundationPlatformLakehouseRegistryError::Backend {
                detail: format!("build registry URL: {source}"),
            })?;
        let response = execute(
            &self.breaker,
            &self.policy,
            "foundation_platform.lakehouse_registry.register_artifact",
            || {
                let client = self.client.clone();
                let auth = self.auth.clone();
                let url = url.clone();
                let artifact = artifact.clone();
                async move {
                    send_foundation_platform_artifact_post(&client, url, &auth, &artifact).await
                }
            },
        )
        .await
        .map_err(|source| FoundationPlatformLakehouseRegistryError::Backend {
            detail: source.to_string(),
        })?;

        let status = response.status();
        if !status.is_success() {
            return Err(FoundationPlatformLakehouseRegistryError::Status { status });
        }

        let receipt = response
            .json::<LakehouseArtifactRegistrationReceipt>()
            .await
            .map_err(|source| FoundationPlatformLakehouseRegistryError::Decode { source })?;
        tracing::info!(
            artifact_id = %receipt.artifact_id,
            qualified_name = %receipt.qualified_name,
            object_key = %receipt.object_key,
            "Foundation Platform Lakehouse artifact registered"
        );
        Ok(receipt)
    }
}

/// Object artifact registration payload sent to Foundation Platform.
#[derive(Clone, Debug, Serialize)]
pub struct LakehouseObjectArtifactRegistration {
    /// Registry qualified name, for example `gongzzang.bronze.onbid_sale`.
    pub qualified_name: String,
    /// Storage namespace declared in the service lakehouse registry policy.
    pub namespace_id: String,
    /// Object key inside the service-owned lakehouse bucket.
    pub object_key: String,
    /// MIME type of the stored artifact.
    pub content_type: String,
    /// SHA-256 checksum of the exact object bytes, lowercase hex.
    pub checksum_sha256: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional record count when the artifact is tabular or line-delimited.
    pub logical_record_count: Option<u64>,
}

impl LakehouseObjectArtifactRegistration {
    fn validate(&self) -> Result<(), FoundationPlatformLakehouseRegistryError> {
        validate_qualified_name(&self.qualified_name)?;
        validate_namespace_id(&self.namespace_id)?;
        validate_object_key(&self.object_key)?;
        validate_content_type(&self.content_type)?;
        validate_checksum_sha256(&self.checksum_sha256)?;
        Ok(())
    }
}

/// Receipt returned by Foundation Platform after an artifact registration.
#[derive(Debug, Deserialize)]
pub struct LakehouseArtifactRegistrationReceipt {
    /// Foundation Platform artifact id.
    pub artifact_id: String,
    /// Registered qualified name.
    pub qualified_name: String,
    /// Registered object key.
    #[allow(dead_code)]
    pub object_key: String,
}

/// Configuration errors for Foundation Platform Lakehouse Registry.
#[derive(Debug, Error)]
pub enum FoundationPlatformLakehouseRegistryConfigError {
    /// Foundation Platform endpoint validation failed.
    #[error(transparent)]
    FoundationEndpoint(#[from] FoundationEndpointUrlError),
    /// HTTP client construction failed.
    #[error("build Foundation Platform Lakehouse Registry HTTP client: {source}")]
    HttpClient {
        /// Underlying HTTP client construction error.
        source: reqwest::Error,
    },
}

/// Runtime errors for Foundation Platform Lakehouse Registry calls.
#[derive(Debug, Error)]
pub enum FoundationPlatformLakehouseRegistryError {
    /// Local artifact payload did not satisfy the registry contract.
    #[error("invalid lakehouse artifact {field}: {reason}")]
    InvalidArtifact {
        /// Invalid field name.
        field: &'static str,
        /// Validation reason.
        reason: &'static str,
    },
    /// Request could not be sent.
    #[error("Foundation Platform Lakehouse Registry HTTP request failed: {source}")]
    Request {
        /// Underlying request error.
        #[source]
        source: reqwest::Error,
    },
    /// Retriable status was returned.
    #[error("Foundation Platform Lakehouse Registry returned retriable status {status}")]
    RetriableStatus {
        /// HTTP status.
        status: StatusCode,
    },
    /// Registry returned a non-success status.
    #[error("Foundation Platform Lakehouse Registry returned status {status}")]
    Status {
        /// HTTP status.
        status: StatusCode,
    },
    /// Service auth could not be applied.
    #[error("Foundation Platform Lakehouse Registry service auth failed: {source}")]
    ServiceAuth {
        /// Underlying service auth error.
        #[source]
        source: FoundationServiceAuthError,
    },
    /// Response body did not match the expected registry receipt contract.
    #[error("decode Foundation Platform Lakehouse Registry response: {source}")]
    Decode {
        /// Underlying response decode error.
        #[source]
        source: reqwest::Error,
    },
    /// Circuit breaker/retry wrapper returned a backend error.
    #[error("Foundation Platform Lakehouse Registry backend failed: {detail}")]
    Backend {
        /// Error detail from the integration wrapper.
        detail: String,
    },
}

async fn send_foundation_platform_artifact_post(
    client: &reqwest::Client,
    url: reqwest::Url,
    auth: &FoundationServiceAuth,
    artifact: &LakehouseObjectArtifactRegistration,
) -> Result<reqwest::Response, FoundationPlatformLakehouseRegistryError> {
    let request = auth
        .apply(client.post(url).json(artifact))
        .map_err(|source| FoundationPlatformLakehouseRegistryError::ServiceAuth { source })?;
    let response = request
        .send()
        .await
        .map_err(|source| FoundationPlatformLakehouseRegistryError::Request { source })?;
    let status = response.status();
    if is_retriable_status(status) {
        return Err(FoundationPlatformLakehouseRegistryError::RetriableStatus { status });
    }
    Ok(response)
}

fn is_retriable_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

fn validate_qualified_name(value: &str) -> Result<(), FoundationPlatformLakehouseRegistryError> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return invalid_artifact("qualified_name", "must have service.layer.asset shape");
    }
    if parts[0] != "gongzzang" {
        return invalid_artifact("qualified_name", "service must be gongzzang");
    }
    if !matches!(parts[1], "bronze" | "silver" | "gold") {
        return invalid_artifact("qualified_name", "layer must be bronze, silver, or gold");
    }
    if !is_lower_snake_token(parts[2]) {
        return invalid_artifact("qualified_name", "asset must be lower snake case");
    }
    Ok(())
}

fn validate_namespace_id(value: &str) -> Result<(), FoundationPlatformLakehouseRegistryError> {
    if !is_lower_snake_token(value) {
        return invalid_artifact("namespace_id", "must be lower snake case");
    }
    Ok(())
}

fn validate_object_key(value: &str) -> Result<(), FoundationPlatformLakehouseRegistryError> {
    if value.trim().is_empty() {
        return invalid_artifact("object_key", "must not be empty");
    }
    if value.starts_with('/')
        || value.starts_with("../")
        || value.starts_with("./")
        || value.contains('\\')
        || value.contains("//")
        || value.contains("/../")
        || value.contains("/./")
        || value.contains('?')
        || value.contains('#')
    {
        return invalid_artifact("object_key", "must be a normalized relative object key");
    }
    Ok(())
}

fn validate_content_type(value: &str) -> Result<(), FoundationPlatformLakehouseRegistryError> {
    if value.trim().is_empty() || value.contains('\r') || value.contains('\n') {
        return invalid_artifact("content_type", "must be a single non-empty MIME value");
    }
    Ok(())
}

fn validate_checksum_sha256(value: &str) -> Result<(), FoundationPlatformLakehouseRegistryError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return invalid_artifact("checksum_sha256", "must be 64 hex characters");
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return invalid_artifact("checksum_sha256", "must be lowercase hex");
    }
    Ok(())
}

fn is_lower_snake_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

const fn invalid_artifact<T>(
    field: &'static str,
    reason: &'static str,
) -> Result<T, FoundationPlatformLakehouseRegistryError> {
    Err(FoundationPlatformLakehouseRegistryError::InvalidArtifact { field, reason })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::Duration;

    use foundation_platform_client::FoundationServiceAuth;

    use super::*;

    #[test]
    fn constructor_enforces_foundation_endpoint_security() {
        let auth = worker_service_auth();
        assert!(FoundationPlatformLakehouseRegistryClient::new(
            "https://foundation.example",
            auth.clone()
        )
        .is_ok());
        assert!(FoundationPlatformLakehouseRegistryClient::new(
            "http://localhost:8080",
            auth.clone()
        )
        .is_ok());
        for invalid in [
            "http://foundation.example",
            "https://user:password@foundation.example",
            "https://foundation.example?tenant=other",
            "https://foundation.example#fragment",
        ] {
            assert!(FoundationPlatformLakehouseRegistryClient::new(invalid, auth.clone()).is_err());
        }
    }

    #[tokio::test]
    async fn register_object_artifact_sends_only_foundation_workload_bearer() {
        let (base_url, requests) = spawn_foundation_platform_response(
            "HTTP/1.1 201 Created",
            r#"{"artifact_id":"artifact-01","qualified_name":"gongzzang.bronze.onbid_sale","object_key":"bronze/source=onbid-sale/run=20260607/page-000001.json"}"#,
        );
        let auth = worker_service_auth();
        let client =
            FoundationPlatformLakehouseRegistryClient::new(&base_url, auth).expect("client");

        let receipt = client
            .register_object_artifact_http(LakehouseObjectArtifactRegistration {
                qualified_name: "gongzzang.bronze.onbid_sale".to_owned(),
                namespace_id: "gongzzang_r2_production".to_owned(),
                object_key: "bronze/source=onbid-sale/run=20260607/page-000001.json".to_owned(),
                content_type: "application/json".to_owned(),
                checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                size_bytes: 512,
                logical_record_count: Some(100),
            })
            .await
            .expect("registered");

        assert_eq!(receipt.artifact_id, "artifact-01");
        assert_eq!(receipt.qualified_name, "gongzzang.bronze.onbid_sale");
        let request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("captured request");
        assert!(
            request.starts_with("POST /internal/lakehouse/artifacts HTTP/1.1"),
            "request path mismatch: {request}"
        );
        assert!(
            request.contains("\r\nauthorization: Bearer zitadel-workload-token-32-valid\r\n"),
            "request missing workload bearer: {request}"
        );
        assert!(
            request.lines().all(|line| {
                let line = line.to_ascii_lowercase();
                !line.starts_with("x-foundation-platform-") && !line.starts_with("x-gongzzang-")
            }),
            "request leaked legacy authorization metadata: {request}"
        );
        assert!(
            request.contains(
                r#""object_key":"bronze/source=onbid-sale/run=20260607/page-000001.json""#
            ),
            "request body missing object key: {request}"
        );
    }

    #[tokio::test]
    async fn register_object_artifact_rejects_untrusted_object_key_before_http() {
        let (base_url, requests) = spawn_foundation_platform_response(
            "HTTP/1.1 201 Created",
            r#"{"artifact_id":"artifact-01","qualified_name":"gongzzang.bronze.onbid_sale","object_key":"ignored"}"#,
        );
        let auth = worker_service_auth();
        let client =
            FoundationPlatformLakehouseRegistryClient::new(&base_url, auth).expect("client");

        let error = client
            .register_object_artifact_http(LakehouseObjectArtifactRegistration {
                qualified_name: "gongzzang.bronze.onbid_sale".to_owned(),
                namespace_id: "gongzzang_r2_production".to_owned(),
                object_key: "../bronze/source=onbid-sale/page-000001.json".to_owned(),
                content_type: "application/json".to_owned(),
                checksum_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                size_bytes: 512,
                logical_record_count: Some(100),
            })
            .await
            .expect_err("invalid object key");

        assert!(error.to_string().contains("object_key"));
        assert!(requests.recv_timeout(Duration::from_millis(200)).is_err());
    }

    fn spawn_foundation_platform_response(
        status_line: &str,
        body: &str,
    ) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let status_line = status_line.to_owned();
        let body = body.to_owned();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            tx.send(request.to_string()).expect("send request");
            let response = format!(
                "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        (format!("http://{addr}"), rx)
    }

    fn worker_service_auth() -> FoundationServiceAuth {
        FoundationServiceAuth::from_bearer_token("zitadel-workload-token-32-valid")
            .expect("workload auth")
    }
}
