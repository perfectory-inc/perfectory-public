//! Zitadel workload bearer authentication for Foundation Platform calls.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

mod catalog;

pub use catalog::{
    CatalogBuildingResponse, CatalogParcelResponse, FoundationCatalogClient,
    FoundationCatalogClientConfigError, FoundationCatalogClientRequestError,
    FoundationCatalogHttpError,
};

/// Parses and validates a Foundation Platform endpoint URL.
///
/// Production endpoints must use HTTPS. Plain HTTP is accepted only for the
/// exact loopback hosts `localhost`, `127.0.0.1`, and `::1`. Credentials,
/// query strings, and fragments are rejected so constructors cannot inherit
/// ambiguous request authority or routing metadata.
///
/// # Errors
///
/// Returns [`FoundationEndpointUrlError`] when the endpoint is blank,
/// malformed, insecure, or contains forbidden URL components.
pub fn parse_foundation_endpoint_url(
    endpoint: &str,
) -> Result<reqwest::Url, FoundationEndpointUrlError> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(FoundationEndpointUrlError::Empty);
    }

    let mut parsed = reqwest::Url::parse(endpoint)
        .map_err(|source| FoundationEndpointUrlError::Invalid(source.to_string()))?;
    let loopback = matches!(
        parsed.host_str(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    );
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback) {
        return Err(FoundationEndpointUrlError::InsecureTransport);
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(FoundationEndpointUrlError::EmbeddedCredentials);
    }
    if parsed.query().is_some() {
        return Err(FoundationEndpointUrlError::Query);
    }
    if parsed.fragment().is_some() {
        return Err(FoundationEndpointUrlError::Fragment);
    }

    if !parsed.path().ends_with('/') {
        let path = format!("{}/", parsed.path());
        parsed.set_path(&path);
    }
    Ok(parsed)
}

/// Invalid Foundation Platform endpoint configuration.
#[derive(Debug, Error)]
pub enum FoundationEndpointUrlError {
    /// The configured endpoint is blank.
    #[error("Foundation Platform endpoint URL must not be empty")]
    Empty,
    /// The configured endpoint is not a valid absolute URL.
    #[error("Foundation Platform endpoint URL is invalid: {0}")]
    Invalid(String),
    /// The endpoint does not use HTTPS or explicit loopback HTTP.
    #[error("Foundation Platform endpoint URL must use HTTPS except for loopback")]
    InsecureTransport,
    /// The endpoint embeds user information.
    #[error("Foundation Platform endpoint URL must not contain credentials")]
    EmbeddedCredentials,
    /// The endpoint contains a query string.
    #[error("Foundation Platform endpoint URL must not contain a query")]
    Query,
    /// The endpoint contains a fragment.
    #[error("Foundation Platform endpoint URL must not contain a fragment")]
    Fragment,
}

/// Redacted workload authentication applied to Foundation Platform requests.
#[derive(Clone)]
pub struct FoundationServiceAuth {
    token_source: FoundationTokenSource,
}

#[derive(Clone)]
enum FoundationTokenSource {
    Bearer(Arc<str>),
    WorkloadIdentityTokenFile(Arc<PathBuf>),
}

impl FoundationServiceAuth {
    /// Builds authentication from an already-issued Zitadel workload bearer.
    ///
    /// # Errors
    ///
    /// Returns an error when the bearer is blank or too short.
    pub fn from_bearer_token(token: &str) -> Result<Self, FoundationServiceAuthError> {
        let token = validate_token(token)?;
        Ok(Self {
            token_source: FoundationTokenSource::Bearer(Arc::from(token)),
        })
    }

    /// Builds authentication from a rotating Zitadel workload token file.
    ///
    /// The file is read before each request so token rotation does not require
    /// a process restart.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is blank, unreadable, or contains an
    /// invalid bearer.
    pub fn from_workload_identity_token_file(
        token_file: impl AsRef<Path>,
    ) -> Result<Self, FoundationServiceAuthError> {
        let token_file = normalize_token_file_path(token_file.as_ref())?;
        let token = read_workload_identity_token(&token_file)?;
        validate_token(&token)?;
        Ok(Self {
            token_source: FoundationTokenSource::WorkloadIdentityTokenFile(Arc::new(token_file)),
        })
    }

    /// Applies only the Zitadel workload bearer to a Foundation request.
    ///
    /// # Errors
    ///
    /// Returns an error when a rotating token file cannot be read or contains
    /// an invalid bearer.
    pub fn apply(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, FoundationServiceAuthError> {
        Ok(request.bearer_auth(self.token_for_request()?))
    }

    fn token_for_request(&self) -> Result<String, FoundationServiceAuthError> {
        match &self.token_source {
            FoundationTokenSource::Bearer(token) => Ok(token.to_string()),
            FoundationTokenSource::WorkloadIdentityTokenFile(token_file) => {
                let token = read_workload_identity_token(token_file)?;
                validate_token(&token)?;
                Ok(token)
            }
        }
    }
}

fn validate_token(token: &str) -> Result<&str, FoundationServiceAuthError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(FoundationServiceAuthError::EmptyToken);
    }
    if token.len() < 16 {
        return Err(FoundationServiceAuthError::TokenTooShort);
    }
    Ok(token)
}

fn normalize_token_file_path(token_file: &Path) -> Result<PathBuf, FoundationServiceAuthError> {
    let path = token_file.as_os_str().to_string_lossy().trim().to_owned();
    if path.is_empty() {
        return Err(FoundationServiceAuthError::EmptyWorkloadIdentityTokenFilePath);
    }
    Ok(PathBuf::from(path))
}

fn read_workload_identity_token(token_file: &Path) -> Result<String, FoundationServiceAuthError> {
    std::fs::read_to_string(token_file)
        .map(|token| token.trim().to_owned())
        .map_err(
            |source| FoundationServiceAuthError::ReadWorkloadIdentityTokenFile {
                path: token_file.display().to_string(),
                source,
            },
        )
}

impl std::fmt::Debug for FoundationServiceAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FoundationServiceAuth")
            .field("token_source", &self.token_source)
            .finish()
    }
}

impl std::fmt::Debug for FoundationTokenSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer(_) => formatter.write_str("Bearer(<redacted>)"),
            Self::WorkloadIdentityTokenFile(path) => formatter
                .debug_tuple("WorkloadIdentityTokenFile")
                .field(path)
                .finish(),
        }
    }
}

/// Configuration failures for Foundation Platform workload authentication.
#[derive(Debug, Error)]
pub enum FoundationServiceAuthError {
    /// Bearer value is blank.
    #[error("Foundation Platform workload bearer must not be empty")]
    EmptyToken,
    /// Bearer value is implausibly short.
    #[error("Foundation Platform workload bearer must be at least 16 characters")]
    TokenTooShort,
    /// Workload identity token file path is blank.
    #[error("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE must not be empty")]
    EmptyWorkloadIdentityTokenFilePath,
    /// Workload identity token file cannot be read.
    #[error("FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE could not be read: {path}")]
    ReadWorkloadIdentityTokenFile {
        /// Configured token file path.
        path: String,
        /// Underlying file read failure.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests;
