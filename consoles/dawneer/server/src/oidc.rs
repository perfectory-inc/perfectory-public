//! Signing a staff member in with Zitadel: authorization code + PKCE, confidential client
//! (root ADR-0116 §1, §4).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::config::Config;

/// The endpoints the issuer publishes.
#[derive(Clone, Debug, Deserialize)]
pub struct Discovery {
    /// Must equal the configured issuer.
    pub issuer: String,
    /// Where the browser goes to sign in.
    pub authorization_endpoint: Url,
    /// Where the server trades a code or refresh token.
    pub token_endpoint: Url,
    /// Who the token belongs to.
    pub userinfo_endpoint: Url,
    /// Where the browser goes to sign out.
    pub end_session_endpoint: Option<Url>,
}

/// Tokens the server keeps; the browser never sees them.
#[derive(Clone, Debug, Deserialize)]
pub struct Tokens {
    /// JWT access token Foundation verifies.
    pub access_token: String,
    /// Refresh token (`offline_access`).
    pub refresh_token: Option<String>,
    /// Seconds until the access token expires.
    pub expires_in: Option<u64>,
    /// The ID token, kept only to hint the sign-out.
    pub id_token: Option<String>,
}

/// Who signed in, as the issuer says.
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct Identity {
    /// Zitadel user id; identity-platform's staff row is keyed by it.
    pub sub: String,
    /// Display name.
    #[serde(default)]
    pub name: String,
    /// Mail.
    #[serde(default)]
    pub email: String,
}

/// Why signing in failed.
#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    /// The issuer could not be reached or answered badly.
    #[error("issuer request failed: {0}")]
    Transport(String),
    /// The issuer refused.
    #[error("issuer refused: {status} {body}")]
    Refused {
        /// HTTP status.
        status: u16,
        /// Body, for the log.
        body: String,
    },
    /// The discovery document names another issuer.
    #[error("discovery names issuer {0}, not the configured one")]
    IssuerMismatch(String),
}

impl From<reqwest::Error> for OidcError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.to_string())
    }
}

/// A fresh random value, base64url, for state, PKCE verifiers, session ids and CSRF tokens.
///
/// # Errors
/// Returns an error if the operating system has no randomness to give.
pub fn random_token(bytes: usize) -> Result<String, getrandom::Error> {
    let mut buffer = vec![0_u8; bytes];
    getrandom::fill(&mut buffer)?;
    Ok(URL_SAFE_NO_PAD.encode(buffer))
}

/// The S256 challenge of a PKCE verifier (RFC 7636 §4.2).
#[must_use]
pub fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// The scopes Dawneer asks for. The project audience scope is what makes Foundation accept the
/// access token (`aud` must hold the project id).
#[must_use]
pub fn scopes(project_id: &str) -> String {
    format!("openid profile email offline_access urn:zitadel:iam:org:project:id:{project_id}:aud")
}

/// Fetches and checks the issuer's discovery document.
///
/// # Errors
/// Returns [`OidcError`] when it cannot be read or names another issuer.
pub async fn discover(http: &reqwest::Client, config: &Config) -> Result<Discovery, OidcError> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        config.issuer.as_str().trim_end_matches('/')
    );
    let discovery: Discovery = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if discovery.issuer.trim_end_matches('/') != config.issuer.as_str().trim_end_matches('/') {
        return Err(OidcError::IssuerMismatch(discovery.issuer));
    }
    Ok(discovery)
}

/// Where to send the browser to sign in.
///
/// # Errors
/// Returns an error when the redirect URI cannot be formed.
pub fn authorize_url(
    discovery: &Discovery,
    config: &Config,
    state: &str,
    verifier: &str,
) -> anyhow::Result<Url> {
    let mut url = discovery.authorization_endpoint.clone();
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", config.redirect_uri()?.as_str())
        .append_pair("scope", &scopes(&config.project_id))
        .append_pair("code_challenge", &pkce_challenge(verifier))
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state);
    Ok(url)
}

async fn token_request(
    http: &reqwest::Client,
    discovery: &Discovery,
    config: &Config,
    form: &[(&str, &str)],
) -> Result<Tokens, OidcError> {
    let response = http
        .post(discovery.token_endpoint.clone())
        .basic_auth(&config.client_id, Some(&config.client_secret))
        .form(form)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(OidcError::Refused {
            status: status.as_u16(),
            body: response.text().await.unwrap_or_default(),
        });
    }
    Ok(response.json().await?)
}

/// Trades an authorization code for tokens.
///
/// # Errors
/// Returns [`OidcError`] when the issuer refuses the code or verifier.
pub async fn exchange_code(
    http: &reqwest::Client,
    discovery: &Discovery,
    config: &Config,
    code: &str,
    verifier: &str,
) -> Result<Tokens, OidcError> {
    let redirect = config
        .redirect_uri()
        .map_err(|error| OidcError::Transport(error.to_string()))?;
    token_request(
        http,
        discovery,
        config,
        &[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect.as_str()),
            ("code_verifier", verifier),
        ],
    )
    .await
}

/// Trades a refresh token for a new access token.
///
/// # Errors
/// Returns [`OidcError`] when the issuer refuses the refresh token.
pub async fn refresh(
    http: &reqwest::Client,
    discovery: &Discovery,
    config: &Config,
    refresh_token: &str,
) -> Result<Tokens, OidcError> {
    token_request(
        http,
        discovery,
        config,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ],
    )
    .await
}

/// Asks the issuer who the access token belongs to.
///
/// # Errors
/// Returns [`OidcError`] when the issuer refuses the token.
pub async fn userinfo(
    http: &reqwest::Client,
    discovery: &Discovery,
    access_token: &str,
) -> Result<Identity, OidcError> {
    let response = http
        .get(discovery.userinfo_endpoint.clone())
        .bearer_auth(access_token)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(OidcError::Refused {
            status: status.as_u16(),
            body: response.text().await.unwrap_or_default(),
        });
    }
    Ok(response.json().await?)
}
