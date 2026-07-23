//! Local Zitadel JWT verification with a bounded JWKS cache.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use reqwest::{Client, Url};
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

const ALLOWED_TOKEN_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
];
const JWKS_CACHE_TTL: chrono::Duration = chrono::Duration::minutes(5);
const UNKNOWN_KID_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityTokenVerificationError {
    Unauthorized,
    Infrastructure,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum VerifiedPrincipalKind {
    Staff,
    Service,
}

impl std::fmt::Display for IdentityTokenVerificationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unauthorized => "identity token is unauthorized",
            Self::Infrastructure => "identity token verification is unavailable",
        })
    }
}

impl std::error::Error for IdentityTokenVerificationError {}

#[derive(Clone)]
pub struct IdentityTokenVerifier {
    issuer: String,
    issuer_url: Url,
    audience: String,
    client: Client,
    discovery_url: Url,
    jwks_cache: Arc<RwLock<Option<JwksCache>>>,
    refresh_gate: Arc<Mutex<RefreshState>>,
    unknown_kid_refresh_cooldown: Duration,
}

#[derive(Default)]
struct RefreshState {
    last_unknown_kid_refresh: Option<Instant>,
}

#[derive(Clone)]
struct JwksCache {
    jwks: JwkSet,
    expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct OidcConfiguration {
    jwks_uri: String,
}

#[derive(Deserialize)]
struct ZitadelClaims {
    sub: String,
    principal_kind: VerifiedPrincipalKind,
}

impl IdentityTokenVerifier {
    pub fn new(
        issuer: impl AsRef<str>,
        audience: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, IdentityTokenVerificationError> {
        Self::new_with_unknown_kid_cooldown(
            issuer,
            audience,
            connect_timeout,
            request_timeout,
            UNKNOWN_KID_REFRESH_COOLDOWN,
        )
    }

    pub(crate) fn new_with_unknown_kid_cooldown(
        issuer: impl AsRef<str>,
        audience: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
        unknown_kid_refresh_cooldown: Duration,
    ) -> Result<Self, IdentityTokenVerificationError> {
        let issuer_url = parse_secure_endpoint_url(issuer.as_ref())?;
        let issuer = issuer_url.as_str().trim_end_matches('/').to_owned();
        let audience = audience.into();
        if audience.trim().is_empty() {
            return Err(IdentityTokenVerificationError::Infrastructure);
        }
        let discovery_url = issuer_url
            .join("/.well-known/openid-configuration")
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?;
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?;
        Ok(Self {
            issuer,
            issuer_url,
            audience,
            client,
            discovery_url,
            jwks_cache: Arc::new(RwLock::new(None)),
            refresh_gate: Arc::new(Mutex::new(RefreshState::default())),
            unknown_kid_refresh_cooldown,
        })
    }

    pub async fn verify(
        &self,
        bearer: &str,
    ) -> Result<VerifiedPrincipalKind, IdentityTokenVerificationError> {
        let header =
            decode_header(bearer).map_err(|_| IdentityTokenVerificationError::Unauthorized)?;
        if !ALLOWED_TOKEN_ALGORITHMS.contains(&header.alg) {
            return Err(IdentityTokenVerificationError::Unauthorized);
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|kid| !kid.trim().is_empty())
            .ok_or(IdentityTokenVerificationError::Unauthorized)?;
        let cache = self.cached_jwks_for_kid(kid).await?;
        let jwk = cache
            .jwks
            .find(kid)
            .ok_or(IdentityTokenVerificationError::Unauthorized)?;
        let decoding_key =
            DecodingKey::from_jwk(jwk).map_err(|_| IdentityTokenVerificationError::Unauthorized)?;
        let mut validation = Validation::new(header.alg);
        validation.algorithms = vec![header.alg];
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[self.audience.trim()]);
        validation.set_required_spec_claims(&["exp", "iss", "sub", "aud", "principal_kind"]);
        validation.validate_aud = true;
        let claims = decode::<ZitadelClaims>(bearer, &decoding_key, &validation)
            .map_err(|_| IdentityTokenVerificationError::Unauthorized)?
            .claims;
        if claims.sub.trim().is_empty() {
            return Err(IdentityTokenVerificationError::Unauthorized);
        }
        Ok(claims.principal_kind)
    }

    async fn cached_jwks_for_kid(
        &self,
        kid: &str,
    ) -> Result<JwksCache, IdentityTokenVerificationError> {
        if let Some(cache) = self.valid_cached_key(kid).await {
            return Ok(cache);
        }

        let mut refresh_state = self.refresh_gate.lock().await;
        if let Some(cache) = self.valid_cached_key(kid).await {
            return Ok(cache);
        }

        let existing_cache = self.jwks_cache.read().await.clone();
        let kid_is_unknown = existing_cache
            .as_ref()
            .is_none_or(|cache| cache.jwks.find(kid).is_none());
        if kid_is_unknown
            && refresh_state
                .last_unknown_kid_refresh
                .is_some_and(|last| last.elapsed() < self.unknown_kid_refresh_cooldown)
        {
            return existing_cache
                .filter(|cache| cache.expires_at > Utc::now())
                .ok_or(IdentityTokenVerificationError::Infrastructure);
        }

        let cache = match self.refresh_jwks_cache().await {
            Ok(cache) => cache,
            Err(error) => {
                if kid_is_unknown {
                    refresh_state.last_unknown_kid_refresh = Some(Instant::now());
                }
                return Err(error);
            }
        };
        if cache.jwks.find(kid).is_none() {
            refresh_state.last_unknown_kid_refresh = Some(Instant::now());
        }
        *self.jwks_cache.write().await = Some(cache.clone());
        drop(refresh_state);
        Ok(cache)
    }

    async fn valid_cached_key(&self, kid: &str) -> Option<JwksCache> {
        self.jwks_cache
            .read()
            .await
            .as_ref()
            .filter(|cache| cache.expires_at > Utc::now() && cache.jwks.find(kid).is_some())
            .cloned()
    }

    async fn refresh_jwks_cache(&self) -> Result<JwksCache, IdentityTokenVerificationError> {
        let configuration = self
            .client
            .get(self.discovery_url.clone())
            .send()
            .await
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?
            .error_for_status()
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?
            .json::<OidcConfiguration>()
            .await
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?;
        let jwks_url = parse_secure_endpoint_url(&configuration.jwks_uri)?;
        if !same_origin(&self.issuer_url, &jwks_url) {
            return Err(IdentityTokenVerificationError::Infrastructure);
        }
        let jwks = self
            .client
            .get(jwks_url)
            .send()
            .await
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?
            .error_for_status()
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?
            .json::<JwkSet>()
            .await
            .map_err(|_| IdentityTokenVerificationError::Infrastructure)?;
        let cache = JwksCache {
            jwks,
            expires_at: Utc::now() + JWKS_CACHE_TTL,
        };
        Ok(cache)
    }
}

pub fn parse_secure_endpoint_url(raw: &str) -> Result<Url, IdentityTokenVerificationError> {
    let url = Url::parse(raw).map_err(|_| IdentityTokenVerificationError::Infrastructure)?;
    let host = url
        .host_str()
        .ok_or(IdentityTokenVerificationError::Infrastructure)?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(IdentityTokenVerificationError::Infrastructure);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(IdentityTokenVerificationError::Infrastructure);
    }
    Ok(url)
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}
