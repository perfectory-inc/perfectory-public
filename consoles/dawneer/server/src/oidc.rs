//! Signing a staff member in with Zitadel: authorization code + PKCE + nonce, confidential client,
//! and the ID token verified before anyone is signed in (root ADR-0116 §1, §4).
//!
//! The protocol is `openidconnect` 4.1, not ours: discovery, the authorization URL, the code and
//! refresh exchanges, and the ID token's signature, issuer, audience, expiry and nonce. What is
//! decided here, against that crate's defaults (`src/verification/mod.rs`, `src/client.rs` of
//! 4.1.0):
//! - Only RS256 signatures count. The crate would take the algorithms the discovery document lists,
//!   and with the client secret also HS256; Zitadel signs ID tokens with its RS256 keys.
//! - Zitadel puts the project id beside the client id in `aud`; the crate refuses any other
//!   audience unless told, so the project id, and nothing else, is trusted.
//! - The signing keys are read at every sign-in, so a key Zitadel rotated in is used at once.
//! - The access token's hash (`at_hash`) is checked when the ID token carries one.
//! - Userinfo must name the same `sub` as the verified ID token (OIDC Core 1.0 §5.3.2).

use std::time::Duration;

use openidconnect::core::{
    CoreAuthDisplay, CoreAuthenticationFlow, CoreClaimName, CoreClaimType, CoreClient,
    CoreClientAuthMethod, CoreGrantType, CoreIdTokenVerifier, CoreJsonWebKey, CoreJsonWebKeySet,
    CoreJweContentEncryptionAlgorithm, CoreJweKeyManagementAlgorithm, CoreJwsSigningAlgorithm,
    CoreResponseMode, CoreResponseType, CoreSubjectIdentifierType, CoreTokenResponse,
    CoreUserInfoClaims,
};
use openidconnect::{
    AccessTokenHash, AdditionalProviderMetadata, AuthUrl, AuthorizationCode, ClientId,
    ClientSecret, CsrfToken, EndpointMaybeSet, EndpointNotSet, EndpointSet, IssuerUrl, Nonce,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, ProviderMetadata, RedirectUrl,
    RefreshToken, Scope, TokenResponse,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::Config;

/// The one discovery field the crate's core metadata leaves out: where the browser signs out.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EndSessionMetadata {
    /// RP-initiated logout endpoint.
    pub end_session_endpoint: Option<Url>,
}
impl AdditionalProviderMetadata for EndSessionMetadata {}

type Metadata = ProviderMetadata<
    EndSessionMetadata,
    CoreAuthDisplay,
    CoreClientAuthMethod,
    CoreClaimName,
    CoreClaimType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseMode,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

type Client = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

/// The issuer as discovered, and the client registered with it.
pub struct Provider {
    metadata: Metadata,
    client: Client,
    client_id: ClientId,
    project_id: String,
    /// Where the browser goes to sign out, on the console's origin.
    pub end_session_endpoint: Option<Url>,
}

/// A sign-in leaving for the issuer: where to send the browser and what to keep until it is back.
pub struct Authorization {
    /// The authorization URL, on the console's origin.
    pub url: Url,
    /// The `state` the callback must bring back.
    pub state: String,
    /// The PKCE verifier.
    pub verifier: String,
    /// The nonce the ID token must carry.
    pub nonce: String,
}

/// Tokens the server keeps; the browser never sees them.
#[derive(Clone, Debug)]
pub struct Tokens {
    /// JWT access token Foundation verifies.
    pub access_token: String,
    /// Refresh token (`offline_access`).
    pub refresh_token: Option<String>,
    /// How long the access token lives.
    pub expires_in: Option<Duration>,
    /// The verified ID token, kept only to hint the sign-out.
    pub id_token: Option<String>,
}

impl Tokens {
    fn from_response(response: &CoreTokenResponse) -> Self {
        Self {
            access_token: response.access_token().secret().clone(),
            refresh_token: response.refresh_token().map(|token| token.secret().clone()),
            expires_in: response.expires_in(),
            id_token: response.id_token().map(ToString::to_string),
        }
    }
}

/// Who signed in: the verified ID token's subject, with the name and mail userinfo gives for it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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
    /// The issuer could not be read, or its discovery document is not this issuer's.
    #[error("discovery failed: {0}")]
    Discovery(String),
    /// The configured URLs cannot be used.
    #[error("configuration: {0}")]
    Configuration(String),
    /// The issuer refused the code, verifier or refresh token, or could not be reached.
    #[error("token request failed: {0}")]
    Token(String),
    /// The token response carries no ID token.
    #[error("the token response carries no ID token")]
    NoIdToken,
    /// The ID token's signature, issuer, audience, expiry or nonce does not hold.
    #[error("the ID token does not verify: {0}")]
    IdToken(String),
    /// The access token is not the one the ID token was issued with.
    #[error("the access token does not match the ID token's at_hash")]
    AccessTokenMismatch,
    /// Userinfo failed or names another subject.
    #[error("userinfo failed: {0}")]
    UserInfo(String),
}

/// The scopes Dawneer asks for besides `openid`. The project audience scope is what makes
/// Foundation accept the access token (`aud` must hold the project id).
fn scopes(project_id: &str) -> [String; 4] {
    [
        "profile".to_owned(),
        "email".to_owned(),
        "offline_access".to_owned(),
        format!("urn:zitadel:iam:org:project:id:{project_id}:aud"),
    ]
}

/// The same path and query on the console's origin.
fn on_public_origin(url: &Url, public_base: &Url) -> Url {
    let mut moved = public_base.clone();
    moved.set_path(url.path());
    moved.set_query(url.query());
    moved
}

impl Provider {
    /// Reads the issuer's discovery document and registers the client with it.
    ///
    /// The two endpoints the browser opens point at the console's own origin: the browser never
    /// reaches the issuer, the console relays its sign-in pages (root ADR-0434 §2). The server's own
    /// calls (token, keys, userinfo) stay on the issuer.
    ///
    /// # Errors
    /// Returns [`OidcError`] when the document cannot be read, names another issuer, or the
    /// console's redirect URI cannot be formed.
    pub async fn discover(http: &reqwest::Client, config: &Config) -> Result<Self, OidcError> {
        let issuer = IssuerUrl::new(config.issuer.as_str().trim_end_matches('/').to_owned())
            .map_err(|error| OidcError::Configuration(error.to_string()))?;
        let metadata = Metadata::discover_async(issuer, http)
            .await
            .map_err(|error| OidcError::Discovery(error.to_string()))?;
        let authorization =
            on_public_origin(metadata.authorization_endpoint().url(), &config.public_base);
        let end_session_endpoint = metadata
            .additional_metadata()
            .end_session_endpoint
            .as_ref()
            .map(|url| on_public_origin(url, &config.public_base));
        let metadata = metadata.set_authorization_endpoint(AuthUrl::from_url(authorization));
        let redirect = config
            .redirect_uri()
            .map_err(|error| OidcError::Configuration(error.to_string()))?;
        let client_id = ClientId::new(config.client_id.clone());
        let client = CoreClient::from_provider_metadata(
            metadata.clone(),
            client_id.clone(),
            Some(ClientSecret::new(config.client_secret.clone())),
        )
        .set_redirect_uri(RedirectUrl::from_url(redirect));
        Ok(Self {
            metadata,
            client,
            client_id,
            project_id: config.project_id.clone(),
            end_session_endpoint,
        })
    }

    /// A new sign-in: the authorization URL with a fresh `state`, PKCE challenge and nonce.
    #[must_use]
    pub fn authorize(&self) -> Authorization {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, state, nonce) = self
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scopes(scopes(&self.project_id).into_iter().map(Scope::new))
            .set_pkce_challenge(challenge)
            .url();
        Authorization {
            url,
            state: state.secret().clone(),
            verifier: verifier.secret().clone(),
            nonce: nonce.secret().clone(),
        }
    }

    fn id_token_verifier(&self, keys: CoreJsonWebKeySet) -> CoreIdTokenVerifier<'_> {
        let project_id = self.project_id.clone();
        // A public-client verifier holds no client secret, so an HMAC-signed token cannot verify
        // even if the allowed algorithms ever widened.
        CoreIdTokenVerifier::new_public_client(
            self.client_id.clone(),
            self.metadata.issuer().clone(),
            keys,
        )
        .set_allowed_algs([CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256])
        .set_other_audience_verifier_fn(move |audience| **audience == project_id)
    }

    /// Trades the authorization code for tokens and signs the staff member in only when the ID
    /// token verifies against the nonce this sign-in sent.
    ///
    /// # Errors
    /// Returns [`OidcError`] when the issuer refuses, the ID token is missing or does not verify,
    /// the access token is not the ID token's, or userinfo names another subject.
    pub async fn finish(
        &self,
        http: &reqwest::Client,
        code: String,
        verifier: String,
        nonce: String,
    ) -> Result<(Identity, Tokens), OidcError> {
        let response = self
            .client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|error| OidcError::Configuration(error.to_string()))?
            .set_pkce_verifier(PkceCodeVerifier::new(verifier))
            .request_async(http)
            .await
            .map_err(|error| OidcError::Token(error.to_string()))?;
        let id_token = response.id_token().ok_or(OidcError::NoIdToken)?;
        let keys = CoreJsonWebKeySet::fetch_async(self.metadata.jwks_uri(), http)
            .await
            .map_err(|error| OidcError::Discovery(error.to_string()))?;
        let id_token_verifier = self.id_token_verifier(keys);
        let claims = id_token
            .claims(&id_token_verifier, &Nonce::new(nonce))
            .map_err(|error| OidcError::IdToken(error.to_string()))?;
        if let Some(expected) = claims.access_token_hash() {
            let actual = AccessTokenHash::from_token(
                response.access_token(),
                id_token
                    .signing_alg()
                    .map_err(|error| OidcError::IdToken(error.to_string()))?,
                id_token
                    .signing_key(&id_token_verifier)
                    .map_err(|error| OidcError::IdToken(error.to_string()))?,
            )
            .map_err(|error| OidcError::IdToken(error.to_string()))?;
            if actual != *expected {
                return Err(OidcError::AccessTokenMismatch);
            }
        }
        let subject = claims.subject().clone();
        let info: CoreUserInfoClaims = self
            .client
            .user_info(response.access_token().clone(), Some(subject.clone()))
            .map_err(|error| OidcError::Configuration(error.to_string()))?
            .request_async(http)
            .await
            .map_err(|error| OidcError::UserInfo(error.to_string()))?;
        let identity = Identity {
            sub: subject.as_str().to_owned(),
            name: info
                .name()
                .and_then(|name| name.get(None))
                .map(|name| name.as_str().to_owned())
                .unwrap_or_default(),
            email: info
                .email()
                .map(|email| email.as_str().to_owned())
                .unwrap_or_default(),
        };
        Ok((identity, Tokens::from_response(&response)))
    }

    /// Trades a refresh token for a new access token.
    ///
    /// # Errors
    /// Returns [`OidcError`] when the issuer refuses the refresh token.
    pub async fn refresh(
        &self,
        http: &reqwest::Client,
        refresh_token: &str,
    ) -> Result<Tokens, OidcError> {
        let refresh_token = RefreshToken::new(refresh_token.to_owned());
        let response = self
            .client
            .exchange_refresh_token(&refresh_token)
            .map_err(|error| OidcError::Configuration(error.to_string()))?
            .request_async(http)
            .await
            .map_err(|error| OidcError::Token(error.to_string()))?;
        Ok(Tokens::from_response(&response))
    }

    /// The client id, for the sign-out request.
    #[must_use]
    pub fn client_id(&self) -> &str {
        self.client_id.as_str()
    }
}

/// A fresh random value, base64url, for session ids and CSRF tokens.
///
/// # Errors
/// Returns an error if the operating system has no randomness to give.
pub fn random_token(bytes: usize) -> Result<String, getrandom::Error> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    let mut buffer = vec![0_u8; bytes];
    getrandom::fill(&mut buffer)?;
    Ok(URL_SAFE_NO_PAD.encode(buffer))
}
