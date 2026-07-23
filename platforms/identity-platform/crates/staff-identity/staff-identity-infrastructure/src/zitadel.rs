//! Zitadel OIDC bearer verification.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use staff_identity_application::ports::{OidcVerifier, VerifiedOidcClaims};
use staff_identity_domain::StaffIdentityError;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::RwLock;

const ALLOWED_TOKEN_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
];
#[cfg(not(test))]
const ZITADEL_HTTP_TIMEOUT: StdDuration = StdDuration::from_secs(5);
#[cfg(test)]
const ZITADEL_HTTP_TIMEOUT: StdDuration = StdDuration::from_millis(50);

/// Zitadel-backed verifier for staff OIDC bearer tokens.
pub struct ZitadelOidcVerifier {
    issuer_url: String,
    audience: String,
    http: reqwest::Client,
    jwks_cache: Arc<RwLock<Option<JwksCache>>>,
}

#[derive(Clone)]
struct JwksCache {
    jwks: JwkSet,
    expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct OpenIdConfiguration {
    jwks_uri: String,
}

#[derive(Deserialize)]
struct ZitadelClaims {
    sub: String,
    principal_kind: String,
    #[serde(default)]
    jti: Option<String>,
    iat: i64,
    exp: i64,
}

impl ZitadelOidcVerifier {
    /// Creates a verifier for one Zitadel issuer and expected audience.
    #[must_use]
    pub fn new(issuer_url: impl Into<String>, audience: impl Into<String>) -> Self {
        Self {
            issuer_url: issuer_url.into().trim_end_matches('/').to_owned(),
            audience: audience.into(),
            http: reqwest::Client::new(),
            jwks_cache: Arc::new(RwLock::new(None)),
        }
    }

    async fn cached_jwks_for_kid(&self, kid: &str) -> Result<JwksCache, StaffIdentityError> {
        let now = Utc::now();
        if let Some(cache) = self.jwks_cache.read().await.as_ref() {
            if cache.expires_at > now && cache.jwks.find(kid).is_some() {
                return Ok(cache.clone());
            }
        }
        self.refresh_jwks_cache().await
    }

    async fn refresh_jwks_cache(&self) -> Result<JwksCache, StaffIdentityError> {
        let now = Utc::now();
        let configuration = self
            .http
            .get(format!(
                "{}/.well-known/openid-configuration",
                self.issuer_url
            ))
            .timeout(ZITADEL_HTTP_TIMEOUT)
            .send()
            .await
            .map_err(infrastructure)?
            .error_for_status()
            .map_err(infrastructure)?
            .json::<OpenIdConfiguration>()
            .await
            .map_err(infrastructure)?;
        let jwks = self
            .http
            .get(configuration.jwks_uri)
            .timeout(ZITADEL_HTTP_TIMEOUT)
            .send()
            .await
            .map_err(infrastructure)?
            .error_for_status()
            .map_err(infrastructure)?
            .json::<JwkSet>()
            .await
            .map_err(infrastructure)?;
        let cache = JwksCache {
            jwks,
            expires_at: now + Duration::minutes(60),
        };
        *self.jwks_cache.write().await = Some(cache.clone());
        Ok(cache)
    }
}

#[async_trait]
impl OidcVerifier for ZitadelOidcVerifier {
    async fn verify_bearer(
        &self,
        bearer_token: &str,
    ) -> Result<VerifiedOidcClaims, StaffIdentityError> {
        let header = decode_header(bearer_token).map_err(invalid_claims)?;
        let kid = header
            .kid
            .as_deref()
            .ok_or_else(|| StaffIdentityError::InvalidClaims("missing kid".to_owned()))?;
        let cache = self.cached_jwks_for_kid(kid).await?;
        let jwk = cache
            .jwks
            .find(kid)
            .ok_or_else(|| StaffIdentityError::InvalidClaims("unknown kid".to_owned()))?;
        let decoding_key = DecodingKey::from_jwk(jwk).map_err(infrastructure)?;
        let validation = build_validation(header.alg, &self.issuer_url, &self.audience)?;
        let claims = decode::<ZitadelClaims>(bearer_token, &decoding_key, &validation)
            .map_err(invalid_claims)?
            .claims;
        if claims.principal_kind != "staff" {
            return Err(StaffIdentityError::InvalidClaims(
                "principal_kind must be staff".to_owned(),
            ));
        }

        Ok(VerifiedOidcClaims {
            subject: claims.sub,
            jti: token_jti(claims.jti.as_deref(), bearer_token),
            issued_at: timestamp_to_utc(claims.iat, "iat")?,
            expires_at: timestamp_to_utc(claims.exp, "exp")?,
        })
    }
}

fn build_validation(
    algorithm: Algorithm,
    issuer: &str,
    audience: &str,
) -> Result<Validation, StaffIdentityError> {
    if !ALLOWED_TOKEN_ALGORITHMS.contains(&algorithm) {
        return Err(StaffIdentityError::InvalidClaims(format!(
            "token algorithm {algorithm:?} is not allowed"
        )));
    }
    let audience = audience.trim();
    if audience.is_empty() {
        return Err(StaffIdentityError::InvalidClaims(
            "OIDC audience is not configured".to_owned(),
        ));
    }

    let mut validation = Validation::new(algorithm);
    validation.algorithms = vec![algorithm];
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["exp", "iss", "sub", "aud", "principal_kind"]);
    validation.validate_aud = true;
    Ok(validation)
}

fn token_jti(claim_jti: Option<&str>, raw_token: &str) -> String {
    claim_jti
        .map(str::trim)
        .filter(|jti| !jti.is_empty())
        .map_or_else(
            || format!("token-sha256:{}", sha256_hex(raw_token.as_bytes())),
            ToOwned::to_owned,
        )
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .into_iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

fn timestamp_to_utc(value: i64, claim: &str) -> Result<DateTime<Utc>, StaffIdentityError> {
    DateTime::<Utc>::from_timestamp(value, 0)
        .ok_or_else(|| StaffIdentityError::InvalidClaims(format!("invalid {claim} timestamp")))
}

fn invalid_claims(error: impl std::fmt::Display) -> StaffIdentityError {
    StaffIdentityError::InvalidClaims(error.to_string())
}

fn infrastructure(error: impl std::fmt::Display) -> StaffIdentityError {
    StaffIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{build_validation, token_jti, JwksCache, ZitadelOidcVerifier};
    use chrono::{Duration, Utc};
    use jsonwebtoken::jwk::JwkSet;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::Serialize;
    use serde_json::json;
    use staff_identity_application::ports::OidcVerifier;
    use staff_identity_domain::StaffIdentityError;
    use std::collections::HashSet;
    use std::error::Error;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_ISSUER: &str = "https://identity.example.test";
    const TEST_AUDIENCE: &str = "identity-api";
    const TEST_SUBJECT: &str = "staff-subject";
    const TEST_JTI: &str = "staff-token-id";
    const TEST_KID: &str = "rsa-test-key";
    const TEST_RSA_MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
    const TEST_RSA_PRIVATE_KEY_BODY: &str = r"MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc
7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z
IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL
eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz
jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T
yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN
ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J
GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl
qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s
2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh
xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW
tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4
CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf
dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS
55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j
m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl
yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV
DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1
zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW
Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf
34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy
pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS
aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW
GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal
2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT
3wc9h4G8BBCtWN2TN/LsGZdB";

    #[derive(Serialize)]
    struct TestTokenClaims {
        sub: String,
        jti: String,
        iat: i64,
        exp: i64,
        iss: String,
        aud: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        principal_kind: Option<String>,
    }

    #[test]
    fn validation_requires_issuer_audience_subject_and_expiry() -> Result<(), StaffIdentityError> {
        let validation = build_validation(
            Algorithm::RS256,
            "https://identity.example.test",
            "identity-api",
        )?;

        assert_eq!(validation.algorithms, vec![Algorithm::RS256]);
        assert_eq!(
            validation.iss,
            Some(HashSet::from(["https://identity.example.test".to_owned()]))
        );
        assert_eq!(
            validation.aud,
            Some(HashSet::from(["identity-api".to_owned()]))
        );
        assert!(validation.validate_aud);
        assert!(validation.required_spec_claims.contains("exp"));
        assert!(validation.required_spec_claims.contains("iss"));
        assert!(validation.required_spec_claims.contains("sub"));
        assert!(validation.required_spec_claims.contains("aud"));
        Ok(())
    }

    #[test]
    fn validation_rejects_symmetric_algorithms() {
        let result = build_validation(
            Algorithm::HS256,
            "https://identity.example.test",
            "identity-api",
        );

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
    }

    #[test]
    fn validation_rejects_blank_audience() {
        let result = build_validation(Algorithm::RS256, "https://identity.example.test", "  ");

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
    }

    #[test]
    fn missing_jti_uses_a_stable_non_secret_token_fingerprint() {
        let first = token_jti(None, "raw-token");
        let second = token_jti(Some("  "), "raw-token");
        let other = token_jti(None, "other-token");

        assert_eq!(first, second);
        assert_ne!(first, other);
        assert!(first.starts_with("token-sha256:"));
        assert_eq!(first.len(), "token-sha256:".len() + 64);
    }

    #[tokio::test]
    async fn valid_staff_token_is_accepted() -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;
        let token = signed_test_token(&test_claims())?;

        let claims = verifier.verify_bearer(&token).await?;

        assert_eq!(claims.subject, TEST_SUBJECT);
        assert_eq!(claims.jti, TEST_JTI);
        Ok(())
    }

    #[tokio::test]
    async fn staff_verifier_rejects_missing_unknown_and_service_principal_kinds(
    ) -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;

        for principal_kind in [None, Some("unknown"), Some("service")] {
            let mut claims = test_claims();
            claims.principal_kind = principal_kind.map(str::to_owned);
            let result = verifier.verify_bearer(&signed_test_token(&claims)?).await;

            assert!(
                matches!(result, Err(StaffIdentityError::InvalidClaims(_))),
                "accepted principal_kind {principal_kind:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn staff_discovery_status_failure_is_infrastructure() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let verifier = ZitadelOidcVerifier::new(server.uri(), TEST_AUDIENCE);

        let result = verifier
            .verify_bearer(&signed_test_token(&test_claims())?)
            .await;

        assert!(matches!(result, Err(StaffIdentityError::Infrastructure(_))));
        Ok(())
    }

    #[tokio::test]
    async fn staff_discovery_decode_failure_is_infrastructure() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
            .mount(&server)
            .await;
        let verifier = ZitadelOidcVerifier::new(server.uri(), TEST_AUDIENCE);

        let result = verifier
            .verify_bearer(&signed_test_token(&test_claims())?)
            .await;

        assert!(matches!(result, Err(StaffIdentityError::Infrastructure(_))));
        Ok(())
    }

    #[tokio::test]
    async fn staff_discovery_timeout_is_infrastructure() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(std::time::Duration::from_millis(200))
                    .set_body_json(json!({"jwks_uri": format!("{}/keys", server.uri())})),
            )
            .mount(&server)
            .await;
        let verifier = ZitadelOidcVerifier::new(server.uri(), TEST_AUDIENCE);

        let result = verifier
            .verify_bearer(&signed_test_token(&test_claims())?)
            .await;

        assert!(matches!(result, Err(StaffIdentityError::Infrastructure(_))));
        Ok(())
    }

    #[tokio::test]
    async fn staff_jwks_status_and_decode_failures_are_infrastructure() -> Result<(), Box<dyn Error>>
    {
        for jwks_response in [
            ResponseTemplate::new(503),
            ResponseTemplate::new(200).set_body_string("not-json"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/.well-known/openid-configuration"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "jwks_uri": format!("{}/keys", server.uri()),
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/keys"))
                .respond_with(jwks_response)
                .mount(&server)
                .await;
            let verifier = ZitadelOidcVerifier::new(server.uri(), TEST_AUDIENCE);

            let result = verifier
                .verify_bearer(&signed_test_token(&test_claims())?)
                .await;

            assert!(matches!(result, Err(StaffIdentityError::Infrastructure(_))));
        }
        Ok(())
    }

    #[tokio::test]
    async fn staff_token_with_bad_signature_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;
        let token = corrupt_signature(&signed_test_token(&test_claims())?);

        let result = verifier.verify_bearer(&token).await;

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
        Ok(())
    }

    #[tokio::test]
    async fn expired_staff_token_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;
        let mut claims = test_claims();
        claims.exp = Utc::now().timestamp() - 300;
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_bearer(&token).await;

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
        Ok(())
    }

    #[tokio::test]
    async fn staff_token_with_wrong_issuer_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;
        let mut claims = test_claims();
        claims.iss = "https://wrong-issuer.example.test".to_owned();
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_bearer(&token).await;

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
        Ok(())
    }

    #[tokio::test]
    async fn staff_token_with_wrong_audience_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier = verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE).await?;
        let mut claims = test_claims();
        claims.aud = "wrong-audience".to_owned();
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_bearer(&token).await;

        assert!(matches!(result, Err(StaffIdentityError::InvalidClaims(_))));
        Ok(())
    }

    #[tokio::test]
    async fn unknown_cached_kid_refreshes_jwks_and_verifies_token() -> Result<(), Box<dyn Error>> {
        let server = MockServer::start().await;
        let jwks_uri = format!("{}/keys", server.uri());
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jwks_uri": jwks_uri,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/keys"))
            .respond_with(ResponseTemplate::new(200).set_body_json(test_jwks_json(TEST_KID)))
            .expect(1)
            .mount(&server)
            .await;

        let verifier = ZitadelOidcVerifier::new(server.uri(), TEST_AUDIENCE);
        *verifier.jwks_cache.write().await = Some(JwksCache {
            jwks: test_jwks("stale-kid")?,
            expires_at: Utc::now() + Duration::minutes(60),
        });
        let mut claims = test_claims();
        claims.iss = server.uri();
        let token = signed_test_token(&claims)?;

        let claims = verifier.verify_bearer(&token).await?;

        assert_eq!(claims.subject, TEST_SUBJECT);
        Ok(())
    }

    async fn verifier_with_cached_test_jwk(
        issuer: &str,
        audience: &str,
    ) -> Result<ZitadelOidcVerifier, serde_json::Error> {
        let verifier = ZitadelOidcVerifier::new(issuer, audience);
        *verifier.jwks_cache.write().await = Some(JwksCache {
            jwks: test_jwks(TEST_KID)?,
            expires_at: Utc::now() + Duration::minutes(60),
        });
        Ok(verifier)
    }

    fn test_claims() -> TestTokenClaims {
        let now = Utc::now().timestamp();
        TestTokenClaims {
            sub: TEST_SUBJECT.to_owned(),
            jti: TEST_JTI.to_owned(),
            iat: now,
            exp: now + 300,
            iss: TEST_ISSUER.to_owned(),
            aud: TEST_AUDIENCE.to_owned(),
            principal_kind: Some("staff".to_owned()),
        }
    }

    fn signed_test_token(claims: &TestTokenClaims) -> Result<String, jsonwebtoken::errors::Error> {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_owned());
        let private_key = format!(
            "-----BEGIN {label}-----\n{TEST_RSA_PRIVATE_KEY_BODY}\n-----END {label}-----",
            label = "PRIVATE KEY"
        );
        encode(
            &header,
            claims,
            &EncodingKey::from_rsa_pem(private_key.as_bytes())?,
        )
    }

    fn corrupt_signature(token: &str) -> String {
        let Some((signing_input, signature)) = token.rsplit_once('.') else {
            return token.to_owned();
        };
        format!("{signing_input}.{}", "A".repeat(signature.len()))
    }

    fn test_jwks(kid: &str) -> Result<JwkSet, serde_json::Error> {
        serde_json::from_value(test_jwks_json(kid))
    }

    fn test_jwks_json(kid: &str) -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "kid": kid,
                "alg": "RS256",
                "n": TEST_RSA_MODULUS,
                "e": "AQAB"
            }]
        })
    }
}
