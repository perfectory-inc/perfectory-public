//! Zitadel machine-user and client-credentials access-token verification.

use crate::ServicePrincipalReader;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use service_identity_application::ports::ServiceCredentialVerifier;
use service_identity_domain::{ServiceIdentityError, ValidatedServicePrincipal};
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

/// Verifies Zitadel machine-user and client-credentials access tokens.
pub struct ZitadelMachineTokenVerifier {
    issuer_url: String,
    audience: String,
    http: reqwest::Client,
    jwks_cache: Arc<RwLock<Option<JwksCache>>>,
    principal_reader: Arc<dyn ServicePrincipalReader>,
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
struct MachineTokenClaims {
    sub: String,
    principal_kind: String,
}

impl ZitadelMachineTokenVerifier {
    /// Creates a machine-token verifier and Identity principal reader composition.
    #[must_use]
    pub fn new(
        issuer_url: impl Into<String>,
        audience: impl Into<String>,
        principal_reader: Arc<dyn ServicePrincipalReader>,
    ) -> Self {
        Self {
            issuer_url: issuer_url.into().trim_end_matches('/').to_owned(),
            audience: audience.into(),
            http: reqwest::Client::new(),
            jwks_cache: Arc::new(RwLock::new(None)),
            principal_reader,
        }
    }

    async fn resolve_subject(
        &self,
        subject: &str,
    ) -> Result<ValidatedServicePrincipal, ServiceIdentityError> {
        self.principal_reader
            .read_by_zitadel_subject(subject)
            .await?
            .ok_or(ServiceIdentityError::InvalidCredential)
    }

    async fn cached_jwks_for_kid(&self, kid: &str) -> Result<JwksCache, ServiceIdentityError> {
        let now = Utc::now();
        if let Some(cache) = self.jwks_cache.read().await.as_ref() {
            if cache.expires_at > now && cache.jwks.find(kid).is_some() {
                return Ok(cache.clone());
            }
        }
        self.refresh_jwks_cache().await
    }

    async fn refresh_jwks_cache(&self) -> Result<JwksCache, ServiceIdentityError> {
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
impl ServiceCredentialVerifier for ZitadelMachineTokenVerifier {
    async fn verify_credential(
        &self,
        bearer_token: &str,
    ) -> Result<ValidatedServicePrincipal, ServiceIdentityError> {
        let header = decode_header(bearer_token).map_err(invalid_credential)?;
        let kid = header
            .kid
            .as_deref()
            .ok_or(ServiceIdentityError::InvalidCredential)?;
        let cache = self.cached_jwks_for_kid(kid).await?;
        let jwk = cache
            .jwks
            .find(kid)
            .ok_or(ServiceIdentityError::InvalidCredential)?;
        let decoding_key = DecodingKey::from_jwk(jwk).map_err(infrastructure)?;
        let validation = build_validation(header.alg, &self.issuer_url, &self.audience)?;
        let claims = decode::<MachineTokenClaims>(bearer_token, &decoding_key, &validation)
            .map_err(invalid_credential)?
            .claims;
        if claims.principal_kind != "service" {
            return Err(ServiceIdentityError::InvalidCredential);
        }
        self.resolve_subject(&claims.sub).await
    }
}

fn build_validation(
    algorithm: Algorithm,
    issuer: &str,
    audience: &str,
) -> Result<Validation, ServiceIdentityError> {
    if !ALLOWED_TOKEN_ALGORITHMS.contains(&algorithm) || audience.trim().is_empty() {
        return Err(ServiceIdentityError::InvalidCredential);
    }
    let audience = audience.trim();
    let mut validation = Validation::new(algorithm);
    validation.algorithms = vec![algorithm];
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["exp", "iss", "sub", "aud", "principal_kind"]);
    validation.validate_aud = true;
    Ok(validation)
}

fn invalid_credential(_error: impl std::fmt::Display) -> ServiceIdentityError {
    ServiceIdentityError::InvalidCredential
}

fn infrastructure(error: impl std::fmt::Display) -> ServiceIdentityError {
    ServiceIdentityError::Infrastructure(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{build_validation, JwksCache, ZitadelMachineTokenVerifier};
    use crate::ServicePrincipalReader;
    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use identity_contracts::PrincipalId;
    use jsonwebtoken::jwk::JwkSet;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::Serialize;
    use serde_json::json;
    use service_identity_application::ports::ServiceCredentialVerifier;
    use service_identity_domain::ServiceIdentityError;
    use service_identity_domain::ValidatedServicePrincipal;
    use std::collections::HashSet;
    use std::error::Error;
    use std::sync::Arc;
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_ISSUER: &str = "https://identity.example.test";
    const TEST_AUDIENCE: &str = "identity-api";
    const TEST_SUBJECT: &str = "machine-subject";
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
        exp: i64,
        iss: String,
        aud: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        principal_kind: Option<String>,
    }

    struct FakeReader {
        principal: Option<ValidatedServicePrincipal>,
    }

    #[async_trait]
    impl ServicePrincipalReader for FakeReader {
        async fn read_by_zitadel_subject(
            &self,
            _subject: &str,
        ) -> Result<Option<ValidatedServicePrincipal>, ServiceIdentityError> {
            Ok(self.principal.clone())
        }
    }

    #[test]
    fn machine_token_validation_pins_issuer_audience_and_asymmetric_algorithm(
    ) -> Result<(), ServiceIdentityError> {
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
    fn machine_token_validation_rejects_symmetric_algorithms() {
        let result = build_validation(
            Algorithm::HS256,
            "https://identity.example.test",
            "identity-api",
        );
        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
    }

    #[test]
    fn machine_token_validation_rejects_blank_audience() {
        let result = build_validation(Algorithm::RS256, "https://identity.example.test", "  ");
        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
    }

    #[tokio::test]
    async fn verified_unknown_subject_is_rejected_as_an_invalid_credential() {
        let verifier = ZitadelMachineTokenVerifier::new(
            "https://identity.example.test",
            "identity-api",
            Arc::new(FakeReader { principal: None }),
        );

        let result = verifier.resolve_subject("unknown-subject").await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
    }

    #[tokio::test]
    async fn valid_machine_token_is_accepted() -> Result<(), Box<dyn Error>> {
        let expected = test_principal();
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(expected.clone()))
                .await?;
        let token = signed_test_token(&test_claims())?;

        let principal = verifier.verify_credential(&token).await?;

        assert_eq!(principal, expected);
        Ok(())
    }

    #[tokio::test]
    async fn service_verifier_rejects_missing_unknown_and_staff_principal_kinds(
    ) -> Result<(), Box<dyn Error>> {
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(test_principal()))
                .await?;

        for principal_kind in [None, Some("unknown"), Some("staff")] {
            let mut claims = test_claims();
            claims.principal_kind = principal_kind.map(str::to_owned);
            let result = verifier
                .verify_credential(&signed_test_token(&claims)?)
                .await;

            assert!(
                matches!(result, Err(ServiceIdentityError::InvalidCredential)),
                "accepted principal_kind {principal_kind:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn service_discovery_status_and_decode_failures_are_infrastructure(
    ) -> Result<(), Box<dyn Error>> {
        for discovery_response in [
            ResponseTemplate::new(503),
            ResponseTemplate::new(200).set_body_string("not-json"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/.well-known/openid-configuration"))
                .respond_with(discovery_response)
                .mount(&server)
                .await;
            let verifier = ZitadelMachineTokenVerifier::new(
                server.uri(),
                TEST_AUDIENCE,
                Arc::new(FakeReader {
                    principal: Some(test_principal()),
                }),
            );

            let result = verifier
                .verify_credential(&signed_test_token(&test_claims())?)
                .await;

            assert!(matches!(
                result,
                Err(ServiceIdentityError::Infrastructure(_))
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn service_discovery_timeout_is_infrastructure() -> Result<(), Box<dyn Error>> {
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
        let verifier = ZitadelMachineTokenVerifier::new(
            server.uri(),
            TEST_AUDIENCE,
            Arc::new(FakeReader {
                principal: Some(test_principal()),
            }),
        );

        let result = verifier
            .verify_credential(&signed_test_token(&test_claims())?)
            .await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::Infrastructure(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn service_jwks_status_and_decode_failures_are_infrastructure(
    ) -> Result<(), Box<dyn Error>> {
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
            let verifier = ZitadelMachineTokenVerifier::new(
                server.uri(),
                TEST_AUDIENCE,
                Arc::new(FakeReader {
                    principal: Some(test_principal()),
                }),
            );

            let result = verifier
                .verify_credential(&signed_test_token(&test_claims())?)
                .await;

            assert!(matches!(
                result,
                Err(ServiceIdentityError::Infrastructure(_))
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn machine_token_with_bad_signature_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(test_principal()))
                .await?;
        let token = corrupt_signature(&signed_test_token(&test_claims())?);

        let result = verifier.verify_credential(&token).await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn expired_machine_token_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(test_principal()))
                .await?;
        let mut claims = test_claims();
        claims.exp = Utc::now().timestamp() - 300;
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_credential(&token).await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn machine_token_with_wrong_issuer_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(test_principal()))
                .await?;
        let mut claims = test_claims();
        claims.iss = "https://wrong-issuer.example.test".to_owned();
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_credential(&token).await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn machine_token_with_wrong_audience_is_rejected() -> Result<(), Box<dyn Error>> {
        let verifier =
            verifier_with_cached_test_jwk(TEST_ISSUER, TEST_AUDIENCE, Some(test_principal()))
                .await?;
        let mut claims = test_claims();
        claims.aud = "wrong-audience".to_owned();
        let token = signed_test_token(&claims)?;

        let result = verifier.verify_credential(&token).await;

        assert!(matches!(
            result,
            Err(ServiceIdentityError::InvalidCredential)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unknown_cached_kid_refreshes_jwks_and_verifies_machine_token(
    ) -> Result<(), Box<dyn Error>> {
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
        let principal = test_principal();
        let verifier = ZitadelMachineTokenVerifier::new(
            server.uri(),
            TEST_AUDIENCE,
            Arc::new(FakeReader {
                principal: Some(principal.clone()),
            }),
        );
        *verifier.jwks_cache.write().await = Some(JwksCache {
            jwks: test_jwks("stale-kid")?,
            expires_at: Utc::now() + Duration::minutes(60),
        });
        let mut claims = test_claims();
        claims.iss = server.uri();
        let token = signed_test_token(&claims)?;

        let verified_principal = verifier.verify_credential(&token).await?;

        assert_eq!(verified_principal, principal);
        Ok(())
    }

    async fn verifier_with_cached_test_jwk(
        issuer: &str,
        audience: &str,
        principal: Option<ValidatedServicePrincipal>,
    ) -> Result<ZitadelMachineTokenVerifier, serde_json::Error> {
        let verifier =
            ZitadelMachineTokenVerifier::new(issuer, audience, Arc::new(FakeReader { principal }));
        *verifier.jwks_cache.write().await = Some(JwksCache {
            jwks: test_jwks(TEST_KID)?,
            expires_at: Utc::now() + Duration::minutes(60),
        });
        Ok(verifier)
    }

    fn test_principal() -> ValidatedServicePrincipal {
        ValidatedServicePrincipal {
            principal_id: PrincipalId::new(Uuid::nil()),
            capabilities: Vec::new(),
        }
    }

    fn test_claims() -> TestTokenClaims {
        TestTokenClaims {
            sub: TEST_SUBJECT.to_owned(),
            exp: Utc::now().timestamp() + 300,
            iss: TEST_ISSUER.to_owned(),
            aud: TEST_AUDIENCE.to_owned(),
            principal_kind: Some("service".to_owned()),
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
