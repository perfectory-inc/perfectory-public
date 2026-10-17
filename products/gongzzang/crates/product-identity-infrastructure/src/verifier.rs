//! `JWT` 검증기 — `RS256` + `JWKS` + `iss`/`aud`/`exp`/`nbf`.

use std::sync::Arc;

use jsonwebtoken::{decode, decode_header, Algorithm, Validation};

use crate::claims::Claims;
use crate::errors::AuthError;
use crate::jwks_cache::JwksCache;

/// Zitadel `JWT` 검증기.
pub struct JwtVerifier {
    issuer: String,
    audience: String,
    jwks: Arc<JwksCache>,
}

impl JwtVerifier {
    /// 검증기 생성. `JWKS` 페치는 첫 verify 호출 시 lazy 수행.
    #[must_use]
    pub const fn new(issuer: String, audience: String, jwks: Arc<JwksCache>) -> Self {
        Self {
            issuer,
            audience,
            jwks,
        }
    }

    /// `JWT` 토큰을 검증해 [`Claims`] 를 반환해요.
    ///
    /// # Errors
    ///
    /// - 헤더 파싱 실패 → [`AuthError::MalformedToken`]
    /// - `kid` 없음 → [`AuthError::UnknownKey`]
    /// - 서명 실패 → [`AuthError::InvalidSignature`]
    /// - `exp` 만료 → [`AuthError::Expired`]
    /// - `nbf` 미도래 → [`AuthError::NotYetValid`]
    /// - `iss` 불일치 → [`AuthError::InvalidIssuer`]
    /// - `aud` 불일치 → [`AuthError::InvalidAudience`]
    /// - `sub` 빈 값 → [`AuthError::MissingSubject`]
    pub async fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::MalformedToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(AuthError::InvalidSignature);
        }
        let kid = header.kid.ok_or(AuthError::UnknownKey)?;
        let key = self.jwks.get_or_fetch(&kid).await?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.validate_aud = false; // 직접 검증 (`Audience::Single|Multiple`)
        validation.leeway = 30; // clock skew 30s
                                // `Validation::new` leaves `nbf` unchecked; the contract above promises NotYetValid. A time
                                // claim of the wrong JSON type is refused, not read as absent (jsonwebtoken >= 10.3,
                                // GHSA-h395-gr6q-cpjc).
        validation.validate_nbf = true;

        let data = decode::<Claims>(token, &key, &validation).map_err(|e| {
            use jsonwebtoken::errors::ErrorKind as E;
            match e.kind() {
                E::ExpiredSignature => AuthError::Expired,
                E::ImmatureSignature => AuthError::NotYetValid,
                E::InvalidIssuer => AuthError::InvalidIssuer,
                // `InvalidSignature` 는 fallback 과 같으므로 명시 안 함 (`clippy::match_same_arms`).
                _ => AuthError::InvalidSignature,
            }
        })?;

        if !data.claims.aud.contains(&self.audience) {
            return Err(AuthError::InvalidAudience);
        }
        if data.claims.sub.trim().is_empty() {
            return Err(AuthError::MissingSubject);
        }
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use super::*;

    fn fixture() -> JwtVerifier {
        let cache = Arc::new(JwksCache::new(
            "http://127.0.0.1:1/jwks".into(),
            reqwest::Client::new(),
        ));
        JwtVerifier::new("http://issuer".into(), "aud".into(), cache)
    }

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
    const TEST_KID: &str = "rsa-test-key";

    /// A verifier whose `JWKS` already holds the test key, and an RS256 signer for it.
    async fn keyed_fixture() -> (JwtVerifier, jsonwebtoken::EncodingKey) {
        let cache = Arc::new(JwksCache::new(
            "http://127.0.0.1:1/jwks".into(),
            reqwest::Client::new(),
        ));
        let key = jsonwebtoken::DecodingKey::from_rsa_components(TEST_RSA_MODULUS, "AQAB").unwrap();
        cache.insert_for_test(TEST_KID, key).await;
        let pem = format!(
            "-----BEGIN {l}-----
{TEST_RSA_PRIVATE_KEY_BODY}
-----END {l}-----",
            l = "PRIVATE KEY"
        );
        let signer = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
        (
            JwtVerifier::new("http://issuer".into(), "aud".into(), cache),
            signer,
        )
    }

    fn claims_json() -> serde_json::Value {
        let now = chrono::Utc::now().timestamp();
        serde_json::json!({"sub": "user-1", "jti": "token-1", "exp": now + 300, "iss": "http://issuer", "aud": "aud"})
    }

    fn signed(
        claims: &serde_json::Value,
        alg: Algorithm,
        key: &jsonwebtoken::EncodingKey,
    ) -> String {
        let mut header = jsonwebtoken::Header::new(alg);
        header.kid = Some(TEST_KID.to_owned());
        jsonwebtoken::encode(&header, claims, key).unwrap()
    }

    #[tokio::test]
    async fn a_valid_token_and_a_started_nbf_are_accepted_and_a_future_nbf_is_not_yet_valid() {
        let (v, key) = keyed_fixture().await;
        let mut claims = claims_json();
        claims["nbf"] = serde_json::json!(chrono::Utc::now().timestamp() - 60);
        assert!(v
            .verify(&signed(&claims, Algorithm::RS256, &key))
            .await
            .is_ok());
        claims["nbf"] = serde_json::json!(chrono::Utc::now().timestamp() + 86_400);
        assert!(matches!(
            expect_err(v.verify(&signed(&claims, Algorithm::RS256, &key)).await),
            AuthError::NotYetValid
        ));
    }

    #[tokio::test]
    async fn a_time_claim_of_the_wrong_type_is_rejected() {
        // GHSA-h395-gr6q-cpjc: a string `nbf` or `exp` must not read as an absent claim.
        let (v, key) = keyed_fixture().await;
        let later = (chrono::Utc::now().timestamp() + 86_400).to_string();
        for claim in ["nbf", "exp"] {
            let mut claims = claims_json();
            claims[claim] = serde_json::json!(later);
            assert!(
                v.verify(&signed(&claims, Algorithm::RS256, &key))
                    .await
                    .is_err(),
                "{claim}"
            );
        }
    }

    #[tokio::test]
    async fn a_token_signed_with_the_public_key_as_an_hmac_secret_is_rejected() {
        // Algorithm confusion: the header says HS256 and the "secret" is the published RSA key.
        let (v, _) = keyed_fixture().await;
        let hmac = jsonwebtoken::EncodingKey::from_secret(TEST_RSA_MODULUS.as_bytes());
        let err = expect_err(
            v.verify(&signed(&claims_json(), Algorithm::HS256, &hmac))
                .await,
        );
        assert!(matches!(err, AuthError::InvalidSignature));
    }

    fn expect_err(r: Result<Claims, AuthError>) -> AuthError {
        match r {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e,
        }
    }

    #[tokio::test]
    async fn malformed_token_returns_malformed() {
        let v = fixture();
        let err = expect_err(v.verify("not-a-jwt").await);
        assert_eq!(err, AuthError::MalformedToken);
    }

    #[tokio::test]
    async fn empty_token_returns_malformed() {
        let v = fixture();
        let err = expect_err(v.verify("").await);
        assert_eq!(err, AuthError::MalformedToken);
    }

    #[tokio::test]
    async fn random_string_returns_malformed() {
        let v = fixture();
        let err = expect_err(v.verify("aaaa.bbbb.cccc").await);
        // base64 가 아닌 random string 도 MalformedToken
        assert_eq!(err, AuthError::MalformedToken);
    }
}

/// 토큰 검증기 — production (`Real(JwtVerifier)`) 또는 dev mock (`Dev`).
///
/// `Dev` 모드는 토큰이 `DEV.<sub>` 형식 시 sub 만 추출해 fake [`Claims`] 반환해요.
/// `JwtVerifier` 의 `RS256`+`JWKS` 검증을 우회하므로 `CI` 빠른 e2e 전용. 실제
/// Zitadel 통합 검증은 별도 sub-project (staging integration test) 에서 처리.
pub enum Verifier {
    /// 진짜 Zitadel `JWT` 검증.
    Real(JwtVerifier),
    /// Dev mock — `DEV.<sub>` 형식 토큰 수용.
    Dev,
}

impl Verifier {
    /// 토큰 검증. `Real` 은 [`JwtVerifier::verify`] 위임, `Dev` 는 `DEV.<sub>` 파싱.
    ///
    /// # Errors
    ///
    /// `Real`: [`JwtVerifier::verify`] 동일.
    /// `Dev`: 형식 불일치 시 [`AuthError::MalformedToken`], `sub` 빈 시 [`AuthError::MissingSubject`].
    pub async fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        match self {
            Self::Real(v) => v.verify(token).await,
            Self::Dev => Self::verify_dev(token),
        }
    }

    fn verify_dev(token: &str) -> Result<Claims, AuthError> {
        let sub = token
            .strip_prefix("DEV.")
            .ok_or(AuthError::MalformedToken)?
            .trim();
        if sub.is_empty() {
            return Err(AuthError::MissingSubject);
        }
        Ok(Claims {
            sub: sub.to_owned(),
            email: Some(format!("{sub}@dev.local")),
            name: Some(sub.to_owned()),
            preferred_username: None,
            jti: format!("dev-{sub}"),
            exp: i64::MAX, // dev 모드는 만료 안 함
            nbf: None,
            iss: "dev-mode".to_owned(),
            aud: crate::claims::Audience::Single("dev-mode".to_owned()),
        })
    }
}

#[cfg(test)]
mod verifier_enum_tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

    use super::*;

    #[tokio::test]
    async fn dev_verify_extracts_sub_and_generates_claims() {
        let v = Verifier::Dev;
        let claims = v.verify("DEV.test-user-1").await.expect("ok");
        assert_eq!(claims.sub, "test-user-1");
        assert_eq!(claims.email.as_deref(), Some("test-user-1@dev.local"));
        assert_eq!(claims.name.as_deref(), Some("test-user-1"));
    }

    #[tokio::test]
    async fn dev_rejects_non_dev_prefix() {
        let v = Verifier::Dev;
        let Err(err) = v.verify("eyJ.something").await else {
            panic!("expected error");
        };
        assert_eq!(err, AuthError::MalformedToken);
    }

    #[tokio::test]
    async fn dev_rejects_empty_sub() {
        let v = Verifier::Dev;
        let Err(err) = v.verify("DEV.").await else {
            panic!("expected error");
        };
        assert_eq!(err, AuthError::MissingSubject);
    }

    #[tokio::test]
    async fn real_dispatches_to_inner_verifier() {
        // Verifier::Real arm 디스패치 자체를 커버 — 토큰 형식 불량으로 즉시 MalformedToken
        let cache = Arc::new(JwksCache::new(
            "http://127.0.0.1:1/jwks".into(),
            reqwest::Client::new(),
        ));
        let inner = JwtVerifier::new("http://issuer".into(), "aud".into(), cache);
        let v = Verifier::Real(inner);
        let Err(err) = v.verify("not-a-jwt").await else {
            panic!("expected error");
        };
        assert_eq!(err, AuthError::MalformedToken);
    }
}
