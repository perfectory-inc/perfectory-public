//! Bounded HTTP adapter for Identity Platform policy decisions (root ADR-0216).
//!
//! Intelligence never decides who a caller is or what it may do: it forwards the caller's
//! bearer to Identity's published `POST /identity/v1/policy/decisions` and trusts only the
//! typed answer.

use std::time::Duration;

use async_trait::async_trait;
use intelligence_normalization_application::{
    IdentityPolicyDecision, IdentityPolicyDecisionPort, IdentityPolicyError,
};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};

use crate::foundation_platform::parse_secure_base_url;

/// Identity's published policy-decision path (`docs/openapi/identity.v1.json`).
pub const POLICY_DECISION_PATH: &str = "/identity/v1/policy/decisions";
const MAX_TRANSPORT_ATTEMPTS: usize = 2;

#[derive(Clone, Debug)]
pub struct HttpIdentityPolicyClient {
    client: Client,
    policy_decision_url: Url,
}

impl HttpIdentityPolicyClient {
    /// Builds the client for an Identity base URL.
    ///
    /// # Errors
    /// Returns a safe message when the URL is not HTTPS (loopback HTTP excepted), carries
    /// credentials, or the HTTP client cannot be built.
    pub fn new(
        base_url: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        let base_url = parse_secure_base_url(base_url, "identity-platform")?;
        let policy_decision_url = base_url
            .join(POLICY_DECISION_PATH)
            .map_err(|_| "identity-platform policy decision endpoint is invalid".to_owned())?;
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| "identity-platform HTTP client could not be built".to_owned())?;
        Ok(Self {
            client,
            policy_decision_url,
        })
    }
}

#[async_trait]
impl IdentityPolicyDecisionPort for HttpIdentityPolicyClient {
    async fn decide(
        &self,
        bearer: &str,
        resource: &str,
        action: &str,
        trace_id: &str,
    ) -> Result<IdentityPolicyDecision, IdentityPolicyError> {
        let request = PolicyDecisionRequest {
            resource,
            action,
            resource_id: None,
            trace_id,
        };
        for attempt in 0..MAX_TRANSPORT_ATTEMPTS {
            let response = self
                .client
                .post(self.policy_decision_url.clone())
                .bearer_auth(bearer)
                .json(&request)
                .send()
                .await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .bytes()
                        .await
                        .map_err(|_| IdentityPolicyError::Unavailable)?;
                    return decode_decision(status, &body);
                }
                // Only a failure before the request reached Identity is retried.
                Err(error) if error.is_connect() && attempt + 1 < MAX_TRANSPORT_ATTEMPTS => {}
                Err(_) => return Err(IdentityPolicyError::Unavailable),
            }
        }
        Err(IdentityPolicyError::Unavailable)
    }
}

/// Maps Identity's answer to a decision; anything outside the published contract fails closed.
pub fn decode_decision(
    status: StatusCode,
    body: &[u8],
) -> Result<IdentityPolicyDecision, IdentityPolicyError> {
    match status {
        StatusCode::OK => {
            let response: PolicyDecisionResponse =
                serde_json::from_slice(body).map_err(|_| IdentityPolicyError::Unavailable)?;
            match response.decision {
                Decision::Allow => Ok(IdentityPolicyDecision::Allow {
                    principal_id: response.principal_id.to_string(),
                }),
                Decision::Deny => Ok(IdentityPolicyDecision::Deny),
            }
        }
        StatusCode::UNAUTHORIZED => Err(IdentityPolicyError::Unauthenticated),
        StatusCode::FORBIDDEN => Ok(IdentityPolicyDecision::Deny),
        _ => Err(IdentityPolicyError::Unavailable),
    }
}

/// Request body; its field names are checked against Identity's OpenAPI document by a test.
#[derive(Serialize)]
pub struct PolicyDecisionRequest<'a> {
    pub resource: &'a str,
    pub action: &'a str,
    pub resource_id: Option<&'a str>,
    pub trace_id: &'a str,
}

/// The response fields this adapter reads, for the OpenAPI contract test.
pub const READ_RESPONSE_FIELDS: [&str; 2] = ["principal_id", "decision"];

#[derive(Deserialize)]
struct PolicyDecisionResponse {
    principal_id: uuid::Uuid,
    decision: Decision,
}

/// Identity's `ResourceAction`; the OpenAPI test checks these are exactly its enum values.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Decision {
    Allow,
    Deny,
}

/// The `ResourceAction` values this adapter understands, for the OpenAPI contract test.
pub const DECISION_VALUES: [&str; 2] = ["allow", "deny"];

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const PRINCIPAL: &str = "018f7c6a-0000-7000-8000-000000000001";

    fn body(decision: &str) -> Vec<u8> {
        format!(
            r#"{{"principal_id":"{PRINCIPAL}","decision":"{decision}","reason_code":"service_capability","evaluated_at":"2026-01-01T00:00:00Z"}}"#
        )
        .into_bytes()
    }

    #[test]
    fn allow_carries_the_principal_identity_evaluated() {
        assert_eq!(
            decode_decision(StatusCode::OK, &body("allow")),
            Ok(IdentityPolicyDecision::Allow {
                principal_id: PRINCIPAL.to_owned()
            })
        );
    }

    #[test]
    fn deny_unauthorized_and_off_contract_answers_never_allow() {
        assert_eq!(
            decode_decision(StatusCode::OK, &body("deny")),
            Ok(IdentityPolicyDecision::Deny)
        );
        assert_eq!(
            decode_decision(StatusCode::UNAUTHORIZED, b""),
            Err(IdentityPolicyError::Unauthenticated)
        );
        assert_eq!(
            decode_decision(StatusCode::OK, &body("maybe")),
            Err(IdentityPolicyError::Unavailable)
        );
        assert_eq!(
            decode_decision(StatusCode::INTERNAL_SERVER_ERROR, b""),
            Err(IdentityPolicyError::Unavailable)
        );
    }

    #[test]
    fn plain_http_is_refused_off_loopback() {
        let error = HttpIdentityPolicyClient::new(
            "http://identity.example",
            Duration::from_secs(1),
            Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(error.contains("HTTPS"));
    }
}
