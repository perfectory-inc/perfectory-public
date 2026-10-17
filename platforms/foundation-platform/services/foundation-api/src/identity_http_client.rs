//! Bounded HTTP adapter for Identity Platform policy decisions.

use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity_authorization::IdentityAuthorizationError;
use crate::identity_token_verifier::parse_secure_endpoint_url;

const POLICY_DECISION_PATH: &str = "/identity/v1/policy/decisions";
const MAX_TRANSPORT_ATTEMPTS: usize = 2;

#[derive(Clone)]
pub struct HttpIdentityClient {
    client: Client,
    policy_decision_url: Url,
}

impl HttpIdentityClient {
    pub fn new(
        base_url: impl AsRef<str>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, IdentityAuthorizationError> {
        let base_url = parse_secure_endpoint_url(base_url.as_ref())
            .map_err(|_| IdentityAuthorizationError::Unavailable)?;
        let policy_decision_url = base_url
            .join(POLICY_DECISION_PATH)
            .map_err(|_| IdentityAuthorizationError::Unavailable)?;
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|_| IdentityAuthorizationError::Unavailable)?;
        Ok(Self {
            client,
            policy_decision_url,
        })
    }

    pub async fn authorize(
        &self,
        bearer: &str,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<Uuid, IdentityAuthorizationError> {
        let request = PolicyDecisionRequest {
            resource,
            action,
            resource_id,
            trace_id,
        };

        for attempt in 1..=MAX_TRANSPORT_ATTEMPTS {
            let response = self
                .client
                .post(self.policy_decision_url.clone())
                .bearer_auth(bearer)
                .json(&request)
                .send()
                .await;
            match response {
                Ok(response) => return decode_decision(response, trace_id).await,
                // Only a request that never reached Identity is sent again: re-sending one Identity
                // is still working on doubles its load and cannot answer sooner.
                Err(error) if error.is_connect() && attempt < MAX_TRANSPORT_ATTEMPTS => {
                    tracing::warn!(%trace_id, attempt, %error, "identity policy decision could not connect; retrying");
                }
                Err(error) => {
                    tracing::warn!(
                        %trace_id,
                        attempt,
                        timeout = error.is_timeout(),
                        connect = error.is_connect(),
                        %error,
                        "identity authorization unavailable: the policy decision request failed"
                    );
                    return Err(IdentityAuthorizationError::Unavailable);
                }
            }
        }
        Err(IdentityAuthorizationError::Unavailable)
    }
}

async fn decode_decision(
    response: reqwest::Response,
    trace_id: &str,
) -> Result<Uuid, IdentityAuthorizationError> {
    match response.status() {
        StatusCode::OK => {
            let response = response
                .json::<PolicyDecisionResponse>()
                .await
                .map_err(|error| {
                    tracing::warn!(%trace_id, %error, "identity authorization unavailable: the policy decision body is unreadable");
                    IdentityAuthorizationError::Unavailable
                })?;
            let _ = (&response.reason_code, response.evaluated_at);
            match response.decision {
                ResourceAction::Allow => Ok(response.principal_id),
                ResourceAction::Deny => Err(IdentityAuthorizationError::Forbidden),
            }
        }
        StatusCode::UNAUTHORIZED => Err(IdentityAuthorizationError::Unauthorized),
        StatusCode::FORBIDDEN => Err(IdentityAuthorizationError::Forbidden),
        status => {
            tracing::warn!(%trace_id, %status, "identity authorization unavailable: Identity answered the policy decision with an unexpected status");
            Err(IdentityAuthorizationError::Unavailable)
        }
    }
}

#[derive(Serialize)]
struct PolicyDecisionRequest<'a> {
    resource: &'a str,
    action: &'a str,
    resource_id: Option<&'a str>,
    trace_id: &'a str,
}

#[derive(Deserialize)]
struct PolicyDecisionResponse {
    principal_id: Uuid,
    decision: ResourceAction,
    reason_code: String,
    evaluated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResourceAction {
    Allow,
    Deny,
}

/// Foundation's private wire types are checked against Identity's published `OpenAPI` document, the
/// one definition of the policy decision exchange (root ADR-0214).
#[cfg(test)]
mod tests {
    use super::{PolicyDecisionRequest, PolicyDecisionResponse, ResourceAction};
    use std::collections::BTreeSet;
    use std::error::Error;
    use std::path::Path;

    const IDENTITY_OPENAPI: &str =
        "../../../../platforms/identity-platform/docs/openapi/identity.v1.json";

    fn schema(name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let raw =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(IDENTITY_OPENAPI))?;
        let document: serde_json::Value = serde_json::from_str(&raw)?;
        Ok(document["components"]["schemas"][name].clone())
    }

    fn names(value: &serde_json::Value) -> BTreeSet<String> {
        match value {
            serde_json::Value::Object(map) => map.keys().cloned().collect(),
            serde_json::Value::Array(items) => items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
            _ => BTreeSet::new(),
        }
    }

    #[test]
    fn the_request_is_identitys_policy_decision_request() -> Result<(), Box<dyn Error>> {
        let published = schema("PolicyDecisionRequest")?;
        let sent = serde_json::to_value(PolicyDecisionRequest {
            resource: "foundation.catalog",
            action: "read",
            resource_id: None,
            trace_id: "trace",
        })?;
        assert_eq!(names(&sent), names(&published["properties"]));
        assert!(names(&published["required"]).is_subset(&names(&sent)));
        Ok(())
    }

    #[test]
    fn the_response_reads_identitys_policy_decision_response() -> Result<(), Box<dyn Error>> {
        let published = schema("PolicyDecisionResponse")?;
        assert_eq!(
            names(&published["required"]),
            ["decision", "evaluated_at", "principal_id", "reason_code"]
                .map(str::to_owned)
                .into(),
            "Identity changed the required decision fields Foundation reads"
        );
        for decision in names(&schema("ResourceAction")?["enum"]) {
            let response: PolicyDecisionResponse = serde_json::from_value(serde_json::json!({
                "principal_id": uuid::Uuid::nil(),
                "decision": decision,
                "reason_code": "role_grant",
                "evaluated_at": "2026-10-14T00:00:00Z",
            }))?;
            assert!(matches!(
                (decision.as_str(), response.decision),
                ("allow", ResourceAction::Allow) | ("deny", ResourceAction::Deny)
            ));
        }
        Ok(())
    }
}
