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

        for attempt in 0..MAX_TRANSPORT_ATTEMPTS {
            let response = self
                .client
                .post(self.policy_decision_url.clone())
                .bearer_auth(bearer)
                .json(&request)
                .send()
                .await;
            match response {
                Ok(response) => return decode_decision(response).await,
                Err(error) if is_safe_transport_failure(&error) => {
                    if attempt + 1 == MAX_TRANSPORT_ATTEMPTS {
                        return Err(IdentityAuthorizationError::Unavailable);
                    }
                }
                Err(_) => return Err(IdentityAuthorizationError::Unavailable),
            }
        }
        Err(IdentityAuthorizationError::Unavailable)
    }
}

fn is_safe_transport_failure(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

async fn decode_decision(response: reqwest::Response) -> Result<Uuid, IdentityAuthorizationError> {
    match response.status() {
        StatusCode::OK => {
            let response = response
                .json::<PolicyDecisionResponse>()
                .await
                .map_err(|_| IdentityAuthorizationError::Unavailable)?;
            let _ = (&response.reason_code, response.evaluated_at);
            match response.decision {
                ResourceAction::Allow => Ok(response.principal_id),
                ResourceAction::Deny => Err(IdentityAuthorizationError::Forbidden),
            }
        }
        StatusCode::UNAUTHORIZED => Err(IdentityAuthorizationError::Unauthorized),
        StatusCode::FORBIDDEN => Err(IdentityAuthorizationError::Forbidden),
        _ => Err(IdentityAuthorizationError::Unavailable),
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
