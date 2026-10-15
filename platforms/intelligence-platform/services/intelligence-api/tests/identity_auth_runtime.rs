// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Identity mode (root ADR-0216): Identity alone decides who the caller is and what it may do.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use intelligence_api::state::AppState;
use intelligence_normalization_application::{
    IdentityPolicyDecision, IdentityPolicyDecisionPort, IdentityPolicyError,
};
use tower::ServiceExt;

const PRINCIPAL: &str = "018f7c6a-0000-7000-8000-000000000101";

type Answer = Result<IdentityPolicyDecision, IdentityPolicyError>;

/// Answers each call from a queue (the last answer repeats) and records what was asked.
struct ScriptedIdentity {
    answers: Mutex<Vec<Answer>>,
    asked: Mutex<Vec<(String, String, String)>>,
}

impl ScriptedIdentity {
    fn new(answers: Vec<Answer>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers),
            asked: Mutex::new(Vec::new()),
        })
    }

    fn asked(&self) -> Vec<(String, String, String)> {
        self.asked.lock().unwrap().clone()
    }
}

#[async_trait]
impl IdentityPolicyDecisionPort for ScriptedIdentity {
    async fn decide(&self, bearer: &str, resource: &str, action: &str, _trace_id: &str) -> Answer {
        self.asked.lock().unwrap().push((
            bearer.to_owned(),
            resource.to_owned(),
            action.to_owned(),
        ));
        let mut answers = self.answers.lock().unwrap();
        if answers.len() > 1 {
            answers.remove(0)
        } else {
            answers[0].clone()
        }
    }
}

fn allow(principal_id: &str) -> Answer {
    Ok(IdentityPolicyDecision::Allow {
        principal_id: principal_id.to_owned(),
    })
}

async fn call(
    identity: Arc<ScriptedIdentity>,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
) -> StatusCode {
    let app = intelligence_api::app(AppState::default().with_identity_inbound_auth(identity));
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    app.oneshot(request.body(Body::from("{}")).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn identity_allow_admits_the_call_with_the_capability_it_asked_for() {
    let identity = ScriptedIdentity::new(vec![allow(PRINCIPAL)]);

    let status = call(
        identity.clone(),
        "GET",
        "/v1/models",
        Some("workload-token"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        identity.asked(),
        vec![(
            "workload-token".to_owned(),
            "intelligence.chat".to_owned(),
            "complete".to_owned()
        )]
    );
}

#[tokio::test]
async fn identity_deny_is_forbidden() {
    let identity = ScriptedIdentity::new(vec![Ok(IdentityPolicyDecision::Deny)]);
    assert_eq!(
        call(identity, "GET", "/v1/models", Some("workload-token")).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn a_credential_identity_rejects_is_unauthorized() {
    let identity = ScriptedIdentity::new(vec![Err(IdentityPolicyError::Unauthenticated)]);
    assert_eq!(
        call(identity, "GET", "/v1/models", Some("expired-token")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn no_bearer_is_unauthorized_without_asking_identity() {
    let identity = ScriptedIdentity::new(vec![allow(PRINCIPAL)]);
    assert_eq!(
        call(identity.clone(), "GET", "/v1/models", None).await,
        StatusCode::UNAUTHORIZED
    );
    assert!(identity.asked().is_empty());
}

#[tokio::test]
async fn an_unreachable_identity_fails_closed() {
    let identity = ScriptedIdentity::new(vec![Err(IdentityPolicyError::Unavailable)]);
    assert_eq!(
        call(identity, "GET", "/v1/models", Some("workload-token")).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn every_action_of_a_route_is_asked_and_one_deny_refuses_it() {
    let identity = ScriptedIdentity::new(vec![
        allow(PRINCIPAL),
        allow(PRINCIPAL),
        Ok(IdentityPolicyDecision::Deny),
    ]);

    let status = call(
        identity.clone(),
        "POST",
        "/intelligence/v1/normalization/generate-validate-submit",
        Some("workload-token"),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let asked: Vec<_> = identity
        .asked()
        .into_iter()
        .map(|(_, resource, action)| format!("{resource}:{action}"))
        .collect();
    assert_eq!(
        asked,
        vec![
            "intelligence.normalization:generate",
            "intelligence.normalization:validate",
            "intelligence.normalization:submit"
        ]
    );
}

#[tokio::test]
async fn answers_about_two_different_principals_are_refused() {
    let identity = ScriptedIdentity::new(vec![
        allow(PRINCIPAL),
        allow("018f7c6a-0000-7000-8000-000000000102"),
    ]);
    assert_eq!(
        call(
            identity,
            "POST",
            "/intelligence/v1/normalization/generate-and-validate",
            Some("workload-token"),
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn metrics_ask_identity_for_the_metrics_capability() {
    let identity = ScriptedIdentity::new(vec![Ok(IdentityPolicyDecision::Deny)]);
    assert_eq!(
        call(identity.clone(), "GET", "/metrics", Some("workload-token")).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        identity.asked()[0].1.as_str(),
        "intelligence.metrics",
        "metrics must be decided by Identity, not by a shared token"
    );
}
