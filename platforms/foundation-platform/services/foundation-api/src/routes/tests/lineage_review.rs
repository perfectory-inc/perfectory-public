//! The parcel-lineage steward routes (root ADR-0115): who may call them, what reaches the store,
//! and what each refusal answers.

use super::*;
use chrono::{TimeZone, Utc};
use stewardship_application::{
    DecideCommand, DecideOutcome, DecisionRecord, LineageStewardshipStore, ReviewItemFilter,
    ReviewItemView,
};
use stewardship_domain::{Claim, Outcome, ReviewItem, ReviewStatus, StewardshipError};
use utoipa::OpenApi;

const REVIEWER_TOKEN: &str = "synthetic-reviewer-token";
const ADJUDICATOR_TOKEN: &str = "synthetic-adjudicator-token";
const ITEM: &str = "00000000-0000-5000-8000-000000000001";
const PNU: &str = "9999930100100010000";

fn reviewer() -> uuid::Uuid {
    uuid::Uuid::from_u128(1)
}

fn adjudicator() -> uuid::Uuid {
    uuid::Uuid::from_u128(2)
}

/// The reviewer token may review; the adjudicator token may also adjudicate.
#[derive(Default)]
struct StewardAuthorization {
    asked: Mutex<Vec<(String, String, Option<String>)>>,
}

#[async_trait]
impl IdentityAuthorization for StewardAuthorization {
    async fn authorize(
        &self,
        bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        self.asked.lock().await.push((
            resource.to_owned(),
            action.to_owned(),
            resource_id.map(ToOwned::to_owned),
        ));
        let principal = match (bearer, action) {
            (REVIEWER_TOKEN, "review") => reviewer(),
            (ADJUDICATOR_TOKEN, "review" | "adjudicate") => adjudicator(),
            _ => return Err(IdentityAuthorizationError::Forbidden),
        };
        if required_principal_kind != RequiredPrincipalKind::Staff
            || resource != "foundation.lineage"
        {
            return Err(IdentityAuthorizationError::Forbidden);
        }
        Ok(AuthorizedPrincipal {
            principal_id: principal,
            trace_id: trace_id.to_owned(),
        })
    }
}

fn record() -> DecisionRecord {
    DecisionRecord {
        decision_id: uuid::Uuid::from_u128(9),
        item_id: uuid::Uuid::from_u128(1),
        subject_code: PNU.to_owned(),
        outcome: Outcome::NotALink,
        predecessor_code: None,
        decided_by: reviewer(),
        decided_at: Utc
            .with_ymd_and_hms(2099, 9, 30, 12, 0, 0)
            .single()
            .unwrap_or_default(),
        requires_approval: false,
        approval: None,
        supersedes_decision_id: None,
    }
}

/// Records every call and answers with a configured error, if any.
#[derive(Default)]
struct RecordingStewardStore {
    decisions: Mutex<Vec<DecideCommand>>,
    approvals: Mutex<Vec<(uuid::Uuid, uuid::Uuid, bool)>>,
    error: Mutex<Option<StewardshipError>>,
}

impl RecordingStewardStore {
    fn failing(error: StewardshipError) -> Self {
        Self {
            error: Mutex::new(Some(error)),
            ..Self::default()
        }
    }

    async fn configured(&self) -> Result<(), StewardshipError> {
        self.error.lock().await.take().map_or(Ok(()), Err)
    }
}

#[async_trait]
impl LineageStewardshipStore for RecordingStewardStore {
    async fn list_items(
        &self,
        _filter: ReviewItemFilter,
        _now: chrono::DateTime<Utc>,
    ) -> Result<Vec<ReviewItemView>, StewardshipError> {
        self.configured().await?;
        Ok(vec![ReviewItemView {
            item: ReviewItem {
                item_id: uuid::Uuid::from_u128(1),
                subject_code: PNU.to_owned(),
                status: ReviewStatus::Pending,
                candidates_json: "[]".to_owned(),
            },
            claim: None,
            decisions: Vec::new(),
        }])
    }

    async fn get_item(
        &self,
        _item_id: uuid::Uuid,
        _now: chrono::DateTime<Utc>,
    ) -> Result<ReviewItemView, StewardshipError> {
        self.configured().await?;
        Err(StewardshipError::ItemNotFound)
    }

    async fn claim(
        &self,
        _item_id: uuid::Uuid,
        steward: uuid::Uuid,
        now: chrono::DateTime<Utc>,
    ) -> Result<Claim, StewardshipError> {
        self.configured().await?;
        Ok(Claim {
            claimed_by: steward,
            expires_at: now,
        })
    }

    async fn release(
        &self,
        _item_id: uuid::Uuid,
        _steward: uuid::Uuid,
    ) -> Result<(), StewardshipError> {
        self.configured().await
    }

    async fn decide(&self, command: DecideCommand) -> Result<DecideOutcome, StewardshipError> {
        self.decisions.lock().await.push(command);
        self.configured().await?;
        Ok(DecideOutcome::Recorded(record()))
    }

    async fn approve(
        &self,
        decision_id: uuid::Uuid,
        approver: uuid::Uuid,
        accept: bool,
        _note: String,
    ) -> Result<DecisionRecord, StewardshipError> {
        self.approvals
            .lock()
            .await
            .push((decision_id, approver, accept));
        self.configured().await?;
        Ok(record())
    }
}

fn app(
    authorization: Arc<StewardAuthorization>,
    store: Arc<RecordingStewardStore>,
) -> Result<Router, Box<dyn Error>> {
    let state = AppState::bootstrap_for_test_with_identity_authorization(authorization)?
        .with_lineage_stewardship_store(store);
    Ok(router(Arc::new(state)))
}

fn decision_body() -> String {
    serde_json::json!({
        "outcome": "not_a_link",
        "reason_code": "site_survey",
        "note": "",
        "evidence_etag": "0".repeat(64),
    })
    .to_string()
}

fn request(
    method: Method,
    uri: &str,
    token: Option<&str>,
    key: Option<&str>,
    body: String,
) -> Result<Request<Body>, axum::http::Error> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(key) = key {
        builder = builder.header("idempotency-key", key);
    }
    builder.body(Body::from(body))
}

fn decide_uri(query: &str) -> String {
    format!("/catalog/v1/lineage-review/items/{ITEM}/decisions{query}")
}

#[tokio::test]
async fn a_decision_without_a_token_is_refused_before_the_store() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(RecordingStewardStore::default());
    let response = app(Arc::default(), store.clone())?
        .oneshot(request(
            Method::POST,
            &decide_uri(""),
            None,
            Some("decide-key-0001"),
            decision_body(),
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(store.decisions.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn a_decision_reaches_the_store_as_the_authorized_steward() -> Result<(), Box<dyn Error>> {
    let authorization = Arc::new(StewardAuthorization::default());
    let store = Arc::new(RecordingStewardStore::default());
    let response = app(authorization.clone(), store.clone())?
        .oneshot(request(
            Method::POST,
            &decide_uri(""),
            Some(REVIEWER_TOKEN),
            Some("decide-key-0001"),
            decision_body(),
        )?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    assert_eq!(body["disposition"], "recorded");
    let decisions = store.decisions.lock().await;
    assert_eq!(decisions.len(), 1);
    assert_eq!(
        decisions[0].decider,
        reviewer(),
        "the decider is the token's principal, not the body"
    );
    assert_eq!(decisions[0].idempotency_key, "decide-key-0001");
    assert!(!decisions[0].dry_run);
    drop(decisions);
    assert_eq!(
        authorization.asked.lock().await.as_slice(),
        [(
            "foundation.lineage".to_owned(),
            "review".to_owned(),
            Some(ITEM.to_owned())
        )]
    );
    Ok(())
}

#[tokio::test]
async fn a_decision_needs_an_idempotency_key_unless_it_is_a_dry_run() -> Result<(), Box<dyn Error>>
{
    let store = Arc::new(RecordingStewardStore::default());
    let router = app(Arc::default(), store.clone())?;
    let missing = router
        .clone()
        .oneshot(request(
            Method::POST,
            &decide_uri(""),
            Some(REVIEWER_TOKEN),
            None,
            decision_body(),
        )?)
        .await?;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    assert!(store.decisions.lock().await.is_empty());

    let dry = router
        .oneshot(request(
            Method::POST,
            &decide_uri("?dry_run=true"),
            Some(REVIEWER_TOKEN),
            None,
            decision_body(),
        )?)
        .await?;
    assert_eq!(dry.status(), StatusCode::OK);
    assert!(store.decisions.lock().await[0].dry_run);
    Ok(())
}

#[tokio::test]
async fn a_body_cannot_smuggle_fields_the_contract_does_not_name() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(RecordingStewardStore::default());
    let mut body: serde_json::Value = serde_json::from_str(&decision_body())?;
    body["decided_by"] = serde_json::json!(adjudicator().to_string());
    let response = app(Arc::default(), store.clone())?
        .oneshot(request(
            Method::POST,
            &decide_uri(""),
            Some(REVIEWER_TOKEN),
            Some("decide-key-0001"),
            body.to_string(),
        )?)
        .await?;
    assert!(response.status().is_client_error(), "{}", response.status());
    assert!(store.decisions.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn store_refusals_answer_with_their_own_status() -> Result<(), Box<dyn Error>> {
    for (error, status) in [
        (
            StewardshipError::StaleEvidence {
                current: "x".to_owned(),
            },
            StatusCode::CONFLICT,
        ),
        (StewardshipError::IdempotencyConflict, StatusCode::CONFLICT),
        (
            StewardshipError::ClaimedByAnother { until: Utc::now() },
            StatusCode::CONFLICT,
        ),
        (
            StewardshipError::NotACandidate(PNU.to_owned()),
            StatusCode::BAD_REQUEST,
        ),
        (StewardshipError::ItemNotFound, StatusCode::NOT_FOUND),
        (
            StewardshipError::Persistence("db down".to_owned()),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let store = Arc::new(RecordingStewardStore::failing(error.clone()));
        let response = app(Arc::default(), store)?
            .oneshot(request(
                Method::POST,
                &decide_uri(""),
                Some(REVIEWER_TOKEN),
                Some("decide-key-0001"),
                decision_body(),
            )?)
            .await?;
        assert_eq!(response.status(), status, "{error:?}");
    }
    Ok(())
}

#[tokio::test]
async fn only_an_adjudicator_rules_and_never_on_their_own_decision() -> Result<(), Box<dyn Error>> {
    let uri = format!("/catalog/v1/lineage-review/decisions/{ITEM}/approval");
    let body = serde_json::json!({"verdict": "approved"}).to_string();

    let store = Arc::new(RecordingStewardStore::default());
    let reviewer_only = app(Arc::default(), store.clone())?
        .oneshot(request(
            Method::POST,
            &uri,
            Some(REVIEWER_TOKEN),
            None,
            body.clone(),
        )?)
        .await?;
    assert_eq!(reviewer_only.status(), StatusCode::FORBIDDEN);
    assert!(store.approvals.lock().await.is_empty());

    let ruled = app(Arc::default(), store.clone())?
        .oneshot(request(
            Method::POST,
            &uri,
            Some(ADJUDICATOR_TOKEN),
            None,
            body.clone(),
        )?)
        .await?;
    assert_eq!(ruled.status(), StatusCode::OK);
    assert_eq!(
        store.approvals.lock().await.as_slice(),
        [(uuid::Uuid::parse_str(ITEM)?, adjudicator(), true)]
    );

    let own = Arc::new(RecordingStewardStore::failing(
        StewardshipError::SelfApproval,
    ));
    let refused = app(Arc::default(), own)?
        .oneshot(request(
            Method::POST,
            &uri,
            Some(ADJUDICATOR_TOKEN),
            None,
            body,
        )?)
        .await?;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn a_list_with_a_malformed_filter_is_refused() -> Result<(), Box<dyn Error>> {
    let router = app(Arc::default(), Arc::default())?;
    for query in ["?limit=0", "?limit=201", "?prefix=28a", "?after=123"] {
        let response = router
            .clone()
            .oneshot(request(
                Method::GET,
                &format!("/catalog/v1/lineage-review/items{query}"),
                Some(REVIEWER_TOKEN),
                None,
                String::new(),
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
    }
    let ok = router
        .oneshot(request(
            Method::GET,
            "/catalog/v1/lineage-review/items?prefix=99999&limit=1",
            Some(REVIEWER_TOKEN),
            None,
            String::new(),
        )?)
        .await?;
    assert_eq!(ok.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(ok.into_body(), usize::MAX).await?)?;
    assert_eq!(body["items"][0]["subject_pnu"], PNU);
    assert_eq!(
        body["next_after"], PNU,
        "a full page names where the next one starts"
    );
    assert_eq!(
        body["items"][0]["evidence_etag"],
        stewardship_domain::evidence_etag(ReviewStatus::Pending, "[]")
    );
    Ok(())
}

#[test]
fn the_openapi_document_names_every_route_and_its_staff_security() -> Result<(), Box<dyn Error>> {
    let document =
        serde_json::to_value(super::super::lineage_review::LineageReviewApiDoc::openapi())?;
    let paths = document["paths"]
        .as_object()
        .ok_or("paths must be an object")?;
    for path in [
        "/catalog/v1/lineage-review/items",
        "/catalog/v1/lineage-review/items/{id}",
        "/catalog/v1/lineage-review/items/{id}/claim",
        "/catalog/v1/lineage-review/items/{id}/release",
        "/catalog/v1/lineage-review/items/{id}/decisions",
        "/catalog/v1/lineage-review/decisions/{id}/approval",
    ] {
        let operations = paths
            .get(path)
            .and_then(serde_json::Value::as_object)
            .ok_or(path)?;
        for operation in operations.values() {
            assert_eq!(
                operation["security"][0]["lineage_staff_bearer"],
                serde_json::json!([]),
                "{path}"
            );
        }
    }
    assert!(document["components"]["schemas"]["LineageDecisionRequest"].is_object());
    Ok(())
}

#[test]
fn every_route_has_one_bounded_metric_label() {
    for (path, label) in [
        (
            "/catalog/v1/lineage-review/items",
            "/catalog/v1/lineage-review/items",
        ),
        (
            &format!("/catalog/v1/lineage-review/items/{ITEM}"),
            "/catalog/v1/lineage-review/items/{id}",
        ),
        (
            &format!("/catalog/v1/lineage-review/items/{ITEM}/decisions"),
            "/catalog/v1/lineage-review/items/{id}/{action}",
        ),
        (
            &format!("/catalog/v1/lineage-review/decisions/{ITEM}/approval"),
            "/catalog/v1/lineage-review/decisions/{id}/approval",
        ),
    ] {
        assert_eq!(super::super::canonical_route_label(path), label);
    }
}
