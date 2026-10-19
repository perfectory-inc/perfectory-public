//! The operator switch route (root ADR-0282): who may call it, and what each outcome answers.

use super::*;
use catalog_application::ports::{
    OperatorActionLedger, OperatorActionLedgerError, OperatorActionRecord,
};

/// Grants exactly the staff job-operating capability and records what it was asked.
#[derive(Default)]
struct StaffJobOperatorAuthorization {
    asked: Mutex<Vec<(String, String, Option<String>)>>,
}

#[async_trait]
impl IdentityAuthorization for StaffJobOperatorAuthorization {
    async fn authorize(
        &self,
        _bearer: &str,
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
        if required_principal_kind == RequiredPrincipalKind::Staff
            && (resource, action) == ("foundation.operations", "operate")
        {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::nil(),
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

/// Refuses every policy: a staff member without the job-operating capability.
struct StaffWithoutOperate;

#[async_trait]
impl IdentityAuthorization for StaffWithoutOperate {
    async fn authorize(
        &self,
        _bearer: &str,
        _required_principal_kind: RequiredPrincipalKind,
        _resource: &str,
        _action: &str,
        _resource_id: Option<&str>,
        _trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        Err(IdentityAuthorizationError::Forbidden)
    }
}

#[derive(Default)]
struct RecordingLedger {
    records: Mutex<Vec<OperatorActionRecord>>,
    down: bool,
}

#[async_trait]
impl OperatorActionLedger for RecordingLedger {
    async fn record(&self, record: &OperatorActionRecord) -> Result<(), OperatorActionLedgerError> {
        if self.down {
            return Err(OperatorActionLedgerError::Unavailable(
                "synthetic outage".to_owned(),
            ));
        }
        self.records.lock().await.push(record.clone());
        Ok(())
    }
}

fn post(job: &str, body: &str, bearer: bool) -> Result<Request<Body>, axum::http::Error> {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/data-catalog/v1/scheduled-jobs/{job}/operator-actions"
        ))
        .header(header::CONTENT_TYPE, "application/json");
    if bearer {
        request = request.header(header::AUTHORIZATION, "Bearer synthetic-staff-token");
    }
    request.body(Body::from(body.to_owned()))
}

async fn ask(
    authorization: Arc<dyn IdentityAuthorization>,
    ledger: Arc<RecordingLedger>,
    request: Request<Body>,
) -> Result<(StatusCode, serde_json::Value), Box<dyn Error>> {
    let state = AppState::bootstrap_for_test_with_identity_authorization(authorization)?
        .with_operator_action_ledger(ledger);
    let response = router(Arc::new(state)).oneshot(request).await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok((
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    ))
}

const RUN_NOW: &str = r#"{"action":"run_now","reason":"a supervised run after the source round"}"#;

#[tokio::test]
async fn a_request_without_a_token_or_the_capability_records_nothing() -> Result<(), Box<dyn Error>>
{
    let ledger = Arc::new(RecordingLedger::default());
    let (status, _) = ask(
        Arc::new(StaffJobOperatorAuthorization::default()),
        ledger.clone(),
        post("shape_ledger_check", RUN_NOW, false)?,
    )
    .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = ask(
        Arc::new(StaffWithoutOperate),
        ledger.clone(),
        post("shape_ledger_check", RUN_NOW, true)?,
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(ledger.records.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn an_authorized_run_now_is_recorded_once_with_its_ten_minute_expiry(
) -> Result<(), Box<dyn Error>> {
    let authorization = Arc::new(StaffJobOperatorAuthorization::default());
    let ledger = Arc::new(RecordingLedger::default());
    let (status, body) = ask(
        authorization.clone(),
        ledger.clone(),
        post("shape_ledger_check", RUN_NOW, true)?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        authorization.asked.lock().await.as_slice(),
        [(
            "foundation.operations".to_owned(),
            "operate".to_owned(),
            Some("shape_ledger_check".to_owned())
        )]
    );
    let records = ledger.records.lock().await.clone();
    let [record] = records.as_slice() else {
        return Err(format!("expected one record, got {}", records.len()).into());
    };
    assert_eq!(record.job_id, "shape_ledger_check");
    assert_eq!(record.reason, "a supervised run after the source round");
    assert_eq!(
        record
            .expires_at
            .map(|at| (at - record.requested_at).num_minutes()),
        Some(10)
    );
    assert_eq!(body["id"], record.id.to_string());
    assert_eq!(body["action"], "run_now");
    assert_eq!(body["actor"], uuid::Uuid::nil().to_string());
    Ok(())
}

#[tokio::test]
async fn a_request_that_breaks_a_rule_is_refused_before_the_ledger() -> Result<(), Box<dyn Error>> {
    let ledger = Arc::new(RecordingLedger::default());
    for (job, body, expected) in [
        ("no_such_job", RUN_NOW, StatusCode::NOT_FOUND),
        (
            "shape_ledger_check",
            r#"{"action":"drop","reason":"a reason"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "shape_ledger_check",
            r#"{"action":"pause","reason":"   "}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "shape_ledger_check",
            r#"{"action":"pause","reason":"two\nlines"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "shape_ledger_check",
            r#"{"action":"run_now","reason":"a reason","drop_pending":true}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let (status, answer) = ask(
            Arc::new(StaffJobOperatorAuthorization::default()),
            ledger.clone(),
            post(job, body, true)?,
        )
        .await?;
        assert_eq!(status, expected, "{job} {body}: {answer}");
    }
    assert!(ledger.records.lock().await.is_empty());
    let (status, _) = ask(
        Arc::new(StaffJobOperatorAuthorization::default()),
        ledger.clone(),
        post(
            "shape_ledger_check",
            r#"{"action":"pause","reason":"a stop for the night","drop_pending":true}"#,
            true,
        )?,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a pause may drop what is pending"
    );
    Ok(())
}

#[tokio::test]
async fn a_ledger_that_cannot_be_written_answers_503() -> Result<(), Box<dyn Error>> {
    let ledger = Arc::new(RecordingLedger {
        down: true,
        ..RecordingLedger::default()
    });
    let (status, _) = ask(
        Arc::new(StaffJobOperatorAuthorization::default()),
        ledger,
        post("shape_ledger_check", RUN_NOW, true)?,
    )
    .await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}
