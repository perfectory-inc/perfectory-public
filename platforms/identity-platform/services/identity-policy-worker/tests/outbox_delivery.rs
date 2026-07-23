//! Leased outbox delivery behavior tests using worker-local fake ports.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use identity_policy_worker::{
    run_until_shutdown, ClaimRequest, DeliveryWorker, EventPublisher, HttpEventPublisher,
    LeasedOutboxEvent, OutboxRepository, PublishError, PublisherEndpoint, RepositoryError,
    ValidatedOutboxEvent, ValidationError, WorkerOptions, CLAIM_DUE_SQL, IDEMPOTENCY_KEY_HEADER,
    MARK_PUBLISHED_SQL, MAX_ATTEMPTS, RECORD_FAILURE_SQL,
};
use serde_json::json;
use tokio::sync::{oneshot, Mutex, Notify};
use uuid::Uuid;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const EVENT_ID: Uuid = Uuid::from_u128(1);
const STAFF_ID: Uuid = Uuid::from_u128(2);

#[tokio::test]
async fn pending_shutdown_stops_before_the_first_claim() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(vec![available_row(valid_event(0))], actions.clone());
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(fake_publisher(Ok(()), actions)),
        worker_options(),
    );

    run_until_shutdown(
        &worker,
        Duration::from_millis(10),
        async { Ok::<(), std::convert::Infallible>(()) },
        |_| {},
    )
    .await?;

    assert!(repository.state.lock().await.claim_tokens.is_empty());
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    ClaimCommitted,
    Publish,
    MarkPublished,
    RecordFailure,
}

#[derive(Clone)]
struct FakeRepository {
    state: Arc<Mutex<FakeRepositoryState>>,
    actions: Arc<Mutex<Vec<Action>>>,
}

struct FakeRepositoryState {
    rows: Vec<FakeRow>,
    claim_tokens: Vec<Uuid>,
    last_failure: Option<RecordedFailure>,
    now: DateTime<Utc>,
}

struct FakeRow {
    event: LeasedOutboxEvent,
    next_attempt_at: DateTime<Utc>,
    lease_owner: Option<String>,
    claim_token: Option<Uuid>,
    lease_expires_at: Option<DateTime<Utc>>,
    published: bool,
}

struct RecordedFailure {
    event_id: Uuid,
    attempt_count: i32,
    retry_after: Duration,
    error_code: &'static str,
}

#[async_trait]
impl OutboxRepository for FakeRepository {
    async fn claim_due(
        &self,
        request: &ClaimRequest,
    ) -> Result<Option<LeasedOutboxEvent>, RepositoryError> {
        let mut state = self.state.lock().await;
        state.claim_tokens.push(request.claim_token);
        let now = state.now;
        let claimed = state.rows.iter_mut().find_map(|row| {
            let lease_available = row
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= now);
            if !row.published
                && row.event.attempt_count < MAX_ATTEMPTS
                && row.next_attempt_at <= now
                && lease_available
            {
                row.lease_owner = Some(request.lease_owner.clone());
                row.claim_token = Some(request.claim_token);
                row.lease_expires_at = TimeDelta::from_std(request.lease_duration)
                    .ok()
                    .and_then(|duration| now.checked_add_signed(duration));
                let mut event = row.event.clone();
                event.claim_token = request.claim_token;
                Some(event)
            } else {
                None
            }
        });
        drop(state);
        self.actions.lock().await.push(Action::ClaimCommitted);
        Ok(claimed)
    }

    async fn mark_published(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
    ) -> Result<(), RepositoryError> {
        let mut state = self.state.lock().await;
        let now = state.now;
        let row = state
            .rows
            .iter_mut()
            .find(|row| {
                row.event.event_id == event_id
                    && row.lease_owner.as_deref() == Some(lease_owner)
                    && row.claim_token == Some(claim_token)
                    && row
                        .lease_expires_at
                        .is_some_and(|expires_at| expires_at > now)
                    && !row.published
            })
            .ok_or(RepositoryError::LostLease)?;
        row.published = true;
        row.lease_owner = None;
        row.claim_token = None;
        row.lease_expires_at = None;
        drop(state);
        self.actions.lock().await.push(Action::MarkPublished);
        Ok(())
    }

    async fn record_failure(
        &self,
        event_id: Uuid,
        lease_owner: &str,
        claim_token: Uuid,
        retry_after: Duration,
        error_code: &'static str,
    ) -> Result<(), RepositoryError> {
        let mut state = self.state.lock().await;
        let now = state.now;
        let row = state
            .rows
            .iter_mut()
            .find(|row| {
                row.event.event_id == event_id
                    && row.lease_owner.as_deref() == Some(lease_owner)
                    && row.claim_token == Some(claim_token)
                    && row
                        .lease_expires_at
                        .is_some_and(|expires_at| expires_at > now)
                    && !row.published
            })
            .ok_or(RepositoryError::LostLease)?;
        row.event.attempt_count = row.event.attempt_count.saturating_add(1).min(MAX_ATTEMPTS);
        row.next_attempt_at = TimeDelta::from_std(retry_after)
            .ok()
            .and_then(|duration| now.checked_add_signed(duration))
            .ok_or(RepositoryError::Update)?;
        row.lease_owner = None;
        row.claim_token = None;
        row.lease_expires_at = None;
        state.last_failure = Some(RecordedFailure {
            event_id,
            attempt_count: row.event.attempt_count,
            retry_after,
            error_code,
        });
        drop(state);
        self.actions.lock().await.push(Action::RecordFailure);
        Ok(())
    }
}

#[tokio::test]
async fn every_claim_attempt_uses_a_fresh_token() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(Vec::new(), actions.clone());
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(fake_publisher(Ok(()), actions)),
        worker_options(),
    );

    worker.tick().await?;
    worker.tick().await?;

    let state = repository.state.lock().await;
    assert_eq!(state.claim_tokens.len(), 2);
    assert_ne!(state.claim_tokens[0], Uuid::nil());
    assert_ne!(state.claim_tokens[0], state.claim_tokens[1]);
    drop(state);
    Ok(())
}

#[tokio::test]
async fn poll_cycle_claims_one_row_at_a_time_and_stops_at_batch_limit() -> Result<(), Box<dyn Error>>
{
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(
        vec![
            available_row(event_with_id(Uuid::from_u128(1), 0)),
            available_row(event_with_id(Uuid::from_u128(3), 0)),
            available_row(event_with_id(Uuid::from_u128(4), 0)),
        ],
        actions.clone(),
    );
    let publisher = fake_publisher(Ok(()), actions);
    let mut options = worker_options();
    options.batch_size = 2;
    let worker = DeliveryWorker::new(Arc::new(repository.clone()), Arc::new(publisher), options);

    let stats = worker.tick().await?;

    assert_eq!(stats.claimed, 2);
    assert_eq!(stats.published, 2);
    let repository_state = repository.state.lock().await;
    assert_eq!(repository_state.claim_tokens.len(), 2);
    assert!(repository_state.rows[0].published);
    assert!(repository_state.rows[1].published);
    assert!(!repository_state.rows[2].published);
    drop(repository_state);
    Ok(())
}

#[derive(Clone)]
struct FakePublisher {
    result: Result<(), PublishError>,
    published: Arc<Mutex<Vec<Uuid>>>,
    actions: Arc<Mutex<Vec<Action>>>,
}

#[async_trait]
impl EventPublisher for FakePublisher {
    async fn publish(&self, event: &ValidatedOutboxEvent) -> Result<(), PublishError> {
        self.actions.lock().await.push(Action::Publish);
        self.published.lock().await.push(event.event_id);
        self.result
    }
}

#[derive(Clone)]
struct DrainingPublisher {
    started: Arc<Notify>,
    release: Arc<Notify>,
    actions: Arc<Mutex<Vec<Action>>>,
}

#[async_trait]
impl EventPublisher for DrainingPublisher {
    async fn publish(&self, _event: &ValidatedOutboxEvent) -> Result<(), PublishError> {
        self.actions.lock().await.push(Action::Publish);
        self.started.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_drains_in_progress_tick_before_stopping_future_claims(
) -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(
        vec![
            available_row(valid_event(0)),
            available_row(event_with_id(Uuid::from_u128(3), 0)),
        ],
        actions.clone(),
    );
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let publisher = DrainingPublisher {
        started: started.clone(),
        release: release.clone(),
        actions: actions.clone(),
    };
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(publisher),
        worker_options(),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        run_until_shutdown(&worker, Duration::from_millis(10), shutdown_rx, |_| {}).await
    });

    tokio::time::timeout(Duration::from_secs(1), started.notified()).await?;
    shutdown_tx.send(()).map_err(|()| "worker stopped early")?;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), worker_task).await???;

    let state = repository.state.lock().await;
    assert_eq!(state.claim_tokens.len(), 1);
    assert!(state.rows[0].published);
    assert!(!state.rows[1].published);
    drop(state);
    assert_eq!(
        actions.lock().await.as_slice(),
        &[
            Action::ClaimCommitted,
            Action::Publish,
            Action::MarkPublished,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn successful_delivery_marks_published_after_network_publish() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(vec![available_row(valid_event(0))], actions.clone());
    let publisher = fake_publisher(Ok(()), actions.clone());
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(publisher.clone()),
        worker_options(),
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.published, 1);
    assert_eq!(stats.failed, 0);
    let repository_state = repository.state.lock().await;
    assert!(repository_state.rows[0].published);
    assert!(repository_state.rows[0].lease_owner.is_none());
    assert!(repository_state.rows[0].claim_token.is_none());
    drop(repository_state);
    assert_eq!(publisher.published.lock().await.as_slice(), &[EVENT_ID]);
    assert_eq!(
        actions.lock().await.as_slice(),
        &[
            Action::ClaimCommitted,
            Action::Publish,
            Action::MarkPublished,
            Action::ClaimCommitted,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn failed_delivery_records_bounded_deterministic_backoff() -> Result<(), Box<dyn Error>> {
    let first = run_publish_failure(0).await?;
    assert_eq!(first.retry_after, Duration::from_secs(2));
    assert_eq!(first.error_code, "publisher.non_success");
    assert_eq!(first.event_id, EVENT_ID);

    let capped = run_publish_failure(20).await?;
    assert_eq!(capped.retry_after, Duration::from_mins(1));
    assert_eq!(capped.error_code, "publisher.non_success");

    let maximum = run_publish_failure(MAX_ATTEMPTS - 1).await?;
    assert_eq!(maximum.attempt_count, MAX_ATTEMPTS);
    assert_eq!(maximum.retry_after, Duration::from_mins(1));
    assert!(RECORD_FAILURE_SQL.contains("LEAST(attempt_count, 999) + 1"));
    Ok(())
}

#[tokio::test]
async fn exhausted_rows_are_not_claimed() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(
        vec![available_row(valid_event(MAX_ATTEMPTS))],
        actions.clone(),
    );
    let publisher = fake_publisher(Ok(()), actions);
    let worker = DeliveryWorker::new(
        Arc::new(repository),
        Arc::new(publisher.clone()),
        worker_options(),
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.claimed, 0);
    assert!(publisher.published.lock().await.is_empty());
    assert!(CLAIM_DUE_SQL.contains("attempt_count < 1000"));
    Ok(())
}

#[tokio::test]
async fn invalid_event_is_not_sent_and_is_retry_recorded() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let mut invalid = valid_event(0);
    invalid.payload = json!({
        "type": "identity.staff.session_revoked.v1",
        "schema_version": "not-an-integer"
    });
    let repository = fake_repository(vec![available_row(invalid)], actions.clone());
    let publisher = fake_publisher(Ok(()), actions.clone());
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(publisher.clone()),
        worker_options(),
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.failed, 1);
    assert!(publisher.published.lock().await.is_empty());
    let repository_state = repository.state.lock().await;
    assert_eq!(repository_state.rows[0].event.attempt_count, 1);
    assert_eq!(
        repository_state
            .last_failure
            .as_ref()
            .map(|failure| failure.error_code),
        Some("event.invalid_payload")
    );
    assert!(repository_state.rows[0].lease_owner.is_none());
    assert!(repository_state.rows[0].claim_token.is_none());
    drop(repository_state);
    assert_eq!(
        actions.lock().await.as_slice(),
        &[
            Action::ClaimCommitted,
            Action::RecordFailure,
            Action::ClaimCommitted,
        ]
    );
    Ok(())
}

#[test]
fn every_identity_event_variant_rejects_non_v1_schema_version() {
    let timestamp = "2026-07-12T10:00:00Z";
    let cases = [
        (
            "identity.staff.invited.v1",
            json!({
                "type": "identity.staff.invited.v1",
                "schema_version": 2,
                "staff_id": STAFF_ID,
                "email": "staff@example.test",
                "invited_at": timestamp,
                "invited_by": STAFF_ID
            }),
        ),
        (
            "identity.staff.role_assigned.v1",
            json!({
                "type": "identity.staff.role_assigned.v1",
                "schema_version": 2,
                "staff_id": STAFF_ID,
                "role_code": "MASTER_ADMIN",
                "assigned_at": timestamp,
                "assigned_by": STAFF_ID
            }),
        ),
        (
            "identity.staff.session_revoked.v1",
            json!({
                "type": "identity.staff.session_revoked.v1",
                "schema_version": 2,
                "staff_id": STAFF_ID,
                "jti": "test-jti",
                "revoked_at": timestamp,
                "reason": "logout"
            }),
        ),
    ];

    for (event_type, payload) in cases {
        let mut row = valid_event(0);
        row.event_type = event_type.to_owned();
        row.payload = payload;
        assert!(
            matches!(ValidatedOutboxEvent::try_from(row), Err(ValidationError)),
            "accepted non-v1 schema for {event_type}"
        );
    }
}

#[test]
fn event_type_must_match_deserialized_payload_variant() {
    let mut row = valid_event(0);
    row.event_type = "identity.staff.invited.v1".to_owned();

    assert!(matches!(
        ValidatedOutboxEvent::try_from(row),
        Err(ValidationError)
    ));
}

#[tokio::test]
async fn expired_leases_are_reclaimed_while_active_leases_are_skipped() -> Result<(), Box<dyn Error>>
{
    let now = Utc::now();
    let mut expired = available_row(valid_event(0));
    expired.lease_owner = Some("crashed-worker".to_owned());
    expired.lease_expires_at = now.checked_sub_signed(TimeDelta::seconds(1));
    let mut active = available_row(event_with_id(Uuid::from_u128(3), 0));
    active.lease_owner = Some("active-worker".to_owned());
    active.lease_expires_at = now.checked_add_signed(TimeDelta::minutes(1));
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(vec![expired, active], actions.clone());
    let publisher = fake_publisher(Ok(()), actions);
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(publisher.clone()),
        worker_options(),
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.claimed, 1);
    assert_eq!(publisher.published.lock().await.as_slice(), &[EVENT_ID]);
    let repository_state = repository.state.lock().await;
    assert_eq!(
        repository_state.rows[1].lease_owner.as_deref(),
        Some("active-worker")
    );
    assert!(!repository_state.rows[1].published);
    drop(repository_state);
    assert!(CLAIM_DUE_SQL.contains("FOR UPDATE SKIP LOCKED"));
    assert!(CLAIM_DUE_SQL.contains("next_attempt_at <= now()"));
    assert!(CLAIM_DUE_SQL.contains("lease_expires_at <= now()"));
    assert!(CLAIM_DUE_SQL.contains("UPDATE identity.outbox_event"));
    assert!(CLAIM_DUE_SQL.contains("lease_owner = $1"));
    assert!(CLAIM_DUE_SQL.contains("claim_token = $2"));
    assert!(CLAIM_DUE_SQL.contains("RETURNING"));
    assert!(CLAIM_DUE_SQL.contains("outbox.claim_token"));
    Ok(())
}

#[test]
fn claim_sql_uses_database_time_and_claims_exactly_one_row() {
    assert!(CLAIM_DUE_SQL.contains("next_attempt_at <= now()"));
    assert!(CLAIM_DUE_SQL.contains("lease_expires_at <= now()"));
    assert!(CLAIM_DUE_SQL.contains("lease_expires_at = now() +"));
    assert!(CLAIM_DUE_SQL.contains("LIMIT 1"));
    assert!(!CLAIM_DUE_SQL.contains("claimed_at"));
}

#[tokio::test]
async fn same_worker_id_cannot_update_with_a_stale_claim_token() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(vec![available_row(valid_event(0))], actions);
    let first_token = Uuid::new_v4();
    let second_token = Uuid::new_v4();
    repository
        .claim_due(&ClaimRequest {
            lease_owner: "worker-a".to_owned(),
            claim_token: first_token,
            lease_duration: Duration::from_secs(30),
        })
        .await?;
    {
        let mut state = repository.state.lock().await;
        state.rows[0].claim_token = Some(second_token);
    }

    let result = repository
        .mark_published(EVENT_ID, "worker-a", first_token)
        .await;

    assert_eq!(result, Err(RepositoryError::LostLease));
    assert!(!repository.state.lock().await.rows[0].published);
    Ok(())
}

#[tokio::test]
async fn expired_lease_cannot_mark_published_or_failed() -> Result<(), Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(vec![available_row(valid_event(0))], actions);
    let claim_token = Uuid::new_v4();
    repository
        .claim_due(&ClaimRequest {
            lease_owner: "worker-a".to_owned(),
            claim_token,
            lease_duration: Duration::from_secs(30),
        })
        .await?;
    repository.state.lock().await.rows[0].lease_expires_at =
        Utc::now().checked_sub_signed(TimeDelta::seconds(1));

    let published = repository
        .mark_published(EVENT_ID, "worker-a", claim_token)
        .await;
    let failed = repository
        .record_failure(
            EVENT_ID,
            "worker-a",
            claim_token,
            Duration::from_secs(2),
            "publisher.request_failed",
        )
        .await;

    assert_eq!(published, Err(RepositoryError::LostLease));
    assert_eq!(failed, Err(RepositoryError::LostLease));
    assert!(MARK_PUBLISHED_SQL.contains("claim_token = $3"));
    assert!(MARK_PUBLISHED_SQL.contains("lease_expires_at > now()"));
    assert!(RECORD_FAILURE_SQL.contains("claim_token = $3"));
    assert!(RECORD_FAILURE_SQL.contains("lease_expires_at > now()"));
    Ok(())
}

#[tokio::test]
async fn http_publisher_sends_event_id_as_stable_idempotency_header() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/identity-events"))
        .and(header(IDEMPOTENCY_KEY_HEADER, EVENT_ID.to_string()))
        .and(body_json(json!({
            "event_id": EVENT_ID,
            "event_type": "identity.staff.session_revoked.v1",
            "occurred_at": "2026-07-12T10:00:00Z",
            "payload": {
                "type": "identity.staff.session_revoked.v1",
                "schema_version": 1,
                "staff_id": STAFF_ID,
                "jti": "test-jti",
                "revoked_at": "2026-07-12T10:00:00Z",
                "reason": "logout"
            }
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let endpoint = PublisherEndpoint::parse(&format!("{}/identity-events", server.uri()))?;
    let publisher = HttpEventPublisher::new(endpoint, Duration::from_secs(2))?;
    let event = ValidatedOutboxEvent::try_from(valid_event(0))?;

    publisher.publish(&event).await?;

    server.verify().await;
    Ok(())
}

#[tokio::test]
async fn http_publisher_returns_typed_non_success_failure() -> Result<(), Box<dyn Error>> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let endpoint = PublisherEndpoint::parse(&server.uri())?;
    let publisher = HttpEventPublisher::new(endpoint, Duration::from_secs(2))?;

    let error = publisher
        .publish(&ValidatedOutboxEvent::try_from(valid_event(0))?)
        .await
        .err()
        .ok_or("expected HTTP failure")?;

    assert_eq!(error, PublishError::NonSuccessStatus { status: 503 });
    assert_eq!(error.error_code(), "publisher.non_success");
    Ok(())
}

#[tokio::test]
async fn http_publisher_does_not_follow_redirects_from_exact_endpoint() -> Result<(), Box<dyn Error>>
{
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/exact"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/redirected", server.uri())),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/redirected"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;
    let endpoint = PublisherEndpoint::parse(&format!("{}/exact", server.uri()))?;
    let publisher = HttpEventPublisher::new(endpoint, Duration::from_secs(2))?;

    let error = publisher
        .publish(&ValidatedOutboxEvent::try_from(valid_event(0))?)
        .await
        .err()
        .ok_or("expected redirect failure")?;

    assert_eq!(error, PublishError::NonSuccessStatus { status: 307 });
    server.verify().await;
    Ok(())
}

#[test]
fn exact_endpoint_rejects_unsafe_or_ambiguous_urls() {
    for endpoint in [
        "ftp://events.example.test/publish",
        "https://events.example.test/publish?token=secret",
        "https://events.example.test/publish#fragment",
        "https://user:secret@events.example.test/publish",
    ] {
        assert!(
            PublisherEndpoint::parse(endpoint).is_err(),
            "accepted {endpoint}"
        );
    }
    assert!(PublisherEndpoint::parse("https://events.example.test/publish").is_ok());
    assert!(PublisherEndpoint::parse("http://127.0.0.1:8080/publish").is_ok());
}

async fn run_publish_failure(attempt_count: i32) -> Result<RecordedFailure, Box<dyn Error>> {
    let actions = Arc::new(Mutex::new(Vec::new()));
    let repository = fake_repository(
        vec![available_row(valid_event(attempt_count))],
        actions.clone(),
    );
    let publisher = fake_publisher(
        Err(PublishError::NonSuccessStatus { status: 503 }),
        actions.clone(),
    );
    let worker = DeliveryWorker::new(
        Arc::new(repository.clone()),
        Arc::new(publisher),
        worker_options(),
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.failed, 1);
    let mut repository_state = repository.state.lock().await;
    assert_eq!(
        repository_state.rows[0].event.attempt_count,
        attempt_count.saturating_add(1).min(MAX_ATTEMPTS)
    );
    assert!(repository_state.rows[0].lease_owner.is_none());
    assert!(!repository_state.rows[0].published);
    assert_eq!(
        actions.lock().await.as_slice(),
        &[
            Action::ClaimCommitted,
            Action::Publish,
            Action::RecordFailure,
            Action::ClaimCommitted,
        ]
    );
    repository_state
        .last_failure
        .take()
        .ok_or_else(|| "missing failure".into())
}

fn fake_repository(rows: Vec<FakeRow>, actions: Arc<Mutex<Vec<Action>>>) -> FakeRepository {
    FakeRepository {
        state: Arc::new(Mutex::new(FakeRepositoryState {
            rows,
            claim_tokens: Vec::new(),
            last_failure: None,
            now: Utc::now(),
        })),
        actions,
    }
}

fn fake_publisher(
    result: Result<(), PublishError>,
    actions: Arc<Mutex<Vec<Action>>>,
) -> FakePublisher {
    FakePublisher {
        result,
        published: Arc::new(Mutex::new(Vec::new())),
        actions,
    }
}

fn worker_options() -> WorkerOptions {
    WorkerOptions {
        worker_id: "worker-a".to_owned(),
        batch_size: 10,
        lease_duration: Duration::from_secs(30),
        base_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_mins(1),
    }
}

fn available_row(event: LeasedOutboxEvent) -> FakeRow {
    FakeRow {
        event,
        next_attempt_at: Utc::now()
            .checked_sub_signed(TimeDelta::seconds(1))
            .unwrap_or_else(Utc::now),
        lease_owner: None,
        claim_token: None,
        lease_expires_at: None,
        published: false,
    }
}

fn valid_event(attempt_count: i32) -> LeasedOutboxEvent {
    event_with_id(EVENT_ID, attempt_count)
}

fn event_with_id(event_id: Uuid, attempt_count: i32) -> LeasedOutboxEvent {
    LeasedOutboxEvent {
        event_id,
        event_type: "identity.staff.session_revoked.v1".to_owned(),
        payload: json!({
            "type": "identity.staff.session_revoked.v1",
            "schema_version": 1,
            "staff_id": STAFF_ID,
            "jti": "test-jti",
            "revoked_at": "2026-07-12T10:00:00Z",
            "reason": "logout"
        }),
        occurred_at: "2026-07-12T10:00:00Z"
            .parse()
            .unwrap_or_else(|_| Utc::now()),
        attempt_count,
        claim_token: Uuid::nil(),
    }
}
