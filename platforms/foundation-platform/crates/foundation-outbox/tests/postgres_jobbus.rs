//! Real-Postgres contract tests for the durable collection `JobBus`.

use std::{error::Error, sync::OnceLock};

use chrono::{DateTime, Duration, Utc};
use foundation_outbox::{
    CollectionJob, CollectionSuccess, FailureDisposition, JobBus, JobFailure, JobLease,
    NackOutcome, PostgresJobBus,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn serialized_test() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn fixed_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap_or_default()
}

fn job(job_id: &str) -> CollectionJob {
    CollectionJob {
        job_id: job_id.to_owned(),
        scope_unit_id: "scope:legal-dong:1111010100".to_owned(),
        shard_id: "national-shard-0001".to_owned(),
        provider: "data.go.kr".to_owned(),
        endpoint: "getBrTitleInfo".to_owned(),
        endpoint_slug: "data-go-kr-building-register-getBrTitleInfo".to_owned(),
        idempotency_key: format!("idempotency:{job_id}"),
        request_fingerprint_sha256: "a".repeat(64),
        request_fingerprint_schema_version: "foundation-platform.bronze_request_fingerprint.v1"
            .to_owned(),
        collection_snapshot_id: "snapshot:test".to_owned(),
        spec: json!({"sigungu_cd": "11680", "bjdong_cd": "10300", "page_no": 1}),
    }
}

fn success() -> CollectionSuccess {
    CollectionSuccess {
        bronze_object_key: "bronze/source=data-go-kr/page=0001.json".to_owned(),
        bronze_object_count: 1,
        bronze_checksum_sha256: "b".repeat(64),
        bronze_size_bytes: 4096,
        source_record_count: 42,
        request_count: 1,
        reused_bronze_object: false,
        license: None,
        srid: None,
        fetched_at_utc: fixed_time(),
    }
}

async fn test_pool() -> TestResult<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required for the ignored Postgres JobBus contract test")?;
    let pool = PgPool::connect(&url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    Ok(pool)
}

async fn cleanup(pool: &PgPool, job_id: &str) -> TestResult {
    sqlx::query("DELETE FROM catalog.collection_job WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM catalog.outbox_event WHERE payload->>'job_id' = $1")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires DATABASE_URL and a migrated PostgreSQL database"]
async fn postgres_jobbus_publishes_claims_acks_and_emits_raw_written_atomically() -> TestResult {
    let _guard = serialized_test().await;
    let pool = test_pool().await?;
    let job_id = format!("jobbus-test-{}", Uuid::new_v4());
    cleanup(&pool, &job_id).await?;

    let bus = PostgresJobBus::new(pool.clone(), 3, Duration::minutes(15));
    bus.publish(job(&job_id)).await?;
    let lease = bus.poll(1).await?.pop().ok_or("expected one leased job")?;
    assert!(
        bus.poll(1).await?.is_empty(),
        "an in-flight job must not be double-claimed"
    );
    bus.ack(&lease.lease, &success()).await?;
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.outbox_event WHERE payload->>'job_id' = $1",
    )
    .bind(&job_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(outbox_count, 1);
    assert!(matches!(
        bus.ack(&lease.lease, &success()).await,
        Err(foundation_outbox::JobBusError::Conflict(_))
    ));

    cleanup(&pool, &job_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires DATABASE_URL and a migrated PostgreSQL database"]
async fn postgres_jobbus_nack_retries_then_dead_letters_poison_jobs() -> TestResult {
    let _guard = serialized_test().await;
    let pool = test_pool().await?;
    let job_id = format!("jobbus-nack-test-{}", Uuid::new_v4());
    cleanup(&pool, &job_id).await?;

    let bus = PostgresJobBus::new(pool.clone(), 2, Duration::minutes(15));
    bus.publish(job(&job_id)).await?;
    let lease = bus.poll(1).await?.remove(0);
    assert_eq!(
        bus.nack(
            &lease.lease,
            &JobFailure {
                disposition: FailureDisposition::Retryable,
                code: "provider.timeout".to_owned(),
                message: "timeout".to_owned(),
            },
        )
        .await?,
        NackOutcome::Retried
    );
    let retry = bus.poll(1).await?.remove(0);
    assert_eq!(
        bus.nack(
            &retry.lease,
            &JobFailure {
                disposition: FailureDisposition::Poison,
                code: "provider.auth".to_owned(),
                message: "auth rejected".to_owned(),
            },
        )
        .await?,
        NackOutcome::DeadLettered
    );
    assert!(bus.poll(1).await?.is_empty());

    cleanup(&pool, &job_id).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires DATABASE_URL and a migrated PostgreSQL database"]
async fn postgres_jobbus_claim_job_targets_requested_id() -> TestResult {
    let _guard = serialized_test().await;
    let pool = test_pool().await?;
    let requested_id = format!("jobbus-claim-requested-{}", Uuid::new_v4());
    let other_id = format!("jobbus-claim-other-{}", Uuid::new_v4());
    cleanup(&pool, &requested_id).await?;
    cleanup(&pool, &other_id).await?;

    let bus = PostgresJobBus::new(pool.clone(), 3, Duration::minutes(15));
    bus.publish(job(&requested_id)).await?;
    bus.ensure_published(job(&requested_id)).await?;
    let mut mismatched = job(&requested_id);
    mismatched.spec = json!({"page_no": 99});
    assert!(matches!(
        bus.ensure_published(mismatched).await,
        Err(foundation_outbox::JobBusError::Conflict(_))
    ));
    bus.publish(job(&other_id)).await?;

    let lease = bus
        .claim_job(&requested_id)
        .await?
        .ok_or("requested job was not claimable")?;
    assert_eq!(lease.job.job_id, requested_id);
    let remaining = bus.poll(1).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].job.job_id, other_id);

    bus.ack(&lease.lease, &success()).await?;
    assert_eq!(
        bus.job_state(&requested_id).await?.as_deref(),
        Some("completed")
    );
    bus.nack(
        &remaining[0].lease,
        &JobFailure {
            disposition: FailureDisposition::Poison,
            code: "test.cleanup".to_owned(),
            message: "cleanup".to_owned(),
        },
    )
    .await?;
    cleanup(&pool, &requested_id).await?;
    cleanup(&pool, &other_id).await?;
    Ok(())
}

#[allow(dead_code)]
fn _lease_shape_is_stable(lease: &JobLease) -> (&str, u32) {
    (&lease.job_id, lease.attempt)
}
