#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![allow(missing_docs)]

use std::{error::Error, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use foundation_outbox::kafka_broadcaster::RdkafkaKafkaPublisher;
use foundation_outbox::{
    EventBroadcaster, EventEnvelope, KafkaBroadcasterConfig, KafkaEventBroadcaster, OutboxScope,
    OutboxWorker, PublishError, PublisherConfig, DEFAULT_RAW_WRITTEN_TOPIC,
    RAW_WRITTEN_AVRO_SCHEMA,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct NoopFallback;

#[async_trait]
impl EventBroadcaster for NoopFallback {
    async fn publish(&self, _event: &EventEnvelope) -> Result<(), PublishError> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; deliberately targets an unreachable Kafka broker"]
async fn live_kafka_outage_retries_then_quarantines_without_acknowledging() -> TestResult {
    // Fail loud, like live_kafka_karapace and live_kafka_outbox_roundtrip do on
    // their own gate. This returned `Ok(())`, so the Kafka lane — whose
    // required_env did not list this variable — could run the outage contract
    // against nothing and report a pass. The test is `#[ignore]`, so reaching
    // this line means a harness deliberately asked for it.
    if std::env::var("FOUNDATION_TEST_KAFKA_REQUIRED").as_deref() != Ok("1") {
        return Err("FOUNDATION_TEST_KAFKA_REQUIRED=1 is required for the live outage test".into());
    }
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| "FOUNDATION_TEST_KAFKA_REQUIRED=1 requires DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;

    let run_id = Uuid::new_v4().simple().to_string();
    let event_id = Uuid::new_v4();
    let topic = format!("{DEFAULT_RAW_WRITTEN_TOPIC}.outage-{run_id}");
    let scope_unit_id = format!("live-outage-scope-{run_id}");
    let payload = json!({
        "type": "catalog.collection.raw_written.v1",
        "schema_version": 1,
        "collection_snapshot_id": format!("snapshot-{run_id}"),
        "job_id": format!("job-{run_id}"),
        "scope_unit_id": scope_unit_id,
        "provider": "live-outage-test",
        "endpoint": "fixture",
        "endpoint_slug": "live-outage-fixture",
        "bronze_object_key": "bronze/live/outage.json",
        "bronze_object_count": 1,
        "bronze_checksum_sha256": "c".repeat(64),
        "bronze_size_bytes": 256,
        "source_record_count": 2,
        "request_count": 1,
        "request_fingerprint_sha256": "d".repeat(64),
        "request_fingerprint_schema_version": "foundation-platform.live-outage.v1",
        "license": null,
        "srid": null,
        "reused_bronze_object": false,
        "fetched_at_utc": "2026-07-26T00:00:00Z",
        "occurred_at": "2026-07-26T00:00:01Z"
    });
    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, $4, 0)",
    )
    .bind(event_id)
    .bind("catalog.collection.raw_written.v1")
    .bind(payload)
    .bind(Utc::now())
    .execute(&pool)
    .await?;

    // Port 1 on loopback is intentionally unreachable. The real librdkafka producer must report
    // delivery failure, allowing the Postgres worker to own retry and quarantine state.
    let config = KafkaBroadcasterConfig {
        bootstrap_servers: "127.0.0.1:1".to_owned(),
        schema_registry_url: "http://127.0.0.1:1".to_owned(),
        raw_written_topic: topic.clone(),
        client_id: format!("foundation-outage-{run_id}"),
        message_timeout_ms: 250,
        schema_registry_timeout_seconds: 1,
        security_protocol: "PLAINTEXT".to_owned(),
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
        ssl_ca_location: None,
        ssl_certificate_location: None,
        ssl_key_location: None,
        schema_registry_username: None,
        schema_registry_password: None,
        dual_publish_legacy: false,
    };
    let publisher = RdkafkaKafkaPublisher::from_config(&config)?;
    let schema = apache_avro::Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA)?;
    let broadcaster =
        KafkaEventBroadcaster::new(publisher, topic, schema, 1, Arc::new(NoopFallback), false)?;
    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(broadcaster),
        PublisherConfig {
            batch_size: 1,
            max_retries: 2,
            lease_duration: Duration::from_secs(1),
            ..PublisherConfig::default()
        },
        OutboxScope::Catalog,
    );

    let first = worker.tick().await?;
    assert_eq!(first.published, 0);
    assert_eq!(first.retried, 1);
    assert_eq!(first.dead_lettered, 0);
    assert_row_state(&pool, event_id, 1).await?;

    let second = worker.tick().await?;
    assert_eq!(second.published, 0);
    assert_eq!(second.retried, 1);
    assert_eq!(second.dead_lettered, 1);
    assert_row_state(&pool, event_id, 2).await?;
    let quarantine_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM catalog.outbox_quarantine
         WHERE source_outbox_table = 'catalog.outbox_event' AND event_id = $1
           AND consumer_key = 'outbox-publisher'",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(quarantine_count, 1);

    sqlx::query("DELETE FROM catalog.outbox_quarantine WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    Ok(())
}

async fn assert_row_state(pool: &PgPool, event_id: Uuid, retry_count: i32) -> TestResult {
    let state: (Option<chrono::DateTime<Utc>>, i32) = sqlx::query_as(
        "SELECT published_at, retry_count FROM catalog.outbox_event WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    assert!(state.0.is_none(), "failed Kafka delivery was acknowledged");
    assert_eq!(state.1, retry_count);
    Ok(())
}
