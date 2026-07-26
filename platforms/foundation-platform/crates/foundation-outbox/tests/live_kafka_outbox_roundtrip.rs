#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![allow(missing_docs)]

use std::{error::Error, sync::Arc, time::Duration};

use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use async_trait::async_trait;
use chrono::Utc;
use foundation_outbox::{
    kafka_broadcaster::from_env, CatalogEventBroadcaster, EventBroadcaster, EventEnvelope,
    LoggingObjectStorage, OutboxScope, OutboxWorker, PgVectorTileManifestReader, PublishError,
    PublisherConfig, RAW_WRITTEN_AVRO_SCHEMA,
};
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    ClientConfig,
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
#[ignore = "requires disposable PostgreSQL plus the pinned Redpanda+Karapace compose stack"]
async fn live_kafka_outbox_roundtrip_marks_postgres_row_published() -> TestResult {
    let Some((bootstrap, _registry)) = required_live_endpoints()? else {
        return Ok(());
    };
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(value) => value,
        Err(_) if std::env::var("FOUNDATION_TEST_KAFKA_REQUIRED").as_deref() == Ok("1") => {
            return Err("FOUNDATION_TEST_KAFKA_REQUIRED=1 requires DATABASE_URL".into())
        }
        Err(_) => return Ok(()),
    };
    if std::env::var("FOUNDATION_PLATFORM_KAFKA_ENABLED").as_deref() != Ok("1") {
        return Err("FOUNDATION_PLATFORM_KAFKA_ENABLED=1 is required for the live test".into());
    }

    let pool = PgPool::connect(&database_url).await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    let topic = std::env::var("FOUNDATION_PLATFORM_KAFKA_RAW_WRITTEN_TOPIC")
        .unwrap_or_else(|_| foundation_outbox::DEFAULT_RAW_WRITTEN_TOPIC.to_owned());
    let run_id = Uuid::new_v4().simple().to_string();
    let scope_unit_id = format!("live-outbox-scope-{run_id}");
    let event_id = Uuid::new_v4();
    let consumer = consumer(&bootstrap, &topic, &run_id)?;
    wait_for_assignment(&consumer).await?;

    let fallback: Arc<dyn EventBroadcaster> = Arc::new(NoopFallback);
    let kafka = from_env(fallback)
        .await?
        .ok_or("Kafka broadcaster was disabled even though it was required")?;
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(PgVectorTileManifestReader::new(pool.clone())),
        Arc::new(LoggingObjectStorage),
        Arc::new(kafka),
    );
    let payload = json!({
        "type": "catalog.collection.raw_written.v1",
        "schema_version": 1,
        "collection_snapshot_id": format!("snapshot-{run_id}"),
        "job_id": format!("job-{run_id}"),
        "scope_unit_id": scope_unit_id,
        "provider": "live-test",
        "endpoint": "fixture",
        "endpoint_slug": "live-test-fixture",
        "bronze_object_key": "bronze/live/outbox.json",
        "bronze_object_count": 1,
        "bronze_checksum_sha256": "c".repeat(64),
        "bronze_size_bytes": 256,
        "source_record_count": 2,
        "request_count": 1,
        "request_fingerprint_sha256": "d".repeat(64),
        "request_fingerprint_schema_version": "foundation-platform.live-test.v1",
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

    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(broadcaster),
        PublisherConfig {
            batch_size: 1,
            ..PublisherConfig::default()
        },
        OutboxScope::Catalog,
    );
    let stats = worker.tick().await?;
    assert_eq!(stats.published, 1);
    let published_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT published_at FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert!(
        published_at.is_some(),
        "outbox row was not marked published"
    );

    let message = receive_key(&consumer, &scope_unit_id).await?;
    let bytes = message.payload().ok_or("Kafka message had no payload")?;
    assert_eq!(bytes[0], 0);
    let schema = Schema::parse_str(RAW_WRITTEN_AVRO_SCHEMA)?;
    let mut datum = &bytes[5..];
    let decoded = from_avro_datum(&schema, &mut datum, None)?;
    let fields = match decoded {
        AvroValue::Record(fields) => fields,
        other => return Err(format!("expected Avro record, got {other:?}").into()),
    };
    assert_eq!(field(&fields, "event_id"), event_id.to_string());
    assert_eq!(field(&fields, "scope_unit_id"), scope_unit_id);
    consumer.commit_message(&message, CommitMode::Sync)?;

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    Ok(())
}

fn required_live_endpoints() -> TestResult<Option<(String, String)>> {
    let required = std::env::var("FOUNDATION_TEST_KAFKA_REQUIRED").as_deref() == Ok("1");
    let bootstrap = std::env::var("FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS").ok();
    let registry = std::env::var("FOUNDATION_TEST_KARAPACE_URL").ok();
    match (bootstrap, registry) {
        (Some(bootstrap), Some(registry)) => Ok(Some((bootstrap, registry))),
        _ if required => Err(
            "FOUNDATION_TEST_KAFKA_REQUIRED=1 requires FOUNDATION_TEST_KAFKA_BOOTSTRAP_SERVERS and FOUNDATION_TEST_KARAPACE_URL".into(),
        ),
        _ => Ok(None),
    }
}

fn consumer(bootstrap: &str, topic: &str, run_id: &str) -> TestResult<StreamConsumer> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("foundation-outbox-live-{run_id}"))
        .set(
            "client.id",
            format!("foundation-outbox-live-consumer-{run_id}"),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000")
        .create::<StreamConsumer>()?;
    consumer.subscribe(&[topic])?;
    Ok(consumer)
}

async fn wait_for_assignment(consumer: &StreamConsumer) -> TestResult {
    for _ in 0..100 {
        if consumer.assignment()?.count() > 0 {
            return Ok(());
        }
        match tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await {
            Ok(Ok(_message)) => {}
            Err(_) => {}
            Ok(Err(error)) => return Err(error.into()),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Kafka consumer did not receive a partition assignment".into())
}

async fn receive_key<'a>(
    consumer: &'a StreamConsumer,
    key: &str,
) -> TestResult<rdkafka::message::BorrowedMessage<'a>> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), consumer.recv()).await??;
        if message.key() == Some(key.as_bytes()) {
            return Ok(message);
        }
    }
}

fn field(fields: &[(String, AvroValue)], name: &str) -> String {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| match value {
            AvroValue::String(value) => value.clone(),
            other => format!("{other:?}"),
        })
        .unwrap_or_default()
}
