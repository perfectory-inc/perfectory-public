//! `PostgreSQL` round-trip tests for outbox publishing.

use std::{
    error::Error,
    io::{self, Read, Write},
    net::TcpListener,
    sync::{Arc, LazyLock},
    thread,
};

use async_trait::async_trait;
use foundation_outbox::{
    broadcaster::EventEnvelope,
    object_storage::{ObjectStorageService, PutObjectRequest},
    vector_tile_manifest::{PgVectorTileManifestReader, MANIFEST_POINTER_OBJECT_KEY},
    CatalogEventBroadcaster, EventBroadcaster, OutboxScope, OutboxWorker, PublishError,
    PublisherConfig, WebhookBroadcaster,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const LOCAL_VECTOR_TILE_MANIFEST_ID: Uuid =
    Uuid::from_u128(0x018f_0000_0000_7000_8000_0000_0003_0001);

struct LocalVectorTileManifestSeed {
    id: Uuid,
    current_version: String,
}

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn pool() -> TestResult<Option<PgPool>> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(None);
    };
    PgPool::connect(&url)
        .await
        .map_or_else(|_| Ok(None), |pool| Ok(Some(pool)))
}

async fn cleanup_test_rows(pool: &PgPool) -> TestResult {
    sqlx::query(
        "DELETE FROM catalog.outbox_quarantine
         WHERE event_type LIKE 'catalog.test.%'
            OR payload->>'test_scope' = 'outbox_publish_roundtrip'",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "DELETE FROM catalog.outbox_event
         WHERE type LIKE 'catalog.test.%'
            OR payload->>'test_scope' = 'outbox_publish_roundtrip'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug)]
struct FailingBroadcaster;

#[async_trait]
impl EventBroadcaster for FailingBroadcaster {
    async fn publish(&self, _event: &EventEnvelope) -> Result<(), PublishError> {
        Err(PublishError::Broadcaster("forced failure".to_owned()))
    }
}

#[derive(Clone, Debug, Default)]
struct RecordingObjectStorage {
    requests: Arc<Mutex<Vec<PutObjectRequest>>>,
}

#[async_trait]
impl ObjectStorageService for RecordingObjectStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        self.requests.lock().await.push(request);
        Ok(())
    }

    async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
        Ok(None)
    }
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn tick_publishes_pending_catalog_rows_and_marks_published_at() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let event_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, '1970-01-01 00:00:00+00', 0)",
    )
    .bind(event_id)
    .bind("catalog.test.published.v1")
    .bind(json!({ "type": "catalog.test.published.v1" }))
    .execute(&pool)
    .await?;

    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(foundation_outbox::LoggingBroadcaster),
        PublisherConfig::default(),
        OutboxScope::Catalog,
    );

    worker.tick().await?;

    let published_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT published_at FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert!(published_at.is_some());

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    cleanup_test_rows(&pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack and local_vector_tile_manifest seed"]
async fn tick_publishes_active_vector_tile_manifest_pointer_from_catalog_outbox() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let active_manifest = load_local_vector_tile_manifest_seed(&pool).await?;
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await?;
    activate_local_vector_tile_manifest_seed(&pool, active_manifest.id).await?;
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, '1970-01-01 00:00:02+00', 0)",
    )
    .bind(event_id)
    .bind("catalog.vector_tile_manifest.promoted.v1")
    .bind(json!({
        "type": "catalog.vector_tile_manifest.promoted.v1",
        "manifest_id": active_manifest.id,
        "test_scope": "outbox_publish_roundtrip",
    }))
    .execute(&pool)
    .await?;

    let storage = RecordingObjectStorage::default();
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(PgVectorTileManifestReader::new(pool.clone())),
        Arc::new(storage.clone()),
        Arc::new(foundation_outbox::LoggingBroadcaster),
    );
    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(broadcaster),
        single_event_config(),
        OutboxScope::Catalog,
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.published, 1);
    assert_eq!(stats.retried, 0);
    let published_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT published_at FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert!(published_at.is_some());

    let request = single_recorded_request(&storage).await?;
    assert_eq!(request.key, MANIFEST_POINTER_OBJECT_KEY);
    assert_eq!(request.content_type, "application/json");
    assert_eq!(request.cache_control, "no-cache, max-age=0");

    let body: Value = serde_json::from_slice(&request.body)?;
    assert_eq!(body["schema_version"], 1);
    assert_eq!(
        body["current_version"].as_str(),
        Some(active_manifest.current_version.as_str())
    );
    assert_eq!(body["artifacts"]["parcels"]["source_layer"], "parcels");
    assert_eq!(
        body["tiles_url_template"],
        "{object_key_prefix}/{z}/{x}/{y}.pbf"
    );

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    cleanup_test_rows(&pool).await?;
    active_snapshot.restore(&pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn tick_delivers_catalog_event_to_webhook_and_marks_published_at() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let server = OneShotHttpServer::spawn(202)?;
    let event_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, '1970-01-01 00:00:03+00', 0)",
    )
    .bind(event_id)
    .bind("catalog.test.webhook_delivered.v1")
    .bind(json!({
        "type": "catalog.test.webhook_delivered.v1",
        "test_scope": "outbox_publish_roundtrip",
        "complex_id": "ic-webhook"
    }))
    .execute(&pool)
    .await?;

    let broadcaster = WebhookBroadcaster::builder()
        .endpoint("local-test-consumer", server.url().as_str())?
        .build()?;
    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(broadcaster),
        single_event_config(),
        OutboxScope::Catalog,
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.published, 1);
    assert_eq!(stats.retried, 0);
    let published_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT published_at FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert!(published_at.is_some());

    let request = server.join()?;
    assert!(request.contains("POST /foundation-platform/events HTTP/1.1"));
    assert!(request.contains("x-foundation-platform-outbox-scope: catalog"));
    let body: Value = json_body(&request)?;
    assert_eq!(body["event_id"], event_id.to_string());
    assert_eq!(body["event_type"], "catalog.test.webhook_delivered.v1");
    assert_eq!(body["scope"], "catalog");
    assert_eq!(body["payload"]["complex_id"], "ic-webhook");

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    cleanup_test_rows(&pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn tick_increments_retry_count_when_broadcaster_fails() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let event_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, '1970-01-01 00:00:01+00', 0)",
    )
    .bind(event_id)
    .bind("catalog.test.retry.v1")
    .bind(json!({ "type": "catalog.test.retry.v1" }))
    .execute(&pool)
    .await?;

    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(FailingBroadcaster),
        PublisherConfig::default(),
        OutboxScope::Catalog,
    );

    worker.tick().await?;

    let row: (i32, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT retry_count, published_at FROM catalog.outbox_event WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, 1);
    assert!(row.1.is_none());

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    cleanup_test_rows(&pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn tick_persists_exhausted_catalog_event_to_quarantine() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let event_id = Uuid::new_v4();
    let config = PublisherConfig {
        batch_size: 1,
        max_retries: 2,
        ..PublisherConfig::default()
    };

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, '1970-01-01 00:00:04+00', 1)",
    )
    .bind(event_id)
    .bind("catalog.test.quarantine.v1")
    .bind(json!({
        "type": "catalog.test.quarantine.v1",
        "test_scope": "outbox_publish_roundtrip"
    }))
    .execute(&pool)
    .await?;

    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(FailingBroadcaster),
        config,
        OutboxScope::Catalog,
    );

    let stats = worker.tick().await?;

    assert_eq!(stats.published, 0);
    assert_eq!(stats.retried, 1);
    assert_eq!(stats.dead_lettered, 1);
    let quarantined: (String, String, String, String, i32, Value) = sqlx::query_as(
        "SELECT source_outbox_table, event_type, consumer_key, failure_stage,
                attempt_count, payload
         FROM catalog.outbox_quarantine
         WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(quarantined.0, "catalog.outbox_event");
    assert_eq!(quarantined.1, "catalog.test.quarantine.v1");
    assert_eq!(quarantined.2, "outbox-publisher");
    assert_eq!(quarantined.3, "retry_exhausted");
    assert_eq!(quarantined.4, 2);
    assert_eq!(quarantined.5["test_scope"], "outbox_publish_roundtrip");

    cleanup_test_rows(&pool).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn tick_skips_rows_that_already_hit_max_retries() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await? else {
        return Ok(());
    };
    cleanup_test_rows(&pool).await?;
    let event_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO catalog.outbox_event (event_id, type, payload, occurred_at, retry_count)
         VALUES ($1, $2, $3, now(), 5)",
    )
    .bind(event_id)
    .bind("catalog.test.skipped.v1")
    .bind(json!({ "type": "catalog.test.skipped.v1" }))
    .execute(&pool)
    .await?;

    let worker = OutboxWorker::new(
        pool.clone(),
        Arc::new(FailingBroadcaster),
        PublisherConfig::default(),
        OutboxScope::Catalog,
    );

    worker.tick().await?;

    let retry_count: i32 =
        sqlx::query_scalar("SELECT retry_count FROM catalog.outbox_event WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(retry_count, 5);

    sqlx::query("DELETE FROM catalog.outbox_event WHERE event_id = $1")
        .bind(event_id)
        .execute(&pool)
        .await?;
    cleanup_test_rows(&pool).await?;
    Ok(())
}

async fn load_local_vector_tile_manifest_seed(
    pool: &PgPool,
) -> TestResult<LocalVectorTileManifestSeed> {
    let (id, current_version): (Uuid, String) = sqlx::query_as(
        "SELECT id, current_version
         FROM catalog.vector_tile_manifest
         WHERE id = $1",
    )
    .bind(LOCAL_VECTOR_TILE_MANIFEST_ID)
    .fetch_one(pool)
    .await?;

    Ok(LocalVectorTileManifestSeed {
        id,
        current_version,
    })
}

async fn activate_local_vector_tile_manifest_seed(pool: &PgPool, manifest_id: Uuid) -> TestResult {
    sqlx::query(
        "UPDATE catalog.vector_tile_manifest
         SET is_active = false
         WHERE is_active = true
           AND id <> $1",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let activated = sqlx::query(
        "UPDATE catalog.vector_tile_manifest
         SET is_active = true
         WHERE id = $1
           AND is_active = false",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    assert_eq!(
        activated.rows_affected(),
        1,
        "the stable local vector-tile manifest seed must be activated exactly once"
    );

    Ok(())
}

struct ActiveManifestSnapshot {
    active_manifest_ids: Vec<Uuid>,
}

impl ActiveManifestSnapshot {
    async fn pause(pool: &PgPool) -> TestResult<Self> {
        let active_manifest_ids = sqlx::query_scalar(
            "SELECT id
             FROM catalog.vector_tile_manifest
             WHERE is_active = true",
        )
        .fetch_all(pool)
        .await?;

        sqlx::query(
            "UPDATE catalog.vector_tile_manifest
             SET is_active = false
             WHERE is_active = true",
        )
        .execute(pool)
        .await?;

        Ok(Self {
            active_manifest_ids,
        })
    }

    async fn restore(&self, pool: &PgPool) -> TestResult {
        for manifest_id in &self.active_manifest_ids {
            sqlx::query(
                "UPDATE catalog.vector_tile_manifest
                 SET is_active = true
                 WHERE id = $1
                   AND is_active = false",
            )
            .bind(manifest_id)
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

async fn single_recorded_request(storage: &RecordingObjectStorage) -> TestResult<PutObjectRequest> {
    let requests = storage.requests.lock().await;
    assert_eq!(requests.len(), 1);
    requests
        .first()
        .cloned()
        .ok_or_else(|| io::Error::other("object storage request was not recorded").into())
}

fn single_event_config() -> PublisherConfig {
    PublisherConfig {
        batch_size: 1,
        ..PublisherConfig::default()
    }
}

struct OneShotHttpServer {
    listener: TcpListener,
    status: u16,
}

impl OneShotHttpServer {
    fn spawn(status: u16) -> TestResult<StartedServer> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = Self { listener, status };
        let handle = thread::spawn(move || server.accept_one());
        Ok(StartedServer {
            url: format!("http://{addr}/foundation-platform/events"),
            handle,
        })
    }

    fn accept_one(self) -> TestResult<String> {
        let (mut stream, _) = self.listener.accept()?;
        let mut buffer = [0_u8; 16_384];
        let mut request = Vec::new();
        loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request);
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .and_then(|raw| raw.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(request.len(), |index| index + 4);
                while request.len().saturating_sub(header_end) < content_length {
                    let read = stream.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                break;
            }
        }

        let reason = if self.status < 400 {
            "Accepted"
        } else {
            "Unavailable"
        };
        let response = format!(
            "HTTP/1.1 {} {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            self.status, reason
        );
        stream.write_all(response.as_bytes())?;
        String::from_utf8(request).map_err(Into::into)
    }
}

struct StartedServer {
    url: String,
    handle: thread::JoinHandle<TestResult<String>>,
}

impl StartedServer {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn join(self) -> TestResult<String> {
        self.handle
            .join()
            .map_err(|_| "HTTP server thread panicked")?
    }
}

fn json_body(request: &str) -> TestResult<Value> {
    let (_, body) = request
        .split_once("\r\n\r\n")
        .ok_or("HTTP request missing body separator")?;
    serde_json::from_str(body).map_err(Into::into)
}
