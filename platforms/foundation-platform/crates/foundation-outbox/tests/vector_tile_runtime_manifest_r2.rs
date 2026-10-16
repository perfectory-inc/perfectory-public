//! Root ADR-0254: every active v2 runtime manifest is written to the tile edge's bucket.

use std::{collections::BTreeMap, error::Error, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use foundation_contracts::catalog::VectorTileRuntimeManifestResponse;
use foundation_outbox::{
    broadcaster::EventEnvelope,
    object_storage::{ObjectStorageService, ObjectWriteMode, PutObjectRequest},
    vector_tile_manifest::{
        CatalogEventBroadcaster, PublishedVectorTileManifest, VectorTileManifestReader,
    },
    EventBroadcaster, OutboxScope, PublishError, RuntimeManifestReader,
};
use foundation_shared_kernel::events::catalog_v1::{
    vector_tile_runtime_manifest_object_key, VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY,
};
use foundation_shared_kernel::ids::VectorTileRuntimeManifestId;
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const PUBLISHED: &str = "catalog.vector_tile_runtime_manifest.published.v2";

struct NoV1Manifest;

#[async_trait]
impl VectorTileManifestReader for NoV1Manifest {
    async fn get_active_manifest(
        &self,
    ) -> Result<Option<PublishedVectorTileManifest>, PublishError> {
        Ok(None)
    }
}

struct FakeRuntimeReader(Option<VectorTileRuntimeManifestResponse>);

#[async_trait]
impl RuntimeManifestReader for FakeRuntimeReader {
    async fn get_active_runtime_manifest(
        &self,
    ) -> Result<Option<VectorTileRuntimeManifestResponse>, PublishError> {
        Ok(self.0.clone())
    }
}

/// Records every write; answers "already exists" for the keys in `existing`.
#[derive(Clone, Default)]
struct RecordingStorage {
    requests: Arc<Mutex<Vec<PutObjectRequest>>>,
    existing: Vec<String>,
}

#[async_trait]
impl ObjectStorageService for RecordingStorage {
    async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
        let exists = self.existing.contains(&request.key)
            && matches!(request.write_mode, ObjectWriteMode::CreateOnly);
        let key = request.key.clone();
        self.requests.lock().await.push(request);
        if exists {
            return Err(PublishError::ObjectAlreadyExists { key });
        }
        Ok(())
    }

    async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
        Ok(None)
    }
}

#[derive(Clone, Default)]
struct RecordingFallback(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl EventBroadcaster for RecordingFallback {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        self.0.lock().await.push(event.event_type.clone());
        Ok(())
    }
}

fn manifest(id: Uuid) -> VectorTileRuntimeManifestResponse {
    VectorTileRuntimeManifestResponse {
        schema_version: 2,
        current_version: id,
        manifest_generation: 14,
        refresh_after_seconds: 4,
        published_at: Utc::now(),
        publication_units: BTreeMap::new(),
    }
}

fn published(id: Uuid) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::now_v7(),
        event_type: PUBLISHED.to_owned(),
        payload: json!({"schema_version": 2, "manifest_id": id}),
        occurred_at: Utc::now(),
        scope: OutboxScope::Catalog,
    }
}

fn broadcaster(
    active: Option<VectorTileRuntimeManifestResponse>,
    storage: &RecordingStorage,
    fallback: &RecordingFallback,
) -> CatalogEventBroadcaster {
    CatalogEventBroadcaster::new(
        Arc::new(NoV1Manifest),
        Arc::new(RecordingStorage::default()),
        Arc::new(fallback.clone()),
    )
    .with_runtime_manifest(
        Arc::new(FakeRuntimeReader(active)),
        Arc::new(storage.clone()),
    )
}

#[tokio::test]
async fn the_active_manifest_is_written_once_under_its_id_and_as_the_pointer() -> TestResult {
    let id = Uuid::now_v7();
    let active = manifest(id);
    let (storage, fallback) = (RecordingStorage::default(), RecordingFallback::default());
    broadcaster(Some(active.clone()), &storage, &fallback)
        .publish(&published(id))
        .await?;

    let requests = std::mem::take(&mut *storage.requests.lock().await);
    let [copy, pointer] = requests.as_slice() else {
        return Err(format!("expected two writes, got {}", requests.len()).into());
    };
    assert_eq!(
        copy.key,
        vector_tile_runtime_manifest_object_key(VectorTileRuntimeManifestId::new(id))
    );
    assert!(matches!(copy.write_mode, ObjectWriteMode::CreateOnly));
    assert_eq!(copy.cache_control, "public, max-age=31536000, immutable");
    assert_eq!(pointer.key, VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY);
    assert!(matches!(
        pointer.write_mode,
        ObjectWriteMode::OverwriteAllowed
    ));
    assert_eq!(pointer.cache_control, "no-cache, max-age=0");
    // The bytes are the API's: the same response type, serialized once.
    assert_eq!(copy.body, serde_json::to_vec(&active)?);
    assert_eq!(pointer.body, copy.body);
    assert_eq!(pointer.sha256, copy.sha256);
    assert_eq!(*fallback.0.lock().await, [PUBLISHED]);
    Ok(())
}

#[tokio::test]
async fn a_retried_event_still_moves_the_pointer_when_the_copy_exists() -> TestResult {
    let id = Uuid::now_v7();
    let fallback = RecordingFallback::default();
    let storage = RecordingStorage {
        existing: vec![vector_tile_runtime_manifest_object_key(
            VectorTileRuntimeManifestId::new(id),
        )],
        ..RecordingStorage::default()
    };
    broadcaster(Some(manifest(id)), &storage, &fallback)
        .publish(&published(id))
        .await?;
    let keys: Vec<String> = storage
        .requests
        .lock()
        .await
        .iter()
        .map(|request| request.key.clone())
        .collect();
    assert_eq!(
        keys.last().map(String::as_str),
        Some(VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY)
    );
    Ok(())
}

#[tokio::test]
async fn a_superseded_publication_writes_nothing_and_is_still_fanned_out() -> TestResult {
    let (storage, fallback) = (RecordingStorage::default(), RecordingFallback::default());
    broadcaster(Some(manifest(Uuid::now_v7())), &storage, &fallback)
        .publish(&published(Uuid::now_v7()))
        .await?;
    assert!(storage.requests.lock().await.is_empty());
    assert_eq!(*fallback.0.lock().await, [PUBLISHED]);
    Ok(())
}

#[tokio::test]
async fn no_active_manifest_is_an_error_so_the_event_is_retried() -> TestResult {
    let (storage, fallback) = (RecordingStorage::default(), RecordingFallback::default());
    let result = broadcaster(None, &storage, &fallback)
        .publish(&published(Uuid::now_v7()))
        .await;
    assert!(matches!(result, Err(PublishError::Infrastructure(_))));
    assert!(storage.requests.lock().await.is_empty());
    assert!(fallback.0.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn without_a_runtime_target_the_event_only_fans_out() -> TestResult {
    let fallback = RecordingFallback::default();
    let storage = RecordingStorage::default();
    CatalogEventBroadcaster::new(
        Arc::new(NoV1Manifest),
        Arc::new(storage.clone()),
        Arc::new(fallback.clone()),
    )
    .publish(&published(Uuid::now_v7()))
    .await?;
    assert!(storage.requests.lock().await.is_empty());
    assert_eq!(*fallback.0.lock().await, [PUBLISHED]);
    Ok(())
}
