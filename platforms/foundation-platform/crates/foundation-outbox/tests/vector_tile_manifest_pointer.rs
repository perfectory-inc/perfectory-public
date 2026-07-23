//! Contract tests for publishing active vector tile manifest pointers.

use std::{collections::BTreeMap, error::Error, io, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use foundation_contracts::catalog::{
    VectorTileArtifactResponse, VectorTileLineageResponse, VectorTileManifestResponse,
};
use foundation_outbox::{
    broadcaster::EventEnvelope,
    object_storage::{ObjectStorageService, PutObjectRequest},
    vector_tile_manifest::{
        CatalogEventBroadcaster, PublishedVectorTileManifest, VectorTileManifestReader,
        MANIFEST_POINTER_OBJECT_KEY,
    },
    EventBroadcaster, OutboxScope, PublishError,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug)]
struct FakeManifestReader {
    manifest: Option<PublishedVectorTileManifest>,
}

#[async_trait]
impl VectorTileManifestReader for FakeManifestReader {
    async fn get_active_manifest(
        &self,
    ) -> Result<Option<PublishedVectorTileManifest>, PublishError> {
        Ok(self.manifest.clone())
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

#[derive(Clone, Debug, Default)]
struct RecordingBroadcaster {
    event_types: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EventBroadcaster for RecordingBroadcaster {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        self.event_types.lock().await.push(event.event_type.clone());
        Ok(())
    }
}

#[tokio::test]
async fn rollback_event_publishes_active_manifest_to_canonical_pointer() -> TestResult {
    let manifest_id = Uuid::now_v7();
    let storage = RecordingObjectStorage::default();
    let fallback = RecordingBroadcaster::default();
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(FakeManifestReader {
            manifest: Some(test_manifest(
                manifest_id,
                "0196e7e0-3c20-7000-8000-000000000041",
                "0196e7e0-3c20-7000-8000-000000000042",
            )),
        }),
        Arc::new(storage.clone()),
        Arc::new(fallback.clone()),
    );

    broadcaster.publish(&rollback_event(manifest_id)).await?;

    let request = single_recorded_request(&storage).await?;
    assert_eq!(request.key, MANIFEST_POINTER_OBJECT_KEY);
    assert_eq!(request.content_type, "application/json");
    assert_eq!(request.cache_control, "no-cache, max-age=0");

    let body: Value = serde_json::from_slice(&request.body)?;
    assert_eq!(body["schema_version"], 1);
    assert_eq!(
        body["current_version"],
        "0196e7e0-3c20-7000-8000-000000000041"
    );
    assert_eq!(
        body["previous_version"],
        "0196e7e0-3c20-7000-8000-000000000042"
    );
    assert_eq!(
        body["tiles_url_template"],
        "{object_key_prefix}/{z}/{x}/{y}.pbf"
    );
    assert_eq!(body["artifacts"]["parcels"]["source_layer"], "parcels");
    let source_file_assets = body
        .pointer("/artifacts/parcels/lineage/source_file_asset_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| io::Error::other("source_file_asset_ids must be an array"))?;
    assert_eq!(source_file_assets.len(), 1);
    assert!(fallback.event_types.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn promote_event_publishes_active_manifest_to_canonical_pointer() -> TestResult {
    let manifest_id = Uuid::now_v7();
    let storage = RecordingObjectStorage::default();
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(FakeManifestReader {
            manifest: Some(test_manifest(
                manifest_id,
                "0196e7e0-3c20-7000-8000-000000000043",
                "0196e7e0-3c20-7000-8000-000000000042",
            )),
        }),
        Arc::new(storage.clone()),
        Arc::new(RecordingBroadcaster::default()),
    );

    broadcaster.publish(&promote_event(manifest_id)).await?;

    let request = single_recorded_request(&storage).await?;
    assert_eq!(request.key, MANIFEST_POINTER_OBJECT_KEY);
    let body: Value = serde_json::from_slice(&request.body)?;
    assert_eq!(
        body["current_version"],
        "0196e7e0-3c20-7000-8000-000000000043"
    );
    assert_eq!(
        body["previous_version"],
        "0196e7e0-3c20-7000-8000-000000000042"
    );
    Ok(())
}

#[tokio::test]
async fn rollback_event_does_not_publish_when_event_manifest_is_no_longer_active() -> TestResult {
    let event_manifest_id = Uuid::now_v7();
    let active_manifest_id = Uuid::now_v7();
    let storage = RecordingObjectStorage::default();
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(FakeManifestReader {
            manifest: Some(test_manifest(
                active_manifest_id,
                "0196e7e0-3c20-7000-8000-000000000043",
                "0196e7e0-3c20-7000-8000-000000000042",
            )),
        }),
        Arc::new(storage.clone()),
        Arc::new(RecordingBroadcaster::default()),
    );

    broadcaster
        .publish(&rollback_event(event_manifest_id))
        .await?;

    assert!(storage.requests.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn non_manifest_pointer_events_are_delegated_to_fallback_broadcaster() -> TestResult {
    let fallback = RecordingBroadcaster::default();
    let broadcaster = CatalogEventBroadcaster::new(
        Arc::new(FakeManifestReader { manifest: None }),
        Arc::new(RecordingObjectStorage::default()),
        Arc::new(fallback.clone()),
    );
    let event = EventEnvelope {
        event_id: Uuid::now_v7(),
        event_type: "catalog.industrial_complex.created.v1".to_owned(),
        payload: json!({ "type": "catalog.industrial_complex.created.v1" }),
        occurred_at: Utc::now(),
        scope: OutboxScope::Catalog,
    };

    broadcaster.publish(&event).await?;

    assert_eq!(
        fallback.event_types.lock().await.as_slice(),
        ["catalog.industrial_complex.created.v1"]
    );
    Ok(())
}

async fn single_recorded_request(storage: &RecordingObjectStorage) -> TestResult<PutObjectRequest> {
    let requests = storage.requests.lock().await;
    assert_eq!(requests.len(), 1);
    requests
        .first()
        .cloned()
        .ok_or_else(|| io::Error::other("object storage request was not recorded").into())
}

fn rollback_event(manifest_id: Uuid) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::now_v7(),
        event_type: "catalog.vector_tile_manifest.rolled_back.v1".to_owned(),
        payload: json!({
            "type": "catalog.vector_tile_manifest.rolled_back.v1",
            "manifest_id": manifest_id,
        }),
        occurred_at: Utc::now(),
        scope: OutboxScope::Catalog,
    }
}

fn promote_event(manifest_id: Uuid) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::now_v7(),
        event_type: "catalog.vector_tile_manifest.promoted.v1".to_owned(),
        payload: json!({
            "type": "catalog.vector_tile_manifest.promoted.v1",
            "manifest_id": manifest_id,
        }),
        occurred_at: Utc::now(),
        scope: OutboxScope::Catalog,
    }
}

fn test_manifest(
    manifest_id: Uuid,
    current_version: &str,
    previous_version: &str,
) -> PublishedVectorTileManifest {
    let manifest_file_asset_id = Uuid::now_v7();
    let source_record_id = Uuid::now_v7();
    let tilejson_file_asset_id = Uuid::now_v7();
    let source_file_asset_id = Uuid::now_v7();
    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        "parcels".to_owned(),
        VectorTileArtifactResponse {
            source_layer: "parcels".to_owned(),
            tile_min_zoom: 8,
            tile_max_zoom: 16,
            render_min_zoom: 10,
            render_max_zoom: 22,
            tilejson_object_key: format!(
                "gold/vector-tiles/artifacts/{current_version}/parcels.json"
            ),
            object_key_prefix: format!("gold/vector-tiles/artifacts/{current_version}/parcels/"),
            flat_tile_count: 42,
            flat_tile_total_bytes: 4096,
            feature_filter_properties: BTreeMap::from([("pnu".to_owned(), "pnu".to_owned())]),
            lineage: VectorTileLineageResponse {
                source_record_id,
                manifest_file_asset_id,
                tilejson_file_asset_id,
                source_file_asset_ids: vec![source_file_asset_id],
            },
        },
    );

    PublishedVectorTileManifest {
        manifest_id,
        document: VectorTileManifestResponse {
            schema_version: 1,
            current_version: current_version.to_owned(),
            previous_version: previous_version.to_owned(),
            tiles_url_template: "{object_key_prefix}/{z}/{x}/{y}.pbf".to_owned(),
            published_at: Utc::now(),
            artifacts,
        },
    }
}
