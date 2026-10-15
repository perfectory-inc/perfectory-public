use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use catalog_application::ports::CatalogRepository;
use catalog_domain::{VectorTileArtifact, VectorTileManifest};
use catalog_infrastructure::PgCatalogRepository;
use foundation_contracts::catalog::{
    VectorTileArtifactResponse, VectorTileLineageResponse, VectorTileManifestResponse,
    VectorTileRuntimeManifestResponse,
};
use foundation_shared_kernel::events::catalog_v1::{
    vector_tile_runtime_manifest_object_key, VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY,
};
use foundation_shared_kernel::ids::VectorTileRuntimeManifestId;
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    broadcaster::{EventBroadcaster, EventEnvelope},
    errors::PublishError,
    object_storage::{ObjectStorageService, ObjectWriteMode, PutObjectRequest},
};

/// Canonical object key for the public static vector tile manifest pointer.
pub const MANIFEST_POINTER_OBJECT_KEY: &str = "gold/manifest.json";

const MANIFEST_POINTER_CONTENT_TYPE: &str = "application/json";
const MANIFEST_POINTER_CACHE_CONTROL: &str = "no-cache, max-age=0";
const ROLLED_BACK_EVENT_TYPE: &str = "catalog.vector_tile_manifest.rolled_back.v1";
/// Root ADR-0254: a complete v2 runtime manifest became the active one.
const RUNTIME_PUBLISHED_EVENT_TYPE: &str = "catalog.vector_tile_runtime_manifest.published.v2";
/// The immutable copy of one runtime manifest: its id names it, so it never changes.
const RUNTIME_COPY_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const PROMOTED_EVENT_TYPE: &str = "catalog.vector_tile_manifest.promoted.v1";

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
/// Active manifest document and its Catalog manifest id.
pub struct PublishedVectorTileManifest {
    /// Catalog `vector_tile_manifest.id`.
    pub manifest_id: Uuid,
    /// Runtime manifest JSON document written to the canonical pointer.
    pub document: VectorTileManifestResponse,
}

#[async_trait]
#[allow(clippy::module_name_repetitions)]
/// Reads the currently active vector tile manifest from Catalog storage.
pub trait VectorTileManifestReader: Send + Sync {
    /// Returns the manifest currently marked active by Catalog.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when Catalog storage cannot be read.
    async fn get_active_manifest(
        &self,
    ) -> Result<Option<PublishedVectorTileManifest>, PublishError>;
}

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
/// Postgres-backed active vector tile manifest reader.
pub struct PgVectorTileManifestReader {
    pool: PgPool,
}

impl PgVectorTileManifestReader {
    /// Creates a Postgres-backed manifest reader.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VectorTileManifestReader for PgVectorTileManifestReader {
    async fn get_active_manifest(
        &self,
    ) -> Result<Option<PublishedVectorTileManifest>, PublishError> {
        let repository = PgCatalogRepository::new(self.pool.clone());
        let manifest = repository
            .get_active_vector_tile_manifest()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        Ok(manifest.map(vector_tile_manifest_document))
    }
}

#[async_trait]
/// Reads the v2 runtime manifest Catalog marks active, in the shape the API serves (root ADR-0254).
pub trait RuntimeManifestReader: Send + Sync {
    /// Returns the active v2 runtime manifest, or `None` when none was published.
    ///
    /// # Errors
    ///
    /// Returns `PublishError` when Catalog storage cannot be read.
    async fn get_active_runtime_manifest(
        &self,
    ) -> Result<Option<VectorTileRuntimeManifestResponse>, PublishError>;
}

#[async_trait]
impl RuntimeManifestReader for PgVectorTileManifestReader {
    async fn get_active_runtime_manifest(
        &self,
    ) -> Result<Option<VectorTileRuntimeManifestResponse>, PublishError> {
        let manifest = PgCatalogRepository::new(self.pool.clone())
            .get_active_vector_tile_runtime_manifest()
            .await
            .map_err(|error| PublishError::Infrastructure(error.to_string()))?;
        Ok(manifest.map(VectorTileRuntimeManifestResponse::from))
    }
}

#[derive(Clone)]
#[allow(clippy::module_name_repetitions)]
/// Catalog event broadcaster that materializes active vector tile manifests to object storage.
pub struct CatalogEventBroadcaster {
    manifest_reader: Arc<dyn VectorTileManifestReader>,
    object_storage: Arc<dyn ObjectStorageService>,
    fallback: Arc<dyn EventBroadcaster>,
    /// Root ADR-0254: where each active v2 runtime manifest is written for the tile edge.
    runtime: Option<(
        Arc<dyn RuntimeManifestReader>,
        Arc<dyn ObjectStorageService>,
    )>,
}

impl CatalogEventBroadcaster {
    /// Creates a Catalog event broadcaster.
    ///
    /// Manifest promote and rollback events write the active manifest pointer; other events are
    /// delegated to `fallback`.
    #[must_use]
    pub fn new(
        manifest_reader: Arc<dyn VectorTileManifestReader>,
        object_storage: Arc<dyn ObjectStorageService>,
        fallback: Arc<dyn EventBroadcaster>,
    ) -> Self {
        Self {
            manifest_reader,
            object_storage,
            fallback,
            runtime: None,
        }
    }

    /// Root ADR-0254: on every runtime manifest publication, also write the active v2 manifest to
    /// `storage` — create-only under its id (`vector_tile_runtime_manifest_object_key`) and as the
    /// pointer the tile edge serves — before the event goes on to `fallback`.
    #[must_use]
    pub fn with_runtime_manifest(
        mut self,
        reader: Arc<dyn RuntimeManifestReader>,
        storage: Arc<dyn ObjectStorageService>,
    ) -> Self {
        self.runtime = Some((reader, storage));
        self
    }

    async fn publish_runtime_manifest(
        &self,
        reader: &dyn RuntimeManifestReader,
        storage: &dyn ObjectStorageService,
        event: &EventEnvelope,
    ) -> Result<(), PublishError> {
        let event_manifest_id = event_manifest_id(event)?;
        let active = reader.get_active_runtime_manifest().await?.ok_or_else(|| {
            PublishError::Infrastructure(
                "no active v2 runtime manifest while writing its published event".to_owned(),
            )
        })?;
        if active.current_version != event_manifest_id {
            // A later publication is active; its own event writes it. Nothing older replaces it.
            tracing::warn!(
                event_id = %event.event_id,
                event_manifest_id = %event_manifest_id,
                active_manifest_id = %active.current_version,
                "skipping a superseded runtime manifest publication"
            );
            return self.fallback.publish(event).await;
        }
        let body = serde_json::to_vec(&active)
            .map_err(|error| PublishError::Broadcaster(error.to_string()))?;
        let sha256 = format!("{:x}", Sha256::digest(&body));
        let copy = PutObjectRequest {
            key: vector_tile_runtime_manifest_object_key(VectorTileRuntimeManifestId::new(
                event_manifest_id,
            )),
            body: body.clone(),
            content_type: MANIFEST_POINTER_CONTENT_TYPE.to_owned(),
            cache_control: RUNTIME_COPY_CACHE_CONTROL.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(sha256.clone()),
        };
        match storage.put_object(copy).await {
            // A retried event: the copy is derived from the immutable manifest row, so it is the same.
            Ok(()) | Err(PublishError::ObjectAlreadyExists { .. }) => {}
            Err(error) => return Err(error),
        }
        storage
            .put_object(PutObjectRequest {
                key: VECTOR_TILE_RUNTIME_MANIFEST_POINTER_OBJECT_KEY.to_owned(),
                body,
                content_type: MANIFEST_POINTER_CONTENT_TYPE.to_owned(),
                cache_control: MANIFEST_POINTER_CACHE_CONTROL.to_owned(),
                // The pointer moves with every publication.
                write_mode: ObjectWriteMode::OverwriteAllowed,
                sha256: Some(sha256),
            })
            .await?;
        self.fallback.publish(event).await
    }

    async fn publish_manifest_pointer(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        let event_manifest_id = event_manifest_id(event)?;
        let active = self
            .manifest_reader
            .get_active_manifest()
            .await?
            .ok_or_else(|| {
                PublishError::Infrastructure(
                    "active vector tile manifest is missing during pointer publish".to_owned(),
                )
            })?;

        if active.manifest_id != event_manifest_id {
            tracing::warn!(
                event_id = %event.event_id,
                event_type = %event.event_type,
                event_manifest_id = %event_manifest_id,
                active_manifest_id = %active.manifest_id,
                "skipping stale vector tile manifest pointer event"
            );
            return Ok(());
        }

        let body = serde_json::to_vec(&active.document)
            .map_err(|error| PublishError::Broadcaster(error.to_string()))?;
        self.object_storage
            .put_object(PutObjectRequest {
                key: MANIFEST_POINTER_OBJECT_KEY.to_owned(),
                body,
                content_type: MANIFEST_POINTER_CONTENT_TYPE.to_owned(),
                cache_control: MANIFEST_POINTER_CACHE_CONTROL.to_owned(),
                // Mutable manifest pointer: re-points each promotion. Stays OverwriteAllowed.
                write_mode: ObjectWriteMode::OverwriteAllowed,
                // The runtime pointer is not a Bronze object; no checksum metadata.
                sha256: None,
            })
            .await
    }
}

#[async_trait]
impl EventBroadcaster for CatalogEventBroadcaster {
    async fn publish(&self, event: &EventEnvelope) -> Result<(), PublishError> {
        if is_manifest_pointer_event(&event.event_type) {
            self.publish_manifest_pointer(event).await
        } else if let (RUNTIME_PUBLISHED_EVENT_TYPE, Some((reader, storage))) =
            (event.event_type.as_str(), &self.runtime)
        {
            self.publish_runtime_manifest(reader.as_ref(), storage.as_ref(), event)
                .await
        } else {
            self.fallback.publish(event).await
        }
    }
}

fn is_manifest_pointer_event(event_type: &str) -> bool {
    matches!(event_type, ROLLED_BACK_EVENT_TYPE | PROMOTED_EVENT_TYPE)
}

fn event_manifest_id(event: &EventEnvelope) -> Result<Uuid, PublishError> {
    let raw_manifest_id = event
        .payload
        .get("manifest_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            PublishError::Broadcaster(format!(
                "{} payload is missing manifest_id",
                event.event_type
            ))
        })?;
    Uuid::parse_str(raw_manifest_id).map_err(|error| {
        PublishError::Broadcaster(format!(
            "{} payload has invalid manifest_id {raw_manifest_id}: {error}",
            event.event_type
        ))
    })
}

fn vector_tile_manifest_document(manifest: VectorTileManifest) -> PublishedVectorTileManifest {
    let manifest_id = manifest.id.as_uuid();
    let artifacts = manifest
        .artifacts
        .into_iter()
        .map(|artifact| {
            let layer = artifact.layer.clone();
            (layer, vector_tile_artifact_response(artifact))
        })
        .collect::<BTreeMap<_, _>>();

    PublishedVectorTileManifest {
        manifest_id,
        document: VectorTileManifestResponse {
            schema_version: 1,
            current_version: manifest.current_version,
            previous_version: manifest.previous_version,
            tiles_url_template: manifest.tiles_url_template.as_str().to_owned(),
            source_snapshot_id: manifest.source_snapshot_id,
            published_at: manifest.published_at,
            artifacts,
        },
    }
}

fn vector_tile_artifact_response(artifact: VectorTileArtifact) -> VectorTileArtifactResponse {
    let feature_filter_properties = artifact.feature_filter_properties();

    VectorTileArtifactResponse {
        source_layer: artifact.source_layer,
        tile_min_zoom: artifact.tile_zoom.min(),
        tile_max_zoom: artifact.tile_zoom.max(),
        render_min_zoom: artifact.render_zoom.min(),
        render_max_zoom: artifact.render_zoom.max(),
        tilejson_object_key: artifact.tilejson_object_key.as_str().to_owned(),
        object_key_prefix: artifact.object_key_prefix.as_str().to_owned(),
        flat_tile_count: artifact.flat_tile_count,
        flat_tile_total_bytes: artifact.flat_tile_total_bytes,
        feature_filter_properties,
        lineage: VectorTileLineageResponse {
            source_record_id: artifact.lineage.source_record_id.as_uuid(),
            manifest_file_asset_id: artifact.lineage.manifest_file_asset_id.as_uuid(),
            tilejson_file_asset_id: artifact.lineage.tilejson_file_asset_id.as_uuid(),
            source_file_asset_ids: artifact
                .lineage
                .source_file_asset_ids
                .into_iter()
                .map(|id| id.as_uuid())
                .collect(),
        },
    }
}

#[cfg(test)]
mod tests {
    use catalog_domain::{
        TilesUrlTemplate, VectorTileArtifact, VectorTileLineage, VectorTileManifest, ZoomRange,
    };
    use chrono::Utc;
    use foundation_shared_kernel::ids::{
        FileAssetId, SourceRecordId, VectorTileArtifactId, VectorTileManifestId,
    };
    use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
    use uuid::Uuid;

    use super::vector_tile_manifest_document;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn manifest_pointer_document_advertises_reference_feature_filter_properties() -> TestResult {
        let now = Utc::now();
        let manifest_id = VectorTileManifestId::new(Uuid::now_v7());
        let document = vector_tile_manifest_document(VectorTileManifest {
            id: manifest_id,
            current_version: "0196e7e0-3c20-7000-8000-000000000042".to_owned(),
            previous_version: "0196e7e0-3c20-7000-8000-000000000041".to_owned(),
            tiles_url_template: TilesUrlTemplate::parse("{object_key_prefix}/{z}/{x}/{y}.pbf")?,
            published_at: now,
            manifest_file_asset_id: FileAssetId::new(Uuid::now_v7()),
            source_record_id: SourceRecordId::new(Uuid::now_v7()),
            source_snapshot_id: "iceberg:test-snapshot".to_owned(),
            artifacts: vec![VectorTileArtifact {
                id: VectorTileArtifactId::new(Uuid::now_v7()),
                manifest_id,
                layer: "complex".to_owned(),
                source_layer: "complex".to_owned(),
                tile_zoom: ZoomRange::new(5, 16)?,
                render_zoom: ZoomRange::new(5, 22)?,
                tilejson_object_key: ObjectKey::parse(
                    "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/complex.json",
                )?,
                object_key_prefix: ObjectKeyPrefix::parse(
                    "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/complex/",
                )?,
                flat_tile_count: 10,
                flat_tile_total_bytes: 2048,
                lineage: VectorTileLineage {
                    source_record_id: SourceRecordId::new(Uuid::now_v7()),
                    manifest_file_asset_id: FileAssetId::new(Uuid::now_v7()),
                    tilejson_file_asset_id: FileAssetId::new(Uuid::now_v7()),
                    source_file_asset_ids: vec![FileAssetId::new(Uuid::now_v7())],
                },
                created_at: now,
                updated_at: now,
                version: 1,
            }],
            created_at: now,
            updated_at: now,
            version: 1,
        });

        let complex = document.document.artifacts.get("complex").ok_or_else(|| {
            std::io::Error::other("complex artifact missing from manifest document")
        })?;
        assert_eq!(
            complex
                .feature_filter_properties
                .get("official_complex_code")
                .map(String::as_str),
            Some("official_complex_code")
        );
        Ok(())
    }
}
