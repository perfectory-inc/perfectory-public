//! `PostgreSQL` tests for atomic vector tile manifest promotion.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::unwrap_used
)]

use std::{collections::BTreeMap, sync::LazyLock};

use catalog_application::ports::{
    CatalogRepository, CatalogUnitOfWork, VectorTileArtifactPromotionCommand,
    VectorTileFileAssetCommand, VectorTileManifestPromotionCommand, VectorTileSourceRecordCommand,
};
use catalog_domain::CatalogError;
use catalog_infrastructure::{PgCatalogRepository, PgCatalogUnitOfWork};
use foundation_shared_kernel::ids::StaffId;
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    match PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skipping - could not connect to DATABASE_URL: {e}");
            None
        }
    }
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn promote_vector_tile_manifest_registers_assets_and_switches_active_atomically() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgCatalogRepository::new(pool.clone());
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let fixture = VectorTilePromoteFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert_active_manifest(&pool).await;

    let promoted = uow
        .promote_vector_tile_manifest(fixture.promote_command(&fixture.active_version))
        .await
        .expect("promote vector tile manifest");

    assert_eq!(promoted.current_version, fixture.target_version);
    assert_eq!(promoted.previous_version, fixture.active_version);
    assert_eq!(promoted.artifacts.len(), 1);
    assert_eq!(promoted.artifacts[0].layer, "parcels");
    assert_eq!(
        promoted.artifacts[0].object_key_prefix.as_str(),
        fixture.target_object_key_prefix
    );
    assert_eq!(promoted.version, 2);

    let active_manifest = repo
        .get_active_vector_tile_manifest()
        .await
        .expect("read active manifest")
        .expect("active manifest exists");
    assert_eq!(active_manifest.current_version, fixture.target_version);
    assert_eq!(active_manifest.previous_version, fixture.active_version);

    let old_active_is_inactive: bool = sqlx::query_scalar(
        "SELECT NOT is_active
         FROM catalog.vector_tile_manifest
         WHERE id = $1",
    )
    .bind(fixture.active_manifest_id)
    .fetch_one(&pool)
    .await
    .expect("old active inactive flag");
    assert!(old_active_is_inactive);

    let object_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM catalog.file_asset
         WHERE object_key = ANY($1)",
    )
    .bind(vec![
        fixture.manifest_object_key.clone(),
        fixture.tilejson_object_key.clone(),
        fixture.source_object_key.clone(),
    ])
    .fetch_one(&pool)
    .await
    .expect("promoted file assets");
    assert_eq!(object_count, 3);

    let outbox_payload: JsonValue = sqlx::query_scalar(
        "SELECT payload
         FROM catalog.outbox_event
         WHERE type = 'catalog.vector_tile_manifest.promoted.v1'
           AND payload->>'current_version' = $1
         ORDER BY occurred_at DESC
         LIMIT 1",
    )
    .bind(&fixture.target_version)
    .fetch_one(&pool)
    .await
    .expect("promote outbox event");
    assert_eq!(
        outbox_payload["previous_version"].as_str(),
        Some(fixture.active_version.as_str())
    );
    assert_eq!(
        outbox_payload["expected_current_version"].as_str(),
        Some(fixture.active_version.as_str())
    );
    assert_eq!(
        outbox_payload["previous_manifest_id"].as_str(),
        Some(fixture.active_manifest_id.to_string().as_str())
    );
    assert_eq!(
        outbox_payload["operator_staff_id"].as_str(),
        Some(fixture.operator_staff_id.as_uuid().to_string().as_str())
    );
    assert_eq!(
        outbox_payload["request_id"].as_str(),
        Some("promote-test-request")
    );

    fixture.cleanup(&pool).await;
    active_snapshot.restore(&pool).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn promote_vector_tile_manifest_rejects_stale_expected_current_version() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let fixture = VectorTilePromoteFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert_active_manifest(&pool).await;

    let err = uow
        .promote_vector_tile_manifest(fixture.promote_command("stale-version"))
        .await
        .expect_err("stale promote must fail");

    match err {
        CatalogError::VectorTileManifestVersionConflict { expected, current } => {
            assert_eq!(expected, "stale-version");
            assert_eq!(current, fixture.active_version);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let promoted_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM catalog.vector_tile_manifest
         WHERE current_version = $1",
    )
    .bind(&fixture.target_version)
    .fetch_one(&pool)
    .await
    .expect("promoted manifest count");
    assert_eq!(promoted_count, 0);

    let promote_outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM catalog.outbox_event
         WHERE type = 'catalog.vector_tile_manifest.promoted.v1'
           AND payload->>'current_version' = $1",
    )
    .bind(&fixture.target_version)
    .fetch_one(&pool)
    .await
    .expect("promote outbox count");
    assert_eq!(promote_outbox_count, 0);

    fixture.cleanup(&pool).await;
    active_snapshot.restore(&pool).await;
}

struct ActiveManifestSnapshot {
    active_manifest_ids: Vec<Uuid>,
}

impl ActiveManifestSnapshot {
    async fn pause(pool: &PgPool) -> Self {
        let active_manifest_ids = sqlx::query_scalar(
            "SELECT id
             FROM catalog.vector_tile_manifest
             WHERE is_active = true
             ORDER BY published_at DESC",
        )
        .fetch_all(pool)
        .await
        .expect("snapshot active manifests");

        sqlx::query(
            "UPDATE catalog.vector_tile_manifest
             SET is_active = false
             WHERE is_active = true",
        )
        .execute(pool)
        .await
        .expect("pause active manifests");

        Self {
            active_manifest_ids,
        }
    }

    async fn restore(&self, pool: &PgPool) {
        for manifest_id in &self.active_manifest_ids {
            sqlx::query(
                "UPDATE catalog.vector_tile_manifest
                 SET is_active = true
                 WHERE id = $1",
            )
            .bind(manifest_id)
            .execute(pool)
            .await
            .expect("restore active manifest");
        }
    }
}

struct VectorTilePromoteFixture {
    active_source_record_id: Uuid,
    active_manifest_file_asset_id: Uuid,
    active_tilejson_file_asset_id: Uuid,
    active_source_file_asset_id: Uuid,
    active_manifest_id: Uuid,
    active_artifact_id: Uuid,
    active_version: String,
    target_version: String,
    target_object_key_prefix: String,
    manifest_object_key: String,
    tilejson_object_key: String,
    source_object_key: String,
    operator_staff_id: StaffId,
}

impl VectorTilePromoteFixture {
    fn new() -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let active_version = format!("v-active-{suffix}");
        let target_version = format!("v-target-{suffix}");
        Self {
            active_source_record_id: Uuid::now_v7(),
            active_manifest_file_asset_id: Uuid::now_v7(),
            active_tilejson_file_asset_id: Uuid::now_v7(),
            active_source_file_asset_id: Uuid::now_v7(),
            active_manifest_id: Uuid::now_v7(),
            active_artifact_id: Uuid::now_v7(),
            target_object_key_prefix: format!("gold/{target_version}/parcels/"),
            manifest_object_key: format!("gold/manifest.{target_version}.json"),
            tilejson_object_key: format!("gold/{target_version}/parcels.json"),
            source_object_key: format!("gold/source/{target_version}.zip"),
            active_version,
            target_version,
            operator_staff_id: StaffId::new(Uuid::now_v7()),
        }
    }

    fn promote_command(
        &self,
        expected_current_version: &str,
    ) -> VectorTileManifestPromotionCommand {
        let mut artifacts = BTreeMap::new();
        artifacts.insert(
            "parcels".to_owned(),
            VectorTileArtifactPromotionCommand {
                source_layer: "parcels".to_owned(),
                tile_min_zoom: 8,
                tile_max_zoom: 16,
                render_min_zoom: 10,
                render_max_zoom: 22,
                tilejson_file_asset: VectorTileFileAssetCommand {
                    object_key: self.tilejson_object_key.clone(),
                    mime_type: "application/json".to_owned(),
                    size_bytes: 2048,
                    checksum_sha256: Some("b".repeat(64)),
                    title: Some("target tilejson".to_owned()),
                    visibility: "public".to_owned(),
                },
                object_key_prefix: self.target_object_key_prefix.clone(),
                flat_tile_count: 11,
                flat_tile_total_bytes: 4096,
                source_file_assets: vec![VectorTileFileAssetCommand {
                    object_key: self.source_object_key.clone(),
                    mime_type: "application/zip".to_owned(),
                    size_bytes: 8192,
                    checksum_sha256: Some("c".repeat(64)),
                    title: Some("target source".to_owned()),
                    visibility: "internal".to_owned(),
                }],
            },
        );

        VectorTileManifestPromotionCommand {
            current_version: self.target_version.clone(),
            expected_current_version: expected_current_version.to_owned(),
            tiles_url_template: "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
                .to_owned(),
            source_record: VectorTileSourceRecordCommand {
                source: "fixture-vector-tile-promote".to_owned(),
                source_url: Some("https://example.test/vector-tile-build".to_owned()),
                external_id: Some(self.target_version.clone()),
                checksum_sha256: Some("a".repeat(64)),
                raw_object_key: Some(self.source_object_key.clone()),
            },
            manifest_file_asset: VectorTileFileAssetCommand {
                object_key: self.manifest_object_key.clone(),
                mime_type: "application/json".to_owned(),
                size_bytes: 1024,
                checksum_sha256: Some("d".repeat(64)),
                title: Some("target manifest".to_owned()),
                visibility: "public".to_owned(),
            },
            artifacts,
            operator_staff_id: self.operator_staff_id,
            request_id: Some("promote-test-request".to_owned()),
        }
    }

    async fn insert_active_manifest(&self, pool: &PgPool) {
        sqlx::query(
            "INSERT INTO catalog.source_record
             (id, source, source_url, external_id, checksum_sha256)
             VALUES ($1, 'fixture-vector-tile-promote-active', 'https://example.test/active.zip', $2, $3)",
        )
        .bind(self.active_source_record_id)
        .bind(&self.active_version)
        .bind("1".repeat(64))
        .execute(pool)
        .await
        .expect("insert active source record");

        self.insert_file_asset(
            pool,
            self.active_manifest_file_asset_id,
            &format!("gold/manifest.{}.json", self.active_version),
        )
        .await;
        self.insert_file_asset(
            pool,
            self.active_tilejson_file_asset_id,
            &format!("gold/{}/parcels.json", self.active_version),
        )
        .await;
        self.insert_file_asset(
            pool,
            self.active_source_file_asset_id,
            &format!("gold/source/{}.zip", self.active_version),
        )
        .await;

        sqlx::query(
            "INSERT INTO catalog.vector_tile_manifest
             (id, current_version, previous_version, tiles_url_template,
              manifest_file_asset_id, source_record_id, is_active, version)
             VALUES ($1, $2, 'v-before-active',
              'https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf',
              $3, $4, true, 3)",
        )
        .bind(self.active_manifest_id)
        .bind(&self.active_version)
        .bind(self.active_manifest_file_asset_id)
        .bind(self.active_source_record_id)
        .execute(pool)
        .await
        .expect("insert active manifest");

        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact
             (id, manifest_id, layer, source_layer, tile_min_zoom, tile_max_zoom,
              render_min_zoom, render_max_zoom, tilejson_file_asset_id, object_key_prefix,
              flat_tile_count, flat_tile_total_bytes, source_record_id, version)
             VALUES ($1, $2, 'parcels', 'parcels', 8, 16, 10, 22, $3, $4, 11, 4096, $5, 1)",
        )
        .bind(self.active_artifact_id)
        .bind(self.active_manifest_id)
        .bind(self.active_tilejson_file_asset_id)
        .bind(format!("gold/{}/parcels/", self.active_version))
        .bind(self.active_source_record_id)
        .execute(pool)
        .await
        .expect("insert active artifact");

        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact_source_file_asset
             (artifact_id, file_asset_id)
             VALUES ($1, $2)",
        )
        .bind(self.active_artifact_id)
        .bind(self.active_source_file_asset_id)
        .execute(pool)
        .await
        .expect("insert active artifact source file");
    }

    async fn insert_file_asset(&self, pool: &PgPool, file_asset_id: Uuid, object_key: &str) {
        sqlx::query(
            "INSERT INTO catalog.file_asset
             (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
             VALUES ($1, $2, 'application/json', 10, $3, 'internal', 1)",
        )
        .bind(file_asset_id)
        .bind(object_key)
        .bind(self.active_source_record_id)
        .execute(pool)
        .await
        .expect("insert active file asset");
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query(
            "DELETE FROM catalog.outbox_event
             WHERE type = 'catalog.vector_tile_manifest.promoted.v1'
               AND payload->>'current_version' = $1",
        )
        .bind(&self.target_version)
        .execute(pool)
        .await
        .expect("cleanup promote outbox");
        sqlx::query(
            "DELETE FROM catalog.vector_tile_manifest
             WHERE current_version = ANY($1)",
        )
        .bind(vec![
            self.active_version.clone(),
            self.target_version.clone(),
        ])
        .execute(pool)
        .await
        .expect("cleanup manifests");
        sqlx::query(
            "DELETE FROM catalog.file_asset
             WHERE object_key = ANY($1)",
        )
        .bind(vec![
            format!("gold/manifest.{}.json", self.active_version),
            format!("gold/{}/parcels.json", self.active_version),
            format!("gold/source/{}.zip", self.active_version),
            self.manifest_object_key.clone(),
            self.tilejson_object_key.clone(),
            self.source_object_key.clone(),
        ])
        .execute(pool)
        .await
        .expect("cleanup file assets");
        sqlx::query(
            "DELETE FROM catalog.source_record
             WHERE external_id = ANY($1)",
        )
        .bind(vec![
            self.active_version.clone(),
            self.target_version.clone(),
        ])
        .execute(pool)
        .await
        .expect("cleanup source records");
    }
}
