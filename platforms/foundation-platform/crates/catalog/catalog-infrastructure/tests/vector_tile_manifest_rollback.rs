//! `PostgreSQL` tests for atomic vector tile manifest rollback.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::unwrap_used
)]

use std::sync::LazyLock;

use catalog_application::ports::{
    CatalogRepository, CatalogUnitOfWork, VectorTileManifestRollbackCommand,
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
async fn rollback_vector_tile_manifest_promotes_existing_version_atomically() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgCatalogRepository::new(pool.clone());
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let fixture = VectorTileRollbackFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert(&pool).await;

    let rolled_back = uow
        .rollback_vector_tile_manifest(fixture.rollback_command(&fixture.active_version))
        .await
        .expect("rollback vector tile manifest");

    assert_eq!(rolled_back.current_version, fixture.target_version);
    assert_eq!(rolled_back.previous_version, fixture.active_version);
    assert_eq!(
        rolled_back.version,
        fixture.target_manifest_initial_version + 1
    );

    let active_manifest = repo
        .get_active_vector_tile_manifest()
        .await
        .expect("read active manifest")
        .expect("active manifest exists");
    assert_eq!(active_manifest.current_version, fixture.target_version);
    assert_eq!(active_manifest.previous_version, fixture.active_version);
    assert_eq!(active_manifest.artifacts.len(), 1);

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

    let outbox_payload: JsonValue = sqlx::query_scalar(
        "SELECT payload
         FROM catalog.outbox_event
         WHERE type = 'catalog.vector_tile_manifest.rolled_back.v1'
           AND payload->>'current_version' = $1
         ORDER BY occurred_at DESC
         LIMIT 1",
    )
    .bind(&fixture.target_version)
    .fetch_one(&pool)
    .await
    .expect("rollback outbox event");
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
        Some("rollback-test-request")
    );
    assert_eq!(
        outbox_payload["rollback_reason"].as_str(),
        Some("bad tile build")
    );

    fixture.cleanup(&pool).await;
    active_snapshot.restore(&pool).await;
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn rollback_vector_tile_manifest_rejects_stale_expected_current_version() {
    let _guard = TEST_LOCK.lock().await;
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgCatalogRepository::new(pool.clone());
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let fixture = VectorTileRollbackFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert(&pool).await;

    let err = uow
        .rollback_vector_tile_manifest(fixture.rollback_command("stale-version"))
        .await
        .expect_err("stale rollback must fail");

    match err {
        CatalogError::VectorTileManifestVersionConflict { expected, current } => {
            assert_eq!(expected, "stale-version");
            assert_eq!(current, fixture.active_version);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let active_manifest = repo
        .get_active_vector_tile_manifest()
        .await
        .expect("read active manifest")
        .expect("active manifest exists");
    assert_eq!(active_manifest.current_version, fixture.active_version);

    let rollback_outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM catalog.outbox_event
         WHERE type = 'catalog.vector_tile_manifest.rolled_back.v1'
           AND payload->>'current_version' = $1",
    )
    .bind(&fixture.target_version)
    .fetch_one(&pool)
    .await
    .expect("rollback outbox count");
    assert_eq!(rollback_outbox_count, 0);

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

struct VectorTileRollbackFixture {
    active_source_record_id: Uuid,
    target_source_record_id: Uuid,
    active_manifest_file_asset_id: Uuid,
    target_manifest_file_asset_id: Uuid,
    active_tilejson_file_asset_id: Uuid,
    target_tilejson_file_asset_id: Uuid,
    active_source_file_asset_id: Uuid,
    target_source_file_asset_id: Uuid,
    active_manifest_id: Uuid,
    target_manifest_id: Uuid,
    active_artifact_id: Uuid,
    target_artifact_id: Uuid,
    active_version: String,
    target_version: String,
    target_manifest_initial_version: i64,
    operator_staff_id: StaffId,
}

impl VectorTileRollbackFixture {
    fn new() -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        Self {
            active_source_record_id: Uuid::now_v7(),
            target_source_record_id: Uuid::now_v7(),
            active_manifest_file_asset_id: Uuid::now_v7(),
            target_manifest_file_asset_id: Uuid::now_v7(),
            active_tilejson_file_asset_id: Uuid::now_v7(),
            target_tilejson_file_asset_id: Uuid::now_v7(),
            active_source_file_asset_id: Uuid::now_v7(),
            target_source_file_asset_id: Uuid::now_v7(),
            active_manifest_id: Uuid::now_v7(),
            target_manifest_id: Uuid::now_v7(),
            active_artifact_id: Uuid::now_v7(),
            target_artifact_id: Uuid::now_v7(),
            active_version: format!("v-active-{suffix}"),
            target_version: format!("v-target-{suffix}"),
            target_manifest_initial_version: 7,
            operator_staff_id: StaffId::new(Uuid::now_v7()),
        }
    }

    fn rollback_command(
        &self,
        expected_current_version: &str,
    ) -> VectorTileManifestRollbackCommand {
        VectorTileManifestRollbackCommand {
            to_version: self.target_version.clone(),
            expected_current_version: expected_current_version.to_owned(),
            reason: "bad tile build".to_owned(),
            operator_staff_id: self.operator_staff_id,
            request_id: Some("rollback-test-request".to_owned()),
        }
    }

    async fn insert(&self, pool: &PgPool) {
        self.insert_manifest_family(
            pool,
            self.target_source_record_id,
            self.target_manifest_file_asset_id,
            self.target_tilejson_file_asset_id,
            self.target_source_file_asset_id,
            self.target_manifest_id,
            self.target_artifact_id,
            &self.target_version,
            "v-before-target",
            false,
            self.target_manifest_initial_version,
        )
        .await;
        self.insert_manifest_family(
            pool,
            self.active_source_record_id,
            self.active_manifest_file_asset_id,
            self.active_tilejson_file_asset_id,
            self.active_source_file_asset_id,
            self.active_manifest_id,
            self.active_artifact_id,
            &self.active_version,
            &self.target_version,
            true,
            3,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_manifest_family(
        &self,
        pool: &PgPool,
        source_record_id: Uuid,
        manifest_file_asset_id: Uuid,
        tilejson_file_asset_id: Uuid,
        source_file_asset_id: Uuid,
        manifest_id: Uuid,
        artifact_id: Uuid,
        current_version: &str,
        previous_version: &str,
        is_active: bool,
        manifest_version: i64,
    ) {
        sqlx::query(
            "INSERT INTO catalog.source_record
             (id, source, source_url, external_id, checksum_sha256)
             VALUES ($1, 'fixture-vector-tile-rollback', 'https://example.test/source.zip', $2, $3)",
        )
        .bind(source_record_id)
        .bind(current_version)
        .bind("3".repeat(64))
        .execute(pool)
        .await
        .expect("insert source record");

        self.insert_file_asset(
            pool,
            source_record_id,
            manifest_file_asset_id,
            &format!("gold/manifest.{current_version}.json"),
        )
        .await;
        self.insert_file_asset(
            pool,
            source_record_id,
            tilejson_file_asset_id,
            &format!("gold/{current_version}/parcels.json"),
        )
        .await;
        self.insert_file_asset(
            pool,
            source_record_id,
            source_file_asset_id,
            &format!("gold/source/{current_version}.zip"),
        )
        .await;

        sqlx::query(
            "INSERT INTO catalog.vector_tile_manifest
             (id, current_version, previous_version, tiles_url_template,
              manifest_file_asset_id, source_record_id, is_active, version)
             VALUES ($1, $2, $3,
              'https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf',
              $4, $5, $6, $7)",
        )
        .bind(manifest_id)
        .bind(current_version)
        .bind(previous_version)
        .bind(manifest_file_asset_id)
        .bind(source_record_id)
        .bind(is_active)
        .bind(manifest_version)
        .execute(pool)
        .await
        .expect("insert manifest");

        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact
             (id, manifest_id, layer, source_layer, tile_min_zoom, tile_max_zoom,
              render_min_zoom, render_max_zoom, tilejson_file_asset_id, object_key_prefix,
              flat_tile_count, flat_tile_total_bytes, source_record_id, version)
             VALUES ($1, $2, 'parcels', 'parcels', 8, 16, 10, 22, $3, $4, 11, 4096, $5, 1)",
        )
        .bind(artifact_id)
        .bind(manifest_id)
        .bind(tilejson_file_asset_id)
        .bind(format!("gold/{current_version}/parcels/"))
        .bind(source_record_id)
        .execute(pool)
        .await
        .expect("insert artifact");

        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact_source_file_asset
             (artifact_id, file_asset_id)
             VALUES ($1, $2)",
        )
        .bind(artifact_id)
        .bind(source_file_asset_id)
        .execute(pool)
        .await
        .expect("insert artifact source file");
    }

    async fn insert_file_asset(
        &self,
        pool: &PgPool,
        source_record_id: Uuid,
        file_asset_id: Uuid,
        object_key: &str,
    ) {
        sqlx::query(
            "INSERT INTO catalog.file_asset
             (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
             VALUES ($1, $2, 'application/json', 10, $3, 'internal', 1)",
        )
        .bind(file_asset_id)
        .bind(object_key)
        .bind(source_record_id)
        .execute(pool)
        .await
        .expect("insert file asset");
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query(
            "DELETE FROM catalog.outbox_event
             WHERE type = 'catalog.vector_tile_manifest.rolled_back.v1'
               AND payload->>'current_version' = $1",
        )
        .bind(&self.target_version)
        .execute(pool)
        .await
        .expect("cleanup outbox");
        sqlx::query("DELETE FROM catalog.vector_tile_manifest WHERE id = ANY($1)")
            .bind(vec![self.active_manifest_id, self.target_manifest_id])
            .execute(pool)
            .await
            .expect("cleanup manifests");
        sqlx::query("DELETE FROM catalog.file_asset WHERE source_record_id = ANY($1)")
            .bind(vec![
                self.active_source_record_id,
                self.target_source_record_id,
            ])
            .execute(pool)
            .await
            .expect("cleanup file assets");
        sqlx::query("DELETE FROM catalog.source_record WHERE id = ANY($1)")
            .bind(vec![
                self.active_source_record_id,
                self.target_source_record_id,
            ])
            .execute(pool)
            .await
            .expect("cleanup source records");
    }
}
