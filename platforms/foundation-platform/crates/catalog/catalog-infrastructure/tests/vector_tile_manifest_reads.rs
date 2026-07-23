//! `PostgreSQL` read tests for active vector tile manifests.

#![allow(clippy::expect_used, clippy::print_stderr, clippy::unwrap_used)]

use catalog_application::ports::CatalogRepository;
use catalog_infrastructure::PgCatalogRepository;
use sqlx::PgPool;
use uuid::Uuid;

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
async fn repository_reads_active_vector_tile_manifest_with_lineage() {
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgCatalogRepository::new(pool.clone());
    let fixture = VectorTileFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert(&pool).await;

    let manifest = repo
        .get_active_vector_tile_manifest()
        .await
        .expect("get active manifest")
        .expect("active manifest must exist");

    assert_eq!(manifest.current_version, fixture.current_version);
    assert_eq!(manifest.previous_version, fixture.previous_version);
    assert_eq!(
        manifest.tiles_url_template.as_str(),
        "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
    );
    assert_eq!(manifest.artifacts.len(), 1);

    let artifact = &manifest.artifacts[0];
    assert_eq!(artifact.layer, "parcels");
    assert_eq!(artifact.source_layer, "parcels");
    assert_eq!(artifact.tile_zoom.min(), 8);
    assert_eq!(artifact.tile_zoom.max(), 16);
    assert_eq!(artifact.render_zoom.min(), 10);
    assert_eq!(artifact.render_zoom.max(), 22);
    assert_eq!(
        artifact.tilejson_object_key.as_str(),
        fixture.tilejson_object_key
    );
    assert_eq!(
        artifact.object_key_prefix.as_str(),
        fixture.object_key_prefix
    );
    assert_eq!(artifact.flat_tile_count, 11);
    assert_eq!(artifact.flat_tile_total_bytes, 4096);
    assert_eq!(
        artifact.lineage.source_record_id.as_uuid(),
        fixture.source_record_id
    );
    assert_eq!(
        artifact.lineage.manifest_file_asset_id.as_uuid(),
        fixture.manifest_file_asset_id
    );
    assert_eq!(
        artifact.lineage.tilejson_file_asset_id.as_uuid(),
        fixture.tilejson_file_asset_id
    );
    assert_eq!(artifact.lineage.source_file_asset_ids.len(), 1);
    assert_eq!(
        artifact.lineage.source_file_asset_ids[0].as_uuid(),
        fixture.source_file_asset_id
    );

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

struct VectorTileFixture {
    source_record_id: Uuid,
    manifest_file_asset_id: Uuid,
    tilejson_file_asset_id: Uuid,
    source_file_asset_id: Uuid,
    manifest_id: Uuid,
    artifact_id: Uuid,
    current_version: String,
    previous_version: String,
    manifest_object_key: String,
    tilejson_object_key: String,
    source_object_key: String,
    object_key_prefix: String,
}

impl VectorTileFixture {
    fn new() -> Self {
        let manifest_id = Uuid::now_v7();
        let suffix = Uuid::new_v4().simple().to_string();
        Self {
            source_record_id: Uuid::now_v7(),
            manifest_file_asset_id: Uuid::now_v7(),
            tilejson_file_asset_id: Uuid::now_v7(),
            source_file_asset_id: Uuid::now_v7(),
            manifest_id,
            artifact_id: Uuid::now_v7(),
            current_version: format!("v-{suffix}"),
            previous_version: "v-previous".to_owned(),
            manifest_object_key: format!("gold/manifest.{manifest_id}.json"),
            tilejson_object_key: format!("gold/{suffix}/parcels.json"),
            source_object_key: format!("gold/source/{manifest_id}.zip"),
            object_key_prefix: format!("gold/{suffix}/parcels/"),
        }
    }

    async fn insert(&self, pool: &PgPool) {
        sqlx::query(
            "INSERT INTO catalog.source_record
             (id, source, source_url, external_id, checksum_sha256)
             VALUES ($1, 'fixture-vector-tile', 'https://example.test/source.zip', $2, $3)",
        )
        .bind(self.source_record_id)
        .bind(&self.current_version)
        .bind("1".repeat(64))
        .execute(pool)
        .await
        .expect("insert source record");

        self.insert_file_asset(pool, self.manifest_file_asset_id, &self.manifest_object_key)
            .await;
        self.insert_file_asset(pool, self.tilejson_file_asset_id, &self.tilejson_object_key)
            .await;
        self.insert_file_asset(pool, self.source_file_asset_id, &self.source_object_key)
            .await;

        sqlx::query(
            "INSERT INTO catalog.vector_tile_manifest
             (id, current_version, previous_version, tiles_url_template,
              manifest_file_asset_id, source_record_id, is_active, version)
             VALUES ($1, $2, $3,
              'https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf',
              $4, $5, true, 1)",
        )
        .bind(self.manifest_id)
        .bind(&self.current_version)
        .bind(&self.previous_version)
        .bind(self.manifest_file_asset_id)
        .bind(self.source_record_id)
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
        .bind(self.artifact_id)
        .bind(self.manifest_id)
        .bind(self.tilejson_file_asset_id)
        .bind(&self.object_key_prefix)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert artifact");

        sqlx::query(
            "INSERT INTO catalog.vector_tile_artifact_source_file_asset
             (artifact_id, file_asset_id)
             VALUES ($1, $2)",
        )
        .bind(self.artifact_id)
        .bind(self.source_file_asset_id)
        .execute(pool)
        .await
        .expect("insert artifact source file");
    }

    async fn insert_file_asset(&self, pool: &PgPool, file_asset_id: Uuid, object_key: &str) {
        sqlx::query(
            "INSERT INTO catalog.file_asset
             (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
             VALUES ($1, $2, 'application/json', 10, $3, 'internal', 1)",
        )
        .bind(file_asset_id)
        .bind(object_key)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert file asset");
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query("DELETE FROM catalog.vector_tile_manifest WHERE id = $1")
            .bind(self.manifest_id)
            .execute(pool)
            .await
            .expect("cleanup manifest");
        sqlx::query("DELETE FROM catalog.file_asset WHERE source_record_id = $1")
            .bind(self.source_record_id)
            .execute(pool)
            .await
            .expect("cleanup file assets");
        sqlx::query("DELETE FROM catalog.source_record WHERE id = $1")
            .bind(self.source_record_id)
            .execute(pool)
            .await
            .expect("cleanup source record");
    }
}
