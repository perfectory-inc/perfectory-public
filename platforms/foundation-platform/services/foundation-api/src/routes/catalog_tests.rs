use super::{
    building_response, complex_anchor_summary_response, db_reference_marker_tile_enabled_from_vars,
    get_marker_tile, get_marker_tile_contract, get_vector_tile_manifest,
    industrial_complex_list_response, industrial_complex_response, list_complex_blueprints,
    manufacturer_response, parcel_marker_anchor_rebuild_response, request_id_from_headers,
    require_exact_manifest_action_path, vector_tile_artifact_response, ApiError, MarkerTilePath,
    MarkerTileQuery, PARCEL_MARKER_ANCHOR_REBUILD_PATH, VECTOR_TILE_MANIFEST_PROMOTE_PATH,
    VECTOR_TILE_MANIFEST_ROLLBACK_PATH,
};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::Json;
use catalog_application::ports::ParcelMarkerAnchorRebuildReport;
use catalog_domain::{
    Building, ComplexAnchorSummary, IndustrialComplex, IndustrialComplexKind, Manufacturer,
    MarkerAnchorAlgorithm, VectorTileArtifact, VectorTileLineage, ZoomRange,
};
use chrono::Utc;
use foundation_shared_kernel::ids::{
    BuildingId, ComplexId, FileAssetId, ManufacturerId, ParcelId, SourceRecordId,
    VectorTileArtifactId, VectorTileManifestId,
};
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
use lakehouse_domain::IndustrialComplexGoldPointer;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

fn assert_f64_near(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < f64::EPSILON,
        "expected {actual} to equal {expected}"
    );
}

#[test]
fn request_id_from_headers_trims_empty_values() {
    let mut headers = HeaderMap::new();
    assert_eq!(request_id_from_headers(&headers), None);

    headers.insert("x-request-id", HeaderValue::from_static("   "));
    assert_eq!(request_id_from_headers(&headers), None);

    headers.insert("x-request-id", HeaderValue::from_static(" rollback-req-1 "));
    assert_eq!(
        request_id_from_headers(&headers),
        Some("rollback-req-1".to_owned())
    );
}

#[test]
fn parcel_marker_anchor_rebuild_response_preserves_lineage_counts() {
    let generation_run_id = Uuid::now_v7();
    let response = parcel_marker_anchor_rebuild_response(ParcelMarkerAnchorRebuildReport {
        generation_run_id,
        source_snapshot_id: "iceberg:parcel-boundary-snapshot-20260522".to_owned(),
        source_table: "silver.parcel_boundaries".to_owned(),
        algorithm: MarkerAnchorAlgorithm::Polylabel,
        algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
        scanned_row_count: 42,
        loaded_row_count: 42,
        rejected_row_count: 0,
        superseded_row_count: 7,
    });

    assert_eq!(response.generation_run_id, generation_run_id);
    assert_eq!(
        response.source_snapshot_id,
        "iceberg:parcel-boundary-snapshot-20260522"
    );
    assert_eq!(response.source_table, "silver.parcel_boundaries");
    assert_eq!(response.algorithm, "polylabel");
    assert_eq!(
        response.algorithm_version,
        "postgis-st_maximuminscribedcircle-v1"
    );
    assert_eq!(response.scanned_row_count, 42);
    assert_eq!(response.loaded_row_count, 42);
    assert_eq!(response.rejected_row_count, 0);
    assert_eq!(response.superseded_row_count, 7);
}

#[test]
fn vector_tile_artifact_response_advertises_reference_feature_filter_properties() {
    let now = Utc::now();
    let response = vector_tile_artifact_response(VectorTileArtifact {
        id: VectorTileArtifactId::new(Uuid::now_v7()),
        manifest_id: VectorTileManifestId::new(Uuid::now_v7()),
        layer: "complex".to_owned(),
        source_layer: "complex".to_owned(),
        tile_zoom: ZoomRange::new(5, 16).expect("valid tile zoom"),
        render_zoom: ZoomRange::new(5, 22).expect("valid render zoom"),
        tilejson_object_key: ObjectKey::parse(
            "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/complex.json",
        )
        .expect("valid tilejson key"),
        object_key_prefix: ObjectKeyPrefix::parse(
            "gold/vector-tiles/artifacts/0196e7e0-3c20-7000-8000-000000000042/complex/",
        )
        .expect("valid object key prefix"),
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
    });

    assert_eq!(
        response
            .feature_filter_properties
            .get("official_complex_code")
            .map(String::as_str),
        Some("official_complex_code")
    );
}

#[test]
fn manifest_action_paths_are_exact() {
    assert!(require_exact_manifest_action_path(
        VECTOR_TILE_MANIFEST_ROLLBACK_PATH,
        VECTOR_TILE_MANIFEST_ROLLBACK_PATH,
    )
    .is_ok());
    assert!(require_exact_manifest_action_path(
        VECTOR_TILE_MANIFEST_PROMOTE_PATH,
        VECTOR_TILE_MANIFEST_PROMOTE_PATH,
    )
    .is_ok());
    assert!(require_exact_manifest_action_path(
        "/catalog/v1/vector-tiles/manifest:anything",
        VECTOR_TILE_MANIFEST_PROMOTE_PATH,
    )
    .is_err());
    assert!(require_exact_manifest_action_path(
        PARCEL_MARKER_ANCHOR_REBUILD_PATH,
        PARCEL_MARKER_ANCHOR_REBUILD_PATH,
    )
    .is_ok());
    assert!(require_exact_manifest_action_path(
        "/catalog/v1/parcel-marker-anchors:anything",
        PARCEL_MARKER_ANCHOR_REBUILD_PATH,
    )
    .is_err());
}

#[tokio::test]
async fn marker_tile_contract_handler_exposes_pnu_anchor_pbf_contract() {
    let Json(response) = get_marker_tile_contract().await;

    assert_eq!(response.response_format, "mvt_pbf");
    assert_eq!(response.position_source, "pnu_anchor");
    assert!(response.bbox_marker_runtime_forbidden);
    assert!(response.dropped_marker_success_forbidden);
    assert_eq!(
        response.launch_runtime_source,
        "r2_cdn_vector_tile_manifest"
    );
    assert_eq!(
        response.runtime_manifest_endpoint,
        "/catalog/v1/vector-tiles/manifest"
    );
    assert!(response.db_reference_endpoint_launch_forbidden);
    assert_eq!(
        response.db_reference_endpoint_scope,
        "diagnostics_bounded_proof_admin"
    );
    assert_eq!(response.aggregate_anchor_max_zoom, 11);
    assert_eq!(response.exact_anchor_min_zoom, 12);
    assert_eq!(
        response.endpoint_template,
        "/map/v1/marker-tiles/{layer}/{z}/{x}/{y}.pbf?filter_hash={hash}"
    );
    assert_eq!(response.supported_layers, vec!["parcel_anchor".to_owned()]);
    assert_eq!(response.default_filter_hash, "all-active-v1");
}

#[test]
fn db_reference_marker_tile_gate_is_closed_by_default_in_production() {
    fn enabled(entries: &[(&str, &str)]) -> bool {
        db_reference_marker_tile_enabled_from_vars(|key| {
            entries
                .iter()
                .find(|(entry_key, _)| *entry_key == key)
                .map(|(_, value)| (*value).to_owned())
        })
    }

    assert!(!enabled(&[(
        "FOUNDATION_PLATFORM_RUNTIME_ENV",
        "production"
    )]));
    assert!(!enabled(&[("FOUNDATION_PLATFORM_RUNTIME_ENV", "prod")]));
    assert!(enabled(&[(
        "FOUNDATION_PLATFORM_RUNTIME_ENV",
        "development"
    )]));
    assert!(enabled(&[
        ("FOUNDATION_PLATFORM_RUNTIME_ENV", "production"),
        (
            "FOUNDATION_PLATFORM_DB_MARKER_TILE_REFERENCE_ENABLED",
            "true"
        ),
    ]));
    assert!(!enabled(&[
        ("FOUNDATION_PLATFORM_RUNTIME_ENV", "development"),
        (
            "FOUNDATION_PLATFORM_DB_MARKER_TILE_REFERENCE_ENABLED",
            "false"
        ),
    ]));
}

#[tokio::test]
async fn marker_tile_handler_rejects_unsupported_product_layer_before_db_read() {
    let state = Arc::new(AppState::bootstrap_for_test().expect("bootstrap app state"));
    let result = get_marker_tile(
        State(state),
        Path(MarkerTilePath {
            layer: "listing".to_owned(),
            z: 0,
            x: 0,
            y_pbf: "0.pbf".to_owned(),
        }),
        Query(MarkerTileQuery {
            filter_hash: "all-active-v1".to_owned(),
        }),
    )
    .await;

    assert!(matches!(
        result,
        Err(ApiError::BadRequest(message))
            if message.contains("unsupported marker tile layer")
    ));
}

#[test]
fn industrial_complex_response_includes_thin_gold_pointer() {
    let complex_id = ComplexId::new(Uuid::now_v7());
    let now = Utc::now();
    let complex = IndustrialComplex {
        id: complex_id,
        official_complex_code: "IC-0001".to_owned(),
        name: "API mapping fixture".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "1234567890".to_owned(),
        area_m2: 1024,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 7,
    };
    let pointer = IndustrialComplexGoldPointer {
        complex_id,
        current_version: "gold-2026-05-18T00-00-00Z".to_owned(),
        previous_version: Some("gold-2026-05-17T00-00-00Z".to_owned()),
        profile_file_asset_id: FileAssetId::new(Uuid::now_v7()),
        profile_object_key: ObjectKey::parse("gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json")
            .expect("valid profile object key"),
        spatial_locator_file_asset_id: Some(FileAssetId::new(Uuid::now_v7())),
        spatial_locator_object_key: Some(
            ObjectKey::parse("gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet")
                .expect("valid spatial locator object key"),
        ),
        source_record_id: SourceRecordId::new(Uuid::now_v7()),
        source_snapshot_id: "source-snapshot-1".to_owned(),
        iceberg_snapshot_id: "iceberg-snapshot-1".to_owned(),
        profile_row_count: 10,
        profile_checksum_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        published_at: now,
        updated_at: now,
        version: 3,
    };
    let source_record_id = pointer.source_record_id.as_uuid();

    let response = industrial_complex_response(complex, Some(pointer));
    let gold_pointer = response.gold_pointer.expect("gold pointer response");

    assert_eq!(response.id, complex_id.as_uuid());
    assert_eq!(response.official_complex_code, "IC-0001");
    assert_eq!(response.kind, "general");
    assert_eq!(gold_pointer.current_version, "gold-2026-05-18T00-00-00Z");
    assert_eq!(
        gold_pointer.profile_object_key,
        "gold/industrial-complex/profiles/0196e7e0-3c20-7000-8000-100000000002.json"
    );
    assert_eq!(
        gold_pointer.spatial_locator_object_key.as_deref(),
        Some(
            "gold/industrial-complex/spatial-locators/0196e7e0-3c20-7000-8000-100000000002.parquet"
        )
    );
    assert_eq!(gold_pointer.source_record_id, source_record_id);
    assert_eq!(gold_pointer.profile_row_count, 10);
}

#[test]
fn complex_anchor_summary_response_preserves_anchor_source_and_extent() {
    let complex_id = ComplexId::new(Uuid::now_v7());
    let summary = ComplexAnchorSummary::new(
        complex_id,
        127.123_470_234_80,
        36.123_430,
        127.123_470,
        36.123_420,
        127.123_470_234_90,
        36.123_440,
        2,
    )
    .expect("valid anchor summary");

    let response = complex_anchor_summary_response(&summary);

    assert_eq!(response.complex_id, complex_id.as_uuid());
    assert_eq!(response.position_source, "pnu_anchor");
    assert_f64_near(response.center_lng, 127.123_470_234_80);
    assert_f64_near(response.center_lat, 36.123_430);
    assert_f64_near(response.min_lng, 127.123_470);
    assert_f64_near(response.min_lat, 36.123_420);
    assert_f64_near(response.max_lng, 127.123_470_234_90);
    assert_f64_near(response.max_lat, 36.123_440);
    assert_eq!(response.anchor_count, 2);
}

#[test]
fn industrial_complex_list_response_attaches_gold_pointer_by_complex_id() {
    let first_id = ComplexId::new(Uuid::now_v7());
    let second_id = ComplexId::new(Uuid::now_v7());
    let source_record_id = SourceRecordId::new(Uuid::now_v7());
    let now = Utc::now();
    let first = IndustrialComplex {
        id: first_id,
        official_complex_code: "IC-0001".to_owned(),
        name: "First list fixture".to_owned(),
        kind: IndustrialComplexKind::General,
        primary_bjdong_code: "1234567890".to_owned(),
        area_m2: 1024,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 1,
    };
    let second = IndustrialComplex {
        id: second_id,
        official_complex_code: "IC-0002".to_owned(),
        name: "Second list fixture".to_owned(),
        kind: IndustrialComplexKind::National,
        primary_bjdong_code: "1234567891".to_owned(),
        area_m2: 2048,
        created_at: now,
        updated_at: now,
        archived_at: None,
        version: 2,
    };
    let mut gold_pointers = HashMap::new();
    gold_pointers.insert(
        second_id,
        IndustrialComplexGoldPointer {
            complex_id: second_id,
            current_version: "gold-list-2026-06-01".to_owned(),
            previous_version: None,
            profile_file_asset_id: FileAssetId::new(Uuid::now_v7()),
            profile_object_key: ObjectKey::parse("gold/industrial-complex/profiles/list.json")
                .expect("valid profile object key"),
            spatial_locator_file_asset_id: None,
            spatial_locator_object_key: None,
            source_record_id,
            source_snapshot_id: "source-snapshot-list".to_owned(),
            iceberg_snapshot_id: "iceberg-snapshot-list".to_owned(),
            profile_row_count: 2,
            profile_checksum_sha256:
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            published_at: now,
            updated_at: now,
            version: 1,
        },
    );

    let response = industrial_complex_list_response(vec![first, second], gold_pointers);

    assert_eq!(response.len(), 2);
    assert_eq!(response[0].id, first_id.as_uuid());
    assert_eq!(response[0].official_complex_code, "IC-0001");
    assert!(response[0].gold_pointer.is_none());
    assert_eq!(response[1].id, second_id.as_uuid());
    assert_eq!(response[1].kind, "national");
    assert_eq!(
        response[1]
            .gold_pointer
            .as_ref()
            .map(|pointer| pointer.current_version.as_str()),
        Some("gold-list-2026-06-01")
    );
}

#[test]
fn building_response_maps_building_read_model() {
    let building_id = BuildingId::new(Uuid::now_v7());
    let parcel_id = ParcelId::new(Uuid::now_v7());
    let updated_at = Utc::now();
    let building = Building {
        id: building_id,
        parcel_id,
        purpose_code: "02000".to_owned(),
        structure_code: "11".to_owned(),
        floor_area_m2: 1234.5,
        stories: 5,
        below_ground_floors: 2,
        has_rooftop: true,
        rooftop_area_m2: Some(13.87),
        rooftop_usage: "기타제2종근린생활시설 · 주차장".to_owned(),
        built_year: 2020,
        updated_at,
    };

    let response = building_response(&building);

    assert_eq!(response.id, building_id.as_uuid());
    assert_eq!(response.parcel_id, parcel_id.as_uuid());
    assert_eq!(response.purpose_code, "02000");
    assert_eq!(response.structure_code, "11");
    assert!((response.floor_area_m2 - 1234.5).abs() < f64::EPSILON);
    assert_eq!(response.stories, 5);
    assert_eq!(response.below_ground_floors, 2);
    assert!(response.has_rooftop);
    assert_eq!(response.rooftop_area_m2, Some(13.87));
    assert_eq!(response.rooftop_usage, "기타제2종근린생활시설 · 주차장");
    assert_eq!(response.built_year, 2020);
    assert_eq!(response.updated_at, updated_at);
}

#[test]
fn manufacturer_response_omits_sensitive_business_number() {
    let manufacturer_id = ManufacturerId::new(Uuid::now_v7());
    let parcel_id = ParcelId::new(Uuid::now_v7());
    let updated_at = Utc::now();
    let manufacturer = Manufacturer {
        id: manufacturer_id,
        primary_parcel_id: parcel_id,
        name: "fixture manufacturer".to_owned(),
        ksic_code: "26299".to_owned(),
        business_registration_number: "123-45-67890".to_owned(),
        updated_at,
    };

    let response = manufacturer_response(&manufacturer);
    let payload = serde_json::to_value(&response).expect("serialize manufacturer response");

    assert_eq!(response.id, manufacturer_id.as_uuid());
    assert_eq!(response.primary_parcel_id, parcel_id.as_uuid());
    assert_eq!(response.name, "fixture manufacturer");
    assert_eq!(response.ksic_code, "26299");
    assert_eq!(response.updated_at, updated_at);
    assert!(payload.get("business_registration_number").is_none());
    assert!(payload.get("business_number").is_none());
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn list_complex_blueprints_returns_catalog_dto() {
    let Some(pool) = pool().await else {
        return;
    };
    let complex_id = Uuid::now_v7();
    let file_asset_id = Uuid::now_v7();
    let blueprint_id = Uuid::now_v7();
    let suffix = Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .filter(char::is_ascii_digit)
        .take(10)
        .collect::<String>();
    let primary_bjdong_code = format!("{suffix:0<10}")[..10].to_owned();
    let official_complex_code = format!("IC-{suffix}");
    let object_key = format!("complexes/api/blueprints/{blueprint_id}.pdf");

    sqlx::query(
        "INSERT INTO catalog.industrial_complex
         (id, official_complex_code, name, kind, primary_bjdong_code, area_m2, version)
         VALUES ($1, $2, 'API blueprint fixture', 'general', $3, 1000, 1)
         ON CONFLICT (official_complex_code) WHERE archived_at IS NULL DO NOTHING",
    )
    .bind(complex_id)
    .bind(official_complex_code)
    .bind(primary_bjdong_code)
    .execute(&pool)
    .await
    .expect("insert complex");

    sqlx::query(
        "INSERT INTO catalog.file_asset
         (id, object_key, mime_type, size_bytes, visibility, version)
         VALUES ($1, $2, 'application/pdf', 1, 'internal', 1)",
    )
    .bind(file_asset_id)
    .bind(&object_key)
    .execute(&pool)
    .await
    .expect("insert file asset");

    sqlx::query(
        "INSERT INTO catalog.blueprint
         (id, complex_id, file_asset_id, blueprint_kind, coordinate_system, version)
         VALUES ($1, $2, $3, 'master_plan', 'EPSG:5186', 1)",
    )
    .bind(blueprint_id)
    .bind(complex_id)
    .bind(file_asset_id)
    .execute(&pool)
    .await
    .expect("insert blueprint");

    let state = Arc::new(AppState::bootstrap_for_test().expect("bootstrap app state"));
    let response = list_complex_blueprints(State(state), Path(complex_id))
        .await
        .expect("blueprints response");

    assert_eq!(response.0.len(), 1);
    assert_eq!(response.0[0].id, blueprint_id);
    assert_eq!(
        response.0[0].complex_id,
        ComplexId::new(complex_id).as_uuid()
    );
    assert_eq!(response.0[0].blueprint_kind, "master_plan");

    sqlx::query("DELETE FROM catalog.blueprint WHERE id = $1")
        .bind(blueprint_id)
        .execute(&pool)
        .await
        .expect("cleanup blueprint");
    sqlx::query("DELETE FROM catalog.file_asset WHERE id = $1")
        .bind(file_asset_id)
        .execute(&pool)
        .await
        .expect("cleanup file asset");
    sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1")
        .bind(complex_id)
        .execute(&pool)
        .await
        .expect("cleanup complex");
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn vector_tile_manifest_endpoint_returns_runtime_contract() {
    let Some(pool) = pool().await else {
        return;
    };
    let fixture = VectorTileApiFixture::new();
    let active_snapshot = ActiveManifestSnapshot::pause(&pool).await;
    fixture.insert(&pool).await;

    let state = Arc::new(AppState::bootstrap_for_test().expect("bootstrap app state"));
    let response = get_vector_tile_manifest(State(state))
        .await
        .expect("vector tile manifest response");

    assert_eq!(response.0.schema_version, 1);
    assert_eq!(response.0.current_version, fixture.current_version);
    assert_eq!(response.0.previous_version, fixture.previous_version);
    assert_eq!(
        response.0.tiles_url_template,
        "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf"
    );
    let artifact = response
        .0
        .artifacts
        .get("parcels")
        .expect("parcels artifact");
    assert_eq!(artifact.source_layer, "parcels");
    assert_eq!(artifact.tile_min_zoom, 8);
    assert_eq!(artifact.tile_max_zoom, 16);
    assert_eq!(artifact.render_min_zoom, 10);
    assert_eq!(artifact.render_max_zoom, 22);
    assert_eq!(artifact.tilejson_object_key, fixture.tilejson_object_key);
    assert_eq!(artifact.object_key_prefix, fixture.object_key_prefix);
    assert_eq!(
        artifact.lineage.manifest_file_asset_id,
        fixture.manifest_file_asset_id
    );
    assert_eq!(
        artifact.lineage.tilejson_file_asset_id,
        fixture.tilejson_file_asset_id
    );
    assert_eq!(
        artifact.lineage.source_file_asset_ids,
        vec![fixture.source_file_asset_id]
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

struct VectorTileApiFixture {
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

impl VectorTileApiFixture {
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
             VALUES ($1, 'fixture-vector-tile-api', 'https://example.test/source.zip', $2, $3)",
        )
        .bind(self.source_record_id)
        .bind(&self.current_version)
        .bind("2".repeat(64))
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

// Codex finding NEW-1: an internal error must not leak server detail (DB DSNs, provider URLs,
// table names) to API clients; the 500 body is opaque and carries a correlation id instead.
#[tokio::test]
async fn internal_error_response_is_opaque_with_correlation_id() {
    use axum::response::IntoResponse;

    let secret_dsn = "postgres://app:s3cr3t@db.internal:5432/foundation_platform";
    let response = ApiError::Internal(format!("connection failed: {secret_dsn}")).into_response();

    assert_eq!(
        response.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("collect response body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8 body");

    assert!(!body.contains("s3cr3t"), "internal detail leaked: {body}");
    assert!(
        !body.contains("postgres://"),
        "internal detail leaked: {body}"
    );
    assert!(
        body.contains("internal server error"),
        "missing opaque message: {body}"
    );
    assert!(
        body.contains("correlation_id"),
        "missing correlation id: {body}"
    );
}

#[tokio::test]
async fn lakehouse_errors_preserve_http_status_and_opaque_internal_body() {
    use axum::response::IntoResponse;
    use lakehouse_domain::LakehouseError;

    let invalid =
        ApiError::from(LakehouseError::InvalidContract("bad input".to_owned())).into_response();
    assert_eq!(invalid.status(), axum::http::StatusCode::BAD_REQUEST);

    let conflict = ApiError::from(
        LakehouseError::IndustrialComplexGoldPointerVersionConflict {
            expected: Some("gold-v1".to_owned()),
            current: Some("gold-v2".to_owned()),
        },
    )
    .into_response();
    assert_eq!(conflict.status(), axum::http::StatusCode::CONFLICT);

    let internal = ApiError::from(LakehouseError::Persistence(
        "postgres://app:secret@db.internal/foundation".to_owned(),
    ))
    .into_response();
    assert_eq!(
        internal.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body = axum::body::to_bytes(internal.into_body(), usize::MAX)
        .await
        .expect("collect Lakehouse error body");
    let body = String::from_utf8(body.to_vec()).expect("utf8 Lakehouse error body");
    assert!(!body.contains("secret"));
    assert!(body.contains("internal server error"));
    assert!(body.contains("correlation_id"));
}
