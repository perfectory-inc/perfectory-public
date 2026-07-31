//! sqlx 기반 `CatalogRepository` — 읽기 전용 조회만.
//!
//! Mutation 은 `unit_of_work.rs` 의 `PgCatalogUnitOfWork` 가 담당. 책임 분리는
//! ADR 0032 기둥 2 의 At-least-once invariant 를 명확히 만들기 위함이다.

use async_trait::async_trait;
use catalog_application::ports::CatalogRepository;
use catalog_domain::{
    ActiveTileSource, Blueprint, Building, CanonicalIcebergSnapshotId, CatalogError,
    ComplexAnchorSummary, ComplexNotice, DigitalTwinAsset, DynamicPostgisSource, FeatureIdProperty,
    FileAsset, IndustrialComplex, IndustryGroup, IndustryGroupMember, ManifestGeneration,
    Manufacturer, MarkerTileRequest, Parcel, ParcelIndustryAssignment, PublicationUnit,
    RuntimeTileLayer, RuntimeTileLineage, RuntimeTilesUrlTemplate, ServingGeneration, SpatialLayer,
    StaticPmtilesSource, VectorTileManifest, VectorTileRuntimeManifest,
};
use foundation_shared_kernel::ids::{
    ComplexId, FileAssetId, NoticeId, ParcelId, SourceRecordId, VectorTileDataRevisionId,
    VectorTileReleaseId, VectorTileRuntimeManifestId,
};
use foundation_shared_kernel::pnu::Pnu;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::row_map::{
    map_sqlx, row_to_blueprint, row_to_building, row_to_complex, row_to_complex_notice,
    row_to_digital_twin_asset, row_to_file_asset, row_to_industry_group,
    row_to_industry_group_member, row_to_manufacturer, row_to_parcel,
    row_to_parcel_industry_assignment, row_to_spatial_layer, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest,
};
use serde_json::Value as JsonValue;

/// Route-facing 전유부 호 (building unit) read row.
pub struct BuildingUnitRow {
    /// Stable foundation-platform unit identifier.
    pub id: Uuid,
    /// Parcel that owns this unit.
    pub parcel_id: Uuid,
    /// 건물명 (normalized building name, may be empty).
    pub building_name: String,
    /// 동명칭 — only real 동 numbers (e.g. `109동`); empty otherwise.
    pub dong_name: String,
    /// 호명칭.
    pub ho_name: String,
    /// Floor label (지상/지하 + number), free text from source.
    pub floor_label: String,
    /// 전유면적 (exclusive area, m²), reconciled from 전유공용면적. `None` when unmatched.
    pub exclusive_area_m2: Option<f64>,
    /// 주용도명, reconciled from 전유공용면적 전유 행. Empty when unmatched.
    pub usage_name: String,
    /// 구조명, reconciled from 전유공용면적 전유 행. Empty when unmatched.
    pub structure_name: String,
}

/// `PostgreSQL` implementation of Catalog read-only repository ports.
pub struct PgCatalogRepository {
    pool: PgPool,
}

impl PgCatalogRepository {
    /// Creates a repository backed by the given `PostgreSQL` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Lists 전유부 호 (units) for a parcel PNU, ordered by floor then 호명.
    ///
    /// # Errors
    ///
    /// Returns a [`CatalogError`] when the query fails.
    pub async fn list_units_by_pnu(&self, pnu: &Pnu) -> Result<Vec<BuildingUnitRow>, CatalogError> {
        let rows = sqlx::query(
            "SELECT u.id, u.parcel_id, u.building_name, u.dong_name, u.ho_name,
                    u.floor_label, u.exclusive_area_m2, u.usage_name, u.structure_name
             FROM catalog.building_unit u
             JOIN catalog.parcel p ON p.id = u.parcel_id
             JOIN catalog.parcel_identifier_lookup pil
               ON pil.parcel_id = p.id
              AND pil.identifier_value = $1
             ORDER BY u.floor_label, u.ho_name, u.id",
        )
        .bind(pnu.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter()
            .map(|row| {
                Ok(BuildingUnitRow {
                    id: row.try_get("id").map_err(map_sqlx)?,
                    parcel_id: row.try_get("parcel_id").map_err(map_sqlx)?,
                    building_name: row.try_get("building_name").map_err(map_sqlx)?,
                    dong_name: row.try_get("dong_name").map_err(map_sqlx)?,
                    ho_name: row.try_get("ho_name").map_err(map_sqlx)?,
                    floor_label: row.try_get("floor_label").map_err(map_sqlx)?,
                    exclusive_area_m2: row.try_get("exclusive_area_m2").map_err(map_sqlx)?,
                    usage_name: row.try_get("usage_name").map_err(map_sqlx)?,
                    structure_name: row.try_get("structure_name").map_err(map_sqlx)?,
                })
            })
            .collect()
    }

    async fn fetch_industrial_complexes(&self) -> Result<Vec<IndustrialComplex>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                    created_at, updated_at, archived_at, version
             FROM catalog.industrial_complex
             WHERE archived_at IS NULL
             ORDER BY official_complex_code, id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_complex).collect()
    }
}

#[async_trait]
impl CatalogRepository for PgCatalogRepository {
    async fn list_complexes(&self) -> Result<Vec<IndustrialComplex>, CatalogError> {
        self.fetch_industrial_complexes().await
    }

    async fn find_complex(&self, id: ComplexId) -> Result<Option<IndustrialComplex>, CatalogError> {
        let row_opt = sqlx::query(
            "SELECT id, official_complex_code, name, kind, primary_bjdong_code, area_m2,
                    created_at, updated_at, archived_at, version
             FROM catalog.industrial_complex
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        row_opt.as_ref().map(row_to_complex).transpose()
    }

    async fn find_complex_anchor_summary(
        &self,
        complex_id: ComplexId,
    ) -> Result<Option<ComplexAnchorSummary>, CatalogError> {
        let row = sqlx::query(
            "SELECT
                 AVG(pma.anchor_lng)::double precision AS center_lng,
                 AVG(pma.anchor_lat)::double precision AS center_lat,
                 MIN(pma.anchor_lng)::double precision AS min_lng,
                 MIN(pma.anchor_lat)::double precision AS min_lat,
                 MAX(pma.anchor_lng)::double precision AS max_lng,
                 MAX(pma.anchor_lat)::double precision AS max_lat,
                 COUNT(*)::bigint AS anchor_count
             FROM catalog.parcel p
             JOIN catalog.parcel_current_identifier pci
               ON pci.parcel_id = p.id
             JOIN catalog.parcel_marker_anchor pma
               ON (pma.parcel_id = p.id
                   OR (pma.parcel_id IS NULL AND pma.pnu = pci.identifier_value))
              AND pma.is_active
             WHERE p.complex_id = $1",
        )
        .bind(complex_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let anchor_count = row.try_get::<i64, _>("anchor_count").map_err(map_sqlx)?;
        if anchor_count == 0 {
            return Ok(None);
        }
        let anchor_count = u64::try_from(anchor_count).map_err(|error| {
            CatalogError::Infrastructure(format!("complex anchor count overflow: {error}"))
        })?;

        ComplexAnchorSummary::new(
            complex_id,
            row.try_get("center_lng").map_err(map_sqlx)?,
            row.try_get("center_lat").map_err(map_sqlx)?,
            row.try_get("min_lng").map_err(map_sqlx)?,
            row.try_get("min_lat").map_err(map_sqlx)?,
            row.try_get("max_lng").map_err(map_sqlx)?,
            row.try_get("max_lat").map_err(map_sqlx)?,
            anchor_count,
        )
        .map(Some)
        .map_err(|error| {
            CatalogError::Infrastructure(format!("invalid complex anchor summary: {error}"))
        })
    }

    async fn find_parcel_by_id(&self, id: ParcelId) -> Result<Option<Parcel>, CatalogError> {
        let row_opt = sqlx::query(
            "SELECT p.id, p.complex_id, pci.identifier_value AS pnu, p.kind,
                    p.area_m2, p.created_at, p.updated_at, p.version
             FROM catalog.parcel p
             JOIN catalog.parcel_current_identifier pci ON pci.parcel_id = p.id
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        row_opt.as_ref().map(row_to_parcel).transpose()
    }

    async fn find_parcel_by_pnu(&self, pnu: &Pnu) -> Result<Option<Parcel>, CatalogError> {
        let row_opt = sqlx::query(
            "SELECT p.id, p.complex_id, pci.identifier_value AS pnu, p.kind,
                    p.area_m2, p.created_at, p.updated_at, p.version
             FROM catalog.parcel p
             JOIN catalog.parcel_identifier_lookup pil ON pil.parcel_id = p.id
             JOIN catalog.parcel_current_identifier pci ON pci.parcel_id = p.id
             WHERE pil.identifier_value = $1",
        )
        .bind(pnu.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        row_opt.as_ref().map(row_to_parcel).transpose()
    }

    async fn list_parcels_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Parcel>, CatalogError> {
        let rows = sqlx::query(
            "SELECT p.id, p.complex_id, pci.identifier_value AS pnu, p.kind,
                    p.area_m2, p.created_at, p.updated_at, p.version
             FROM catalog.parcel p
             JOIN catalog.parcel_current_identifier pci ON pci.parcel_id = p.id
             WHERE complex_id = $1
             ORDER BY pci.identifier_value",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_parcel).collect()
    }

    async fn list_buildings_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Building>, CatalogError> {
        let rows = sqlx::query(
            "SELECT b.id, b.parcel_id, b.purpose_code, b.structure_code,
                    b.floor_area_m2, b.stories, b.below_ground_floors, b.has_rooftop,
                    b.rooftop_area_m2, b.rooftop_usage,
                    b.built_year, b.updated_at
             FROM catalog.building b
             JOIN catalog.parcel p ON p.id = b.parcel_id
             JOIN catalog.parcel_current_identifier pci ON pci.parcel_id = p.id
             WHERE p.complex_id = $1
             ORDER BY pci.identifier_value, b.updated_at DESC, b.id",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_building).collect()
    }

    async fn list_buildings_by_pnu(&self, pnu: &Pnu) -> Result<Vec<Building>, CatalogError> {
        let rows = sqlx::query(
            "SELECT b.id, b.parcel_id, b.purpose_code, b.structure_code,
                    b.floor_area_m2, b.stories, b.below_ground_floors, b.has_rooftop,
                    b.rooftop_area_m2, b.rooftop_usage,
                    b.built_year, b.updated_at
             FROM catalog.building b
             JOIN catalog.parcel p ON p.id = b.parcel_id
             JOIN catalog.parcel_identifier_lookup pil ON pil.parcel_id = p.id
             WHERE pil.identifier_value = $1
             ORDER BY b.updated_at DESC, b.id",
        )
        .bind(pnu.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_building).collect()
    }

    async fn list_manufacturers_by_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Manufacturer>, CatalogError> {
        let rows = sqlx::query(
            "SELECT m.id, m.primary_parcel_id, m.name, m.ksic_code,
                    m.business_registration_number, m.updated_at
             FROM catalog.manufacturer m
             JOIN catalog.parcel p ON p.id = m.primary_parcel_id
             WHERE p.complex_id = $1
             ORDER BY m.name, m.id",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_manufacturer).collect()
    }

    async fn list_complex_notices(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<ComplexNotice>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, complex_id, notice_type, title, summary, published_at,
                    source_record_id, created_at, updated_at, version
             FROM catalog.complex_notice
             WHERE complex_id = $1
             ORDER BY published_at DESC NULLS LAST, updated_at DESC",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_complex_notice).collect()
    }

    async fn list_notice_file_assets(
        &self,
        notice_id: NoticeId,
    ) -> Result<Vec<FileAsset>, CatalogError> {
        let rows = sqlx::query(
            "SELECT fa.id, fa.object_key, fa.mime_type, fa.size_bytes, fa.checksum_sha256,
                    fa.title, fa.source_record_id, fa.visibility, fa.created_at,
                    fa.updated_at, fa.version
             FROM catalog.notice_attachment na
             JOIN catalog.file_asset fa ON fa.id = na.file_asset_id
             WHERE na.notice_id = $1
             ORDER BY na.display_order, fa.updated_at DESC",
        )
        .bind(notice_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_file_asset).collect()
    }

    async fn list_complex_attachments(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<FileAsset>, CatalogError> {
        let rows = sqlx::query(
            "SELECT fa.id, fa.object_key, fa.mime_type, fa.size_bytes, fa.checksum_sha256,
                    fa.title, fa.source_record_id, fa.visibility, fa.created_at,
                    fa.updated_at, fa.version
             FROM catalog.complex_attachment ca
             JOIN catalog.file_asset fa ON fa.id = ca.file_asset_id
             WHERE ca.complex_id = $1
             ORDER BY ca.display_order, fa.updated_at DESC",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_file_asset).collect()
    }

    async fn list_complex_blueprints(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<Blueprint>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, complex_id, file_asset_id, blueprint_kind, coordinate_system,
                    scale, source_record_id, created_at, updated_at, version
             FROM catalog.blueprint
             WHERE complex_id = $1
             ORDER BY updated_at DESC",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_blueprint).collect()
    }

    async fn list_complex_spatial_layers(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<SpatialLayer>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, complex_id, parcel_id, blueprint_id, layer_kind,
                    geometry_object_key, source_record_id, created_at, updated_at, version
             FROM catalog.spatial_layer
             WHERE complex_id = $1
             ORDER BY layer_kind, updated_at DESC",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_spatial_layer).collect()
    }

    async fn list_complex_digital_twin_assets(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<DigitalTwinAsset>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, complex_id, parcel_id, building_id, file_asset_id, asset_kind,
                    coordinate_transform, source_record_id, created_at, updated_at, version
             FROM catalog.digital_twin_asset
             WHERE complex_id = $1
             ORDER BY updated_at DESC",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_digital_twin_asset).collect()
    }

    async fn list_industry_groups(
        &self,
        complex_id: Option<ComplexId>,
    ) -> Result<Vec<IndustryGroup>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, complex_id, name, description, created_at, updated_at, version
             FROM catalog.industry_group
             WHERE ($1::uuid IS NULL OR complex_id = $1)
             ORDER BY name",
        )
        .bind(complex_id.map(|id| id.as_uuid()))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_industry_group).collect()
    }

    async fn list_industry_group_members_for_complex(
        &self,
        complex_id: ComplexId,
    ) -> Result<Vec<IndustryGroupMember>, CatalogError> {
        let rows = sqlx::query(
            "SELECT igm.industry_group_id, igm.industry_code, igm.industry_code_system
             FROM catalog.industry_group_member igm
             JOIN catalog.industry_group ig ON ig.id = igm.industry_group_id
             WHERE ig.complex_id = $1
             ORDER BY igm.industry_code",
        )
        .bind(complex_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_industry_group_member).collect()
    }

    async fn list_parcel_industry_assignments(
        &self,
        parcel_id: ParcelId,
    ) -> Result<Vec<ParcelIndustryAssignment>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, parcel_id, industry_group_id, assignment_kind,
                    source_record_id, updated_at, version
             FROM catalog.parcel_industry_assignment
             WHERE parcel_id = $1
             ORDER BY updated_at DESC",
        )
        .bind(parcel_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(row_to_parcel_industry_assignment).collect()
    }

    async fn get_active_vector_tile_manifest(
        &self,
    ) -> Result<Option<VectorTileManifest>, CatalogError> {
        let manifest_row = sqlx::query(
            "SELECT id, current_version, previous_version, tiles_url_template,
                    source_snapshot_id, manifest_file_asset_id, source_record_id, published_at,
                    created_at, updated_at, version
             FROM catalog.vector_tile_manifest
             WHERE is_active = true
             ORDER BY published_at DESC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let Some(row) = manifest_row else {
            return Ok(None);
        };

        let manifest_id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
        let manifest_file_asset_id = FileAssetId::new(
            row.try_get::<uuid::Uuid, _>("manifest_file_asset_id")
                .map_err(map_sqlx)?,
        );
        let artifact_rows = sqlx::query(
            "SELECT vta.id, vta.manifest_id, vta.layer, vta.source_layer,
                    vta.tile_min_zoom, vta.tile_max_zoom, vta.render_min_zoom,
                    vta.render_max_zoom, vta.tilejson_file_asset_id,
                    fa.object_key AS tilejson_object_key, vta.object_key_prefix,
                    vta.flat_tile_count, vta.flat_tile_total_bytes,
                    vta.source_record_id, vta.created_at, vta.updated_at, vta.version
             FROM catalog.vector_tile_artifact vta
             JOIN catalog.file_asset fa ON fa.id = vta.tilejson_file_asset_id
             WHERE vta.manifest_id = $1
             ORDER BY vta.layer",
        )
        .bind(manifest_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let mut artifacts = Vec::with_capacity(artifact_rows.len());
        for artifact_row in &artifact_rows {
            let artifact_id: uuid::Uuid = artifact_row.try_get("id").map_err(map_sqlx)?;
            let source_file_asset_rows = sqlx::query(
                "SELECT file_asset_id
                 FROM catalog.vector_tile_artifact_source_file_asset
                 WHERE artifact_id = $1
                 ORDER BY file_asset_id",
            )
            .bind(artifact_id)
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
            let source_file_asset_ids = source_file_asset_rows
                .iter()
                .map(|source_row| {
                    source_row
                        .try_get::<uuid::Uuid, _>("file_asset_id")
                        .map(FileAssetId::new)
                        .map_err(map_sqlx)
                })
                .collect::<Result<Vec<_>, _>>()?;
            artifacts.push(row_to_vector_tile_artifact(
                artifact_row,
                manifest_file_asset_id,
                source_file_asset_ids,
            )?);
        }

        row_to_vector_tile_manifest(&row, artifacts).map(Some)
    }

    async fn get_active_vector_tile_runtime_manifest(
        &self,
    ) -> Result<Option<VectorTileRuntimeManifest>, CatalogError> {
        // A checked-out connection rather than the pool, so this path and the activation
        // transaction reach the same definition below. Reading through the pool after a commit
        // would let another promotion land in between and return a manifest the caller never
        // published.
        let mut connection = self.pool.acquire().await.map_err(map_sqlx)?;
        load_active_vector_tile_runtime_manifest(&mut connection).await
    }

    async fn get_marker_tile(&self, request: MarkerTileRequest) -> Result<Vec<u8>, CatalogError> {
        let z = i32::from(request.z);
        let x = i32::try_from(request.x).map_err(|error| {
            CatalogError::Infrastructure(format!("marker tile x overflow: {error}"))
        })?;
        let y = i32::try_from(request.y).map_err(|error| {
            CatalogError::Infrastructure(format!("marker tile y overflow: {error}"))
        })?;
        let layer = request.layer.wire_name();

        sqlx::query_scalar::<_, Vec<u8>>(
            "WITH bounds AS (
                 SELECT
                     ST_TileEnvelope($1::integer, $2::integer, $3::integer) AS mercator_geom,
                     ST_Transform(ST_TileEnvelope($1::integer, $2::integer, $3::integer), 4326)
                         AS wgs84_geom
             ),
             features AS (
                 SELECT
                     pma.pnu::text AS id,
                     pma.pnu::text AS pnu,
                     $4::text AS kind,
                     1::integer AS count,
                     pma.pnu::text AS detail_ref,
                     pma.algorithm,
                     pma.algorithm_version,
                     pma.source_geometry_version,
                     ST_AsMVTGeom(
                         ST_Transform(pma.anchor_point, 3857),
                         bounds.mercator_geom,
                         4096,
                         64,
                         true
                     ) AS geom
                 FROM catalog.parcel_marker_anchor pma
                 CROSS JOIN bounds
                 WHERE pma.is_active
                   -- EPSG:4326 anchor point intersects EPSG:4326 tile bounds.
                   AND ST_Intersects(pma.anchor_point, bounds.wgs84_geom)
             )
             -- EPSG:3857 feature geom was produced by ST_AsMVTGeom above.
             SELECT COALESCE(ST_AsMVT(features, $4::text, 4096, 'geom'), decode('', 'hex'))
             FROM features",
        )
        .bind(z)
        .bind(x)
        .bind(y)
        .bind(layer)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx)
    }
}

/// Loads the runtime manifest the singleton pointer currently selects.
///
/// The connection is a parameter rather than the repository's pool because the activation
/// transaction has to return the manifest it just published. Reading through the pool after the
/// commit would observe whatever generation is current *then*, which is not necessarily the one the
/// caller wrote — so both callers read this one definition instead.
///
/// # Errors
///
/// Returns a [`CatalogError`] when the normalized publication ledger is inconsistent or unreadable.
pub(crate) async fn load_active_vector_tile_runtime_manifest(
    connection: &mut PgConnection,
) -> Result<Option<VectorTileRuntimeManifest>, CatalogError> {
    let pointed_at: Option<Uuid> = sqlx::query_scalar(
        "SELECT manifest_id
         FROM catalog.vector_tile_runtime_manifest_pointer
         WHERE singleton = true",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    let Some(pointed_at) = pointed_at else {
        return Ok(None);
    };
    load_vector_tile_runtime_manifest_by_id(
        connection,
        VectorTileRuntimeManifestId::new(pointed_at),
    )
    .await
}

/// Loads one immutable runtime manifest by identity, whether or not it is the selected one.
///
/// Split from the pointer read because a replayed command has to be answered with the manifest *it*
/// published, and by the time the replay arrives another publication may have moved the pointer. The
/// manifest and its unit rows are immutable, so reading by id reproduces the original reply exactly
/// rather than storing a second copy of it.
///
/// # Errors
///
/// Returns a [`CatalogError`] when the manifest is absent, its ledger rows are inconsistent, or the
/// read fails.
#[allow(clippy::too_many_lines)]
pub(crate) async fn load_vector_tile_runtime_manifest_by_id(
    connection: &mut PgConnection,
    manifest_id: VectorTileRuntimeManifestId,
) -> Result<Option<VectorTileRuntimeManifest>, CatalogError> {
    let manifest_row = sqlx::query(
        "SELECT id, manifest_generation, published_at
         FROM catalog.vector_tile_runtime_manifest
         WHERE id = $1",
    )
    .bind(manifest_id.as_uuid())
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    let Some(manifest_row) = manifest_row else {
        return Ok(None);
    };
    let manifest_id =
        VectorTileRuntimeManifestId::new(manifest_row.try_get("id").map_err(map_sqlx)?);
    let manifest_generation = ManifestGeneration::new(
        u64::try_from(
            manifest_row
                .try_get::<i64, _>("manifest_generation")
                .map_err(map_sqlx)?,
        )
        .map_err(|error| CatalogError::Infrastructure(error.to_string()))?,
    )
    .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?;
    let rows = sqlx::query(
        "SELECT pu.unit_key, mu.data_revision, mu.serving_generation, mu.release_id,
                    mu.canonical_iceberg_snapshot_id, r.source_kind, r.martin_source_id,
                    r.tiles_url_template, r.pmtiles_object_key,
                    r.pmtiles_file_asset_id, r.pmtiles_sha256, r.pmtiles_bytes,
                    r.source_record_id, r.source_file_asset_ids
             FROM catalog.vector_tile_runtime_manifest_unit mu
             JOIN catalog.vector_tile_publication_unit pu ON pu.id = mu.publication_unit_id
             JOIN catalog.vector_tile_release r ON r.id = mu.release_id
             WHERE mu.manifest_id = $1
             ORDER BY pu.unit_key",
    )
    .bind(manifest_id.as_uuid())
    .fetch_all(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if rows.is_empty() {
        return Err(CatalogError::InvalidVectorTileRuntimeManifest(
            "active runtime manifest has no publication units".to_owned(),
        ));
    }
    let mut publication_units = std::collections::BTreeMap::new();
    for row in rows {
        let unit_key: String = row.try_get("unit_key").map_err(map_sqlx)?;
        let release_id = VectorTileReleaseId::new(row.try_get("release_id").map_err(map_sqlx)?);
        let tiles_url_template =
            RuntimeTilesUrlTemplate::new(row.try_get("tiles_url_template").map_err(map_sqlx)?)
                .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?;
        let source_kind: String = row.try_get("source_kind").map_err(map_sqlx)?;
        let source = match source_kind.as_str() {
            "dynamic_postgis" => ActiveTileSource::DynamicPostgis(DynamicPostgisSource {
                martin_source_id: row.try_get("martin_source_id").map_err(map_sqlx)?,
                tiles_url_template,
                cache_policy: "no_store".to_owned(),
            }),
            "static_pmtiles" => ActiveTileSource::StaticPmtiles(StaticPmtilesSource {
                martin_source_id: row.try_get("martin_source_id").map_err(map_sqlx)?,
                tiles_url_template,
                pmtiles_object_key: row.try_get("pmtiles_object_key").map_err(map_sqlx)?,
                pmtiles_file_asset_id: FileAssetId::new(
                    row.try_get("pmtiles_file_asset_id").map_err(map_sqlx)?,
                ),
                pmtiles_sha256: row.try_get("pmtiles_sha256").map_err(map_sqlx)?,
                pmtiles_bytes: u64::try_from(
                    row.try_get::<i64, _>("pmtiles_bytes").map_err(map_sqlx)?,
                )
                .map_err(|error| CatalogError::Infrastructure(error.to_string()))?,
            }),
            other => {
                return Err(CatalogError::InvalidVectorTileRuntimeManifest(format!(
                    "unknown source kind: {other}"
                )))
            }
        };
        let layer_rows = sqlx::query(
            "SELECT layer_id, source_layer, feature_id_property, tile_min_zoom,
                        tile_max_zoom, render_min_zoom, render_max_zoom,
                        feature_filter_properties
                 FROM catalog.vector_tile_release_layer
                 WHERE release_id = $1 ORDER BY layer_id",
        )
        .bind(release_id.as_uuid())
        .fetch_all(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        let mut layers = std::collections::BTreeMap::new();
        for layer_row in layer_rows {
            let feature_id_property =
                FeatureIdProperty::new(layer_row.try_get("feature_id_property").map_err(map_sqlx)?)
                    .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?;
            let zoom = |name: &str| -> Result<u8, CatalogError> {
                u8::try_from(layer_row.try_get::<i16, _>(name).map_err(map_sqlx)?).map_err(
                    |error| {
                        CatalogError::InvalidVectorTileRuntimeManifest(format!(
                            "invalid layer zoom: {error}"
                        ))
                    },
                )
            };
            let filter: JsonValue = layer_row
                .try_get("feature_filter_properties")
                .map_err(map_sqlx)?;
            let filter_properties = filter
                .as_object()
                .ok_or_else(|| {
                    CatalogError::InvalidVectorTileRuntimeManifest(
                        "feature_filter_properties must be an object".to_owned(),
                    )
                })?
                .iter()
                .map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.clone(), value.to_owned()))
                        .ok_or_else(|| {
                            CatalogError::InvalidVectorTileRuntimeManifest(
                                "feature filter values must be strings".to_owned(),
                            )
                        })
                })
                .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
            let layer_id: String = layer_row.try_get("layer_id").map_err(map_sqlx)?;
            layers.insert(
                layer_id,
                RuntimeTileLayer {
                    source_layer: layer_row.try_get("source_layer").map_err(map_sqlx)?,
                    feature_id_property,
                    tile_min_zoom: zoom("tile_min_zoom")?,
                    tile_max_zoom: zoom("tile_max_zoom")?,
                    render_min_zoom: zoom("render_min_zoom")?,
                    render_max_zoom: zoom("render_max_zoom")?,
                    feature_filter_properties: filter_properties,
                },
            );
        }
        let source_file_asset_ids: Vec<Uuid> =
            row.try_get("source_file_asset_ids").map_err(map_sqlx)?;
        let unit = PublicationUnit {
            data_revision: VectorTileDataRevisionId::new(
                row.try_get("data_revision").map_err(map_sqlx)?,
            ),
            serving_generation: ServingGeneration::new(
                u64::try_from(
                    row.try_get::<i64, _>("serving_generation")
                        .map_err(map_sqlx)?,
                )
                .map_err(|error| CatalogError::Infrastructure(error.to_string()))?,
            )
            .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
            active_release_id: release_id,
            canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
                row.try_get("canonical_iceberg_snapshot_id")
                    .map_err(map_sqlx)?,
            )
            .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?,
            source,
            layers,
            lineage: RuntimeTileLineage {
                source_record_id: SourceRecordId::new(
                    row.try_get("source_record_id").map_err(map_sqlx)?,
                ),
                source_file_asset_ids: source_file_asset_ids
                    .into_iter()
                    .map(FileAssetId::new)
                    .collect(),
            },
        };
        publication_units.insert(unit_key, unit);
    }
    let result = VectorTileRuntimeManifest {
        schema_version: 2,
        current_version: manifest_id,
        manifest_generation,
        refresh_after_seconds: 4,
        published_at: manifest_row.try_get("published_at").map_err(map_sqlx)?,
        publication_units,
    };
    result
        .validate()
        .map_err(CatalogError::InvalidVectorTileRuntimeManifest)?;
    Ok(Some(result))
}
