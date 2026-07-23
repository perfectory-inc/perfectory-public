//! sqlx 기반 `CatalogRepository` — 읽기 전용 조회만.
//!
//! Mutation 은 `unit_of_work.rs` 의 `PgCatalogUnitOfWork` 가 담당. 책임 분리는
//! ADR 0032 기둥 2 의 At-least-once invariant 를 명확히 만들기 위함이다.

use async_trait::async_trait;
use catalog_application::ports::CatalogRepository;
use catalog_domain::{
    Blueprint, Building, CatalogError, ComplexAnchorSummary, ComplexNotice, DigitalTwinAsset,
    FileAsset, IndustrialComplex, IndustryGroup, IndustryGroupMember, Manufacturer,
    MarkerTileRequest, Parcel, ParcelIndustryAssignment, SpatialLayer, VectorTileManifest,
};
use foundation_shared_kernel::ids::{ComplexId, NoticeId, ParcelId};
use foundation_shared_kernel::pnu::Pnu;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::row_map::{
    map_sqlx, row_to_blueprint, row_to_building, row_to_complex, row_to_complex_notice,
    row_to_digital_twin_asset, row_to_file_asset, row_to_industry_group,
    row_to_industry_group_member, row_to_manufacturer, row_to_parcel,
    row_to_parcel_industry_assignment, row_to_spatial_layer, row_to_vector_tile_artifact,
    row_to_vector_tile_manifest,
};
use foundation_shared_kernel::ids::FileAssetId;

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
             WHERE p.pnu = $1
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
             JOIN catalog.parcel_marker_anchor pma
               ON pma.pnu = p.pnu
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
            "SELECT id, complex_id, pnu, kind, area_m2, created_at, updated_at, version
             FROM catalog.parcel
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
            "SELECT id, complex_id, pnu, kind, area_m2, created_at, updated_at, version
             FROM catalog.parcel
             WHERE pnu = $1",
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
            "SELECT id, complex_id, pnu, kind, area_m2, created_at, updated_at, version
             FROM catalog.parcel
             WHERE complex_id = $1
             ORDER BY pnu",
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
             WHERE p.complex_id = $1
             ORDER BY p.pnu, b.updated_at DESC, b.id",
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
             WHERE p.pnu = $1
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
                    manifest_file_asset_id, source_record_id, published_at,
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
