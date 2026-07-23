//! sqlx `PgRow` ↔ Catalog domain entity 변환 + 공통 sqlx 에러 매핑.
//!
//! `repository` 와 `unit_of_work` 가 공유한다 — wire format 검증/변환 SSOT 는 도메인의
//! `IndustrialComplexKind::from_wire` / `ParcelKind::from_wire` 가 담당.

use catalog_domain::{
    Blueprint, BlueprintKind, Building, CatalogError, ComplexNotice, DigitalTwinAsset,
    DigitalTwinAssetKind, FileAsset, FileAssetVisibility, IndustrialComplex, IndustrialComplexKind,
    IndustryAssignmentKind, IndustryCodeSystem, IndustryGroup, IndustryGroupMember, Manufacturer,
    NoticeType, Parcel, ParcelIndustryAssignment, ParcelKind, SpatialLayer, SpatialLayerKind,
    TilesUrlTemplate, VectorTileArtifact, VectorTileLineage, VectorTileManifest, ZoomRange,
};
use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    BlueprintId, BuildingId, ComplexId, DigitalTwinAssetId, FileAssetId, IndustryAssignmentId,
    IndustryGroupId, ManufacturerId, NoticeId, ParcelId, SourceRecordId, SpatialLayerId,
    VectorTileArtifactId, VectorTileManifestId,
};
use foundation_shared_kernel::pnu::Pnu;
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

pub fn row_to_complex(row: &PgRow) -> Result<IndustrialComplex, CatalogError> {
    let kind_raw: String = row.try_get("kind").map_err(map_sqlx)?;
    let kind = IndustrialComplexKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let area_i64: i64 = row.try_get("area_m2").map_err(map_sqlx)?;
    let area = i64_to_u64(area_i64)?;
    Ok(IndustrialComplex {
        id: ComplexId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        official_complex_code: row.try_get("official_complex_code").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        kind,
        primary_bjdong_code: row.try_get("primary_bjdong_code").map_err(map_sqlx)?,
        area_m2: area,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        archived_at: row.try_get("archived_at").map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_parcel(row: &PgRow) -> Result<Parcel, CatalogError> {
    let kind_raw: String = row.try_get("kind").map_err(map_sqlx)?;
    let kind = ParcelKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let area_i64: i64 = row.try_get("area_m2").map_err(map_sqlx)?;
    let area = i64_to_u64(area_i64)?;
    let pnu_raw: String = row.try_get("pnu").map_err(map_sqlx)?;
    let pnu = Pnu::parse(pnu_raw).map_err(CatalogError::InvalidPnu)?;
    Ok(Parcel {
        id: ParcelId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        pnu,
        kind,
        area_m2: area,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_building(row: &PgRow) -> Result<Building, CatalogError> {
    Ok(Building {
        id: BuildingId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        parcel_id: ParcelId::new(row.try_get::<Uuid, _>("parcel_id").map_err(map_sqlx)?),
        purpose_code: row.try_get("purpose_code").map_err(map_sqlx)?,
        structure_code: row.try_get("structure_code").map_err(map_sqlx)?,
        floor_area_m2: row.try_get("floor_area_m2").map_err(map_sqlx)?,
        stories: row.try_get("stories").map_err(map_sqlx)?,
        below_ground_floors: row.try_get("below_ground_floors").map_err(map_sqlx)?,
        has_rooftop: row.try_get("has_rooftop").map_err(map_sqlx)?,
        rooftop_area_m2: row.try_get("rooftop_area_m2").map_err(map_sqlx)?,
        rooftop_usage: row.try_get("rooftop_usage").map_err(map_sqlx)?,
        built_year: row.try_get("built_year").map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
    })
}

pub fn row_to_manufacturer(row: &PgRow) -> Result<Manufacturer, CatalogError> {
    Ok(Manufacturer {
        id: ManufacturerId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        primary_parcel_id: ParcelId::new(
            row.try_get::<Uuid, _>("primary_parcel_id")
                .map_err(map_sqlx)?,
        ),
        name: row.try_get("name").map_err(map_sqlx)?,
        ksic_code: row.try_get("ksic_code").map_err(map_sqlx)?,
        business_registration_number: row
            .try_get("business_registration_number")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
    })
}

pub fn row_to_file_asset(row: &PgRow) -> Result<FileAsset, CatalogError> {
    let object_key_raw: String = row.try_get("object_key").map_err(map_sqlx)?;
    let object_key = ObjectKey::parse(&object_key_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let visibility_raw: String = row.try_get("visibility").map_err(map_sqlx)?;
    let visibility = FileAssetVisibility::from_wire(&visibility_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let size_i64: i64 = row.try_get("size_bytes").map_err(map_sqlx)?;
    let size_bytes = i64_to_u64(size_i64)?;
    Ok(FileAsset {
        id: FileAssetId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        object_key,
        mime_type: row.try_get("mime_type").map_err(map_sqlx)?,
        size_bytes,
        checksum_sha256: row.try_get("checksum_sha256").map_err(map_sqlx)?,
        title: row.try_get("title").map_err(map_sqlx)?,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        visibility,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_complex_notice(row: &PgRow) -> Result<ComplexNotice, CatalogError> {
    let notice_type_raw: String = row.try_get("notice_type").map_err(map_sqlx)?;
    let notice_type = NoticeType::from_wire(&notice_type_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(ComplexNotice {
        id: NoticeId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        notice_type,
        title: row.try_get("title").map_err(map_sqlx)?,
        summary: row.try_get("summary").map_err(map_sqlx)?,
        published_at: row.try_get("published_at").map_err(map_sqlx)?,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_blueprint(row: &PgRow) -> Result<Blueprint, CatalogError> {
    let kind_raw: String = row.try_get("blueprint_kind").map_err(map_sqlx)?;
    let blueprint_kind = BlueprintKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(Blueprint {
        id: BlueprintId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        file_asset_id: FileAssetId::new(row.try_get::<Uuid, _>("file_asset_id").map_err(map_sqlx)?),
        blueprint_kind,
        coordinate_system: row.try_get("coordinate_system").map_err(map_sqlx)?,
        scale: row.try_get("scale").map_err(map_sqlx)?,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_spatial_layer(row: &PgRow) -> Result<SpatialLayer, CatalogError> {
    let kind_raw: String = row.try_get("layer_kind").map_err(map_sqlx)?;
    let layer_kind = SpatialLayerKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let geometry_object_key = row
        .try_get::<Option<String>, _>("geometry_object_key")
        .map_err(map_sqlx)?
        .map(|raw| ObjectKey::parse(&raw))
        .transpose()
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(SpatialLayer {
        id: SpatialLayerId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        parcel_id: row
            .try_get::<Option<Uuid>, _>("parcel_id")
            .map_err(map_sqlx)?
            .map(ParcelId::new),
        blueprint_id: row
            .try_get::<Option<Uuid>, _>("blueprint_id")
            .map_err(map_sqlx)?
            .map(BlueprintId::new),
        layer_kind,
        geometry_object_key,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_digital_twin_asset(row: &PgRow) -> Result<DigitalTwinAsset, CatalogError> {
    let kind_raw: String = row.try_get("asset_kind").map_err(map_sqlx)?;
    let asset_kind = DigitalTwinAssetKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(DigitalTwinAsset {
        id: DigitalTwinAssetId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        parcel_id: row
            .try_get::<Option<Uuid>, _>("parcel_id")
            .map_err(map_sqlx)?
            .map(ParcelId::new),
        building_id: row
            .try_get::<Option<Uuid>, _>("building_id")
            .map_err(map_sqlx)?
            .map(BuildingId::new),
        file_asset_id: FileAssetId::new(row.try_get::<Uuid, _>("file_asset_id").map_err(map_sqlx)?),
        asset_kind,
        coordinate_transform: row.try_get("coordinate_transform").map_err(map_sqlx)?,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_industry_group(row: &PgRow) -> Result<IndustryGroup, CatalogError> {
    Ok(IndustryGroup {
        id: IndustryGroupId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        complex_id: ComplexId::new(row.try_get::<Uuid, _>("complex_id").map_err(map_sqlx)?),
        name: row.try_get("name").map_err(map_sqlx)?,
        description: row.try_get("description").map_err(map_sqlx)?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_industry_group_member(row: &PgRow) -> Result<IndustryGroupMember, CatalogError> {
    let code_system_raw: String = row.try_get("industry_code_system").map_err(map_sqlx)?;
    let industry_code_system = IndustryCodeSystem::from_wire(&code_system_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(IndustryGroupMember {
        industry_group_id: IndustryGroupId::new(
            row.try_get::<Uuid, _>("industry_group_id")
                .map_err(map_sqlx)?,
        ),
        industry_code: row.try_get("industry_code").map_err(map_sqlx)?,
        industry_code_system,
    })
}

pub fn row_to_parcel_industry_assignment(
    row: &PgRow,
) -> Result<ParcelIndustryAssignment, CatalogError> {
    let kind_raw: String = row.try_get("assignment_kind").map_err(map_sqlx)?;
    let assignment_kind = IndustryAssignmentKind::from_wire(&kind_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(ParcelIndustryAssignment {
        id: IndustryAssignmentId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        parcel_id: ParcelId::new(row.try_get::<Uuid, _>("parcel_id").map_err(map_sqlx)?),
        industry_group_id: IndustryGroupId::new(
            row.try_get::<Uuid, _>("industry_group_id")
                .map_err(map_sqlx)?,
        ),
        assignment_kind,
        source_record_id: row
            .try_get::<Option<Uuid>, _>("source_record_id")
            .map_err(map_sqlx)?
            .map(SourceRecordId::new),
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_vector_tile_manifest(
    row: &PgRow,
    artifacts: Vec<VectorTileArtifact>,
) -> Result<VectorTileManifest, CatalogError> {
    let template_raw: String = row.try_get("tiles_url_template").map_err(map_sqlx)?;
    let tiles_url_template = TilesUrlTemplate::parse(&template_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    Ok(VectorTileManifest {
        id: VectorTileManifestId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        current_version: row.try_get("current_version").map_err(map_sqlx)?,
        previous_version: row.try_get("previous_version").map_err(map_sqlx)?,
        tiles_url_template,
        published_at: row
            .try_get::<DateTime<Utc>, _>("published_at")
            .map_err(map_sqlx)?,
        manifest_file_asset_id: FileAssetId::new(
            row.try_get::<Uuid, _>("manifest_file_asset_id")
                .map_err(map_sqlx)?,
        ),
        source_record_id: SourceRecordId::new(
            row.try_get::<Uuid, _>("source_record_id")
                .map_err(map_sqlx)?,
        ),
        artifacts,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub fn row_to_vector_tile_artifact(
    row: &PgRow,
    manifest_file_asset_id: FileAssetId,
    source_file_asset_ids: Vec<FileAssetId>,
) -> Result<VectorTileArtifact, CatalogError> {
    let tile_min_zoom: i16 = row.try_get("tile_min_zoom").map_err(map_sqlx)?;
    let tile_max_zoom: i16 = row.try_get("tile_max_zoom").map_err(map_sqlx)?;
    let render_min_zoom: i16 = row.try_get("render_min_zoom").map_err(map_sqlx)?;
    let render_max_zoom: i16 = row.try_get("render_max_zoom").map_err(map_sqlx)?;
    let tile_zoom = ZoomRange::new(i16_to_u8(tile_min_zoom)?, i16_to_u8(tile_max_zoom)?)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let render_zoom = ZoomRange::new(i16_to_u8(render_min_zoom)?, i16_to_u8(render_max_zoom)?)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let tilejson_object_key_raw: String = row.try_get("tilejson_object_key").map_err(map_sqlx)?;
    let tilejson_object_key = ObjectKey::parse(&tilejson_object_key_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let object_key_prefix_raw: String = row.try_get("object_key_prefix").map_err(map_sqlx)?;
    let object_key_prefix = ObjectKeyPrefix::parse(&object_key_prefix_raw)
        .map_err(|e| CatalogError::Infrastructure(e.to_string()))?;
    let flat_tile_count = i64_to_u64(row.try_get("flat_tile_count").map_err(map_sqlx)?)?;
    let flat_tile_total_bytes =
        i64_to_u64(row.try_get("flat_tile_total_bytes").map_err(map_sqlx)?)?;
    let source_record_id = SourceRecordId::new(
        row.try_get::<Uuid, _>("source_record_id")
            .map_err(map_sqlx)?,
    );
    let tilejson_file_asset_id = FileAssetId::new(
        row.try_get::<Uuid, _>("tilejson_file_asset_id")
            .map_err(map_sqlx)?,
    );

    Ok(VectorTileArtifact {
        id: VectorTileArtifactId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        manifest_id: VectorTileManifestId::new(
            row.try_get::<Uuid, _>("manifest_id").map_err(map_sqlx)?,
        ),
        layer: row.try_get("layer").map_err(map_sqlx)?,
        source_layer: row.try_get("source_layer").map_err(map_sqlx)?,
        tile_zoom,
        render_zoom,
        tilejson_object_key,
        object_key_prefix,
        flat_tile_count,
        flat_tile_total_bytes,
        lineage: VectorTileLineage {
            source_record_id,
            manifest_file_asset_id,
            tilejson_file_asset_id,
            source_file_asset_ids,
        },
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(map_sqlx)?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

#[allow(clippy::needless_pass_by_value)]
pub fn map_sqlx(e: sqlx::Error) -> CatalogError {
    CatalogError::Infrastructure(e.to_string())
}

pub fn u64_to_i64(value: u64) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| {
        CatalogError::Infrastructure(format!("area_m2 {value} overflows i64 (Postgres BIGINT)"))
    })
}

fn i64_to_u64(value: i64) -> Result<u64, CatalogError> {
    i64_to_u64_named("area_m2", value)
}

fn i64_to_u64_named(field_name: &str, value: i64) -> Result<u64, CatalogError> {
    u64::try_from(value).map_err(|_| {
        CatalogError::Infrastructure(format!(
            "{field_name} {value} is negative in DB (CHECK constraint should have caught this)"
        ))
    })
}

fn i16_to_u8(value: i16) -> Result<u8, CatalogError> {
    u8::try_from(value).map_err(|_| {
        CatalogError::Infrastructure(format!(
            "zoom {value} is outside u8 range (CHECK constraint should have caught this)"
        ))
    })
}

/// Postgres UNIQUE constraint 위반 (`SQLSTATE 23505`).
pub fn is_unique_violation_code(code: Option<&str>) -> bool {
    code == Some("23505")
}
