//! Deterministic `OpenAPI` document for the published Catalog v1 surface.

use foundation_contracts::catalog::{
    ArchiveComplexRequest, BlueprintResponse, BuildingResponse, ComplexAnchorSummaryResponse,
    ComplexNoticeResponse, DigitalTwinAssetResponse, FileAssetResponse,
    IndustrialComplexGoldPointerResponse, IndustrialComplexResponse, IndustryGroupMemberResponse,
    IndustryGroupResponse, ManufacturerResponse, MarkerTileContractResponse,
    ParcelIndustryAssignmentResponse, ParcelMarkerAnchorRebuildRequest,
    ParcelMarkerAnchorRebuildResponse, ParcelResponse, PromoteFileAssetRequest,
    PromoteSourceRecordRequest, PromoteVectorTileArtifactRequest, PromoteVectorTileManifestRequest,
    RegisterComplexRequest, RollbackVectorTileManifestRequest, SpatialLayerResponse, UnitResponse,
    UpdateComplexRequest, UpdateParcelKindRequest, VectorTileArtifactResponse,
    VectorTileLineageResponse, VectorTileManifestResponse,
};
use utoipa::openapi::info::LicenseBuilder;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::OpenApi;
use utoipa::{Modify, OpenApi as OpenApiDerive};

use super::{HealthResponse, ReadinessResponse};

#[derive(OpenApiDerive)]
#[openapi(
    info(
        title = "Foundation Platform Catalog API",
        version = "1.0.0",
        description = "Versioned canonical Catalog, parcel, building, and spatial artifact API.",
        contact(name = "Perfectory", email = "engineering@perfectory.invalid")
    ),
    paths(
        super::health,
        super::ready,
        super::metrics,
        super::catalog::register_complex,
        super::catalog::get_complex,
        super::catalog::get_complex_anchor_summary,
        super::catalog::update_complex,
        super::catalog::archive_complex,
        super::catalog::list_complexes,
        super::catalog::list_complex_parcels,
        super::catalog::list_complex_buildings,
        super::catalog::list_parcel_buildings_by_pnu,
        super::catalog::list_parcel_units_by_pnu,
        super::catalog::get_parcel_by_pnu,
        super::catalog::list_complex_manufacturers,
        super::catalog::get_parcel,
        super::catalog::list_complex_notices,
        super::catalog::list_complex_attachments,
        super::catalog::list_complex_blueprints,
        super::catalog::list_complex_spatial_layers,
        super::catalog::list_complex_digital_twin_assets,
        super::catalog::list_industry_groups,
        super::catalog::list_parcel_industry_assignments,
        super::catalog::get_vector_tile_manifest,
        super::catalog::get_marker_tile_contract,
        super::catalog::get_marker_tile,
        super::catalog::rebuild_parcel_marker_anchors,
        super::catalog::record_lakehouse_batch_run,
        super::catalog::rollback_vector_tile_manifest,
        super::catalog::promote_vector_tile_manifest,
        super::catalog::update_parcel_kind
    ),
    components(schemas(
        ArchiveComplexRequest,
        BlueprintResponse,
        BuildingResponse,
        ComplexAnchorSummaryResponse,
        ComplexNoticeResponse,
        DigitalTwinAssetResponse,
        FileAssetResponse,
        HealthResponse,
        IndustrialComplexGoldPointerResponse,
        IndustrialComplexResponse,
        IndustryGroupMemberResponse,
        IndustryGroupResponse,
        ManufacturerResponse,
        MarkerTileContractResponse,
        ParcelIndustryAssignmentResponse,
        ParcelMarkerAnchorRebuildRequest,
        ParcelMarkerAnchorRebuildResponse,
        ParcelResponse,
        PromoteFileAssetRequest,
        PromoteSourceRecordRequest,
        PromoteVectorTileArtifactRequest,
        PromoteVectorTileManifestRequest,
        ReadinessResponse,
        RegisterComplexRequest,
        RollbackVectorTileManifestRequest,
        SpatialLayerResponse,
        UnitResponse,
        UpdateComplexRequest,
        UpdateParcelKindRequest,
        VectorTileArtifactResponse,
        VectorTileLineageResponse,
        VectorTileManifestResponse
    )),
    modifiers(&BearerSecurity, &CatalogLicense)
)]
struct CatalogApiDoc;

const PROPRIETARY_LICENSE_ID: &str = "LicenseRef-Proprietary";

struct CatalogLicense;

impl Modify for CatalogLicense {
    fn modify(&self, openapi: &mut OpenApi) {
        openapi.info.license = Some(
            LicenseBuilder::new()
                .name(PROPRIETARY_LICENSE_ID)
                .identifier(Some(PROPRIETARY_LICENSE_ID))
                .build(),
        );
    }
}

struct BearerSecurity;

impl Modify for BearerSecurity {
    fn modify(&self, openapi: &mut OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}

/// Returns the deterministic Catalog v1 `OpenAPI` model.
#[must_use]
pub fn catalog_openapi_document() -> OpenApi {
    CatalogApiDoc::openapi()
}
