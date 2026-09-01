//! Pure Catalog domain model.
//!
//! This crate owns canonical industrial-complex facts and subresources. It deliberately avoids
//! database, HTTP client, async runtime, and cross-platform Identity dependencies.

/// Blueprint and drawing metadata.
pub mod blueprint;

/// Building metadata assigned to parcels.
pub mod building;

/// Digital twin and 3D asset metadata.
pub mod digital_twin;

/// Catalog domain errors.
pub mod errors;

/// Provider-neutral file asset metadata.
pub mod file_asset;

/// Industrial complex aggregate.
pub mod industrial_complex;

/// Industry taxonomy and parcel assignment rules.
pub mod industry;

/// PNU-anchor backed marker tile contract.
pub mod marker_tile;

/// Manufacturer metadata assigned to parcels.
pub mod manufacturer;

/// Request identity for keyed Catalog publication mutations.
pub mod mutation_idempotency;

/// Official notices and attachments.
pub mod notice;

/// Parcel aggregate.
pub mod parcel;

/// Effective-dated membership of a parcel in an industrial complex.
pub mod parcel_complex_membership;

/// Source lineage for imported facts.
pub mod source_record;

/// Geospatial layer metadata.
pub mod spatial_layer;

/// Static vector tile manifest model.
pub mod vector_tile;

/// Single-source v2 spatial serving publication model.
pub mod serving_publication;

pub use blueprint::{Blueprint, BlueprintKind, ParseBlueprintKindError};
pub use building::Building;
pub use digital_twin::{DigitalTwinAsset, DigitalTwinAssetKind, ParseDigitalTwinAssetKindError};
pub use errors::CatalogError;
pub use file_asset::{
    FileAsset, FileAssetKind, FileAssetVisibility, ParseFileAssetKindError,
    ParseFileAssetVisibilityError,
};
pub use industrial_complex::{
    ComplexMutation, IndustrialComplex, IndustrialComplexKind, IndustrialComplexLotSalesStatus,
    IndustrialComplexStatus, ParseIndustrialComplexKindError,
    ParseIndustrialComplexLotSalesStatusError, ParseIndustrialComplexStatusError,
};
pub use industry::{
    AllowedIndustry, IndustryAssignmentKind, IndustryCodeSystem, IndustryGroup,
    IndustryGroupMember, ParcelIndustryAssignment, ParseIndustryAssignmentKindError,
    ParseIndustryCodeSystemError,
};
pub use manufacturer::Manufacturer;
pub use marker_tile::{
    ComplexAnchorSummary, MarkerAnchorAlgorithm, MarkerTileContract, MarkerTileContractError,
    MarkerTileFeature, MarkerTileLayer, MarkerTileRequest, ParcelMarkerAnchor,
    ALL_ACTIVE_MARKER_FILTER_HASH, PARCEL_ANCHOR_MARKER_TILE_LAYER,
    PNU_ANCHOR_PBF_MARKER_TILE_CONTRACT,
};
pub use mutation_idempotency::{
    CatalogMutationKind, RequestFingerprint, RequestFingerprintBuilder,
    CATALOG_MUTATION_FINGERPRINT_SCHEMA_VERSION,
};
pub use notice::{ComplexNotice, NoticeAttachment, NoticeType, ParseNoticeTypeError};
pub use parcel::{parcel_id_for_pnu, Parcel, ParcelKind, ParcelKindEdit, ParseParcelKindError};
pub use parcel_complex_membership::MembershipAssertedBy;
pub use serving_publication::{
    is_publication_unit_key, static_file_asset_id_for_build, static_release_id_for_build,
    static_release_martin_source_id, static_release_pmtiles_object_key, validate_build_promotion,
    validate_build_result_report, validate_build_snapshot_binding, validate_serving_transition,
    ActiveTileSource, BuildEvidenceDigest, CanonicalIcebergSnapshotId, DynamicPostgisSource,
    FeatureIdProperty, ManifestGeneration, PmtilesChecksum, PublicationUnit, RuntimeTileLayer,
    RuntimeTileLineage, RuntimeTilesUrlTemplate, ServingGeneration, ServingSelection,
    ServingSourceKind, StaticPmtilesSource, ValidatedPmtilesArtifact, VectorTileBuildOutcome,
    VectorTileBuildPromotionInput, VectorTileBuildPromotionVerdict, VectorTileBuildStatus,
    VectorTileRuntimeManifest, STATIC_RELEASE_OBJECT_ROOT,
};
pub use source_record::SourceRecord;
pub use spatial_layer::{ParseSpatialLayerKindError, SpatialLayer, SpatialLayerKind};
pub use vector_tile::{
    vector_tile_feature_filter_properties, TilesUrlTemplate, TilesUrlTemplateError,
    VectorTileArtifact, VectorTileLineage, VectorTileManifest, ZoomRange, ZoomRangeError,
};
