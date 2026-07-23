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

/// Official notices and attachments.
pub mod notice;

/// Parcel aggregate.
pub mod parcel;

/// Source lineage for imported facts.
pub mod source_record;

/// Geospatial layer metadata.
pub mod spatial_layer;

/// Static vector tile manifest model.
pub mod vector_tile;

pub use blueprint::{Blueprint, BlueprintKind, ParseBlueprintKindError};
pub use building::Building;
pub use digital_twin::{DigitalTwinAsset, DigitalTwinAssetKind, ParseDigitalTwinAssetKindError};
pub use errors::CatalogError;
pub use file_asset::{
    FileAsset, FileAssetKind, FileAssetVisibility, ParseFileAssetKindError,
    ParseFileAssetVisibilityError,
};
pub use industrial_complex::{
    ComplexMutation, IndustrialComplex, IndustrialComplexKind, ParseIndustrialComplexKindError,
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
pub use notice::{ComplexNotice, NoticeAttachment, NoticeType, ParseNoticeTypeError};
pub use parcel::{Parcel, ParcelKind, ParseParcelKindError};
pub use source_record::SourceRecord;
pub use spatial_layer::{ParseSpatialLayerKindError, SpatialLayer, SpatialLayerKind};
pub use vector_tile::{
    vector_tile_feature_filter_properties, TilesUrlTemplate, TilesUrlTemplateError,
    VectorTileArtifact, VectorTileLineage, VectorTileManifest, ZoomRange, ZoomRangeError,
};
