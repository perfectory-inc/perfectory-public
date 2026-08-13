//! Catalog application layer use cases and outbound ports.
//!
//! This crate is the application boundary between HTTP/API DTOs and the Catalog domain.
//! Infrastructure crates implement only these port traits, while use cases orchestrate domain
//! rules and transaction boundaries.

#![deny(missing_docs)]

/// Use case for archiving an industrial complex without hard deletion.
pub mod archive_complex;

/// Shared industrial-complex input validation helpers.
mod industrial_complex_input;

/// Catalog-owned parsing for canonical industrial-complex patch commands.
pub mod industrial_complex_patch;

/// Use case for importing source-side industrial-complex seed rows into Catalog.
pub mod import_industrial_complex_catalog_seed;

/// Use case for activating a complete dynamic `PostGIS` tile source for one publication unit.
pub mod mark_tile_layer_dynamic;

/// Outbound ports implemented by Catalog infrastructure.
pub mod ports;

/// Use case for promoting a validated static build to the active serving source.
pub mod promote_tile_layer_static;

/// Use case for promoting the active static vector tile manifest.
pub mod promote_vector_tile_manifest;

/// Use case for rebuilding PNU-backed parcel marker anchors.
pub mod rebuild_parcel_marker_anchors;

/// Use case for registering an industrial complex.
pub mod register_complex;

/// Use case for returning one publication unit to its preserved dynamic release.
pub mod rollback_tile_layer_source;

/// Use case for rolling back the active static vector tile manifest.
pub mod rollback_vector_tile_manifest;

/// Use case for updating an industrial complex.
pub mod update_complex;

/// Use case for updating a parcel kind.
pub mod update_parcel_kind;

/// Use cases for the static vector tile build lifecycle.
pub mod vector_tile_build_lifecycle;

pub use archive_complex::{ArchiveIndustrialComplex, ArchiveIndustrialComplexInput};
pub use import_industrial_complex_catalog_seed::{
    ImportIndustrialComplexCatalogSeed, ImportIndustrialComplexCatalogSeedInput,
    ImportIndustrialComplexCatalogSeedReport, IndustrialComplexCatalogSeedRow,
};
pub use mark_tile_layer_dynamic::MarkTileLayerDynamic;
pub use promote_tile_layer_static::PromoteTileLayerStatic;
pub use promote_vector_tile_manifest::{PromoteVectorTileManifest, PromoteVectorTileManifestInput};
pub use rebuild_parcel_marker_anchors::{
    RebuildParcelMarkerAnchors, RebuildParcelMarkerAnchorsInput,
};
pub use register_complex::{RegisterIndustrialComplex, RegisterIndustrialComplexInput};
pub use rollback_tile_layer_source::RollbackTileLayerSource;
pub use rollback_vector_tile_manifest::{
    RollbackVectorTileManifest, RollbackVectorTileManifestInput,
};
pub use update_complex::{UpdateIndustrialComplex, UpdateIndustrialComplexInput};
pub use update_parcel_kind::{UpdateParcelKind, UpdateParcelKindInput};
pub use vector_tile_build_lifecycle::VectorTileBuildLifecycle;
