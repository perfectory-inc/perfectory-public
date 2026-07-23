//! Raw-data collection domain contracts and invariants.

/// Bronze ingestion metadata and object-key contracts.
pub mod bronze;

/// Collection domain errors shared by application ports and adapters.
pub mod errors;

/// Provider operation to canonical dataset-slug mappings.
pub mod operation_dataset_slug;

/// Provider-side acquisition jobs for non-plain-HTTP sources.
pub mod provider_acquisition;

/// Canonical Collection source-slug generator.
pub mod source_slug;

/// `VWorld` cadastral feature reconciliation rules.
pub mod vworld_cadastral;

pub use bronze::{
    build_bronze_object_key, validate_bronze_object_key_contract, BronzeObject,
    BronzeObjectKeyError, BronzeObjectKeyParts, BronzeSnapshotMetadataError, IngestionRun,
    IngestionRunStatus, IngestionTrigger, ParseIngestionRunStatusError, ParseIngestionTriggerError,
    ParseSchemaObservedTypeError, ParseSourceAuthKindError, ParseSourcePayloadFormatError,
    SchemaObservedType, SchemaProfile, SnapshotBasis, SnapshotGranularity, SourceAuthKind,
    SourceCatalogEntry, SourcePayloadFormat,
};
pub use errors::CollectionError;
pub use operation_dataset_slug::{
    building_register_dataset_slug, canonical_page_size, operation_collapses_into_slug,
    real_transaction_dataset_slug, vworld_ned_dataset_slug,
};
pub use provider_acquisition::{
    ProviderAcquisitionError, ProviderAcquisitionEvidence, ProviderAcquisitionJob,
    ProviderAcquisitionMethod, ProviderAcquisitionResource,
};
pub use source_slug::{
    assert_canonical_source_slug, is_canonical_source_slug, provider_id, source_slug,
    SourceSlugError, KNOWN_PROVIDER_IDS,
};
pub use vworld_cadastral::{
    dedupe_vworld_cadastral_features_by_pnu, VWorldCadastralDedupedFeature,
    VWorldCadastralFeatureDedupeAccumulator, VWorldCadastralFeatureDedupeReport,
    VWorldCadastralFeatureError,
};
