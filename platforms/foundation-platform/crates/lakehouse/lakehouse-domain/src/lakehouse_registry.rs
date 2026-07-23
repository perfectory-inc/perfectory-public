//! Lakehouse Registry domain model.
//!
//! The registry is the control-plane metadata layer for service-owned lakehouse buckets. Bulk
//! payloads remain in R2/Iceberg; these types validate ownership, naming, and object-key boundaries.

use chrono::{DateTime, Utc};
use foundation_shared_kernel::ids::{
    IngestionRunId, LakehouseDataAssetId, LakehouseDatasetVersionId, LakehouseObjectArtifactId,
    LakehouseStorageNamespaceId,
};
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::errors::LakehouseError;

/// Physical storage provider used by a lakehouse namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseStorageProvider {
    /// Cloudflare R2 using the S3-compatible API.
    R2,
}

impl LakehouseStorageProvider {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::R2 => "r2",
        }
    }

    /// Parses a storage provider wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known provider.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "r2" => Ok(Self::R2),
            other => Err(ParseLakehouseRegistryWireError::UnknownProvider(
                other.to_owned(),
            )),
        }
    }
}

/// Runtime environment represented by a storage namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseEnvironment {
    /// Developer-local environment.
    Local,
    /// Pre-production staging environment.
    Staging,
    /// Production environment.
    Production,
}

impl LakehouseEnvironment {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }

    /// Parses an environment wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known environment.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "local" => Ok(Self::Local),
            "staging" => Ok(Self::Staging),
            "production" => Ok(Self::Production),
            other => Err(ParseLakehouseRegistryWireError::UnknownEnvironment(
                other.to_owned(),
            )),
        }
    }
}

/// Service that owns the data body in a namespace or asset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseOwnerService {
    /// Foundation Platform owned Catalog/common data.
    FoundationPlatform,
    /// Gongzzang owned product and market data.
    Gongzzang,
    /// Dawneer owned workbench data.
    Dawneer,
}

impl LakehouseOwnerService {
    /// Returns the stable service slug used by ownership policy.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::FoundationPlatform => "foundation-platform",
            Self::Gongzzang => "gongzzang",
            Self::Dawneer => "dawneer",
        }
    }

    /// Returns the prefix used in qualified data asset names.
    #[must_use]
    pub const fn qualified_name_prefix(self) -> &'static str {
        match self {
            Self::FoundationPlatform => "foundation_platform",
            Self::Gongzzang => "gongzzang",
            Self::Dawneer => "dawneer",
        }
    }

    /// Returns the production R2 bucket name owned by this service.
    #[must_use]
    pub const fn production_r2_bucket_name(self) -> &'static str {
        match self {
            Self::FoundationPlatform => "foundation-platform-lakehouse-prod",
            Self::Gongzzang => "gongzzang-lakehouse-prod",
            Self::Dawneer => "dawneer-lakehouse-prod",
        }
    }

    /// Parses a service slug.
    ///
    /// # Errors
    /// Returns an error when the value is not a governed service owner.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "foundation-platform" => Ok(Self::FoundationPlatform),
            "gongzzang" => Ok(Self::Gongzzang),
            "dawneer" => Ok(Self::Dawneer),
            other => Err(ParseLakehouseRegistryWireError::UnknownOwnerService(
                other.to_owned(),
            )),
        }
    }
}

/// Logical medallion layer managed by the registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseRegistryLayer {
    /// Raw immutable source object set.
    Bronze,
    /// Cleaned, typed, source-aligned dataset.
    Silver,
    /// Serving-oriented dataset or artifact.
    Gold,
}

impl LakehouseRegistryLayer {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Bronze => "bronze",
            Self::Silver => "silver",
            Self::Gold => "gold",
        }
    }

    /// Parses a layer wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a governed medallion layer.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "bronze" => Ok(Self::Bronze),
            "silver" => Ok(Self::Silver),
            "gold" => Ok(Self::Gold),
            other => Err(ParseLakehouseRegistryWireError::UnknownLayer(
                other.to_owned(),
            )),
        }
    }
}

/// Provider used for table metadata inside a namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseCatalogProvider {
    /// No table catalog is attached.
    None,
    /// Cloudflare R2 Data Catalog.
    R2DataCatalog,
    /// External Iceberg REST catalog.
    IcebergRest,
}

impl LakehouseCatalogProvider {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::R2DataCatalog => "r2_data_catalog",
            Self::IcebergRest => "iceberg_rest",
        }
    }

    /// Parses a catalog-provider wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known catalog provider.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "none" => Ok(Self::None),
            "r2_data_catalog" => Ok(Self::R2DataCatalog),
            "iceberg_rest" => Ok(Self::IcebergRest),
            other => Err(ParseLakehouseRegistryWireError::UnknownCatalogProvider(
                other.to_owned(),
            )),
        }
    }
}

/// Lifecycle state of a storage namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseNamespaceStatus {
    /// Namespace is available for governed reads/writes.
    Active,
    /// Namespace remains readable but should not receive new writes.
    Deprecated,
    /// Namespace is blocked because it needs investigation.
    Quarantined,
}

impl LakehouseNamespaceStatus {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Quarantined => "quarantined",
        }
    }

    /// Parses a namespace status wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known status.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(ParseLakehouseRegistryWireError::UnknownStatus(
                other.to_owned(),
            )),
        }
    }
}

/// Type of asset tracked by the registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseAssetKind {
    /// Raw immutable object set.
    RawObjectSet,
    /// Iceberg table.
    IcebergTable,
    /// Vector/PBF tile set.
    PbfTileSet,
    /// Small manifest or pointer object.
    Manifest,
    /// Media object set.
    MediaSet,
}

impl LakehouseAssetKind {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::RawObjectSet => "raw_object_set",
            Self::IcebergTable => "iceberg_table",
            Self::PbfTileSet => "pbf_tile_set",
            Self::Manifest => "manifest",
            Self::MediaSet => "media_set",
        }
    }

    /// Parses an asset-kind wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known asset kind.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "raw_object_set" => Ok(Self::RawObjectSet),
            "iceberg_table" => Ok(Self::IcebergTable),
            "pbf_tile_set" => Ok(Self::PbfTileSet),
            "manifest" => Ok(Self::Manifest),
            "media_set" => Ok(Self::MediaSet),
            other => Err(ParseLakehouseRegistryWireError::UnknownAssetKind(
                other.to_owned(),
            )),
        }
    }
}

/// Registry status of a data asset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseAssetStatus {
    /// Asset is usable.
    Active,
    /// Asset is visible but not a valid write target.
    Deprecated,
    /// Asset is blocked for investigation.
    Quarantined,
}

impl LakehouseAssetStatus {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Quarantined => "quarantined",
        }
    }

    /// Parses an asset-status wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known asset status.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(ParseLakehouseRegistryWireError::UnknownAssetStatus(
                other.to_owned(),
            )),
        }
    }
}

/// Lifecycle state of a dataset version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseDatasetVersionState {
    /// Version was produced but is not active yet.
    Candidate,
    /// Version is the active pointer for its asset.
    Active,
    /// Version was previously active.
    Previous,
    /// Version is no longer retained for normal consumption.
    Retired,
    /// Version is blocked because it needs investigation.
    Quarantined,
}

impl LakehouseDatasetVersionState {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Active => "active",
            Self::Previous => "previous",
            Self::Retired => "retired",
            Self::Quarantined => "quarantined",
        }
    }

    /// Parses a dataset-version state wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known version state.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "candidate" => Ok(Self::Candidate),
            "active" => Ok(Self::Active),
            "previous" => Ok(Self::Previous),
            "retired" => Ok(Self::Retired),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(ParseLakehouseRegistryWireError::UnknownVersionState(
                other.to_owned(),
            )),
        }
    }
}

/// Physical artifact format represented by a dataset version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LakehouseArtifactFormat {
    /// JSON object.
    Json,
    /// Newline-delimited JSON object set.
    Jsonl,
    /// Apache Parquet object set.
    Parquet,
    /// `GeoParquet` object set.
    GeoParquet,
    /// Apache Iceberg table snapshot.
    Iceberg,
    /// Protocol Buffer vector tile object set.
    Pbf,
    /// ZIP archive.
    Zip,
    /// Arbitrary object set whose file-level content type is tracked per object.
    ObjectSet,
}

impl LakehouseArtifactFormat {
    /// Returns the stable database/API wire value.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Parquet => "parquet",
            Self::GeoParquet => "geoparquet",
            Self::Iceberg => "iceberg",
            Self::Pbf => "pbf",
            Self::Zip => "zip",
            Self::ObjectSet => "object_set",
        }
    }

    /// Parses an artifact-format wire value.
    ///
    /// # Errors
    /// Returns an error when the value is not a known artifact format.
    pub fn from_wire(raw: &str) -> Result<Self, ParseLakehouseRegistryWireError> {
        match raw {
            "json" => Ok(Self::Json),
            "jsonl" => Ok(Self::Jsonl),
            "parquet" => Ok(Self::Parquet),
            "geoparquet" => Ok(Self::GeoParquet),
            "iceberg" => Ok(Self::Iceberg),
            "pbf" => Ok(Self::Pbf),
            "zip" => Ok(Self::Zip),
            "object_set" => Ok(Self::ObjectSet),
            other => Err(ParseLakehouseRegistryWireError::UnknownArtifactFormat(
                other.to_owned(),
            )),
        }
    }
}

/// Metadata for a service-owned storage namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LakehouseStorageNamespace {
    /// Stable namespace identifier.
    pub id: LakehouseStorageNamespaceId,
    /// Physical storage provider.
    pub provider: LakehouseStorageProvider,
    /// Runtime environment.
    pub environment: LakehouseEnvironment,
    /// Service that owns the data body.
    pub owner_service: LakehouseOwnerService,
    /// Physical bucket name.
    pub bucket_name: String,
    /// Optional root prefix for shared-bucket fallback layouts.
    pub root_prefix: Option<ObjectKeyPrefix>,
    /// Table metadata provider.
    pub catalog_provider: LakehouseCatalogProvider,
    /// Namespace lifecycle status.
    pub status: LakehouseNamespaceStatus,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
    /// Optimistic version.
    pub version: i64,
}

impl LakehouseStorageNamespace {
    /// Builds a validated storage namespace.
    ///
    /// # Errors
    /// Returns an error when ownership, bucket naming, or prefix rules are violated.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: LakehouseStorageNamespaceId,
        provider: LakehouseStorageProvider,
        environment: LakehouseEnvironment,
        owner_service: LakehouseOwnerService,
        bucket_name: String,
        root_prefix: Option<ObjectKeyPrefix>,
        catalog_provider: LakehouseCatalogProvider,
        status: LakehouseNamespaceStatus,
    ) -> Result<Self, LakehouseError> {
        validate_bucket_name(&bucket_name)?;
        if provider == LakehouseStorageProvider::R2
            && environment == LakehouseEnvironment::Production
            && bucket_name != owner_service.production_r2_bucket_name()
        {
            return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
                "owner_service={} must use production bucket {} not {}",
                owner_service.wire_name(),
                owner_service.production_r2_bucket_name(),
                bucket_name
            )));
        }

        let now = Utc::now();
        Ok(Self {
            id,
            provider,
            environment,
            owner_service,
            bucket_name,
            root_prefix,
            catalog_provider,
            status,
            created_at: now,
            updated_at: now,
            version: 1,
        })
    }

    /// Returns whether an object key is valid for the requested layer inside this namespace.
    ///
    /// # Errors
    /// Returns an error when the object key is malformed.
    pub fn allows_object_key_for_layer(
        &self,
        layer: LakehouseRegistryLayer,
        object_key: &str,
    ) -> Result<bool, LakehouseError> {
        let object_key = ObjectKey::parse(object_key)
            .map_err(|error| LakehouseError::InvalidLakehouseRegistryInput(error.to_string()))?;
        let expected_prefix = self.layer_object_key_prefix(layer);
        Ok(object_key.as_str().starts_with(&expected_prefix))
    }

    /// Returns whether an object key is valid for a concrete data asset in this namespace.
    ///
    /// # Errors
    /// Returns an error when the object key is malformed.
    pub fn allows_object_key_for_asset(
        &self,
        asset: &LakehouseDataAsset,
        object_key: &str,
    ) -> Result<bool, LakehouseError> {
        if asset.owner_service != self.owner_service {
            return Ok(false);
        }

        let object_key = ObjectKey::parse(object_key)
            .map_err(|error| LakehouseError::InvalidLakehouseRegistryInput(error.to_string()))?;
        Ok(self
            .asset_object_key_prefixes(asset)
            .iter()
            .any(|prefix| object_key.as_str().starts_with(prefix)))
    }

    /// Returns the expected object-key prefix for a layer.
    #[must_use]
    pub fn layer_object_key_prefix(&self, layer: LakehouseRegistryLayer) -> String {
        self.root_prefix.as_ref().map_or_else(
            || format!("{}/", layer.wire_name()),
            |root_prefix| {
                format!(
                    "{}/{}/",
                    root_prefix.as_str().trim_end_matches('/'),
                    layer.wire_name()
                )
            },
        )
    }

    fn asset_object_key_prefixes(&self, asset: &LakehouseDataAsset) -> Vec<String> {
        if asset.asset_kind == LakehouseAssetKind::MediaSet {
            return vec![self.media_object_key_prefix()];
        }
        vec![self.layer_object_key_prefix(asset.layer)]
    }

    fn media_object_key_prefix(&self) -> String {
        self.root_prefix.as_ref().map_or_else(
            || "media/".to_owned(),
            |root_prefix| format!("{}/media/", root_prefix.as_str().trim_end_matches('/')),
        )
    }
}

/// Registry metadata for a logical data asset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LakehouseDataAsset {
    /// Stable asset identifier.
    pub id: LakehouseDataAssetId,
    /// Stable qualified name, e.g. `gongzzang.gold.listing_marker_tiles`.
    pub qualified_name: String,
    /// Service that owns the data body.
    pub owner_service: LakehouseOwnerService,
    /// Medallion layer.
    pub layer: LakehouseRegistryLayer,
    /// Asset type.
    pub asset_kind: LakehouseAssetKind,
    /// Optional schema or contract reference.
    pub schema_contract_ref: Option<String>,
    /// Asset lifecycle status.
    pub status: LakehouseAssetStatus,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
    /// Optimistic version.
    pub version: i64,
}

impl LakehouseDataAsset {
    /// Builds a validated data asset.
    ///
    /// # Errors
    /// Returns an error when the qualified name does not match owner/layer rules.
    pub fn new(
        id: LakehouseDataAssetId,
        qualified_name: String,
        owner_service: LakehouseOwnerService,
        layer: LakehouseRegistryLayer,
        asset_kind: LakehouseAssetKind,
        schema_contract_ref: Option<String>,
    ) -> Result<Self, LakehouseError> {
        validate_qualified_name(&qualified_name, owner_service, layer)?;
        if schema_contract_ref.as_deref().is_some_and(str::is_empty) {
            return Err(LakehouseError::InvalidLakehouseRegistryInput(
                "schema_contract_ref must not be empty".to_owned(),
            ));
        }

        let now = Utc::now();
        Ok(Self {
            id,
            qualified_name,
            owner_service,
            layer,
            asset_kind,
            schema_contract_ref,
            status: LakehouseAssetStatus::Active,
            created_at: now,
            updated_at: now,
            version: 1,
        })
    }
}

/// Immutable dataset version tracked by the registry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LakehouseDatasetVersion {
    /// Stable version identifier.
    pub id: LakehouseDatasetVersionId,
    /// Data asset this version belongs to.
    pub data_asset_id: LakehouseDataAssetId,
    /// Stable version string emitted by the producer.
    pub version: String,
    /// Version lifecycle state.
    pub state: LakehouseDatasetVersionState,
    /// Schema version of the materialized dataset.
    pub schema_version: String,
    /// Artifact format for this version.
    pub artifact_format: LakehouseArtifactFormat,
    /// Optional Bronze ingestion run that produced this version.
    pub created_by_ingestion_run_id: Option<IngestionRunId>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl LakehouseDatasetVersion {
    /// Builds a validated dataset version.
    ///
    /// # Errors
    /// Returns an error when version or schema fields are empty.
    pub fn new(
        id: LakehouseDatasetVersionId,
        data_asset_id: LakehouseDataAssetId,
        version: String,
        state: LakehouseDatasetVersionState,
        schema_version: String,
        artifact_format: LakehouseArtifactFormat,
        created_by_ingestion_run_id: Option<IngestionRunId>,
    ) -> Result<Self, LakehouseError> {
        validate_nonempty("version", &version)?;
        validate_nonempty("schema_version", &schema_version)?;
        Ok(Self {
            id,
            data_asset_id,
            version,
            state,
            schema_version,
            artifact_format,
            created_by_ingestion_run_id,
            created_at: Utc::now(),
        })
    }
}

/// Object-level artifact metadata stored by the registry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LakehouseObjectArtifact {
    /// Stable artifact identifier.
    pub id: LakehouseObjectArtifactId,
    /// Namespace where the object is stored.
    pub namespace_id: LakehouseStorageNamespaceId,
    /// Dataset version represented by this object.
    pub dataset_version_id: LakehouseDatasetVersionId,
    /// Provider-neutral object key.
    pub object_key: ObjectKey,
    /// MIME/content type.
    pub content_type: String,
    /// Lowercase SHA-256 checksum.
    pub checksum_sha256: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional logical record count represented by this object.
    pub logical_record_count: Option<u64>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl LakehouseObjectArtifact {
    /// Builds validated object-level metadata for a dataset version.
    ///
    /// # Errors
    /// Returns an error when the object key escapes the namespace/layer prefix or metadata is
    /// malformed.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: LakehouseObjectArtifactId,
        namespace: &LakehouseStorageNamespace,
        layer: LakehouseRegistryLayer,
        dataset_version_id: LakehouseDatasetVersionId,
        object_key: &str,
        content_type: String,
        checksum_sha256: String,
        size_bytes: u64,
        logical_record_count: Option<u64>,
    ) -> Result<Self, LakehouseError> {
        if !namespace.allows_object_key_for_layer(layer, object_key)? {
            return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
                "object_key must stay under {} layer prefix {}",
                layer.wire_name(),
                namespace.layer_object_key_prefix(layer)
            )));
        }
        validate_nonempty("content_type", &content_type)?;
        validate_sha256_hex("checksum_sha256", &checksum_sha256)?;

        Ok(Self {
            id,
            namespace_id: namespace.id,
            dataset_version_id,
            object_key: ObjectKey::parse(object_key).map_err(|error| {
                LakehouseError::InvalidLakehouseRegistryInput(error.to_string())
            })?,
            content_type,
            checksum_sha256,
            size_bytes,
            logical_record_count,
            created_at: Utc::now(),
        })
    }

    /// Builds validated object-level metadata for a concrete data asset.
    ///
    /// # Errors
    /// Returns an error when the object key escapes the namespace/asset prefix or metadata is
    /// malformed.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_asset(
        id: LakehouseObjectArtifactId,
        namespace: &LakehouseStorageNamespace,
        asset: &LakehouseDataAsset,
        dataset_version_id: LakehouseDatasetVersionId,
        object_key: &str,
        content_type: String,
        checksum_sha256: String,
        size_bytes: u64,
        logical_record_count: Option<u64>,
    ) -> Result<Self, LakehouseError> {
        if !namespace.allows_object_key_for_asset(asset, object_key)? {
            return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
                "object_key must stay under {} asset prefix for {}",
                asset.layer.wire_name(),
                asset.qualified_name
            )));
        }
        validate_nonempty("content_type", &content_type)?;
        validate_sha256_hex("checksum_sha256", &checksum_sha256)?;

        Ok(Self {
            id,
            namespace_id: namespace.id,
            dataset_version_id,
            object_key: ObjectKey::parse(object_key).map_err(|error| {
                LakehouseError::InvalidLakehouseRegistryInput(error.to_string())
            })?,
            content_type,
            checksum_sha256,
            size_bytes,
            logical_record_count,
            created_at: Utc::now(),
        })
    }
}

/// Error returned while parsing Lakehouse Registry wire values.
#[derive(Debug, Error)]
pub enum ParseLakehouseRegistryWireError {
    /// Unknown storage provider.
    #[error("unknown lakehouse storage provider: {0:?}")]
    UnknownProvider(String),
    /// Unknown environment.
    #[error("unknown lakehouse environment: {0:?}")]
    UnknownEnvironment(String),
    /// Unknown owner service.
    #[error("unknown lakehouse owner service: {0:?}")]
    UnknownOwnerService(String),
    /// Unknown medallion layer.
    #[error("unknown lakehouse layer: {0:?}")]
    UnknownLayer(String),
    /// Unknown catalog provider.
    #[error("unknown lakehouse catalog provider: {0:?}")]
    UnknownCatalogProvider(String),
    /// Unknown lifecycle status.
    #[error("unknown lakehouse status: {0:?}")]
    UnknownStatus(String),
    /// Unknown asset kind.
    #[error("unknown lakehouse asset kind: {0:?}")]
    UnknownAssetKind(String),
    /// Unknown asset status.
    #[error("unknown lakehouse asset status: {0:?}")]
    UnknownAssetStatus(String),
    /// Unknown dataset-version state.
    #[error("unknown lakehouse dataset-version state: {0:?}")]
    UnknownVersionState(String),
    /// Unknown artifact format.
    #[error("unknown lakehouse artifact format: {0:?}")]
    UnknownArtifactFormat(String),
}

fn validate_bucket_name(bucket_name: &str) -> Result<(), LakehouseError> {
    if bucket_name.is_empty() {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "bucket_name must not be empty".to_owned(),
        ));
    }
    if bucket_name.len() < 3 || bucket_name.len() > 63 {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "bucket_name must be 3..=63 characters".to_owned(),
        ));
    }
    if !bucket_name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "bucket_name must use lowercase letters, digits, and '-' only".to_owned(),
        ));
    }
    if bucket_name.starts_with('-') || bucket_name.ends_with('-') || bucket_name.contains("--") {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "bucket_name must not start/end with '-' or contain '--'".to_owned(),
        ));
    }
    Ok(())
}

fn validate_nonempty(field: &str, value: &str) -> Result<(), LakehouseError> {
    if value.trim().is_empty() {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn validate_sha256_hex(field: &str, value: &str) -> Result<(), LakehouseError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "{field} must be lowercase SHA-256 hex"
        )));
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "{field} must be lowercase SHA-256 hex"
        )));
    }
    Ok(())
}

fn validate_qualified_name(
    qualified_name: &str,
    owner_service: LakehouseOwnerService,
    layer: LakehouseRegistryLayer,
) -> Result<(), LakehouseError> {
    let segments = qualified_name.split('.').collect::<Vec<_>>();
    if segments.len() != 3 {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "qualified_name must have exactly three segments".to_owned(),
        ));
    }
    if segments[0] != owner_service.qualified_name_prefix() {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "qualified_name owner segment must be {}",
            owner_service.qualified_name_prefix()
        )));
    }
    if segments[1] != layer.wire_name() {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "qualified_name layer segment must be {}",
            layer.wire_name()
        )));
    }
    if !segments.iter().all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    }) {
        return Err(LakehouseError::InvalidLakehouseRegistryInput(
            "qualified_name segments must use lowercase letters, digits, and '_'".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foundation_platform_owns_production_lakehouse_bucket() {
        assert_eq!(
            LakehouseOwnerService::FoundationPlatform.production_r2_bucket_name(),
            "foundation-platform-lakehouse-prod"
        );
    }
}
