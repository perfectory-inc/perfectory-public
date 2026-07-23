//! Use case for registering governed object artifacts in the Lakehouse Registry.

use std::sync::Arc;

use foundation_shared_kernel::ids::IngestionRunId;
use lakehouse_domain::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseEnvironment, LakehouseError,
    LakehouseOwnerService, LakehouseRegistryLayer,
};

use crate::ports::LakehouseRegistryUnitOfWork;

/// Input for registering one object artifact after the writer has verified object bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterLakehouseObjectArtifactInput {
    /// Registry qualified name, for example `gongzzang.gold.listing_photo_media`.
    pub qualified_name: String,
    /// Logical namespace id declared by the owning service registry policy.
    pub namespace_id: String,
    /// Provider-neutral object key inside the service-owned bucket.
    pub object_key: String,
    /// MIME/content type of the object.
    pub content_type: String,
    /// Lowercase SHA-256 checksum of the exact object bytes.
    pub checksum_sha256: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional logical record count for tabular or line-delimited artifacts.
    pub logical_record_count: Option<u64>,
}

/// Receipt returned after a registry object artifact write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterLakehouseObjectArtifactReceipt {
    /// Foundation Platform artifact id.
    pub artifact_id: String,
    /// Registered qualified name.
    pub qualified_name: String,
    /// Registered object key.
    pub object_key: String,
}

/// Complete application command committed atomically by Registry infrastructure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterLakehouseObjectArtifactCommand {
    /// Registry qualified name.
    pub qualified_name: String,
    /// Service that owns the storage namespace and asset.
    pub owner_service: LakehouseOwnerService,
    /// Runtime environment containing the namespace.
    pub environment: LakehouseEnvironment,
    /// Medallion layer of the asset.
    pub layer: LakehouseRegistryLayer,
    /// Physical/logical asset kind.
    pub asset_kind: LakehouseAssetKind,
    /// Schema contract reference stored with the asset.
    pub schema_contract_ref: String,
    /// Dataset version identity.
    pub dataset_version: String,
    /// Schema version represented by the dataset version.
    pub schema_version: String,
    /// Physical artifact format.
    pub artifact_format: LakehouseArtifactFormat,
    /// Optional ingestion run responsible for this version.
    pub created_by_ingestion_run_id: Option<IngestionRunId>,
    /// Provider-neutral object key.
    pub object_key: String,
    /// MIME/content type of the object.
    pub content_type: String,
    /// Lowercase SHA-256 checksum of the exact object bytes.
    pub checksum_sha256: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Optional logical record count.
    pub logical_record_count: Option<u64>,
}

/// Registers object artifacts against governed service-owned lakehouse assets.
pub struct RegisterLakehouseObjectArtifact {
    unit_of_work: Arc<dyn LakehouseRegistryUnitOfWork>,
}

impl RegisterLakehouseObjectArtifact {
    /// Creates the use case backed by an atomic Lakehouse Registry unit of work.
    #[must_use]
    pub fn new(unit_of_work: Arc<dyn LakehouseRegistryUnitOfWork>) -> Self {
        Self { unit_of_work }
    }

    /// Registers one object artifact idempotently.
    ///
    /// # Errors
    /// Returns `LakehouseError` when policy validation or the atomic Registry write fails.
    pub async fn execute(
        &self,
        input: RegisterLakehouseObjectArtifactInput,
    ) -> Result<RegisterLakehouseObjectArtifactReceipt, LakehouseError> {
        let policy = governed_asset_policy(&input.qualified_name)?;
        policy.validate_namespace_id(&input.namespace_id)?;
        policy.validate_object_key(&input.object_key)?;

        self.unit_of_work
            .register_object_artifact(RegisterLakehouseObjectArtifactCommand {
                qualified_name: policy.qualified_name.to_owned(),
                owner_service: policy.owner_service,
                environment: policy.environment,
                layer: policy.layer,
                asset_kind: policy.asset_kind,
                schema_contract_ref: policy.schema_contract_ref.to_owned(),
                dataset_version: policy.dataset_version.to_owned(),
                schema_version: policy.schema_version.to_owned(),
                artifact_format: policy.artifact_format,
                created_by_ingestion_run_id: None,
                object_key: input.object_key,
                content_type: input.content_type,
                checksum_sha256: input.checksum_sha256,
                size_bytes: input.size_bytes,
                logical_record_count: input.logical_record_count,
            })
            .await
    }
}

#[derive(Clone, Copy)]
struct GovernedAssetPolicy {
    qualified_name: &'static str,
    namespace_id: &'static str,
    owner_service: LakehouseOwnerService,
    environment: LakehouseEnvironment,
    layer: LakehouseRegistryLayer,
    asset_kind: LakehouseAssetKind,
    schema_contract_ref: &'static str,
    schema_version: &'static str,
    dataset_version: &'static str,
    artifact_format: LakehouseArtifactFormat,
    allowed_object_prefixes: &'static [&'static str],
}

impl GovernedAssetPolicy {
    fn validate_namespace_id(&self, namespace_id: &str) -> Result<(), LakehouseError> {
        if namespace_id != self.namespace_id {
            return Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
                "namespace_id must be {} for {}",
                self.namespace_id, self.qualified_name
            )));
        }
        Ok(())
    }

    fn validate_object_key(&self, object_key: &str) -> Result<(), LakehouseError> {
        if self
            .allowed_object_prefixes
            .iter()
            .any(|prefix| object_key.starts_with(prefix))
        {
            return Ok(());
        }
        Err(LakehouseError::InvalidLakehouseRegistryInput(format!(
            "object_key must stay under one of the governed prefixes for {}",
            self.qualified_name
        )))
    }
}

fn governed_asset_policy(qualified_name: &str) -> Result<GovernedAssetPolicy, LakehouseError> {
    GOVERNED_ASSET_POLICIES
        .iter()
        .copied()
        .find(|policy| policy.qualified_name == qualified_name)
        .ok_or_else(|| {
            LakehouseError::InvalidLakehouseRegistryInput(format!(
                "unknown governed lakehouse asset: {qualified_name}"
            ))
        })
}

const GONGZZANG_R2_PRODUCTION: &str = "gongzzang_r2_production";

const GOVERNED_ASSET_POLICIES: &[GovernedAssetPolicy] = &[
    GovernedAssetPolicy {
        qualified_name: "gongzzang.bronze.onbid_sale",
        namespace_id: GONGZZANG_R2_PRODUCTION,
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Bronze,
        asset_kind: LakehouseAssetKind::RawObjectSet,
        schema_contract_ref: "gongzzang.onbid_sale.bronze.v1",
        schema_version: "gongzzang.onbid_sale.bronze.v1",
        dataset_version: "append_only_v1",
        artifact_format: LakehouseArtifactFormat::Json,
        allowed_object_prefixes: &["bronze/source=onbid-sale/"],
    },
    GovernedAssetPolicy {
        qualified_name: "gongzzang.bronze.court_auction",
        namespace_id: GONGZZANG_R2_PRODUCTION,
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Bronze,
        asset_kind: LakehouseAssetKind::RawObjectSet,
        schema_contract_ref: "gongzzang.court_auction.bronze.v1",
        schema_version: "gongzzang.court_auction.bronze.v1",
        dataset_version: "append_only_v1",
        artifact_format: LakehouseArtifactFormat::Json,
        allowed_object_prefixes: &["bronze/source=court-auction/"],
    },
    GovernedAssetPolicy {
        qualified_name: "gongzzang.gold.listing_marker_tiles",
        namespace_id: GONGZZANG_R2_PRODUCTION,
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Gold,
        asset_kind: LakehouseAssetKind::PbfTileSet,
        schema_contract_ref: "gongzzang.listing_marker_tiles.v1",
        schema_version: "gongzzang.listing_marker_tiles.v1",
        dataset_version: "active_v1",
        artifact_format: LakehouseArtifactFormat::Pbf,
        allowed_object_prefixes: &["gold/listing-marker-tiles/"],
    },
    GovernedAssetPolicy {
        qualified_name: "gongzzang.gold.listing_marker_serving_index",
        namespace_id: GONGZZANG_R2_PRODUCTION,
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Gold,
        asset_kind: LakehouseAssetKind::Manifest,
        schema_contract_ref: "gongzzang.listing_marker_serving_index.v1",
        schema_version: "gongzzang.listing_marker_serving_index.v1",
        dataset_version: "active_v1",
        artifact_format: LakehouseArtifactFormat::Json,
        allowed_object_prefixes: &["gold/listing-marker-serving-index/"],
    },
    GovernedAssetPolicy {
        qualified_name: "gongzzang.gold.listing_photo_media",
        namespace_id: GONGZZANG_R2_PRODUCTION,
        owner_service: LakehouseOwnerService::Gongzzang,
        environment: LakehouseEnvironment::Production,
        layer: LakehouseRegistryLayer::Gold,
        asset_kind: LakehouseAssetKind::MediaSet,
        schema_contract_ref: "gongzzang.listing_photo_media.v1",
        schema_version: "gongzzang.listing_photo_media.v1",
        dataset_version: "append_only_v1",
        artifact_format: LakehouseArtifactFormat::ObjectSet,
        allowed_object_prefixes: &["media/listing-photo/"],
    },
];
