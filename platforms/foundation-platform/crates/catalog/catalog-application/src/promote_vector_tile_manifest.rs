//! Use case for promoting a validated static vector tile manifest.

use std::{collections::BTreeMap, sync::Arc};

use catalog_domain::{
    file_asset::FileAssetVisibility,
    vector_tile::{TilesUrlTemplate, ZoomRange},
    CatalogError, VectorTileManifest,
};
use foundation_shared_kernel::ids::StaffId;
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};

use crate::ports::{
    CatalogUnitOfWork, VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
    VectorTileManifestPromotionCommand, VectorTileSourceRecordCommand,
};

/// Input required to promote a vector tile build into the active manifest pointer.
pub struct PromoteVectorTileManifestInput {
    /// Version that should become active after promote.
    pub current_version: String,
    /// Active version observed by the caller before promote.
    pub expected_current_version: String,
    /// URL template clients use to request vector tiles.
    pub tiles_url_template: String,
    /// Source record describing the build input.
    pub source_record: VectorTileSourceRecordCommand,
    /// File asset metadata for the manifest JSON artifact.
    pub manifest_file_asset: VectorTileFileAssetCommand,
    /// Layer artifacts keyed by logical layer name.
    pub artifacts: BTreeMap<String, VectorTileArtifactPromotionCommand>,
    /// Staff operator that requested the promote.
    pub operator_staff_id: StaffId,
    /// Optional request id used for idempotency and trace correlation.
    pub request_id: Option<String>,
}

/// Promotes a vector tile build through the Catalog unit of work.
pub struct PromoteVectorTileManifest {
    uow: Arc<dyn CatalogUnitOfWork>,
}

impl PromoteVectorTileManifest {
    /// Creates a use case instance backed by the given Catalog unit of work.
    #[must_use]
    pub fn new(uow: Arc<dyn CatalogUnitOfWork>) -> Self {
        Self { uow }
    }

    /// Promotes a validated immutable tile build to the active manifest pointer.
    ///
    /// # Errors
    /// Returns `CatalogError` when required fields are empty, lineage metadata is invalid,
    /// the expected active version is stale, or persistence/outbox writes fail.
    pub async fn execute(
        &self,
        input: PromoteVectorTileManifestInput,
    ) -> Result<VectorTileManifest, CatalogError> {
        validate_promotion(&input)?;
        self.uow
            .promote_vector_tile_manifest(VectorTileManifestPromotionCommand {
                current_version: normalize_required(&input.current_version, "current_version")?,
                expected_current_version: normalize_required(
                    &input.expected_current_version,
                    "expected_current_version",
                )?,
                tiles_url_template: input.tiles_url_template.trim().to_owned(),
                source_record: normalize_source_record(input.source_record)?,
                manifest_file_asset: normalize_file_asset(input.manifest_file_asset)?,
                artifacts: input
                    .artifacts
                    .into_iter()
                    .map(|(layer, artifact)| {
                        Ok((
                            normalize_required(&layer, "artifact layer")?,
                            normalize_artifact(artifact)?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, CatalogError>>()?,
                operator_staff_id: input.operator_staff_id,
                request_id: normalize_optional_text(input.request_id),
            })
            .await
    }
}

fn validate_promotion(input: &PromoteVectorTileManifestInput) -> Result<(), CatalogError> {
    let current_version = normalize_required(&input.current_version, "current_version")?;
    let expected_current_version =
        normalize_required(&input.expected_current_version, "expected_current_version")?;
    if current_version == expected_current_version {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "current_version must differ from expected_current_version".to_owned(),
        ));
    }
    TilesUrlTemplate::parse(input.tiles_url_template.trim())
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    if input.artifacts.is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "artifacts must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn normalize_artifact(
    artifact: VectorTileArtifactPromotionCommand,
) -> Result<VectorTileArtifactPromotionCommand, CatalogError> {
    let source_layer = normalize_required(&artifact.source_layer, "source_layer")?;
    ZoomRange::new(artifact.tile_min_zoom, artifact.tile_max_zoom)
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    ZoomRange::new(artifact.render_min_zoom, artifact.render_max_zoom)
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    ObjectKeyPrefix::parse(artifact.object_key_prefix.trim())
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    if artifact.source_file_assets.is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "source_file_assets must not be empty".to_owned(),
        ));
    }

    Ok(VectorTileArtifactPromotionCommand {
        source_layer,
        tile_min_zoom: artifact.tile_min_zoom,
        tile_max_zoom: artifact.tile_max_zoom,
        render_min_zoom: artifact.render_min_zoom,
        render_max_zoom: artifact.render_max_zoom,
        tilejson_file_asset: normalize_file_asset(artifact.tilejson_file_asset)?,
        object_key_prefix: artifact.object_key_prefix.trim().to_owned(),
        flat_tile_count: artifact.flat_tile_count,
        flat_tile_total_bytes: artifact.flat_tile_total_bytes,
        source_file_assets: artifact
            .source_file_assets
            .into_iter()
            .map(normalize_file_asset)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn normalize_source_record(
    source_record: VectorTileSourceRecordCommand,
) -> Result<VectorTileSourceRecordCommand, CatalogError> {
    if let Some(raw_object_key) = source_record.raw_object_key.as_deref() {
        ObjectKey::parse(raw_object_key.trim())
            .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    }
    validate_checksum(source_record.checksum_sha256.as_deref())?;
    Ok(VectorTileSourceRecordCommand {
        source: normalize_required(&source_record.source, "source")?,
        source_url: normalize_optional_text(source_record.source_url),
        external_id: normalize_optional_text(source_record.external_id),
        checksum_sha256: normalize_optional_text(source_record.checksum_sha256),
        raw_object_key: normalize_optional_text(source_record.raw_object_key),
    })
}

fn normalize_file_asset(
    file_asset: VectorTileFileAssetCommand,
) -> Result<VectorTileFileAssetCommand, CatalogError> {
    ObjectKey::parse(file_asset.object_key.trim())
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    FileAssetVisibility::from_wire(file_asset.visibility.trim())
        .map_err(|error| CatalogError::InvalidVectorTileManifestPromotion(error.to_string()))?;
    validate_checksum(file_asset.checksum_sha256.as_deref())?;
    Ok(VectorTileFileAssetCommand {
        object_key: file_asset.object_key.trim().to_owned(),
        mime_type: normalize_required(&file_asset.mime_type, "mime_type")?,
        size_bytes: file_asset.size_bytes,
        checksum_sha256: normalize_optional_text(file_asset.checksum_sha256),
        title: normalize_optional_text(file_asset.title),
        visibility: file_asset.visibility.trim().to_owned(),
    })
}

fn normalize_required(raw: &str, field: &str) -> Result<String, CatalogError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value.to_owned())
}

fn normalize_optional_text(raw: Option<String>) -> Option<String> {
    raw.map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn validate_checksum(raw: Option<&str>) -> Result<(), CatalogError> {
    let Some(checksum) = raw else {
        return Ok(());
    };
    let checksum = checksum.trim();
    if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CatalogError::InvalidVectorTileManifestPromotion(
            "checksum_sha256 must be 64 hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}
