//! Database row conversion for the Lakehouse Registry adapter.

use foundation_shared_kernel::ids::{
    IngestionRunId, LakehouseDataAssetId, LakehouseDatasetVersionId, LakehouseObjectArtifactId,
    LakehouseStorageNamespaceId,
};
use lakehouse_domain::{
    LakehouseArtifactFormat, LakehouseAssetKind, LakehouseAssetStatus, LakehouseCatalogProvider,
    LakehouseDataAsset, LakehouseDatasetVersion, LakehouseDatasetVersionState,
    LakehouseEnvironment, LakehouseError, LakehouseNamespaceStatus, LakehouseObjectArtifact,
    LakehouseOwnerService, LakehouseRegistryLayer, LakehouseStorageNamespace,
    LakehouseStorageProvider, ParseLakehouseRegistryWireError,
};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use crate::postgres_error::map_sqlx;

pub(super) const NAMESPACE_COLUMNS: &str = "id, provider, environment, owner_service, bucket_name,
 root_prefix, catalog_provider, status, created_at, updated_at, version";

pub(super) const DATA_ASSET_COLUMNS: &str = "id, qualified_name, owner_service, layer, asset_kind,
 schema_contract_ref, status, created_at, updated_at, version";

pub(super) const DATASET_VERSION_COLUMNS: &str =
    "id, data_asset_id, version, state, schema_version,
 artifact_format, created_by_ingestion_run_id, created_at";

pub(super) const QUALIFIED_DATASET_VERSION_COLUMNS: &str = "version.id, version.data_asset_id,
 version.version, version.state, version.schema_version, version.artifact_format,
 version.created_by_ingestion_run_id, version.created_at";

pub(super) const OBJECT_ARTIFACT_COLUMNS: &str = "id, dataset_version_id, namespace_id, object_key,
 content_type, checksum_sha256, size_bytes, logical_record_count, created_at";

pub(super) fn row_to_namespace(row: &PgRow) -> Result<LakehouseStorageNamespace, LakehouseError> {
    let provider = parse_wire(
        "provider",
        string_col(row, "provider")?.as_str(),
        LakehouseStorageProvider::from_wire,
    )?;
    let environment = parse_wire(
        "environment",
        string_col(row, "environment")?.as_str(),
        LakehouseEnvironment::from_wire,
    )?;
    let owner_service = parse_wire(
        "owner_service",
        string_col(row, "owner_service")?.as_str(),
        LakehouseOwnerService::from_wire,
    )?;
    let catalog_provider = parse_wire(
        "catalog_provider",
        string_col(row, "catalog_provider")?.as_str(),
        LakehouseCatalogProvider::from_wire,
    )?;
    let status = parse_wire(
        "status",
        string_col(row, "status")?.as_str(),
        LakehouseNamespaceStatus::from_wire,
    )?;
    let root_prefix = row
        .try_get::<Option<String>, _>("root_prefix")
        .map_err(map_sqlx)?
        .map(|raw| {
            foundation_shared_kernel::ObjectKeyPrefix::parse(&raw)
                .map_err(|error| LakehouseError::Persistence(error.to_string()))
        })
        .transpose()?;

    Ok(LakehouseStorageNamespace {
        id: LakehouseStorageNamespaceId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        provider,
        environment,
        owner_service,
        bucket_name: row.try_get("bucket_name").map_err(map_sqlx)?,
        root_prefix,
        catalog_provider,
        status,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub(super) fn row_to_asset(row: &PgRow) -> Result<LakehouseDataAsset, LakehouseError> {
    let owner_service = parse_wire(
        "owner_service",
        string_col(row, "owner_service")?.as_str(),
        LakehouseOwnerService::from_wire,
    )?;
    let layer = parse_wire(
        "layer",
        string_col(row, "layer")?.as_str(),
        LakehouseRegistryLayer::from_wire,
    )?;
    let asset_kind = parse_wire(
        "asset_kind",
        string_col(row, "asset_kind")?.as_str(),
        LakehouseAssetKind::from_wire,
    )?;
    let status = parse_wire(
        "status",
        string_col(row, "status")?.as_str(),
        LakehouseAssetStatus::from_wire,
    )?;

    Ok(LakehouseDataAsset {
        id: LakehouseDataAssetId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        qualified_name: row.try_get("qualified_name").map_err(map_sqlx)?,
        owner_service,
        layer,
        asset_kind,
        schema_contract_ref: row.try_get("schema_contract_ref").map_err(map_sqlx)?,
        status,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
    })
}

pub(super) fn row_to_dataset_version(
    row: &PgRow,
) -> Result<LakehouseDatasetVersion, LakehouseError> {
    let state = parse_wire(
        "state",
        string_col(row, "state")?.as_str(),
        LakehouseDatasetVersionState::from_wire,
    )?;
    let artifact_format = parse_wire(
        "artifact_format",
        string_col(row, "artifact_format")?.as_str(),
        LakehouseArtifactFormat::from_wire,
    )?;
    let created_by_ingestion_run_id = row
        .try_get::<Option<Uuid>, _>("created_by_ingestion_run_id")
        .map_err(map_sqlx)?
        .map(IngestionRunId::new);

    Ok(LakehouseDatasetVersion {
        id: LakehouseDatasetVersionId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        data_asset_id: LakehouseDataAssetId::new(
            row.try_get::<Uuid, _>("data_asset_id").map_err(map_sqlx)?,
        ),
        version: row.try_get("version").map_err(map_sqlx)?,
        state,
        schema_version: row.try_get("schema_version").map_err(map_sqlx)?,
        artifact_format,
        created_by_ingestion_run_id,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
    })
}

pub(super) fn row_to_object_artifact(
    row: &PgRow,
) -> Result<LakehouseObjectArtifact, LakehouseError> {
    let object_key = foundation_shared_kernel::ObjectKey::parse(&string_col(row, "object_key")?)
        .map_err(|error| LakehouseError::Persistence(error.to_string()))?;
    let size_bytes = i64_to_u64("size_bytes", row.try_get("size_bytes").map_err(map_sqlx)?)?;
    let logical_record_count = row
        .try_get::<Option<i64>, _>("logical_record_count")
        .map_err(map_sqlx)?
        .map(|count| i64_to_u64("logical_record_count", count))
        .transpose()?;

    Ok(LakehouseObjectArtifact {
        id: LakehouseObjectArtifactId::new(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?),
        dataset_version_id: LakehouseDatasetVersionId::new(
            row.try_get::<Uuid, _>("dataset_version_id")
                .map_err(map_sqlx)?,
        ),
        namespace_id: LakehouseStorageNamespaceId::new(
            row.try_get::<Uuid, _>("namespace_id").map_err(map_sqlx)?,
        ),
        object_key,
        content_type: row.try_get("content_type").map_err(map_sqlx)?,
        checksum_sha256: row.try_get("checksum_sha256").map_err(map_sqlx)?,
        size_bytes,
        logical_record_count,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
    })
}

pub(super) fn u64_to_i64(field: &str, value: u64) -> Result<i64, LakehouseError> {
    i64::try_from(value).map_err(|_| {
        LakehouseError::InvalidLakehouseRegistryInput(format!("{field} exceeds i64::MAX"))
    })
}

fn string_col(row: &PgRow, column: &str) -> Result<String, LakehouseError> {
    row.try_get(column).map_err(map_sqlx)
}

fn parse_wire<T>(
    field: &str,
    raw: &str,
    parse: impl FnOnce(&str) -> Result<T, ParseLakehouseRegistryWireError>,
) -> Result<T, LakehouseError> {
    parse(raw).map_err(|error| LakehouseError::Persistence(format!("{field}: {error}")))
}

fn i64_to_u64(field: &str, value: i64) -> Result<u64, LakehouseError> {
    u64::try_from(value).map_err(|_| {
        LakehouseError::Persistence(format!("{field} must not be negative in database"))
    })
}
