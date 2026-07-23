//! Promotes a validated parcel marker anchor PBF artifact manifest to the Catalog runtime pointer.

use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc};

use anyhow::{bail, Context};
use catalog_application::{
    ports::{
        VectorTileArtifactPromotionCommand, VectorTileFileAssetCommand,
        VectorTileSourceRecordCommand,
    },
    PromoteVectorTileManifest, PromoteVectorTileManifestInput,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use chrono::Utc;
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    FileObjectStorage, ObjectStorageService, R2ObjectStorage,
};
use foundation_shared_kernel::ids::StaffId;
use foundation_shared_kernel::{ObjectKey, ObjectKeyPrefix};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::r2_layout::{vector_tile_artifact_prefix, vector_tile_manifest_key};

const MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_pbf_artifact_manifest.v1";
const AGGREGATE_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_aggregate_pbf_artifact_manifest.v1";
const RUNTIME_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_runtime_manifest.v1";
const PROMOTE_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_pbf_manifest_promote_summary.v1";
const RUNTIME_PROMOTE_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_runtime_manifest_promote_summary.v1";
const LAYER: &str = "parcel_anchor";
const AGGREGATE_LAYER: &str = "parcel_anchor_aggregate";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_CONFIRM";
const STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_STORAGE_DRIVER";
const LOCAL_ROOT_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_LOCAL_ROOT";
const MANIFEST_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_MANIFEST_OBJECT_KEY";
const EXPECTED_CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_EXPECTED_CURRENT_VERSION";
const TILES_URL_TEMPLATE_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_TILES_URL_TEMPLATE";
const OPERATOR_STAFF_ID_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_OPERATOR_STAFF_ID";
const REQUEST_ID_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_REQUEST_ID";
const SUMMARY_PATH_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_PROMOTE_SUMMARY_PATH";
const RUNTIME_CONFIRM_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_CONFIRM";
const RUNTIME_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_STORAGE_DRIVER";
const RUNTIME_LOCAL_ROOT_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_LOCAL_ROOT";
const RUNTIME_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_VERSION";
const RUNTIME_MANIFEST_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_MANIFEST_OBJECT_KEY";
const RUNTIME_EXACT_MANIFEST_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_EXACT_MANIFEST_OBJECT_KEY";
const RUNTIME_AGGREGATE_MANIFEST_OBJECT_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_AGGREGATE_MANIFEST_OBJECT_KEY";
const RUNTIME_EXPECTED_CURRENT_VERSION_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_EXPECTED_CURRENT_VERSION";
const RUNTIME_TILES_URL_TEMPLATE_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_TILES_URL_TEMPLATE";
const RUNTIME_OPERATOR_STAFF_ID_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_OPERATOR_STAFF_ID";
const RUNTIME_REQUEST_ID_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_REQUEST_ID";
const RUNTIME_SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_RUNTIME_PROMOTE_SUMMARY_PATH";
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const RUNTIME_MANIFEST_CACHE_CONTROL: &str = "no-store";

/// Promotes a parcel marker anchor PBF artifact manifest.
pub async fn run() -> anyhow::Result<()> {
    let config = PromoteConfig::from_env()?;
    let storage = ArtifactStorage::from_config(&config.storage).await?;
    let manifest_bytes = storage
        .read_object_bytes(config.manifest_object_key.as_str())
        .await
        .context("failed to read parcel marker anchor PBF manifest")?;
    let manifest = parse_manifest(&manifest_bytes)?;
    let tilejson_bytes = storage
        .read_object_bytes(manifest.tilejson_object_key.as_str())
        .await
        .context("failed to read parcel marker anchor TileJSON")?;
    let source_anchor_manifest_bytes = storage
        .read_object_bytes(manifest.source_anchor_manifest_object_key.as_str())
        .await
        .context("failed to read source anchor manifest")?;
    let input = build_promotion_input(
        &config,
        &ArtifactObjectBytes {
            manifest: manifest_bytes,
            tilejson: tilejson_bytes,
            source_anchor_manifest: source_anchor_manifest_bytes,
        },
    )?;

    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for PBF manifest promote")?;
    let use_case = PromoteVectorTileManifest::new(Arc::new(PgCatalogUnitOfWork::new(pool)));
    let promoted = use_case
        .execute(input)
        .await
        .context("failed to promote parcel marker anchor PBF manifest")?;

    if let Some(summary_path) = &config.summary_path {
        write_summary(summary_path, &promoted)?;
    }

    tracing::info!(
        manifest_id = %promoted.id,
        current_version = %promoted.current_version,
        artifact_count = promoted.artifacts.len(),
        "parcel marker anchor PBF manifest promote succeeded"
    );
    Ok(())
}

/// Promotes the aggregate low-zoom and exact high-zoom parcel marker anchor artifacts together.
pub async fn run_runtime() -> anyhow::Result<()> {
    let config = RuntimePromoteConfig::from_env()?;
    let storage = ArtifactStorage::from_config(&config.storage).await?;
    let exact_manifest_bytes = storage
        .read_object_bytes(config.exact_manifest_object_key.as_str())
        .await
        .context("failed to read exact parcel marker anchor PBF manifest")?;
    let exact_manifest = parse_manifest(&exact_manifest_bytes)?;
    let exact_tilejson_bytes = storage
        .read_object_bytes(exact_manifest.tilejson_object_key.as_str())
        .await
        .context("failed to read exact parcel marker anchor TileJSON")?;
    let aggregate_manifest_bytes = storage
        .read_object_bytes(config.aggregate_manifest_object_key.as_str())
        .await
        .context("failed to read aggregate parcel marker anchor PBF manifest")?;
    let aggregate_manifest = parse_manifest(&aggregate_manifest_bytes)?;
    let aggregate_tilejson_bytes = storage
        .read_object_bytes(aggregate_manifest.tilejson_object_key.as_str())
        .await
        .context("failed to read aggregate parcel marker anchor TileJSON")?;

    let runtime = build_runtime_promotion_input(
        &config,
        &RuntimeArtifactObjectBytes {
            exact_manifest: exact_manifest_bytes,
            exact_tilejson: exact_tilejson_bytes,
            aggregate_manifest: aggregate_manifest_bytes,
            aggregate_tilejson: aggregate_tilejson_bytes,
        },
    )?;
    storage
        .write_object_bytes(
            config.runtime_manifest_object_key.as_str(),
            runtime.runtime_manifest_bytes.clone(),
            JSON_CONTENT_TYPE,
            RUNTIME_MANIFEST_CACHE_CONTROL,
        )
        .await
        .context("failed to write parcel marker anchor runtime manifest")?;

    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for runtime PBF manifest promote")?;
    let use_case = PromoteVectorTileManifest::new(Arc::new(PgCatalogUnitOfWork::new(pool)));
    let promoted = use_case
        .execute(runtime.input)
        .await
        .context("failed to promote parcel marker anchor runtime manifest")?;

    if let Some(summary_path) = &config.summary_path {
        write_runtime_summary(summary_path, &promoted)?;
    }

    tracing::info!(
        manifest_id = %promoted.id,
        current_version = %promoted.current_version,
        artifact_count = promoted.artifacts.len(),
        "parcel marker anchor runtime manifest promote succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PromoteConfig {
    database_url: String,
    storage: ArtifactStorageConfig,
    manifest_object_key: String,
    expected_current_version: String,
    tiles_url_template: String,
    operator_staff_id: StaffId,
    request_id: Option<String>,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimePromoteConfig {
    database_url: String,
    storage: ArtifactStorageConfig,
    runtime_version: String,
    runtime_manifest_object_key: String,
    exact_manifest_object_key: String,
    aggregate_manifest_object_key: String,
    expected_current_version: String,
    tiles_url_template: String,
    operator_staff_id: StaffId,
    request_id: Option<String>,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArtifactStorageConfig {
    Local { root: PathBuf },
    R2,
}

enum ArtifactStorage {
    Local(FileObjectStorage),
    R2(R2ObjectStorage),
}

struct ArtifactObjectBytes {
    manifest: Vec<u8>,
    tilejson: Vec<u8>,
    source_anchor_manifest: Vec<u8>,
}

struct RuntimeArtifactObjectBytes {
    exact_manifest: Vec<u8>,
    exact_tilejson: Vec<u8>,
    aggregate_manifest: Vec<u8>,
    aggregate_tilejson: Vec<u8>,
}

struct RuntimePromotionInput {
    input: PromoteVectorTileManifestInput,
    runtime_manifest_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize)]
struct PbfManifest {
    schema_version: String,
    artifact_version: String,
    source_anchor_manifest_object_key: String,
    source_anchor_artifact_version: String,
    source_snapshot_id: String,
    source_table: String,
    source_anchor_row_count: u64,
    algorithm: String,
    algorithm_version: String,
    layer: String,
    min_zoom: u8,
    max_zoom: u8,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    checksum_sha256: String,
    tiles: Vec<PbfTileObject>,
}

#[derive(Clone, Debug, Deserialize)]
struct PbfTileObject {
    object_key: String,
    feature_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Serialize)]
struct PromoteSummary {
    schema_version: &'static str,
    manifest_id: String,
    current_version: String,
    previous_version: String,
    artifact_count: usize,
}

#[derive(Serialize)]
struct RuntimeManifestDocument {
    schema_version: &'static str,
    runtime_version: String,
    generated_at_utc: String,
    exact_manifest_object_key: String,
    aggregate_manifest_object_key: String,
    source_anchor_manifest_object_key: String,
    source_anchor_artifact_version: String,
    source_snapshot_id: String,
    source_table: String,
    source_anchor_row_count: u64,
    layers: BTreeMap<String, RuntimeManifestLayer>,
}

#[derive(Serialize)]
struct RuntimeManifestLayer {
    artifact_version: String,
    manifest_object_key: String,
    source_layer: String,
    min_zoom: u8,
    max_zoom: u8,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    object_key_prefix: String,
    checksum_sha256: String,
}

impl PromoteConfig {
    fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(error) => bail!("invalid {name} environment variable: {error}"),
        })
    }

    fn from_lookup<F>(mut lookup: F) -> anyhow::Result<Self>
    where
        F: FnMut(&str) -> anyhow::Result<Option<String>>,
    {
        let confirm = optional_lookup_value(&mut lookup, CONFIRM_ENV)?.unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!("{CONFIRM_ENV} must be true");
        }
        let database_url = required_lookup_value(&mut lookup, DATABASE_URL_ENV)?;
        let storage_driver = optional_lookup_value(&mut lookup, STORAGE_DRIVER_ENV)?
            .unwrap_or_else(|| "local".to_owned())
            .to_ascii_lowercase();
        let storage = match storage_driver.as_str() {
            "local" => ArtifactStorageConfig::Local {
                root: PathBuf::from(required_lookup_value(&mut lookup, LOCAL_ROOT_ENV)?),
            },
            "r2" => ArtifactStorageConfig::R2,
            "" => bail!("{STORAGE_DRIVER_ENV} must not be empty"),
            other => bail!("{STORAGE_DRIVER_ENV} must be 'local' or 'r2', got '{other}'"),
        };
        let manifest_object_key = required_lookup_value(&mut lookup, MANIFEST_OBJECT_KEY_ENV)?;
        ObjectKey::parse(manifest_object_key.as_str())
            .map_err(|error| anyhow::anyhow!("{MANIFEST_OBJECT_KEY_ENV}: {error}"))?;
        let operator_staff_id =
            Uuid::parse_str(required_lookup_value(&mut lookup, OPERATOR_STAFF_ID_ENV)?.as_str())
                .map(StaffId::new)
                .with_context(|| format!("{OPERATOR_STAFF_ID_ENV} must be a UUID"))?;

        Ok(Self {
            database_url,
            storage,
            manifest_object_key,
            expected_current_version: required_lookup_value(
                &mut lookup,
                EXPECTED_CURRENT_VERSION_ENV,
            )?,
            tiles_url_template: required_lookup_value(&mut lookup, TILES_URL_TEMPLATE_ENV)?,
            operator_staff_id,
            request_id: optional_lookup_value(&mut lookup, REQUEST_ID_ENV)?,
            summary_path: optional_lookup_value(&mut lookup, SUMMARY_PATH_ENV)?.map(PathBuf::from),
        })
    }
}

impl RuntimePromoteConfig {
    fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| match env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(env::VarError::NotPresent) => Ok(None),
            Err(error) => bail!("invalid {name} environment variable: {error}"),
        })
    }

    fn from_lookup<F>(mut lookup: F) -> anyhow::Result<Self>
    where
        F: FnMut(&str) -> anyhow::Result<Option<String>>,
    {
        let confirm = optional_lookup_value(&mut lookup, RUNTIME_CONFIRM_ENV)?.unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!("{RUNTIME_CONFIRM_ENV} must be true");
        }
        let database_url = required_lookup_value(&mut lookup, DATABASE_URL_ENV)?;
        let storage_driver = optional_lookup_value(&mut lookup, RUNTIME_STORAGE_DRIVER_ENV)?
            .unwrap_or_else(|| "local".to_owned())
            .to_ascii_lowercase();
        let storage = match storage_driver.as_str() {
            "local" => ArtifactStorageConfig::Local {
                root: PathBuf::from(required_lookup_value(&mut lookup, RUNTIME_LOCAL_ROOT_ENV)?),
            },
            "r2" => ArtifactStorageConfig::R2,
            "" => bail!("{RUNTIME_STORAGE_DRIVER_ENV} must not be empty"),
            other => bail!("{RUNTIME_STORAGE_DRIVER_ENV} must be 'local' or 'r2', got '{other}'"),
        };
        let runtime_version = required_lookup_value(&mut lookup, RUNTIME_VERSION_ENV)?;
        Uuid::parse_str(runtime_version.as_str())
            .with_context(|| format!("{RUNTIME_VERSION_ENV} must be a UUID"))?;
        let runtime_manifest_object_key =
            required_lookup_value(&mut lookup, RUNTIME_MANIFEST_OBJECT_KEY_ENV)?;
        validate_runtime_manifest_object_key(
            runtime_version.as_str(),
            runtime_manifest_object_key.as_str(),
        )?;
        let exact_manifest_object_key =
            required_lookup_value(&mut lookup, RUNTIME_EXACT_MANIFEST_OBJECT_KEY_ENV)?;
        ObjectKey::parse(exact_manifest_object_key.as_str())
            .map_err(|error| anyhow::anyhow!("{RUNTIME_EXACT_MANIFEST_OBJECT_KEY_ENV}: {error}"))?;
        let aggregate_manifest_object_key =
            required_lookup_value(&mut lookup, RUNTIME_AGGREGATE_MANIFEST_OBJECT_KEY_ENV)?;
        ObjectKey::parse(aggregate_manifest_object_key.as_str()).map_err(|error| {
            anyhow::anyhow!("{RUNTIME_AGGREGATE_MANIFEST_OBJECT_KEY_ENV}: {error}")
        })?;
        let operator_staff_id = Uuid::parse_str(
            required_lookup_value(&mut lookup, RUNTIME_OPERATOR_STAFF_ID_ENV)?.as_str(),
        )
        .map(StaffId::new)
        .with_context(|| format!("{RUNTIME_OPERATOR_STAFF_ID_ENV} must be a UUID"))?;

        Ok(Self {
            database_url,
            storage,
            runtime_version,
            runtime_manifest_object_key,
            exact_manifest_object_key,
            aggregate_manifest_object_key,
            expected_current_version: required_lookup_value(
                &mut lookup,
                RUNTIME_EXPECTED_CURRENT_VERSION_ENV,
            )?,
            tiles_url_template: required_lookup_value(&mut lookup, RUNTIME_TILES_URL_TEMPLATE_ENV)?,
            operator_staff_id,
            request_id: optional_lookup_value(&mut lookup, RUNTIME_REQUEST_ID_ENV)?,
            summary_path: optional_lookup_value(&mut lookup, RUNTIME_SUMMARY_PATH_ENV)?
                .map(PathBuf::from),
        })
    }
}

impl ArtifactStorage {
    async fn from_config(config: &ArtifactStorageConfig) -> anyhow::Result<Self> {
        match config {
            ArtifactStorageConfig::Local { root } => Ok(Self::Local(FileObjectStorage::new(root)?)),
            ArtifactStorageConfig::R2 => Ok(Self::R2(R2ObjectStorage::from_env()?)),
        }
    }

    async fn read_object_bytes(&self, object_key: &str) -> anyhow::Result<Vec<u8>> {
        ObjectKey::parse(object_key)
            .map_err(|error| anyhow::anyhow!("invalid object key {object_key}: {error}"))?;
        match self {
            Self::Local(storage) => storage.get_object_bytes(object_key).map_err(Into::into),
            Self::R2(storage) => storage
                .get_object_bytes(object_key)
                .await
                .map_err(Into::into),
        }
    }

    async fn write_object_bytes(
        &self,
        object_key: &str,
        bytes: Vec<u8>,
        content_type: &str,
        cache_control: &str,
    ) -> anyhow::Result<()> {
        ObjectKey::parse(object_key)
            .map_err(|error| anyhow::anyhow!("invalid object key {object_key}: {error}"))?;
        let request = PutObjectRequest {
            key: object_key.to_owned(),
            sha256: Some(sha256_hex(&bytes)),
            body: bytes,
            content_type: content_type.to_owned(),
            cache_control: cache_control.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
        };
        match self {
            Self::Local(storage) => storage.put_object(request).await.map_err(Into::into),
            Self::R2(storage) => storage.put_object(request).await.map_err(Into::into),
        }
    }
}

fn build_promotion_input(
    config: &PromoteConfig,
    objects: &ArtifactObjectBytes,
) -> anyhow::Result<PromoteVectorTileManifestInput> {
    let manifest = parse_manifest(objects.manifest.as_slice())?;
    validate_manifest_for_promote(config, &manifest)?;
    let version_prefix = config
        .manifest_object_key
        .strip_suffix("/manifest.json")
        .context("manifest object key must end with /manifest.json")?;
    let object_key_prefix = format!("{version_prefix}/{}", manifest.layer);
    ObjectKeyPrefix::parse(object_key_prefix.as_str())
        .map_err(|error| anyhow::anyhow!("object_key_prefix: {error}"))?;

    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        manifest.layer.clone(),
        VectorTileArtifactPromotionCommand {
            source_layer: manifest.layer.clone(),
            tile_min_zoom: manifest.min_zoom,
            tile_max_zoom: manifest.max_zoom,
            render_min_zoom: manifest.min_zoom,
            render_max_zoom: manifest.max_zoom,
            tilejson_file_asset: file_asset(
                manifest.tilejson_object_key.as_str(),
                "application/json",
                &objects.tilejson,
                "parcel marker anchor TileJSON",
                "public",
            )?,
            object_key_prefix,
            flat_tile_count: manifest.tile_count,
            flat_tile_total_bytes: manifest.tile_total_bytes,
            source_file_assets: vec![file_asset(
                manifest.source_anchor_manifest_object_key.as_str(),
                "application/json",
                &objects.source_anchor_manifest,
                "parcel marker anchor source manifest",
                "internal",
            )?],
        },
    );

    Ok(PromoteVectorTileManifestInput {
        current_version: manifest.artifact_version.clone(),
        expected_current_version: config.expected_current_version.clone(),
        tiles_url_template: config.tiles_url_template.clone(),
        source_record: VectorTileSourceRecordCommand {
            source: "foundation-platform.parcel_marker_anchor_pbf_artifact_build".to_owned(),
            source_url: None,
            external_id: Some(manifest.artifact_version),
            checksum_sha256: Some(manifest.checksum_sha256),
            raw_object_key: Some(manifest.source_anchor_manifest_object_key),
        },
        manifest_file_asset: file_asset(
            config.manifest_object_key.as_str(),
            "application/json",
            &objects.manifest,
            "parcel marker anchor PBF manifest",
            "public",
        )?,
        artifacts,
        operator_staff_id: config.operator_staff_id,
        request_id: config.request_id.clone(),
    })
}

fn build_runtime_promotion_input(
    config: &RuntimePromoteConfig,
    objects: &RuntimeArtifactObjectBytes,
) -> anyhow::Result<RuntimePromotionInput> {
    validate_runtime_manifest_object_key(
        config.runtime_version.as_str(),
        config.runtime_manifest_object_key.as_str(),
    )?;
    let exact_manifest = parse_manifest(objects.exact_manifest.as_slice())?;
    let aggregate_manifest = parse_manifest(objects.aggregate_manifest.as_slice())?;
    validate_exact_manifest_for_runtime(
        config.exact_manifest_object_key.as_str(),
        &exact_manifest,
    )?;
    validate_aggregate_manifest_for_runtime(
        config.aggregate_manifest_object_key.as_str(),
        &aggregate_manifest,
    )?;
    validate_runtime_pair(&exact_manifest, &aggregate_manifest)?;

    let exact_object_key_prefix = manifest_layer_object_key_prefix(
        config.exact_manifest_object_key.as_str(),
        exact_manifest.layer.as_str(),
    )?;
    let aggregate_object_key_prefix = manifest_layer_object_key_prefix(
        config.aggregate_manifest_object_key.as_str(),
        aggregate_manifest.layer.as_str(),
    )?;
    let runtime_manifest_bytes = build_runtime_manifest_bytes(
        config,
        &exact_manifest,
        &aggregate_manifest,
        exact_object_key_prefix.as_str(),
        aggregate_object_key_prefix.as_str(),
    )?;
    let runtime_manifest_checksum = sha256_hex(&runtime_manifest_bytes);

    let mut artifacts = BTreeMap::new();
    artifacts.insert(
        aggregate_manifest.layer.clone(),
        runtime_artifact_command(
            &aggregate_manifest,
            aggregate_object_key_prefix,
            config.aggregate_manifest_object_key.as_str(),
            &objects.aggregate_manifest,
            &objects.aggregate_tilejson,
            "parcel marker anchor aggregate PBF manifest",
            "parcel marker anchor aggregate TileJSON",
        )?,
    );
    artifacts.insert(
        exact_manifest.layer.clone(),
        runtime_artifact_command(
            &exact_manifest,
            exact_object_key_prefix,
            config.exact_manifest_object_key.as_str(),
            &objects.exact_manifest,
            &objects.exact_tilejson,
            "parcel marker anchor exact PBF manifest",
            "parcel marker anchor exact TileJSON",
        )?,
    );

    Ok(RuntimePromotionInput {
        input: PromoteVectorTileManifestInput {
            current_version: config.runtime_version.clone(),
            expected_current_version: config.expected_current_version.clone(),
            tiles_url_template: config.tiles_url_template.clone(),
            source_record: VectorTileSourceRecordCommand {
                source: "foundation-platform.parcel_marker_anchor_runtime_manifest_promote"
                    .to_owned(),
                source_url: None,
                external_id: Some(config.runtime_version.clone()),
                checksum_sha256: Some(runtime_manifest_checksum),
                raw_object_key: Some(config.runtime_manifest_object_key.clone()),
            },
            manifest_file_asset: file_asset(
                config.runtime_manifest_object_key.as_str(),
                JSON_CONTENT_TYPE,
                runtime_manifest_bytes.as_slice(),
                "parcel marker anchor runtime manifest",
                "public",
            )?,
            artifacts,
            operator_staff_id: config.operator_staff_id,
            request_id: config.request_id.clone(),
        },
        runtime_manifest_bytes,
    })
}

fn parse_manifest(bytes: &[u8]) -> anyhow::Result<PbfManifest> {
    serde_json::from_slice(bytes).context("parcel marker anchor PBF manifest is not valid JSON")
}

fn validate_manifest_for_promote(
    config: &PromoteConfig,
    manifest: &PbfManifest,
) -> anyhow::Result<()> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        bail!("PBF manifest schema_version mismatch");
    }
    Uuid::parse_str(manifest.artifact_version.as_str())
        .context("PBF manifest artifact_version must be a UUID")?;
    if manifest.layer != LAYER {
        bail!("PBF manifest layer must be {LAYER}");
    }
    if manifest.source_anchor_row_count == 0 {
        bail!("PBF manifest source_anchor_row_count must be positive");
    }
    if manifest.source_snapshot_id.trim().is_empty() || manifest.source_table.trim().is_empty() {
        bail!("PBF manifest source lineage must not be empty");
    }
    if manifest.algorithm.trim().is_empty() || manifest.algorithm_version.trim().is_empty() {
        bail!("PBF manifest algorithm lineage must not be empty");
    }
    if manifest.min_zoom > manifest.max_zoom {
        bail!("PBF manifest zoom range is inverted");
    }
    if !is_lowercase_sha256(manifest.checksum_sha256.as_str()) {
        bail!("PBF manifest checksum_sha256 must be lowercase SHA-256");
    }
    let expected_prefix = vector_tile_artifact_prefix(manifest.artifact_version.as_str())?;
    let expected_manifest_object_key = format!("{expected_prefix}/manifest.json");
    if config.manifest_object_key != expected_manifest_object_key {
        bail!(
            "PBF manifest object key must include artifact_version: expected {expected_manifest_object_key}"
        );
    }
    let expected_tilejson_object_key = format!("{expected_prefix}/tilejson.json");
    if manifest.tilejson_object_key != expected_tilejson_object_key {
        bail!("PBF TileJSON object key must share the artifact_version prefix");
    }
    if manifest.tile_count == 0 || manifest.tile_total_bytes == 0 {
        bail!("PBF manifest tile counts must be positive");
    }
    if manifest.tile_count
        != u64::try_from(manifest.tiles.len()).context("PBF tile vector length overflow")?
    {
        bail!("PBF manifest tile_count does not match tile entries");
    }
    let mut total_bytes = 0_u64;
    let tile_prefix = format!("{expected_prefix}/{}/", manifest.layer);
    for tile in &manifest.tiles {
        if !tile.object_key.starts_with(tile_prefix.as_str()) {
            bail!("PBF tile object key is outside artifact_version prefix");
        }
        if tile.feature_count == 0 || tile.size_bytes == 0 {
            bail!("PBF tile entry counts must be positive");
        }
        if !is_lowercase_sha256(tile.checksum_sha256.as_str()) {
            bail!("PBF tile checksum_sha256 must be lowercase SHA-256");
        }
        total_bytes = total_bytes
            .checked_add(tile.size_bytes)
            .context("PBF tile_total_bytes overflow")?;
    }
    if total_bytes != manifest.tile_total_bytes {
        bail!("PBF manifest tile_total_bytes does not match tile entries");
    }
    ObjectKey::parse(manifest.source_anchor_manifest_object_key.as_str())
        .map_err(|error| anyhow::anyhow!("source_anchor_manifest_object_key: {error}"))?;
    Ok(())
}

fn validate_runtime_manifest_object_key(
    runtime_version: &str,
    object_key: &str,
) -> anyhow::Result<()> {
    ObjectKey::parse(object_key)
        .map_err(|error| anyhow::anyhow!("runtime manifest object key: {error}"))?;
    let expected = vector_tile_manifest_key(runtime_version)?;
    if object_key != expected {
        bail!("runtime manifest object key must be {expected}");
    }
    Ok(())
}

fn validate_exact_manifest_for_runtime(
    object_key: &str,
    manifest: &PbfManifest,
) -> anyhow::Result<()> {
    validate_runtime_artifact_manifest(object_key, manifest, MANIFEST_SCHEMA_VERSION, LAYER)?;
    if manifest.min_zoom < 12 {
        bail!("exact parcel anchor runtime artifact must start at z12 or above");
    }
    Ok(())
}

fn validate_aggregate_manifest_for_runtime(
    object_key: &str,
    manifest: &PbfManifest,
) -> anyhow::Result<()> {
    validate_runtime_artifact_manifest(
        object_key,
        manifest,
        AGGREGATE_MANIFEST_SCHEMA_VERSION,
        AGGREGATE_LAYER,
    )?;
    if manifest.max_zoom > 11 {
        bail!("aggregate parcel anchor runtime artifact must end at z11 or below");
    }
    Ok(())
}

fn validate_runtime_artifact_manifest(
    object_key: &str,
    manifest: &PbfManifest,
    schema_version: &str,
    layer: &str,
) -> anyhow::Result<()> {
    if manifest.schema_version != schema_version {
        bail!("PBF runtime artifact manifest schema_version mismatch");
    }
    Uuid::parse_str(manifest.artifact_version.as_str())
        .context("PBF runtime artifact_version must be a UUID")?;
    if manifest.layer != layer {
        bail!("PBF runtime artifact layer mismatch");
    }
    validate_pbf_manifest_lineage_and_counts(manifest)?;
    let expected_prefix = vector_tile_artifact_prefix(manifest.artifact_version.as_str())?;
    let expected_manifest_object_key = format!("{expected_prefix}/manifest.json");
    if object_key != expected_manifest_object_key {
        bail!("PBF runtime artifact manifest object key must include artifact_version");
    }
    let expected_tilejson_object_key = format!("{expected_prefix}/tilejson.json");
    if manifest.tilejson_object_key != expected_tilejson_object_key {
        bail!("PBF runtime artifact TileJSON object key must share artifact_version prefix");
    }
    let tile_prefix = format!("{expected_prefix}/{}/", manifest.layer);
    validate_pbf_manifest_tiles(manifest, tile_prefix.as_str())?;
    ObjectKey::parse(manifest.source_anchor_manifest_object_key.as_str())
        .map_err(|error| anyhow::anyhow!("source_anchor_manifest_object_key: {error}"))?;
    Ok(())
}

fn validate_pbf_manifest_lineage_and_counts(manifest: &PbfManifest) -> anyhow::Result<()> {
    if manifest.source_anchor_row_count == 0 {
        bail!("PBF manifest source_anchor_row_count must be positive");
    }
    if manifest.source_anchor_artifact_version.trim().is_empty()
        || manifest.source_snapshot_id.trim().is_empty()
        || manifest.source_table.trim().is_empty()
    {
        bail!("PBF manifest source lineage must not be empty");
    }
    if manifest.algorithm.trim().is_empty() || manifest.algorithm_version.trim().is_empty() {
        bail!("PBF manifest algorithm lineage must not be empty");
    }
    if manifest.min_zoom > manifest.max_zoom {
        bail!("PBF manifest zoom range is inverted");
    }
    if manifest.tile_count == 0 || manifest.tile_total_bytes == 0 {
        bail!("PBF manifest tile counts must be positive");
    }
    if !is_lowercase_sha256(manifest.checksum_sha256.as_str()) {
        bail!("PBF manifest checksum_sha256 must be lowercase SHA-256");
    }
    if manifest.tile_count
        != u64::try_from(manifest.tiles.len()).context("PBF tile vector length overflow")?
    {
        bail!("PBF manifest tile_count does not match tile entries");
    }
    Ok(())
}

fn validate_pbf_manifest_tiles(manifest: &PbfManifest, tile_prefix: &str) -> anyhow::Result<()> {
    let mut total_bytes = 0_u64;
    for tile in &manifest.tiles {
        if !tile.object_key.starts_with(tile_prefix) {
            bail!("PBF tile object key is outside artifact_version prefix");
        }
        if tile.feature_count == 0 || tile.size_bytes == 0 {
            bail!("PBF tile entry counts must be positive");
        }
        if !is_lowercase_sha256(tile.checksum_sha256.as_str()) {
            bail!("PBF tile checksum_sha256 must be lowercase SHA-256");
        }
        total_bytes = total_bytes
            .checked_add(tile.size_bytes)
            .context("PBF tile_total_bytes overflow")?;
    }
    if total_bytes != manifest.tile_total_bytes {
        bail!("PBF manifest tile_total_bytes does not match tile entries");
    }
    Ok(())
}

fn validate_runtime_pair(exact: &PbfManifest, aggregate: &PbfManifest) -> anyhow::Result<()> {
    if exact.source_anchor_manifest_object_key != aggregate.source_anchor_manifest_object_key
        || exact.source_anchor_artifact_version != aggregate.source_anchor_artifact_version
        || exact.source_snapshot_id != aggregate.source_snapshot_id
        || exact.source_table != aggregate.source_table
        || exact.source_anchor_row_count != aggregate.source_anchor_row_count
    {
        bail!("runtime exact and aggregate artifacts must share the same anchor source lineage");
    }
    if aggregate.max_zoom.saturating_add(1) != exact.min_zoom {
        bail!("runtime exact and aggregate zoom ranges must be contiguous");
    }
    Ok(())
}

fn manifest_layer_object_key_prefix(
    manifest_object_key: &str,
    layer: &str,
) -> anyhow::Result<String> {
    let version_prefix = manifest_object_key
        .strip_suffix("/manifest.json")
        .context("manifest object key must end with /manifest.json")?;
    let object_key_prefix = format!("{version_prefix}/{layer}");
    ObjectKeyPrefix::parse(object_key_prefix.as_str())
        .map_err(|error| anyhow::anyhow!("object_key_prefix: {error}"))?;
    Ok(object_key_prefix)
}

fn build_runtime_manifest_bytes(
    config: &RuntimePromoteConfig,
    exact: &PbfManifest,
    aggregate: &PbfManifest,
    exact_object_key_prefix: &str,
    aggregate_object_key_prefix: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut layers = BTreeMap::new();
    layers.insert(
        aggregate.layer.clone(),
        RuntimeManifestLayer {
            artifact_version: aggregate.artifact_version.clone(),
            manifest_object_key: config.aggregate_manifest_object_key.clone(),
            source_layer: aggregate.layer.clone(),
            min_zoom: aggregate.min_zoom,
            max_zoom: aggregate.max_zoom,
            tile_count: aggregate.tile_count,
            tile_total_bytes: aggregate.tile_total_bytes,
            tilejson_object_key: aggregate.tilejson_object_key.clone(),
            object_key_prefix: aggregate_object_key_prefix.to_owned(),
            checksum_sha256: aggregate.checksum_sha256.clone(),
        },
    );
    layers.insert(
        exact.layer.clone(),
        RuntimeManifestLayer {
            artifact_version: exact.artifact_version.clone(),
            manifest_object_key: config.exact_manifest_object_key.clone(),
            source_layer: exact.layer.clone(),
            min_zoom: exact.min_zoom,
            max_zoom: exact.max_zoom,
            tile_count: exact.tile_count,
            tile_total_bytes: exact.tile_total_bytes,
            tilejson_object_key: exact.tilejson_object_key.clone(),
            object_key_prefix: exact_object_key_prefix.to_owned(),
            checksum_sha256: exact.checksum_sha256.clone(),
        },
    );
    let document = RuntimeManifestDocument {
        schema_version: RUNTIME_MANIFEST_SCHEMA_VERSION,
        runtime_version: config.runtime_version.clone(),
        generated_at_utc: Utc::now().to_rfc3339(),
        exact_manifest_object_key: config.exact_manifest_object_key.clone(),
        aggregate_manifest_object_key: config.aggregate_manifest_object_key.clone(),
        source_anchor_manifest_object_key: exact.source_anchor_manifest_object_key.clone(),
        source_anchor_artifact_version: exact.source_anchor_artifact_version.clone(),
        source_snapshot_id: exact.source_snapshot_id.clone(),
        source_table: exact.source_table.clone(),
        source_anchor_row_count: exact.source_anchor_row_count,
        layers,
    };
    serde_json::to_vec_pretty(&document)
        .context("failed to serialize parcel marker anchor runtime manifest")
}

fn runtime_artifact_command(
    manifest: &PbfManifest,
    object_key_prefix: String,
    manifest_object_key: &str,
    manifest_bytes: &[u8],
    tilejson_bytes: &[u8],
    manifest_title: &str,
    tilejson_title: &str,
) -> anyhow::Result<VectorTileArtifactPromotionCommand> {
    Ok(VectorTileArtifactPromotionCommand {
        source_layer: manifest.layer.clone(),
        tile_min_zoom: manifest.min_zoom,
        tile_max_zoom: manifest.max_zoom,
        render_min_zoom: manifest.min_zoom,
        render_max_zoom: manifest.max_zoom,
        tilejson_file_asset: file_asset(
            manifest.tilejson_object_key.as_str(),
            JSON_CONTENT_TYPE,
            tilejson_bytes,
            tilejson_title,
            "public",
        )?,
        object_key_prefix,
        flat_tile_count: manifest.tile_count,
        flat_tile_total_bytes: manifest.tile_total_bytes,
        source_file_assets: vec![file_asset(
            manifest_object_key,
            JSON_CONTENT_TYPE,
            manifest_bytes,
            manifest_title,
            "internal",
        )?],
    })
}

fn file_asset(
    object_key: &str,
    mime_type: &str,
    bytes: &[u8],
    title: &str,
    visibility: &str,
) -> anyhow::Result<VectorTileFileAssetCommand> {
    ObjectKey::parse(object_key).map_err(|error| anyhow::anyhow!("object_key: {error}"))?;
    Ok(VectorTileFileAssetCommand {
        object_key: object_key.to_owned(),
        mime_type: mime_type.to_owned(),
        size_bytes: u64::try_from(bytes.len()).context("file asset size overflow")?,
        checksum_sha256: Some(sha256_hex(bytes)),
        title: Some(title.to_owned()),
        visibility: visibility.to_owned(),
    })
}

fn write_summary(
    path: &PathBuf,
    manifest: &catalog_domain::VectorTileManifest,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create promote summary directory {}",
                parent.display()
            )
        })?;
    }
    let summary = PromoteSummary {
        schema_version: PROMOTE_SUMMARY_SCHEMA_VERSION,
        manifest_id: manifest.id.to_string(),
        current_version: manifest.current_version.clone(),
        previous_version: manifest.previous_version.clone(),
        artifact_count: manifest.artifacts.len(),
    };
    let bytes = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize parcel marker anchor PBF promote summary")?;
    std::fs::write(path, bytes)
        .with_context(|| format!("failed to write promote summary {}", path.display()))
}

fn write_runtime_summary(
    path: &PathBuf,
    manifest: &catalog_domain::VectorTileManifest,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create runtime promote summary directory {}",
                parent.display()
            )
        })?;
    }
    let summary = PromoteSummary {
        schema_version: RUNTIME_PROMOTE_SUMMARY_SCHEMA_VERSION,
        manifest_id: manifest.id.to_string(),
        current_version: manifest.current_version.clone(),
        previous_version: manifest.previous_version.clone(),
        artifact_count: manifest.artifacts.len(),
    };
    let bytes = serde_json::to_vec_pretty(&summary)
        .context("failed to serialize parcel marker anchor runtime promote summary")?;
    std::fs::write(path, bytes)
        .with_context(|| format!("failed to write runtime promote summary {}", path.display()))
}

fn required_lookup_value<F>(lookup: &mut F, name: &str) -> anyhow::Result<String>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    optional_lookup_value(lookup, name)?.map_or_else(|| bail!("{name} is required"), Ok)
}

fn optional_lookup_value<F>(lookup: &mut F, name: &str) -> anyhow::Result<Option<String>>
where
    F: FnMut(&str) -> anyhow::Result<Option<String>>,
{
    lookup(name).map(|value| {
        value.and_then(|raw| {
            let trimmed = raw.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
    })
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .fold(String::with_capacity(digest.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        build_promotion_input, build_runtime_promotion_input, ArtifactObjectBytes, ArtifactStorage,
        ArtifactStorageConfig, PromoteConfig, RuntimeArtifactObjectBytes, RuntimePromoteConfig,
        CONFIRM_ENV, DATABASE_URL_ENV, EXPECTED_CURRENT_VERSION_ENV, LOCAL_ROOT_ENV,
        MANIFEST_OBJECT_KEY_ENV, OPERATOR_STAFF_ID_ENV, STORAGE_DRIVER_ENV, TILES_URL_TEMPLATE_ENV,
    };

    const SOURCE_ANCHOR_ROW_COUNT: u64 = 39_862_470;
    const SOURCE_ANCHOR_MANIFEST_OBJECT_KEY: &str =
        "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000000/manifest.json";
    const SOURCE_ANCHOR_ARTIFACT_VERSION: &str = "018f0000-0000-7000-8000-000000000000";
    const SOURCE_SNAPSHOT_ID: &str = "national-promotion:silver-parcel-boundaries-vworld-0002";
    const SOURCE_TABLE: &str = "silver.parcel_boundaries";

    #[tokio::test]
    async fn immutable_runtime_manifest_refuses_overwrite() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-runtime-manifest-create-only-{}",
            uuid::Uuid::now_v7()
        ));
        let storage =
            ArtifactStorage::from_config(&ArtifactStorageConfig::Local { root: root.clone() })
                .await?;
        let key = "gold/vector-tiles/manifests/018f0000-0000-7000-8000-000000000099.json";

        storage
            .write_object_bytes(
                key,
                br#"{"schema_version":1}"#.to_vec(),
                "application/json",
                "no-store",
            )
            .await?;
        let duplicate = storage
            .write_object_bytes(
                key,
                br#"{"schema_version":1}"#.to_vec(),
                "application/json",
                "no-store",
            )
            .await;

        std::fs::remove_dir_all(root)?;
        assert!(
            duplicate.is_err(),
            "immutable runtime manifest was overwritten"
        );
        Ok(())
    }

    #[test]
    fn runtime_promotion_input_contains_aggregate_and_exact_layers() -> anyhow::Result<()> {
        let runtime_version = "018f0000-0000-7000-8000-000000000099";
        let exact_version = "018f0000-0000-7000-8000-000000000012";
        let aggregate_version = "018f0000-0000-7000-8000-000000000011";
        let exact_manifest_object_key =
            format!("gold/vector-tiles/artifacts/{exact_version}/manifest.json");
        let aggregate_manifest_object_key =
            format!("gold/vector-tiles/artifacts/{aggregate_version}/manifest.json");
        let runtime_manifest_object_key =
            format!("gold/vector-tiles/manifests/{runtime_version}.json");
        let config = RuntimePromoteConfig {
            database_url: "postgres://example".to_owned(),
            storage: super::ArtifactStorageConfig::Local {
                root: "target/test-object-storage".into(),
            },
            runtime_version: runtime_version.to_owned(),
            runtime_manifest_object_key: runtime_manifest_object_key.clone(),
            exact_manifest_object_key,
            aggregate_manifest_object_key,
            expected_current_version: "018f0000-0000-7000-8000-000000000098".to_owned(),
            tiles_url_template: "{object_key_prefix}/{z}/{x}/{y}.pbf".to_owned(),
            operator_staff_id: uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000100")
                .map(foundation_shared_kernel::ids::StaffId::new)?,
            request_id: Some("runtime-promote-test".to_owned()),
            summary_path: None,
        };
        let runtime = build_runtime_promotion_input(
            &config,
            &RuntimeArtifactObjectBytes {
                exact_manifest: exact_manifest_bytes(exact_version)?,
                exact_tilejson: br#"{"tilejson":"3.0.0"}"#.to_vec(),
                aggregate_manifest: aggregate_manifest_bytes(aggregate_version)?,
                aggregate_tilejson: br#"{"tilejson":"3.0.0"}"#.to_vec(),
            },
        )?;

        assert_eq!(runtime.input.current_version, runtime_version);
        assert_eq!(
            runtime.input.expected_current_version,
            "018f0000-0000-7000-8000-000000000098"
        );
        assert_eq!(
            runtime.input.manifest_file_asset.object_key,
            runtime_manifest_object_key
        );
        assert_eq!(
            runtime.input.source_record.raw_object_key.as_deref(),
            Some(runtime_manifest_object_key.as_str())
        );
        assert_eq!(runtime.input.artifacts.len(), 2);
        assert_eq!(
            runtime.input.artifacts["parcel_anchor_aggregate"].object_key_prefix,
            format!("gold/vector-tiles/artifacts/{aggregate_version}/parcel_anchor_aggregate")
        );
        assert_eq!(
            runtime.input.artifacts["parcel_anchor"].object_key_prefix,
            format!("gold/vector-tiles/artifacts/{exact_version}/parcel_anchor")
        );
        assert_eq!(
            runtime.input.artifacts["parcel_anchor_aggregate"].tile_min_zoom,
            0
        );
        assert_eq!(
            runtime.input.artifacts["parcel_anchor_aggregate"].tile_max_zoom,
            11
        );
        assert_eq!(runtime.input.artifacts["parcel_anchor"].tile_min_zoom, 12);
        assert_eq!(runtime.input.artifacts["parcel_anchor"].tile_max_zoom, 12);
        assert!(runtime
            .runtime_manifest_bytes
            .windows(exact_version.len())
            .any(|window| window == exact_version.as_bytes()));
        assert!(runtime
            .runtime_manifest_bytes
            .windows(aggregate_version.len())
            .any(|window| window == aggregate_version.as_bytes()));
        Ok(())
    }

    fn exact_manifest_bytes(artifact_version: &str) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": "foundation-platform.parcel_marker_anchor_pbf_artifact_manifest.v1",
            "artifact_version": artifact_version,
            "generated_at_utc": "2026-05-25T00:00:00Z",
            "build_run_id": artifact_version,
            "source_anchor_manifest_object_key": SOURCE_ANCHOR_MANIFEST_OBJECT_KEY,
            "source_anchor_artifact_version": SOURCE_ANCHOR_ARTIFACT_VERSION,
            "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            "source_table": SOURCE_TABLE,
            "source_anchor_row_count": SOURCE_ANCHOR_ROW_COUNT,
            "algorithm": "polylabel",
            "algorithm_version": "postgis-st_maximuminscribedcircle-v1",
            "layer": "parcel_anchor",
            "min_zoom": 12,
            "max_zoom": 12,
            "tile_count": 1,
            "tile_total_bytes": 128,
            "tilejson_object_key": format!(
                "gold/vector-tiles/artifacts/{artifact_version}/tilejson.json"
            ),
            "checksum_sha256": "a".repeat(64),
            "tiles": [{
                "z": 12,
                "x": 3500,
                "y": 1600,
                "object_key": format!(
                    "gold/vector-tiles/artifacts/{artifact_version}/parcel_anchor/12/3500/1600.pbf"
                ),
                "feature_count": 100,
                "size_bytes": 128,
                "checksum_sha256": "b".repeat(64)
            }]
        }))
        .map_err(Into::into)
    }

    fn aggregate_manifest_bytes(artifact_version: &str) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": "foundation-platform.parcel_marker_anchor_aggregate_pbf_artifact_manifest.v1",
            "artifact_version": artifact_version,
            "generated_at_utc": "2026-05-25T00:00:00Z",
            "build_run_id": artifact_version,
            "source_anchor_manifest_object_key": SOURCE_ANCHOR_MANIFEST_OBJECT_KEY,
            "source_anchor_artifact_version": SOURCE_ANCHOR_ARTIFACT_VERSION,
            "source_snapshot_id": SOURCE_SNAPSHOT_ID,
            "source_table": SOURCE_TABLE,
            "source_anchor_row_count": SOURCE_ANCHOR_ROW_COUNT,
            "algorithm": "web-mercator-tile-centroid",
            "algorithm_version": "rust-streaming-aggregate-v1",
            "layer": "parcel_anchor_aggregate",
            "min_zoom": 0,
            "max_zoom": 11,
            "aggregate_feature_count": 914,
            "tile_count": 1,
            "tile_total_bytes": 64,
            "tilejson_object_key": format!(
                "gold/vector-tiles/artifacts/{artifact_version}/tilejson.json"
            ),
            "checksum_sha256": "c".repeat(64),
            "tiles": [{
                "z": 0,
                "x": 0,
                "y": 0,
                "object_key": format!(
                    "gold/vector-tiles/artifacts/{artifact_version}/parcel_anchor_aggregate/0/0/0.pbf"
                ),
                "feature_count": 1,
                "source_anchor_count": SOURCE_ANCHOR_ROW_COUNT,
                "size_bytes": 64,
                "checksum_sha256": "d".repeat(64)
            }]
        }))
        .map_err(Into::into)
    }

    #[test]
    fn promotion_input_is_derived_from_manifest_and_object_bytes() -> anyhow::Result<()> {
        let artifact_version = "018f0000-0000-7000-8000-000000000001";
        let manifest_object_key =
            format!("gold/vector-tiles/artifacts/{artifact_version}/manifest.json");
        let tilejson_object_key =
            format!("gold/vector-tiles/artifacts/{artifact_version}/tilejson.json");
        let source_anchor_manifest_object_key = "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000000/manifest.json";
        let manifest_bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": "foundation-platform.parcel_marker_anchor_pbf_artifact_manifest.v1",
            "artifact_version": artifact_version,
            "generated_at_utc": "2026-05-25T00:00:00Z",
            "build_run_id": artifact_version,
            "source_anchor_manifest_object_key": source_anchor_manifest_object_key,
            "source_anchor_artifact_version": "018f0000-0000-7000-8000-000000000000",
            "source_snapshot_id": "national-promotion:silver-parcel-boundaries-vworld-0002",
            "source_table": "silver.parcel_boundaries",
            "source_anchor_row_count": 3320,
            "algorithm": "polylabel",
            "algorithm_version": "postgis-st_maximuminscribedcircle-v1",
            "layer": "parcel_anchor",
            "min_zoom": 12,
            "max_zoom": 12,
            "tile_count": 1,
            "tile_total_bytes": 128,
            "tilejson_object_key": tilejson_object_key,
            "checksum_sha256": "a".repeat(64),
            "tiles": [{
                "z": 12,
                "x": 3494,
                "y": 1591,
                "object_key": format!("gold/vector-tiles/artifacts/{artifact_version}/parcel_anchor/12/3494/1591.pbf"),
                "feature_count": 3320,
                "size_bytes": 128,
                "checksum_sha256": "b".repeat(64)
            }]
        }))?;
        let config = PromoteConfig::from_lookup(|name| {
            Ok(BTreeMap::from([
                (CONFIRM_ENV, "true"),
                (DATABASE_URL_ENV, "postgres://example"),
                (STORAGE_DRIVER_ENV, "local"),
                (LOCAL_ROOT_ENV, "target/test-object-storage"),
                (MANIFEST_OBJECT_KEY_ENV, manifest_object_key.as_str()),
                (
                    EXPECTED_CURRENT_VERSION_ENV,
                    "018f0000-0000-7000-8000-000000000000",
                ),
                (
                    TILES_URL_TEMPLATE_ENV,
                    "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf",
                ),
                (
                    OPERATOR_STAFF_ID_ENV,
                    "018f0000-0000-7000-8000-000000000100",
                ),
            ])
            .get(name)
            .map(ToString::to_string))
        })?;
        let input = build_promotion_input(
            &config,
            &ArtifactObjectBytes {
                manifest: manifest_bytes,
                tilejson: br#"{"tilejson":"3.0.0"}"#.to_vec(),
                source_anchor_manifest: br#"{"schema_version":"anchor"}"#.to_vec(),
            },
        )?;

        assert_eq!(input.current_version, artifact_version);
        assert_eq!(
            input.expected_current_version,
            "018f0000-0000-7000-8000-000000000000"
        );
        assert_eq!(input.manifest_file_asset.object_key, manifest_object_key);
        assert_eq!(
            input.artifacts["parcel_anchor"].object_key_prefix,
            format!("gold/vector-tiles/artifacts/{artifact_version}/parcel_anchor")
        );
        assert_eq!(
            input.artifacts["parcel_anchor"].source_file_assets[0].object_key,
            source_anchor_manifest_object_key
        );
        assert_eq!(input.artifacts["parcel_anchor"].flat_tile_count, 1);
        assert_eq!(input.artifacts["parcel_anchor"].flat_tile_total_bytes, 128);
        Ok(())
    }

    #[test]
    fn manifest_object_key_must_include_artifact_version_segment() -> anyhow::Result<()> {
        let artifact_version = "018f0000-0000-7000-8000-000000000001";
        let manifest_bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": "foundation-platform.parcel_marker_anchor_pbf_artifact_manifest.v1",
            "artifact_version": artifact_version,
            "generated_at_utc": "2026-05-25T00:00:00Z",
            "build_run_id": artifact_version,
            "source_anchor_manifest_object_key": "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000000/manifest.json",
            "source_anchor_artifact_version": "018f0000-0000-7000-8000-000000000000",
            "source_snapshot_id": "national-promotion:silver-parcel-boundaries-vworld-0002",
            "source_table": "silver.parcel_boundaries",
            "source_anchor_row_count": 3320,
            "algorithm": "polylabel",
            "algorithm_version": "postgis-st_maximuminscribedcircle-v1",
            "layer": "parcel_anchor",
            "min_zoom": 12,
            "max_zoom": 12,
            "tile_count": 1,
            "tile_total_bytes": 128,
            "tilejson_object_key": format!("gold/vector-tiles/artifacts/{artifact_version}/tilejson.json"),
            "checksum_sha256": "a".repeat(64),
            "tiles": [{
                "z": 12,
                "x": 3494,
                "y": 1591,
                "object_key": format!("gold/vector-tiles/artifacts/{artifact_version}/parcel_anchor/12/3494/1591.pbf"),
                "feature_count": 3320,
                "size_bytes": 128,
                "checksum_sha256": "b".repeat(64)
            }]
        }))?;
        let config = PromoteConfig::from_lookup(|name| {
            Ok(BTreeMap::from([
                (CONFIRM_ENV, "true"),
                (DATABASE_URL_ENV, "postgres://example"),
                (STORAGE_DRIVER_ENV, "local"),
                (LOCAL_ROOT_ENV, "target/test-object-storage"),
                (
                    MANIFEST_OBJECT_KEY_ENV,
                    "gold/vector-tiles/artifacts/vworld-0002/manifest.json",
                ),
                (
                    EXPECTED_CURRENT_VERSION_ENV,
                    "018f0000-0000-7000-8000-000000000000",
                ),
                (
                    TILES_URL_TEMPLATE_ENV,
                    "https://static.example.com/{object_key_prefix}/{z}/{x}/{y}.pbf",
                ),
                (
                    OPERATOR_STAFF_ID_ENV,
                    "018f0000-0000-7000-8000-000000000100",
                ),
            ])
            .get(name)
            .map(ToString::to_string))
        })?;

        let error = build_promotion_input(
            &config,
            &ArtifactObjectBytes {
                manifest: manifest_bytes,
                tilejson: Vec::new(),
                source_anchor_manifest: Vec::new(),
            },
        )
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected non-versioned manifest key rejection"))?;
        assert!(error.to_string().contains("artifact_version"));
        Ok(())
    }
}
