//! Builds immutable PBF marker tile artifacts from parcel marker anchor artifacts.
//!
//! This command keeps national marker serving off the operational Catalog database. It reads the
//! anchor JSONL manifest, uses `PostGIS` only as a scratch MVT encoder, and writes flat `.pbf`
//! tile objects plus `TileJSON` and build manifest objects to local object storage or R2.

use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::Utc;
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    FileObjectStorage, ObjectStorageService, R2ObjectStorage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection, Row};
use uuid::Uuid;

use crate::r2_layout::{vector_tile_artifact_prefix, VECTOR_TILE_ARTIFACT_ROOT};

pub mod aggregate;

const INPUT_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_artifact_manifest.v1";
const INPUT_ENTRY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_artifact_entry.v1";
const OUTPUT_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_pbf_artifact_manifest.v1";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_pbf_artifact_build_summary.v1";
const TILE_LAYER: &str = "parcel_anchor";
const TILE_EXTENT: i32 = 4096;
const TILE_BUFFER: i32 = 64;
const MAX_WEB_MERCATOR_ZOOM: u8 = 24;
const DEFAULT_MIN_ZOOM: u8 = 0;
const DEFAULT_MAX_ZOOM: u8 = 12;
const DEFAULT_MAX_INPUT_OBJECT_COUNT: u64 = 1;
const DEFAULT_MAX_INPUT_ROW_COUNT: u64 = 1_000_000;
const MAX_EXACT_POINT_LOW_ZOOM_ROW_COUNT: u64 = 1_000_000;
const EXACT_POINT_MIN_ZOOM_FOR_NATIONAL_INPUT: u8 = 12;
const PBF_CONTENT_TYPE: &str = "application/x-protobuf";
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const TILE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
const MANIFEST_CACHE_CONTROL: &str = "no-store";

/// Runs the parcel marker anchor PBF artifact build.
pub async fn run() -> anyhow::Result<()> {
    let config = PbfArtifactBuildConfig::from_env()?;
    let input = AnchorArtifactInput::from_config(&config.input).await?;
    let manifest_bytes = input
        .read_object_bytes(config.input.manifest_object_key())
        .await
        .context("failed to read parcel marker anchor artifact manifest")?;
    let manifest = parse_anchor_manifest(&manifest_bytes)?;
    validate_build_scope(&config, &manifest)?;
    let build_run_id = Uuid::now_v7();
    let artifact_version = build_run_id.to_string();
    let output = PbfArtifactOutput::from_config(&config.output, artifact_version.as_str()).await?;

    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL for PBF artifact build")?;
    let report =
        execute_build(&mut conn, &input, &output, &config, &manifest, build_run_id).await?;

    if let Some(summary_path) = &config.summary_path {
        write_local_summary(summary_path, &report)?;
    }

    tracing::info!(
        build_run_id = %report.build_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        manifest_object_key = %report.manifest_object_key,
        tile_count = report.tile_count,
        tile_total_bytes = report.tile_total_bytes,
        "parcel marker anchor PBF artifact build succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PbfArtifactBuildConfig {
    database_url: String,
    input: AnchorArtifactInputConfig,
    output: PbfArtifactOutputConfig,
    min_zoom: u8,
    max_zoom: u8,
    expected_anchor_row_count: Option<u64>,
    max_input_object_count: u64,
    max_input_row_count: u64,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AnchorArtifactInputConfig {
    Local { root: PathBuf, manifest_key: String },
    R2 { manifest_key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PbfArtifactOutputConfig {
    Local { root: PathBuf, prefix: String },
    R2 { prefix: String },
}

enum AnchorArtifactInput {
    Local(FileObjectStorage),
    R2(R2ObjectStorage),
}

enum PbfArtifactOutput {
    Local(FileObjectStorage, String),
    R2(R2ObjectStorage, String),
}

#[derive(Clone, Debug, Deserialize)]
struct AnchorArtifactManifest {
    schema_version: String,
    artifact_version: String,
    source_snapshot_id: String,
    source_table: String,
    anchor_srid: String,
    algorithm: String,
    algorithm_version: String,
    source_row_count: u64,
    artifact_object_count: u64,
    artifact_row_count: u64,
    rejected_object_count: u64,
    rejected_row_count: u64,
    checksum_sha256: String,
    objects: Vec<AnchorArtifactObject>,
    rejected_objects: Vec<AnchorArtifactRejectObject>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AnchorArtifactObject {
    shard_id: String,
    source_object_key: String,
    artifact_object_key: String,
    row_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
struct AnchorArtifactRejectObject {
    shard_id: String,
    source_object_key: String,
    rejected_object_key: String,
    row_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
struct AnchorArtifactEntry {
    schema_version: String,
    pnu: String,
    anchor_lng: f64,
    anchor_lat: f64,
    anchor_srid: String,
    algorithm: String,
    algorithm_version: String,
    source_snapshot_id: String,
    source_table: String,
    source_row_id: String,
    source_object_key: String,
    source_geometry_checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
struct PbfArtifactManifest {
    schema_version: &'static str,
    artifact_version: String,
    generated_at_utc: String,
    build_run_id: Uuid,
    source_anchor_manifest_object_key: String,
    source_anchor_artifact_version: String,
    source_snapshot_id: String,
    source_table: String,
    source_anchor_row_count: u64,
    algorithm: String,
    algorithm_version: String,
    layer: &'static str,
    min_zoom: u8,
    max_zoom: u8,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    checksum_sha256: String,
    tiles: Vec<PbfTileObject>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct PbfTileObject {
    z: u8,
    x: u32,
    y: u32,
    object_key: String,
    feature_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Debug, Serialize)]
struct PbfArtifactBuildSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    build_run_id: Uuid,
    input_storage_driver: &'static str,
    input_manifest_object_key: String,
    output_storage_driver: &'static str,
    output_object_prefix: String,
    source_snapshot_id: String,
    source_anchor_artifact_version: String,
    source_anchor_row_count: u64,
    min_zoom: u8,
    max_zoom: u8,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    manifest_object_key: String,
    checksum_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TileAddress {
    z: u8,
    x: u32,
    y: u32,
    feature_count: u64,
}

struct TileBuildResult {
    tiles: Vec<PbfTileObject>,
    tile_count: u64,
    tile_total_bytes: u64,
    manifest_digest: Sha256,
}

struct OutputManifestWrite<'a> {
    config: &'a PbfArtifactBuildConfig,
    manifest: &'a AnchorArtifactManifest,
    build_run_id: Uuid,
    tilejson_object_key: &'a str,
    checksum_sha256: &'a str,
    tile_count: u64,
    tile_total_bytes: u64,
    tiles: Vec<PbfTileObject>,
}

impl PbfArtifactBuildConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_CONFIRM_BUILD")?
            .unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_CONFIRM_BUILD must be true");
        }

        let min_zoom = optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_MIN_ZOOM")?
            .map(|value| parse_zoom(&value, "min zoom"))
            .transpose()?
            .unwrap_or(DEFAULT_MIN_ZOOM);
        let max_zoom = optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_MAX_ZOOM")?
            .map(|value| parse_zoom(&value, "max zoom"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_ZOOM);
        validate_zoom_range(min_zoom, max_zoom)?;

        Ok(Self {
            database_url: required_env("DATABASE_URL")?,
            input: AnchorArtifactInputConfig::from_env()?,
            output: PbfArtifactOutputConfig::from_env()?,
            min_zoom,
            max_zoom,
            expected_anchor_row_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_EXPECTED_ANCHOR_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "expected anchor row count"))
            .transpose()?,
            max_input_object_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_MAX_INPUT_OBJECT_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max input object count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_INPUT_OBJECT_COUNT),
            max_input_row_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_MAX_INPUT_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "max input row count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_INPUT_ROW_COUNT),
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
        })
    }
}

impl AnchorArtifactInputConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_INPUT_STORAGE_DRIVER")?
                .unwrap_or_else(|| "local".to_owned())
                .to_ascii_lowercase();
        let manifest_key =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_INPUT_MANIFEST_OBJECT_KEY")?;
        validate_object_key(manifest_key.as_str())?;

        match driver.as_str() {
            "local" => Ok(Self::Local {
                root: PathBuf::from(required_env(
                    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_INPUT_ROOT",
                )?),
                manifest_key,
            }),
            "r2" => Ok(Self::R2 { manifest_key }),
            "" => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_INPUT_STORAGE_DRIVER must not be empty"
            ),
            other => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_INPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
            ),
        }
    }

    const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local { .. } => "local",
            Self::R2 { .. } => "r2",
        }
    }

    fn manifest_object_key(&self) -> &str {
        match self {
            Self::Local { manifest_key, .. } | Self::R2 { manifest_key } => manifest_key,
        }
    }
}

impl PbfArtifactOutputConfig {
    fn from_env() -> anyhow::Result<Self> {
        let driver =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_STORAGE_DRIVER")?
                .unwrap_or_else(|| "local".to_owned())
                .to_ascii_lowercase();
        let prefix =
            required_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_OBJECT_PREFIX")?;
        validate_object_key_prefix(prefix.as_str())?;
        if prefix != VECTOR_TILE_ARTIFACT_ROOT {
            bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_OBJECT_PREFIX must be {VECTOR_TILE_ARTIFACT_ROOT}"
            );
        }

        match driver.as_str() {
            "local" => Ok(Self::Local {
                root: PathBuf::from(required_env(
                    "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_ROOT",
                )?),
                prefix,
            }),
            "r2" => Ok(Self::R2 { prefix }),
            "" => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_STORAGE_DRIVER must not be empty"
            ),
            other => bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_PBF_OUTPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
            ),
        }
    }
}

impl AnchorArtifactInput {
    async fn from_config(config: &AnchorArtifactInputConfig) -> anyhow::Result<Self> {
        match config {
            AnchorArtifactInputConfig::Local { root, .. } => {
                Ok(Self::Local(FileObjectStorage::new(root)?))
            }
            AnchorArtifactInputConfig::R2 { .. } => Ok(Self::R2(R2ObjectStorage::from_env()?)),
        }
    }

    async fn read_object_bytes(&self, object_key: &str) -> anyhow::Result<Vec<u8>> {
        validate_object_key(object_key)?;
        match self {
            Self::Local(storage) => storage.get_object_bytes(object_key).map_err(Into::into),
            Self::R2(storage) => storage
                .get_object_bytes_range_retried(object_key)
                .await
                .map_err(Into::into),
        }
    }
}

impl PbfArtifactOutput {
    async fn from_config(
        config: &PbfArtifactOutputConfig,
        artifact_version: &str,
    ) -> anyhow::Result<Self> {
        if config.prefix() != VECTOR_TILE_ARTIFACT_ROOT {
            bail!("PBF output prefix must be {VECTOR_TILE_ARTIFACT_ROOT}");
        }
        let prefix = vector_tile_artifact_prefix(artifact_version)?;
        match config {
            PbfArtifactOutputConfig::Local { root, .. } => {
                Ok(Self::Local(FileObjectStorage::new(root)?, prefix))
            }
            PbfArtifactOutputConfig::R2 { .. } => {
                Ok(Self::R2(R2ObjectStorage::from_env()?, prefix))
            }
        }
    }

    const fn storage_driver(&self) -> &'static str {
        match self {
            Self::Local(_, _) => "local",
            Self::R2(_, _) => "r2",
        }
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Local(_, prefix) | Self::R2(_, prefix) => prefix,
        }
    }

    async fn put_object(
        &self,
        key: String,
        body: Vec<u8>,
        content_type: &'static str,
        cache_control: &'static str,
    ) -> anyhow::Result<()> {
        validate_object_key(key.as_str())?;
        let sha256 = sha256_hex(&body);
        let request = PutObjectRequest {
            key,
            body,
            content_type: content_type.to_owned(),
            cache_control: cache_control.to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: Some(sha256),
        };
        match self {
            Self::Local(storage, _) => storage.put_object(request).await.map_err(Into::into),
            Self::R2(storage, _) => storage.put_object(request).await.map_err(Into::into),
        }
    }

    fn tile_object_key(&self, tile: TileAddress) -> anyhow::Result<String> {
        let key = format!(
            "{}/{}/{}/{}/{}.pbf",
            self.prefix(),
            TILE_LAYER,
            tile.z,
            tile.x,
            tile.y
        );
        validate_object_key(key.as_str())?;
        Ok(key)
    }

    fn tilejson_object_key(&self) -> anyhow::Result<String> {
        let key = format!("{}/tilejson.json", self.prefix());
        validate_object_key(key.as_str())?;
        Ok(key)
    }

    fn manifest_object_key(&self) -> anyhow::Result<String> {
        let key = format!("{}/manifest.json", self.prefix());
        validate_object_key(key.as_str())?;
        Ok(key)
    }
}

impl PbfArtifactOutputConfig {
    fn prefix(&self) -> &str {
        match self {
            Self::Local { prefix, .. } | Self::R2 { prefix } => prefix,
        }
    }
}

async fn execute_build(
    conn: &mut PgConnection,
    input: &AnchorArtifactInput,
    output: &PbfArtifactOutput,
    config: &PbfArtifactBuildConfig,
    manifest: &AnchorArtifactManifest,
    build_run_id: Uuid,
) -> anyhow::Result<PbfArtifactBuildSummary> {
    let loaded_row_count = load_stage_for_build(conn, input, manifest).await?;
    if loaded_row_count != manifest.artifact_row_count {
        bail!(
            "loaded anchor row count mismatch: expected={} actual={loaded_row_count}",
            manifest.artifact_row_count
        );
    }

    let mut tile_build = build_tile_objects(conn, output, config).await?;
    let artifact_version = build_run_id.to_string();
    let tilejson_object_key =
        write_tilejson_object(output, config, artifact_version.as_str(), &mut tile_build).await?;
    let tile_count = tile_build.tile_count;
    let tile_total_bytes = tile_build.tile_total_bytes;
    let tiles = std::mem::take(&mut tile_build.tiles);
    let checksum_sha256 = hex_lower(&tile_build.manifest_digest.finalize());
    let manifest_object_key = write_output_manifest(
        output,
        OutputManifestWrite {
            config,
            manifest,
            build_run_id,
            tilejson_object_key: &tilejson_object_key,
            checksum_sha256: &checksum_sha256,
            tile_count,
            tile_total_bytes,
            tiles,
        },
    )
    .await?;

    Ok(PbfArtifactBuildSummary {
        schema_version: SUMMARY_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339(),
        build_run_id,
        input_storage_driver: config.input.storage_driver(),
        input_manifest_object_key: config.input.manifest_object_key().to_owned(),
        output_storage_driver: output.storage_driver(),
        output_object_prefix: output.prefix().to_owned(),
        source_snapshot_id: manifest.source_snapshot_id.clone(),
        source_anchor_artifact_version: manifest.artifact_version.clone(),
        source_anchor_row_count: manifest.artifact_row_count,
        min_zoom: config.min_zoom,
        max_zoom: config.max_zoom,
        tile_count,
        tile_total_bytes,
        tilejson_object_key,
        manifest_object_key,
        checksum_sha256,
    })
}

async fn load_stage_for_build(
    conn: &mut PgConnection,
    input: &AnchorArtifactInput,
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<u64> {
    create_stage_tables(conn).await?;
    let loaded_row_count = load_anchor_artifacts(conn, input, manifest).await?;
    index_stage_table(conn).await?;
    Ok(loaded_row_count)
}

async fn build_tile_objects(
    conn: &mut PgConnection,
    output: &PbfArtifactOutput,
    config: &PbfArtifactBuildConfig,
) -> anyhow::Result<TileBuildResult> {
    let tile_addresses = list_tile_addresses(conn, config.min_zoom, config.max_zoom).await?;
    let mut tile_objects = Vec::with_capacity(tile_addresses.len());
    let mut tile_total_bytes = 0_u64;
    let mut manifest_digest = Sha256::new();

    for tile in tile_addresses {
        let tile_body = render_tile(conn, tile).await?;
        let checksum_sha256 = sha256_hex(&tile_body);
        let size_bytes = u64::try_from(tile_body.len()).context("tile byte size overflow")?;
        let object_key = output.tile_object_key(tile)?;
        output
            .put_object(
                object_key.clone(),
                tile_body,
                PBF_CONTENT_TYPE,
                TILE_CACHE_CONTROL,
            )
            .await?;
        manifest_digest.update(object_key.as_bytes());
        manifest_digest.update(checksum_sha256.as_bytes());
        tile_total_bytes = tile_total_bytes
            .checked_add(size_bytes)
            .context("tile total byte size overflow")?;
        tile_objects.push(PbfTileObject {
            z: tile.z,
            x: tile.x,
            y: tile.y,
            object_key,
            feature_count: tile.feature_count,
            size_bytes,
            checksum_sha256,
        });
    }

    let tile_count = u64::try_from(tile_objects.len()).context("tile count overflow")?;
    Ok(TileBuildResult {
        tiles: tile_objects,
        tile_count,
        tile_total_bytes,
        manifest_digest,
    })
}

async fn write_tilejson_object(
    output: &PbfArtifactOutput,
    config: &PbfArtifactBuildConfig,
    artifact_version: &str,
    tile_build: &mut TileBuildResult,
) -> anyhow::Result<String> {
    let tilejson_object_key = output.tilejson_object_key()?;
    let tilejson_body = build_tilejson(
        output.prefix(),
        artifact_version,
        config.min_zoom,
        config.max_zoom,
    )?;
    let tilejson_checksum = sha256_hex(&tilejson_body);
    output
        .put_object(
            tilejson_object_key.clone(),
            tilejson_body,
            JSON_CONTENT_TYPE,
            TILE_CACHE_CONTROL,
        )
        .await?;
    tile_build
        .manifest_digest
        .update(tilejson_object_key.as_bytes());
    tile_build
        .manifest_digest
        .update(tilejson_checksum.as_bytes());
    Ok(tilejson_object_key)
}

async fn write_output_manifest(
    output: &PbfArtifactOutput,
    input: OutputManifestWrite<'_>,
) -> anyhow::Result<String> {
    let output_manifest = PbfArtifactManifest {
        schema_version: OUTPUT_MANIFEST_SCHEMA_VERSION,
        artifact_version: input.build_run_id.to_string(),
        generated_at_utc: Utc::now().to_rfc3339(),
        build_run_id: input.build_run_id,
        source_anchor_manifest_object_key: input.config.input.manifest_object_key().to_owned(),
        source_anchor_artifact_version: input.manifest.artifact_version.clone(),
        source_snapshot_id: input.manifest.source_snapshot_id.clone(),
        source_table: input.manifest.source_table.clone(),
        source_anchor_row_count: input.manifest.artifact_row_count,
        algorithm: input.manifest.algorithm.clone(),
        algorithm_version: input.manifest.algorithm_version.clone(),
        layer: TILE_LAYER,
        min_zoom: input.config.min_zoom,
        max_zoom: input.config.max_zoom,
        tile_count: input.tile_count,
        tile_total_bytes: input.tile_total_bytes,
        tilejson_object_key: input.tilejson_object_key.to_owned(),
        checksum_sha256: input.checksum_sha256.to_owned(),
        tiles: input.tiles,
    };
    let manifest_object_key = output.manifest_object_key()?;
    let manifest_body = serde_json::to_vec_pretty(&output_manifest)
        .context("failed to serialize PBF artifact manifest")?;
    output
        .put_object(
            manifest_object_key.clone(),
            manifest_body,
            JSON_CONTENT_TYPE,
            MANIFEST_CACHE_CONTROL,
        )
        .await?;
    Ok(manifest_object_key)
}

fn parse_anchor_manifest(bytes: &[u8]) -> anyhow::Result<AnchorArtifactManifest> {
    let manifest: AnchorArtifactManifest =
        serde_json::from_slice(bytes).context("anchor artifact manifest is not valid JSON")?;
    validate_anchor_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_anchor_manifest(manifest: &AnchorArtifactManifest) -> anyhow::Result<()> {
    if manifest.schema_version != INPUT_MANIFEST_SCHEMA_VERSION {
        bail!("anchor artifact manifest schema mismatch");
    }
    if manifest.anchor_srid != "EPSG:4326" {
        bail!("anchor artifact manifest anchor_srid must be EPSG:4326");
    }
    if manifest.algorithm != "polylabel" {
        bail!("anchor artifact manifest algorithm must be polylabel");
    }
    validate_source_snapshot_id(manifest.source_snapshot_id.as_str())?;
    validate_object_key(manifest.artifact_version.as_str())?;
    validate_sha256("checksum_sha256", manifest.checksum_sha256.as_str())?;
    let object_count = u64::try_from(manifest.objects.len()).context("object count overflow")?;
    if object_count != manifest.artifact_object_count {
        bail!("anchor artifact manifest object count mismatch");
    }
    let rejected_object_count =
        u64::try_from(manifest.rejected_objects.len()).context("rejected object count overflow")?;
    if rejected_object_count != manifest.rejected_object_count {
        bail!("anchor artifact manifest rejected object count mismatch");
    }
    let mut rejected_by_shard = BTreeMap::new();
    for object in &manifest.rejected_objects {
        if rejected_by_shard
            .insert(object.shard_id.as_str(), object)
            .is_some()
        {
            bail!(
                "anchor artifact manifest has duplicate rejected shard_id {}",
                object.shard_id
            );
        }
    }

    let mut row_count = 0_u64;
    let mut rejected_row_count = 0_u64;
    let mut digest = Sha256::new();
    for object in &manifest.objects {
        validate_object_key(object.source_object_key.as_str())?;
        validate_object_key(object.artifact_object_key.as_str())?;
        validate_sha256("object checksum_sha256", object.checksum_sha256.as_str())?;
        row_count = row_count
            .checked_add(object.row_count)
            .context("anchor manifest row count overflow")?;
        digest.update(object.checksum_sha256.as_bytes());
        if let Some(rejected_object) = rejected_by_shard.remove(object.shard_id.as_str()) {
            if rejected_object.source_object_key != object.source_object_key {
                bail!(
                    "anchor artifact manifest rejected object source mismatch for shard {}",
                    object.shard_id
                );
            }
            validate_rejected_anchor_object(rejected_object)?;
            rejected_row_count = rejected_row_count
                .checked_add(rejected_object.row_count)
                .context("anchor manifest rejected row count overflow")?;
            digest.update(rejected_object.checksum_sha256.as_bytes());
        }
    }
    if let Some(orphan) = rejected_by_shard.values().next() {
        bail!(
            "anchor artifact manifest rejected object has no matching anchor shard {}",
            orphan.shard_id
        );
    }
    if row_count != manifest.artifact_row_count {
        bail!("anchor artifact manifest row count mismatch");
    }
    if rejected_row_count != manifest.rejected_row_count {
        bail!("anchor artifact manifest rejected row count mismatch");
    }
    if manifest
        .artifact_row_count
        .checked_add(manifest.rejected_row_count)
        .context("anchor artifact manifest source row count overflow")?
        != manifest.source_row_count
    {
        bail!("anchor artifact manifest source row accounting mismatch");
    }
    if hex_lower(&digest.finalize()) != manifest.checksum_sha256 {
        bail!("anchor artifact manifest checksum mismatch");
    }
    Ok(())
}

fn validate_rejected_anchor_object(object: &AnchorArtifactRejectObject) -> anyhow::Result<()> {
    if object.shard_id.trim().is_empty() {
        bail!("anchor artifact manifest rejected shard_id must not be empty");
    }
    if object.size_bytes == 0 {
        bail!("anchor artifact manifest rejected object size_bytes must be greater than zero");
    }
    validate_object_key(object.source_object_key.as_str())?;
    validate_object_key(object.rejected_object_key.as_str())?;
    validate_sha256(
        "rejected object checksum_sha256",
        object.checksum_sha256.as_str(),
    )?;
    Ok(())
}

fn validate_build_scope(
    config: &PbfArtifactBuildConfig,
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<()> {
    if let Some(expected) = config.expected_anchor_row_count {
        if expected != manifest.artifact_row_count {
            bail!(
                "configured expected anchor row count {expected} does not match manifest {}",
                manifest.artifact_row_count
            );
        }
    }
    if manifest.artifact_object_count > config.max_input_object_count {
        bail!(
            "anchor artifact object count {} exceeds configured max {}",
            manifest.artifact_object_count,
            config.max_input_object_count
        );
    }
    if manifest.artifact_row_count > config.max_input_row_count {
        bail!(
            "anchor artifact row count {} exceeds configured max {}",
            manifest.artifact_row_count,
            config.max_input_row_count
        );
    }
    if manifest.artifact_row_count > MAX_EXACT_POINT_LOW_ZOOM_ROW_COUNT
        && config.min_zoom < EXACT_POINT_MIN_ZOOM_FOR_NATIONAL_INPUT
    {
        bail!(
            "national-scale exact-point PBF below zoom {EXACT_POINT_MIN_ZOOM_FOR_NATIONAL_INPUT} requires an aggregate marker tile path"
        );
    }
    Ok(())
}

async fn create_stage_tables(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_pbf_raw (
             pnu text NOT NULL,
             anchor_lng double precision NOT NULL,
             anchor_lat double precision NOT NULL,
             algorithm text NOT NULL,
             algorithm_version text NOT NULL,
             source_snapshot_id text NOT NULL,
             source_table text NOT NULL,
             source_row_id text NOT NULL,
             source_object_key text NOT NULL,
             source_geometry_checksum_sha256 text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor PBF raw stage")?;
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_pbf_stage (
             pnu text NOT NULL,
             anchor_point geometry(Point, 4326) NOT NULL,
             algorithm text NOT NULL,
             algorithm_version text NOT NULL,
             source_snapshot_id text NOT NULL,
             source_table text NOT NULL,
             source_row_id text NOT NULL,
             source_object_key text NOT NULL,
             source_geometry_checksum_sha256 text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor PBF geometry stage")?;
    conn.execute("TRUNCATE TABLE parcel_marker_anchor_pbf_raw, parcel_marker_anchor_pbf_stage")
        .await
        .context("failed to truncate parcel marker anchor PBF stages")?;
    Ok(())
}

async fn load_anchor_artifacts(
    conn: &mut PgConnection,
    input: &AnchorArtifactInput,
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<u64> {
    let mut total_rows = 0_u64;
    for object in &manifest.objects {
        let object_bytes = input
            .read_object_bytes(object.artifact_object_key.as_str())
            .await
            .with_context(|| {
                format!(
                    "failed to read anchor artifact object {}",
                    object.artifact_object_key
                )
            })?;
        if sha256_hex(&object_bytes) != object.checksum_sha256 {
            bail!(
                "anchor artifact object checksum mismatch for {}",
                object.artifact_object_key
            );
        }
        let copied = copy_anchor_object_to_raw_stage(conn, object, &object_bytes, manifest).await?;
        if copied != object.row_count {
            bail!(
                "anchor artifact object row count mismatch for {}: expected={} actual={copied}",
                object.artifact_object_key,
                object.row_count
            );
        }
        total_rows = total_rows
            .checked_add(copied)
            .context("anchor artifact loaded row count overflow")?;
    }

    let inserted = insert_geometry_stage(conn).await?;
    if inserted != total_rows {
        bail!("geometry stage insert mismatch: raw={total_rows} stage={inserted}");
    }
    Ok(total_rows)
}

async fn copy_anchor_object_to_raw_stage(
    conn: &mut PgConnection,
    object: &AnchorArtifactObject,
    object_bytes: &[u8],
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<u64> {
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_marker_anchor_pbf_raw
             (pnu, anchor_lng, anchor_lat, algorithm, algorithm_version, source_snapshot_id,
              source_table, source_row_id, source_object_key, source_geometry_checksum_sha256)
             FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\"', NULL '\\N')",
        )
        .await
        .context("failed to start COPY into parcel marker anchor PBF raw stage")?;
    let mut buffer = Vec::new();
    let mut row_count = 0_u64;

    for (index, raw_line) in object_bytes.split(|byte| *byte == b'\n').enumerate() {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }
        let line_number = u64::try_from(index + 1).context("anchor artifact line overflow")?;
        let entry = parse_anchor_entry(line, object, manifest, line_number)?;
        push_anchor_copy_row(&mut buffer, &entry);
        row_count = row_count
            .checked_add(1)
            .context("anchor row count overflow")?;
        if buffer.len() >= 8 * 1024 * 1024 {
            copy.send(buffer.as_slice())
                .await
                .context("COPY send failed")?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        copy.send(buffer.as_slice())
            .await
            .context("COPY send failed")?;
    }
    let copied = copy.finish().await.context("COPY finish failed")?;
    if copied != row_count {
        bail!("COPY reported {copied} rows but parser saw {row_count} rows");
    }
    Ok(copied)
}

fn parse_anchor_entry(
    line: &[u8],
    object: &AnchorArtifactObject,
    manifest: &AnchorArtifactManifest,
    line_number: u64,
) -> anyhow::Result<AnchorArtifactEntry> {
    let entry: AnchorArtifactEntry = serde_json::from_slice(line).with_context(|| {
        format!(
            "anchor artifact object {} line {line_number} is not valid JSON",
            object.artifact_object_key
        )
    })?;
    if entry.schema_version != INPUT_ENTRY_SCHEMA_VERSION {
        bail!("anchor artifact entry schema mismatch at line {line_number}");
    }
    validate_pnu(entry.pnu.as_str())?;
    validate_lng_lat(entry.anchor_lng, entry.anchor_lat)?;
    if entry.anchor_srid != "EPSG:4326" {
        bail!("anchor artifact entry anchor_srid must be EPSG:4326 at line {line_number}");
    }
    if entry.algorithm != manifest.algorithm
        || entry.algorithm_version != manifest.algorithm_version
    {
        bail!("anchor artifact entry algorithm lineage mismatch at line {line_number}");
    }
    if entry.source_snapshot_id != manifest.source_snapshot_id
        || entry.source_table != manifest.source_table
    {
        bail!("anchor artifact entry source lineage mismatch at line {line_number}");
    }
    if entry.source_object_key != object.source_object_key {
        bail!("anchor artifact entry source object mismatch at line {line_number}");
    }
    validate_sha256(
        "source_geometry_checksum_sha256",
        entry.source_geometry_checksum_sha256.as_str(),
    )?;
    Ok(entry)
}

async fn insert_geometry_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    let result = conn
        .execute(
            "INSERT INTO parcel_marker_anchor_pbf_stage (
                 pnu,
                 anchor_point,
                 algorithm,
                 algorithm_version,
                 source_snapshot_id,
                 source_table,
                 source_row_id,
                 source_object_key,
                 source_geometry_checksum_sha256
             )
             SELECT
                 pnu,
                 ST_SetSRID(ST_MakePoint(anchor_lng, anchor_lat), 4326),
                 algorithm,
                 algorithm_version,
                 source_snapshot_id,
                 source_table,
                 source_row_id,
                 source_object_key,
                 source_geometry_checksum_sha256
             FROM parcel_marker_anchor_pbf_raw",
        )
        .await
        .context("failed to insert parcel marker anchor PBF geometry stage")?;
    Ok(result.rows_affected())
}

async fn index_stage_table(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS parcel_marker_anchor_pbf_stage_anchor_gix
         ON parcel_marker_anchor_pbf_stage USING GIST(anchor_point)",
    )
    .await
    .context("failed to index parcel marker anchor PBF stage")?;
    Ok(())
}

async fn list_tile_addresses(
    conn: &mut PgConnection,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<Vec<TileAddress>> {
    let rows = sqlx::query(tile_address_sql())
        .bind(i32::from(min_zoom))
        .bind(i32::from(max_zoom))
        .fetch_all(&mut *conn)
        .await
        .context("failed to list parcel marker anchor PBF tile addresses")?;

    rows.iter()
        .map(|row| {
            let z = i64_to_u8("z", row.try_get::<i64, _>("z")?)?;
            let x = i64_to_u32("x", row.try_get::<i64, _>("x")?)?;
            let y = i64_to_u32("y", row.try_get::<i64, _>("y")?)?;
            let feature_count =
                i64_to_u64("feature_count", row.try_get::<i64, _>("feature_count")?)?;
            Ok(TileAddress {
                z,
                x,
                y,
                feature_count,
            })
        })
        .collect()
}

const fn tile_address_sql() -> &'static str {
    "WITH zooms AS (
         SELECT generate_series($1::integer, $2::integer) AS z
     ),
     bucketed AS (
         -- anchor_point is EPSG:4326; tile address math consumes longitude/latitude degrees.
         SELECT
             z,
             floor(((ST_X(anchor_point) + 180.0) / 360.0) * power(2.0, z))::bigint AS raw_x,
             floor(
                 (
                     1.0 - ln(
                         tan(radians(least(85.05112878, greatest(-85.05112878, ST_Y(anchor_point)))))
                         + (1.0 / cos(radians(least(85.05112878, greatest(-85.05112878, ST_Y(anchor_point))))))
                     ) / pi()
                 ) / 2.0 * power(2.0, z)
             )::bigint AS raw_y,
             power(2.0, z)::bigint AS tiles_per_axis
         FROM parcel_marker_anchor_pbf_stage
         CROSS JOIN zooms
     )
     SELECT
         z::bigint AS z,
         least(tiles_per_axis - 1, greatest(0, raw_x))::bigint AS x,
         least(tiles_per_axis - 1, greatest(0, raw_y))::bigint AS y,
         count(*)::bigint AS feature_count
     FROM bucketed
     GROUP BY z, x, y
     ORDER BY z, x, y"
}

async fn render_tile(conn: &mut PgConnection, tile: TileAddress) -> anyhow::Result<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>(render_tile_sql())
        .bind(i32::from(tile.z))
        .bind(i32::try_from(tile.x).context("tile x overflow")?)
        .bind(i32::try_from(tile.y).context("tile y overflow")?)
        .bind(TILE_LAYER)
        .bind(TILE_EXTENT)
        .bind(TILE_BUFFER)
        .fetch_one(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "failed to render parcel marker anchor PBF tile z={} x={} y={}",
                tile.z, tile.x, tile.y
            )
        })
}

const fn render_tile_sql() -> &'static str {
    "WITH bounds AS (
         SELECT
             ST_TileEnvelope($1::integer, $2::integer, $3::integer) AS mercator_geom,
             ST_Transform(ST_TileEnvelope($1::integer, $2::integer, $3::integer), 4326)
                 AS wgs84_geom
     ),
     features AS (
         SELECT
             pnu AS id,
             pnu,
             $4::text AS kind,
             1::integer AS count,
             pnu AS detail_ref,
             algorithm,
             algorithm_version,
             source_snapshot_id,
             source_table,
             ST_AsMVTGeom(
                 ST_Transform(anchor_point, 3857),
                 bounds.mercator_geom,
                 $5::integer,
                 $6::integer,
                 true
             ) AS geom
         FROM parcel_marker_anchor_pbf_stage
         CROSS JOIN bounds
         WHERE ST_Intersects(anchor_point, bounds.wgs84_geom)
     )
    SELECT COALESCE(ST_AsMVT(features, $4::text, $5::integer, 'geom'), decode('', 'hex')) -- EPSG:3857 MVT geom
     FROM features"
}

fn build_tilejson(
    output_prefix: &str,
    artifact_version: &str,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<Vec<u8>> {
    validate_object_key_prefix(output_prefix)?;
    let tile_template = format!("{output_prefix}/{TILE_LAYER}/{{z}}/{{x}}/{{y}}.pbf");
    let value = serde_json::json!({
        "tilejson": "3.0.0",
        "name": "foundation-platform parcel anchor markers",
        "version": artifact_version,
        "scheme": "xyz",
        "tiles": [tile_template],
        "minzoom": min_zoom,
        "maxzoom": max_zoom,
        "format": "pbf",
        "vector_layers": [{
            "id": TILE_LAYER,
            "description": "PNU-backed parcel marker anchors",
            "fields": {
                "id": "String",
                "pnu": "String",
                "kind": "String",
                "count": "Number",
                "detail_ref": "String",
                "algorithm": "String",
                "algorithm_version": "String",
                "source_snapshot_id": "String",
                "source_table": "String"
            }
        }]
    });
    serde_json::to_vec_pretty(&value).context("failed to serialize TileJSON")
}

fn write_local_summary(path: &Path, report: &PbfArtifactBuildSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report)
        .context("failed to serialize parcel marker anchor PBF build summary")?;
    std::fs::write(path, payload).with_context(|| {
        format!(
            "failed to write parcel marker anchor PBF build summary {}",
            path.display()
        )
    })
}

fn push_anchor_copy_row(buffer: &mut Vec<u8>, entry: &AnchorArtifactEntry) {
    push_csv_field(buffer, entry.pnu.as_str());
    push_csv_field(buffer, &entry.anchor_lng.to_string());
    push_csv_field(buffer, &entry.anchor_lat.to_string());
    push_csv_field(buffer, entry.algorithm.as_str());
    push_csv_field(buffer, entry.algorithm_version.as_str());
    push_csv_field(buffer, entry.source_snapshot_id.as_str());
    push_csv_field(buffer, entry.source_table.as_str());
    push_csv_field(buffer, entry.source_row_id.as_str());
    push_csv_field(buffer, entry.source_object_key.as_str());
    push_csv_last_field(buffer, entry.source_geometry_checksum_sha256.as_str());
}

fn push_csv_field(buffer: &mut Vec<u8>, value: &str) {
    push_csv_value(buffer, value);
    buffer.push(b'\t');
}

fn push_csv_last_field(buffer: &mut Vec<u8>, value: &str) {
    push_csv_value(buffer, value);
    buffer.push(b'\n');
}

fn push_csv_value(buffer: &mut Vec<u8>, value: &str) {
    buffer.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' {
            buffer.push(b'"');
            buffer.push(b'"');
        } else {
            buffer.push(byte);
        }
    }
    buffer.push(b'"');
}

fn validate_zoom_range(min_zoom: u8, max_zoom: u8) -> anyhow::Result<()> {
    if min_zoom > max_zoom {
        bail!("min zoom must be less than or equal to max zoom");
    }
    Ok(())
}

fn parse_zoom(value: &str, label: &str) -> anyhow::Result<u8> {
    let parsed = value
        .parse::<u8>()
        .with_context(|| format!("{label} must be an integer"))?;
    if parsed > MAX_WEB_MERCATOR_ZOOM {
        bail!("{label} must be <= {MAX_WEB_MERCATOR_ZOOM}");
    }
    Ok(parsed)
}

fn validate_pnu(value: &str) -> anyhow::Result<()> {
    if value.len() == 19 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    bail!("pnu must be a 19-digit legal parcel identifier");
}

fn validate_lng_lat(lng: f64, lat: f64) -> anyhow::Result<()> {
    if !lng.is_finite() || !(-180.0..=180.0).contains(&lng) {
        bail!("anchor_lng must be finite and within EPSG:4326 longitude bounds");
    }
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        bail!("anchor_lat must be finite and within EPSG:4326 latitude bounds");
    }
    Ok(())
}

fn validate_source_snapshot_id(value: &str) -> anyhow::Result<()> {
    if value.len() < 3
        || value.len() > 256
        || value.trim() != value
        || value.contains('\\')
        || value.contains("..")
    {
        bail!("source_snapshot_id must be a stable non-path lineage identifier");
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> anyhow::Result<()> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(());
    }
    bail!("{field} must be a 64-character SHA-256 hex string");
}

fn validate_object_key_prefix(value: &str) -> anyhow::Result<()> {
    validate_object_key(value)?;
    if value.ends_with('/') {
        bail!("object key prefix must not end with slash");
    }
    if value == "gold" {
        bail!("object key prefix must be versioned below gold, not gold itself");
    }
    Ok(())
}

fn validate_object_key(value: &str) -> anyhow::Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("object key must not be empty");
    }
    if trimmed != value {
        bail!("object key must not contain leading or trailing whitespace");
    }
    if trimmed.starts_with('/') || trimmed.contains('\\') || trimmed.contains("..") {
        bail!("object key must be a safe provider-relative path");
    }
    if trimmed == "gold/manifest.json" {
        bail!("object key must not target the runtime gold/manifest.json pointer");
    }
    if trimmed
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        bail!("object key must not contain empty, '.', or '..' path segments");
    }
    Ok(())
}

fn parse_positive_u64(value: &str, label: &str) -> anyhow::Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{label} must be a positive integer"))?;
    if parsed == 0 {
        bail!("{label} must be greater than zero");
    }
    Ok(parsed)
}

fn i64_to_u8(field: &str, value: i64) -> anyhow::Result<u8> {
    u8::try_from(value).with_context(|| format!("{field} {value} cannot fit in u8"))
}

fn i64_to_u32(field: &str, value: i64) -> anyhow::Result<u32> {
    u32::try_from(value).with_context(|| format!("{field} {value} cannot fit in u32"))
}

fn i64_to_u64(field: &str, value: i64) -> anyhow::Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} {value} cannot fit in u64"))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
            hex
        })
}

#[cfg(test)]
mod tests;
