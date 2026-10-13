//! Builds low-zoom aggregate PBF marker tile artifacts from parcel marker anchor artifacts.
//!
//! Low zoom must not repeat every national PNU anchor. This builder precomputes one aggregate
//! feature per populated tile, preserving counts and source lineage while deliberately omitting
//! individual parcel identity.

use std::{
    collections::BTreeMap,
    f64::consts::PI,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection, Row};
use tokio::{sync::Semaphore, task::JoinSet};
use uuid::Uuid;

use crate::r2_layout::VECTOR_TILE_ARTIFACT_ROOT;

use super::{
    hex_lower, i64_to_u32, i64_to_u64, i64_to_u8, optional_env, parse_anchor_entry,
    parse_anchor_manifest, parse_positive_u64, parse_zoom, required_env, sha256_hex,
    validate_lng_lat, validate_object_key, validate_object_key_prefix, validate_zoom_range,
    AnchorArtifactInput, AnchorArtifactInputConfig, AnchorArtifactManifest, PbfArtifactOutput,
    PbfArtifactOutputConfig, JSON_CONTENT_TYPE, MANIFEST_CACHE_CONTROL, PBF_CONTENT_TYPE,
    TILE_BUFFER, TILE_CACHE_CONTROL, TILE_EXTENT,
};

const OUTPUT_MANIFEST_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_aggregate_pbf_artifact_manifest.v1";
const SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.parcel_marker_anchor_aggregate_pbf_artifact_build_summary.v1";
const AGGREGATE_TILE_LAYER: &str = "parcel_anchor_aggregate";
const DEFAULT_MIN_ZOOM: u8 = 0;
const DEFAULT_MAX_ZOOM: u8 = 11;
const MAX_AGGREGATE_ZOOM: u8 = 11;
const DEFAULT_MAX_INPUT_OBJECT_COUNT: u64 = 100;
const DEFAULT_MAX_INPUT_ROW_COUNT: u64 = 50_000_000;
const DEFAULT_INPUT_CONCURRENCY: usize = 4;
const MAX_INPUT_CONCURRENCY: usize = 16;

/// Runs the parcel marker anchor aggregate PBF artifact build.
pub async fn run() -> anyhow::Result<()> {
    let config = AggregatePbfArtifactBuildConfig::from_env()?;
    let input = Arc::new(AnchorArtifactInput::from_config(&config.input).await?);
    let manifest_bytes = input
        .read_object_bytes(config.input.manifest_object_key())
        .await
        .context("failed to read parcel marker anchor artifact manifest")?;
    let manifest = parse_anchor_manifest(&manifest_bytes)?;
    validate_aggregate_build_scope(&config, &manifest)?;
    let build_run_id = Uuid::now_v7();
    let artifact_version = build_run_id.to_string();
    let output = PbfArtifactOutput::from_config(&config.output, artifact_version.as_str()).await?;

    let mut conn = PgConnection::connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL for aggregate PBF artifact build")?;
    let report =
        execute_aggregate_build(&mut conn, input, &output, &config, &manifest, build_run_id)
            .await?;

    if let Some(summary_path) = &config.summary_path {
        write_aggregate_local_summary(summary_path, &report)?;
    }

    tracing::info!(
        build_run_id = %report.build_run_id,
        source_snapshot_id = %report.source_snapshot_id,
        manifest_object_key = %report.manifest_object_key,
        tile_count = report.tile_count,
        tile_total_bytes = report.tile_total_bytes,
        "parcel marker anchor aggregate PBF artifact build succeeded"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AggregatePbfArtifactBuildConfig {
    database_url: String,
    input: AnchorArtifactInputConfig,
    output: PbfArtifactOutputConfig,
    min_zoom: u8,
    max_zoom: u8,
    expected_anchor_row_count: Option<u64>,
    max_input_object_count: u64,
    max_input_row_count: u64,
    input_concurrency: usize,
    summary_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AggregateTileAddress {
    z: u8,
    x: u32,
    y: u32,
    source_anchor_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct AggregateTileKey {
    z: u8,
    x: u32,
    y: u32,
}

#[derive(Clone, Debug)]
struct AggregateAccumulator {
    source_anchor_count: u64,
    source_anchor_count_f64: f64,
    lng_sum: f64,
    lat_sum: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateStageRow {
    key: AggregateTileKey,
    source_anchor_count: u64,
    anchor_lng: f64,
    anchor_lat: f64,
}

#[derive(Clone, Debug)]
struct AggregateRows {
    rows: Vec<AggregateStageRow>,
    source_anchor_count: u64,
}

#[derive(Debug)]
struct ObjectAggregateRows {
    object_key: String,
    source_anchor_count: u64,
    accumulators: BTreeMap<AggregateTileKey, AggregateAccumulator>,
}

#[derive(Debug)]
struct AggregateTileBuildResult {
    tiles: Vec<AggregatePbfTileObject>,
    tile_count: u64,
    tile_total_bytes: u64,
    manifest_digest: Sha256,
}

#[derive(Clone, Debug, Serialize)]
struct AggregatePbfArtifactManifest {
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
    aggregate_feature_count: u64,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    checksum_sha256: String,
    tiles: Vec<AggregatePbfTileObject>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AggregatePbfTileObject {
    z: u8,
    x: u32,
    y: u32,
    object_key: String,
    feature_count: u64,
    source_anchor_count: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Debug, Serialize)]
struct AggregatePbfArtifactBuildSummary {
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
    aggregate_feature_count: u64,
    tile_count: u64,
    tile_total_bytes: u64,
    tilejson_object_key: String,
    manifest_object_key: String,
    checksum_sha256: String,
}

struct AggregateOutputManifestWrite<'a> {
    config: &'a AggregatePbfArtifactBuildConfig,
    manifest: &'a AnchorArtifactManifest,
    build_run_id: Uuid,
    tilejson_object_key: &'a str,
    checksum_sha256: &'a str,
    tile_count: u64,
    tile_total_bytes: u64,
    tiles: Vec<AggregatePbfTileObject>,
}

impl AggregatePbfArtifactBuildConfig {
    fn from_env() -> anyhow::Result<Self> {
        let confirm =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_CONFIRM_BUILD")?
                .unwrap_or_default();
        if !confirm.eq_ignore_ascii_case("true") {
            bail!(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_CONFIRM_BUILD must be true"
            );
        }

        let min_zoom =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_MIN_ZOOM")?
                .map(|value| parse_zoom(&value, "aggregate min zoom"))
                .transpose()?
                .unwrap_or(DEFAULT_MIN_ZOOM);
        let max_zoom =
            optional_env("FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_MAX_ZOOM")?
                .map(|value| parse_zoom(&value, "aggregate max zoom"))
                .transpose()?
                .unwrap_or(DEFAULT_MAX_ZOOM);
        validate_zoom_range(min_zoom, max_zoom)?;

        Ok(Self {
            database_url: required_env("DATABASE_URL")?,
            input: aggregate_input_config_from_env()?,
            output: aggregate_output_config_from_env()?,
            min_zoom,
            max_zoom,
            expected_anchor_row_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_EXPECTED_ANCHOR_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "aggregate expected anchor row count"))
            .transpose()?,
            max_input_object_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_MAX_INPUT_OBJECT_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "aggregate max input object count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_INPUT_OBJECT_COUNT),
            max_input_row_count: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_MAX_INPUT_ROW_COUNT",
            )?
            .map(|value| parse_positive_u64(&value, "aggregate max input row count"))
            .transpose()?
            .unwrap_or(DEFAULT_MAX_INPUT_ROW_COUNT),
            input_concurrency: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_CONCURRENCY",
            )?
            .map(|value| parse_input_concurrency(&value))
            .transpose()?
            .unwrap_or(DEFAULT_INPUT_CONCURRENCY),
            summary_path: optional_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_SUMMARY_PATH",
            )?
            .map(PathBuf::from),
        })
    }
}

fn aggregate_input_config_from_env() -> anyhow::Result<AnchorArtifactInputConfig> {
    let driver = optional_env(
        "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_STORAGE_DRIVER",
    )?
    .unwrap_or_else(|| "local".to_owned())
    .to_ascii_lowercase();
    let manifest_key = required_env(
        "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_MANIFEST_OBJECT_KEY",
    )?;
    validate_object_key(manifest_key.as_str())?;

    match driver.as_str() {
        "local" => Ok(AnchorArtifactInputConfig::Local {
            root: PathBuf::from(required_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_ROOT",
            )?),
            manifest_key,
        }),
        "r2" => Ok(AnchorArtifactInputConfig::R2 { manifest_key }),
        "" => bail!(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_STORAGE_DRIVER must not be empty"
        ),
        other => bail!(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_INPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
        ),
    }
}

fn aggregate_output_config_from_env() -> anyhow::Result<PbfArtifactOutputConfig> {
    let driver = optional_env(
        "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_STORAGE_DRIVER",
    )?
    .unwrap_or_else(|| "local".to_owned())
    .to_ascii_lowercase();
    let prefix = required_env(
        "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_OBJECT_PREFIX",
    )?;
    if prefix != VECTOR_TILE_ARTIFACT_ROOT {
        bail!(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_OBJECT_PREFIX must be {VECTOR_TILE_ARTIFACT_ROOT}"
        );
    }

    match driver.as_str() {
        "local" => Ok(PbfArtifactOutputConfig::Local {
            root: PathBuf::from(required_env(
                "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_ROOT",
            )?),
            prefix,
        }),
        "r2" => Ok(PbfArtifactOutputConfig::R2 { prefix }),
        "" => bail!(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_STORAGE_DRIVER must not be empty"
        ),
        other => bail!(
            "FOUNDATION_PLATFORM_PARCEL_MARKER_ANCHOR_AGGREGATE_PBF_OUTPUT_STORAGE_DRIVER must be 'local' or 'r2', got '{other}'"
        ),
    }
}

fn validate_aggregate_build_scope(
    config: &AggregatePbfArtifactBuildConfig,
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<()> {
    validate_zoom_range(config.min_zoom, config.max_zoom)?;
    if config.max_zoom > MAX_AGGREGATE_ZOOM {
        bail!("aggregate PBF max zoom must be <= {MAX_AGGREGATE_ZOOM}");
    }
    if let Some(expected) = config.expected_anchor_row_count {
        if expected != manifest.artifact_row_count {
            bail!(
                "configured aggregate expected anchor row count {expected} does not match manifest {}",
                manifest.artifact_row_count
            );
        }
    }
    if manifest.artifact_object_count > config.max_input_object_count {
        bail!(
            "anchor artifact object count {} exceeds aggregate configured max {}",
            manifest.artifact_object_count,
            config.max_input_object_count
        );
    }
    if manifest.artifact_row_count > config.max_input_row_count {
        bail!(
            "anchor artifact row count {} exceeds aggregate configured max {}",
            manifest.artifact_row_count,
            config.max_input_row_count
        );
    }
    Ok(())
}

fn parse_input_concurrency(value: &str) -> anyhow::Result<usize> {
    let parsed = parse_positive_u64(value, "aggregate input concurrency")?;
    let parsed =
        usize::try_from(parsed).context("aggregate input concurrency cannot fit in usize")?;
    if parsed > MAX_INPUT_CONCURRENCY {
        bail!("aggregate input concurrency must be <= {MAX_INPUT_CONCURRENCY}");
    }
    Ok(parsed)
}

async fn execute_aggregate_build(
    conn: &mut PgConnection,
    input: Arc<AnchorArtifactInput>,
    output: &PbfArtifactOutput,
    config: &AggregatePbfArtifactBuildConfig,
    manifest: &AnchorArtifactManifest,
    build_run_id: Uuid,
) -> anyhow::Result<AggregatePbfArtifactBuildSummary> {
    let loaded_row_count = load_aggregate_stage_for_build(conn, input, manifest, config).await?;
    if loaded_row_count != manifest.artifact_row_count {
        bail!(
            "loaded anchor row count mismatch: expected={} actual={loaded_row_count}",
            manifest.artifact_row_count
        );
    }

    let mut tile_build = build_aggregate_tile_objects(conn, output).await?;
    let artifact_version = build_run_id.to_string();
    let tilejson_object_key = write_aggregate_tilejson_object(
        output,
        config.min_zoom,
        config.max_zoom,
        artifact_version.as_str(),
        &mut tile_build,
    )
    .await?;
    let tile_count = tile_build.tile_count;
    let tile_total_bytes = tile_build.tile_total_bytes;
    let tiles = std::mem::take(&mut tile_build.tiles);
    let checksum_sha256 = hex_lower(&tile_build.manifest_digest.finalize());
    let manifest_object_key = write_aggregate_output_manifest(
        output,
        AggregateOutputManifestWrite {
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

    Ok(AggregatePbfArtifactBuildSummary {
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
        aggregate_feature_count: tile_count,
        tile_count,
        tile_total_bytes,
        tilejson_object_key,
        manifest_object_key,
        checksum_sha256,
    })
}

async fn load_aggregate_stage_for_build(
    conn: &mut PgConnection,
    input: Arc<AnchorArtifactInput>,
    manifest: &AnchorArtifactManifest,
    config: &AggregatePbfArtifactBuildConfig,
) -> anyhow::Result<u64> {
    create_aggregate_stage_tables(conn).await?;
    let aggregate_rows = build_aggregate_rows(input, manifest, config).await?;
    copy_aggregate_rows_to_raw_stage(conn, &aggregate_rows.rows, manifest).await?;
    let inserted = insert_aggregate_geometry_stage(conn).await?;
    let expected_inserted =
        u64::try_from(aggregate_rows.rows.len()).context("aggregate row count overflow")?;
    if inserted != expected_inserted {
        bail!("aggregate geometry stage insert mismatch: raw={expected_inserted} stage={inserted}");
    }
    index_aggregate_stage_table(conn).await?;
    Ok(aggregate_rows.source_anchor_count)
}

async fn create_aggregate_stage_tables(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_aggregate_pbf_raw (
             z integer NOT NULL,
             x integer NOT NULL,
             y integer NOT NULL,
             source_anchor_count bigint NOT NULL,
             anchor_lng double precision NOT NULL,
             anchor_lat double precision NOT NULL,
             algorithm text NOT NULL,
             algorithm_version text NOT NULL,
             source_snapshot_id text NOT NULL,
             source_table text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor aggregate PBF raw stage")?;
    conn.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS parcel_marker_anchor_aggregate_pbf_stage (
             z integer NOT NULL,
             x integer NOT NULL,
             y integer NOT NULL,
             source_anchor_count bigint NOT NULL,
             anchor_point geometry(Point, 4326) NOT NULL,
             algorithm text NOT NULL,
             algorithm_version text NOT NULL,
             source_snapshot_id text NOT NULL,
             source_table text NOT NULL
         ) ON COMMIT PRESERVE ROWS",
    )
    .await
    .context("failed to create parcel marker anchor aggregate PBF stage")?;
    conn.execute(
        "TRUNCATE TABLE
             parcel_marker_anchor_aggregate_pbf_raw,
             parcel_marker_anchor_aggregate_pbf_stage",
    )
    .await
    .context("failed to truncate parcel marker anchor aggregate PBF stages")?;
    Ok(())
}

async fn build_aggregate_rows(
    input: Arc<AnchorArtifactInput>,
    manifest: &AnchorArtifactManifest,
    config: &AggregatePbfArtifactBuildConfig,
) -> anyhow::Result<AggregateRows> {
    let mut accumulators = BTreeMap::<AggregateTileKey, AggregateAccumulator>::new();
    let mut total_rows = 0_u64;
    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(config.input_concurrency));

    for object in &manifest.objects {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("aggregate input concurrency semaphore closed")?;
        let input = Arc::clone(&input);
        let manifest = manifest.clone();
        let object = object.clone();
        let min_zoom = config.min_zoom;
        let max_zoom = config.max_zoom;
        join_set.spawn(async move {
            let _permit = permit;
            build_object_aggregate_rows(input, manifest, object, min_zoom, max_zoom).await
        });
    }

    let object_count = manifest.objects.len();
    let mut completed_object_count = 0_usize;
    while let Some(result) = join_set.join_next().await {
        let object_result = result
            .context("aggregate object task panicked")?
            .context("aggregate object task failed")?;
        total_rows = total_rows
            .checked_add(object_result.source_anchor_count)
            .context("anchor artifact source row count overflow")?;
        merge_aggregate_accumulators(&mut accumulators, object_result.accumulators)?;
        completed_object_count += 1;
        tracing::info!(
            completed_object_count,
            object_count,
            object_key = %object_result.object_key,
            object_source_anchor_count = object_result.source_anchor_count,
            aggregate_tile_key_count = accumulators.len(),
            "parcel marker anchor aggregate object processed"
        );
    }

    let rows = accumulators
        .into_iter()
        .map(|(key, accumulator)| AggregateStageRow {
            key,
            source_anchor_count: accumulator.source_anchor_count,
            anchor_lng: accumulator.lng_sum / accumulator.source_anchor_count_f64,
            anchor_lat: accumulator.lat_sum / accumulator.source_anchor_count_f64,
        })
        .collect();

    Ok(AggregateRows {
        rows,
        source_anchor_count: total_rows,
    })
}

async fn build_object_aggregate_rows(
    input: Arc<AnchorArtifactInput>,
    manifest: AnchorArtifactManifest,
    object: super::AnchorArtifactObject,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<ObjectAggregateRows> {
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

    let mut accumulators = BTreeMap::<AggregateTileKey, AggregateAccumulator>::new();
    let mut object_rows = 0_u64;
    for (index, raw_line) in object_bytes.split(|byte| *byte == b'\n').enumerate() {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }
        let line_number = u64::try_from(index + 1).context("anchor artifact line overflow")?;
        let entry = parse_anchor_entry(line, &object, &manifest, line_number)?;
        accumulate_anchor(
            &mut accumulators,
            entry.anchor_lng,
            entry.anchor_lat,
            min_zoom,
            max_zoom,
        )?;
        object_rows = object_rows
            .checked_add(1)
            .context("anchor object row count overflow")?;
    }
    if object_rows != object.row_count {
        bail!(
            "anchor artifact object row count mismatch for {}: expected={} actual={object_rows}",
            object.artifact_object_key,
            object.row_count
        );
    }

    Ok(ObjectAggregateRows {
        object_key: object.artifact_object_key,
        source_anchor_count: object_rows,
        accumulators,
    })
}

fn merge_aggregate_accumulators(
    target: &mut BTreeMap<AggregateTileKey, AggregateAccumulator>,
    source: BTreeMap<AggregateTileKey, AggregateAccumulator>,
) -> anyhow::Result<()> {
    for (key, source_accumulator) in source {
        let target_accumulator = target.entry(key).or_insert(AggregateAccumulator {
            source_anchor_count: 0,
            source_anchor_count_f64: 0.0,
            lng_sum: 0.0,
            lat_sum: 0.0,
        });
        target_accumulator.source_anchor_count = target_accumulator
            .source_anchor_count
            .checked_add(source_accumulator.source_anchor_count)
            .context("aggregate merged source anchor count overflow")?;
        target_accumulator.source_anchor_count_f64 += source_accumulator.source_anchor_count_f64;
        target_accumulator.lng_sum += source_accumulator.lng_sum;
        target_accumulator.lat_sum += source_accumulator.lat_sum;
    }
    Ok(())
}

fn accumulate_anchor(
    accumulators: &mut BTreeMap<AggregateTileKey, AggregateAccumulator>,
    anchor_lng: f64,
    anchor_lat: f64,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<()> {
    for key in aggregate_tile_keys(anchor_lng, anchor_lat, min_zoom, max_zoom)? {
        let accumulator = accumulators.entry(key).or_insert(AggregateAccumulator {
            source_anchor_count: 0,
            source_anchor_count_f64: 0.0,
            lng_sum: 0.0,
            lat_sum: 0.0,
        });
        accumulator.source_anchor_count = accumulator
            .source_anchor_count
            .checked_add(1)
            .context("aggregate source anchor count overflow")?;
        accumulator.source_anchor_count_f64 += 1.0;
        accumulator.lng_sum += anchor_lng;
        accumulator.lat_sum += anchor_lat;
    }
    Ok(())
}

fn aggregate_tile_keys(
    anchor_lng: f64,
    anchor_lat: f64,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<Vec<AggregateTileKey>> {
    validate_zoom_range(min_zoom, max_zoom)?;
    validate_lng_lat(anchor_lng, anchor_lat)?;
    let max_key = aggregate_tile_key_at_zoom(anchor_lng, anchor_lat, max_zoom);
    let mut keys = Vec::with_capacity(usize::from(max_zoom - min_zoom) + 1);
    for z in min_zoom..=max_zoom {
        let shift = u32::from(max_zoom - z);
        keys.push(AggregateTileKey {
            z,
            x: max_key.x >> shift,
            y: max_key.y >> shift,
        });
    }
    Ok(keys)
}

fn aggregate_tile_key_at_zoom(anchor_lng: f64, anchor_lat: f64, zoom: u8) -> AggregateTileKey {
    let tiles_per_axis = 2_u32.pow(u32::from(zoom));
    let max_index = tiles_per_axis.saturating_sub(1);
    let clamped_lat = anchor_lat.clamp(-85.051_128_78, 85.051_128_78);
    let lat_rad = clamped_lat.to_radians();
    let n = f64::from(tiles_per_axis);
    let raw_x = (((anchor_lng + 180.0) / 360.0) * n).floor();
    let raw_y = ((1.0 - (lat_rad.tan() + (1.0 / lat_rad.cos())).ln() / PI) / 2.0 * n).floor();
    AggregateTileKey {
        z: zoom,
        x: clamp_tile_index(raw_x, max_index),
        y: clamp_tile_index(raw_y, max_index),
    }
}

fn clamp_tile_index(value: f64, max_index: u32) -> u32 {
    if !value.is_finite() || value < 0.0 {
        0
    } else if value > f64::from(max_index) {
        max_index
    } else {
        finite_nonnegative_f64_to_u32(value)
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn finite_nonnegative_f64_to_u32(value: f64) -> u32 {
    debug_assert!(value.is_finite());
    debug_assert!(value >= 0.0);
    debug_assert!(value <= f64::from(u32::MAX));
    value as u32
}

async fn copy_aggregate_rows_to_raw_stage(
    conn: &mut PgConnection,
    rows: &[AggregateStageRow],
    manifest: &AnchorArtifactManifest,
) -> anyhow::Result<u64> {
    let mut copy = conn
        .copy_in_raw(
            "COPY parcel_marker_anchor_aggregate_pbf_raw
             (z, x, y, source_anchor_count, anchor_lng, anchor_lat, algorithm,
              algorithm_version, source_snapshot_id, source_table)
             FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', QUOTE '\"', ESCAPE '\"', NULL '\\N')",
        )
        .await
        .context("failed to start COPY into parcel marker anchor aggregate PBF raw stage")?;
    let mut buffer = Vec::new();

    for row in rows {
        push_aggregate_copy_row(&mut buffer, row, manifest);
        if buffer.len() >= 8 * 1024 * 1024 {
            copy.send(buffer.as_slice())
                .await
                .context("aggregate COPY send failed")?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        copy.send(buffer.as_slice())
            .await
            .context("aggregate COPY send failed")?;
    }
    let copied = copy
        .finish()
        .await
        .context("aggregate COPY finish failed")?;
    let expected = u64::try_from(rows.len()).context("aggregate raw row count overflow")?;
    if copied != expected {
        bail!("aggregate COPY reported {copied} rows but builder produced {expected} rows");
    }
    Ok(copied)
}

fn push_aggregate_copy_row(
    buffer: &mut Vec<u8>,
    row: &AggregateStageRow,
    manifest: &AnchorArtifactManifest,
) {
    push_aggregate_csv_field(buffer, &row.key.z.to_string());
    push_aggregate_csv_field(buffer, &row.key.x.to_string());
    push_aggregate_csv_field(buffer, &row.key.y.to_string());
    push_aggregate_csv_field(buffer, &row.source_anchor_count.to_string());
    push_aggregate_csv_field(buffer, &row.anchor_lng.to_string());
    push_aggregate_csv_field(buffer, &row.anchor_lat.to_string());
    push_aggregate_csv_field(buffer, manifest.algorithm.as_str());
    push_aggregate_csv_field(buffer, manifest.algorithm_version.as_str());
    push_aggregate_csv_field(buffer, manifest.source_snapshot_id.as_str());
    push_aggregate_csv_last_field(buffer, manifest.source_table.as_str());
}

fn push_aggregate_csv_field(buffer: &mut Vec<u8>, value: &str) {
    push_aggregate_csv_value(buffer, value);
    buffer.push(b'\t');
}

fn push_aggregate_csv_last_field(buffer: &mut Vec<u8>, value: &str) {
    push_aggregate_csv_value(buffer, value);
    buffer.push(b'\n');
}

fn push_aggregate_csv_value(buffer: &mut Vec<u8>, value: &str) {
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

async fn insert_aggregate_geometry_stage(conn: &mut PgConnection) -> anyhow::Result<u64> {
    let result = sqlx::query(aggregate_stage_insert_sql())
        .execute(&mut *conn)
        .await
        .context("failed to populate parcel marker anchor aggregate PBF stage")?;
    Ok(result.rows_affected())
}

async fn index_aggregate_stage_table(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS parcel_marker_anchor_aggregate_pbf_stage_tile_uidx
         ON parcel_marker_anchor_aggregate_pbf_stage (z, x, y)",
    )
    .await
    .context("failed to index parcel marker anchor aggregate PBF stage")?;
    Ok(())
}

const fn aggregate_stage_insert_sql() -> &'static str {
    "INSERT INTO parcel_marker_anchor_aggregate_pbf_stage (
         z,
         x,
         y,
         source_anchor_count,
         anchor_point,
         algorithm,
         algorithm_version,
         source_snapshot_id,
         source_table
     )
     SELECT
             z,
             x,
             y,
             source_anchor_count,
             ST_SetSRID(ST_MakePoint(anchor_lng, anchor_lat), 4326),
             algorithm,
             algorithm_version,
             source_snapshot_id,
             source_table
     FROM parcel_marker_anchor_aggregate_pbf_raw
     ORDER BY z, x, y"
}

async fn build_aggregate_tile_objects(
    conn: &mut PgConnection,
    output: &PbfArtifactOutput,
) -> anyhow::Result<AggregateTileBuildResult> {
    let tile_addresses = list_aggregate_tile_addresses(conn).await?;
    let mut tile_objects = Vec::with_capacity(tile_addresses.len());
    let mut tile_total_bytes = 0_u64;
    let mut manifest_digest = Sha256::new();

    for tile in tile_addresses {
        let tile_body = render_aggregate_tile(conn, tile).await?;
        let checksum_sha256 = sha256_hex(&tile_body);
        let size_bytes = u64::try_from(tile_body.len()).context("tile byte size overflow")?;
        let object_key = aggregate_tile_object_key(output, tile)?;
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
            .context("aggregate tile total byte size overflow")?;
        tile_objects.push(AggregatePbfTileObject {
            z: tile.z,
            x: tile.x,
            y: tile.y,
            object_key,
            feature_count: 1,
            source_anchor_count: tile.source_anchor_count,
            size_bytes,
            checksum_sha256,
        });
    }

    let tile_count = u64::try_from(tile_objects.len()).context("aggregate tile count overflow")?;
    Ok(AggregateTileBuildResult {
        tiles: tile_objects,
        tile_count,
        tile_total_bytes,
        manifest_digest,
    })
}

async fn list_aggregate_tile_addresses(
    conn: &mut PgConnection,
) -> anyhow::Result<Vec<AggregateTileAddress>> {
    let rows = sqlx::query(
        "SELECT
             z::bigint AS z,
             x::bigint AS x,
             y::bigint AS y,
             source_anchor_count::bigint AS source_anchor_count
         FROM parcel_marker_anchor_aggregate_pbf_stage
         ORDER BY z, x, y",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to list parcel marker anchor aggregate PBF tile addresses")?;

    rows.iter()
        .map(|row| {
            Ok(AggregateTileAddress {
                z: i64_to_u8("z", row.try_get::<i64, _>("z")?)?,
                x: i64_to_u32("x", row.try_get::<i64, _>("x")?)?,
                y: i64_to_u32("y", row.try_get::<i64, _>("y")?)?,
                source_anchor_count: i64_to_u64(
                    "source_anchor_count",
                    row.try_get::<i64, _>("source_anchor_count")?,
                )?,
            })
        })
        .collect()
}

async fn render_aggregate_tile(
    conn: &mut PgConnection,
    tile: AggregateTileAddress,
) -> anyhow::Result<Vec<u8>> {
    sqlx::query_scalar::<_, Vec<u8>>(render_aggregate_tile_sql())
        .bind(i32::from(tile.z))
        .bind(i32::try_from(tile.x).context("tile x overflow")?)
        .bind(i32::try_from(tile.y).context("tile y overflow")?)
        .bind(AGGREGATE_TILE_LAYER)
        .bind(TILE_EXTENT)
        .bind(TILE_BUFFER)
        .fetch_one(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "failed to render parcel marker anchor aggregate PBF tile z={} x={} y={}",
                tile.z, tile.x, tile.y
            )
        })
}

const fn render_aggregate_tile_sql() -> &'static str {
    "WITH bounds AS (
         SELECT ST_TileEnvelope($1::integer, $2::integer, $3::integer) AS mercator_geom
     ),
     features AS (
         SELECT
             format('agg_%s_%s_%s', z, x, y) AS id,
             $4::text AS kind,
             source_anchor_count AS count,
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
         FROM parcel_marker_anchor_aggregate_pbf_stage
         CROSS JOIN bounds
         WHERE z = $1::integer
           AND x = $2::integer
           AND y = $3::integer
     )
    SELECT COALESCE(ST_AsMVT(features, $4::text, $5::integer, 'geom'), decode('', 'hex')) -- EPSG:3857 MVT geom
     FROM features"
}

async fn write_aggregate_tilejson_object(
    output: &PbfArtifactOutput,
    min_zoom: u8,
    max_zoom: u8,
    artifact_version: &str,
    tile_build: &mut AggregateTileBuildResult,
) -> anyhow::Result<String> {
    let tilejson_object_key = output.tilejson_object_key()?;
    let tilejson_body =
        build_aggregate_tilejson(output.prefix(), artifact_version, min_zoom, max_zoom)?;
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

fn build_aggregate_tilejson(
    output_prefix: &str,
    artifact_version: &str,
    min_zoom: u8,
    max_zoom: u8,
) -> anyhow::Result<Vec<u8>> {
    validate_object_key_prefix(output_prefix)?;
    let tile_template = format!("{output_prefix}/{AGGREGATE_TILE_LAYER}/{{z}}/{{x}}/{{y}}.pbf");
    let value = serde_json::json!({
        "tilejson": "3.0.0",
        "name": "foundation-platform parcel anchor aggregate markers",
        "version": artifact_version,
        "scheme": "xyz",
        "tiles": [tile_template],
        "minzoom": min_zoom,
        "maxzoom": max_zoom,
        "format": "pbf",
        "vector_layers": [{
            "id": AGGREGATE_TILE_LAYER,
            "description": "Aggregated low-zoom PNU-backed parcel marker anchor counts",
            "fields": {
                "id": "String",
                "kind": "String",
                "count": "Number",
                "algorithm": "String",
                "algorithm_version": "String",
                "source_snapshot_id": "String",
                "source_table": "String"
            }
        }]
    });
    serde_json::to_vec_pretty(&value).context("failed to serialize aggregate TileJSON")
}

async fn write_aggregate_output_manifest(
    output: &PbfArtifactOutput,
    input: AggregateOutputManifestWrite<'_>,
) -> anyhow::Result<String> {
    let output_manifest = AggregatePbfArtifactManifest {
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
        layer: AGGREGATE_TILE_LAYER,
        min_zoom: input.config.min_zoom,
        max_zoom: input.config.max_zoom,
        aggregate_feature_count: input.tile_count,
        tile_count: input.tile_count,
        tile_total_bytes: input.tile_total_bytes,
        tilejson_object_key: input.tilejson_object_key.to_owned(),
        checksum_sha256: input.checksum_sha256.to_owned(),
        tiles: input.tiles,
    };
    let manifest_object_key = output.manifest_object_key()?;
    let manifest_body = serde_json::to_vec_pretty(&output_manifest)
        .context("failed to serialize aggregate PBF artifact manifest")?;
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

fn aggregate_tile_object_key(
    output: &PbfArtifactOutput,
    tile: AggregateTileAddress,
) -> anyhow::Result<String> {
    let key = format!(
        "{}/{}/{}/{}/{}.pbf",
        output.prefix(),
        AGGREGATE_TILE_LAYER,
        tile.z,
        tile.x,
        tile.y
    );
    validate_object_key(key.as_str())?;
    Ok(key)
}

fn write_aggregate_local_summary(
    path: &Path,
    report: &AggregatePbfArtifactBuildSummary,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(report)
        .context("failed to serialize parcel marker anchor aggregate PBF build summary")?;
    std::fs::write(path, payload).with_context(|| {
        format!(
            "failed to write parcel marker anchor aggregate PBF build summary {}",
            path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_stage_insert_sql, aggregate_tile_keys, build_aggregate_tilejson,
        merge_aggregate_accumulators, render_aggregate_tile_sql, validate_aggregate_build_scope,
        AggregateAccumulator, AggregatePbfArtifactBuildConfig, AggregateTileKey,
    };
    use crate::parcel_marker_anchor_pbf_artifact_build::{
        AnchorArtifactInputConfig, AnchorArtifactManifest, PbfArtifactOutputConfig,
    };

    #[test]
    fn national_aggregate_tiles_are_limited_to_low_zoom_range() -> anyhow::Result<()> {
        let manifest = national_anchor_manifest();
        let config = AggregatePbfArtifactBuildConfig {
            database_url: "postgres://example".to_owned(),
            input: AnchorArtifactInputConfig::R2 {
                manifest_key: "gold/parcel-marker-anchors/artifacts/018f0000-0000-7000-8000-000000000001/manifest.json".to_owned(),
            },
            output: PbfArtifactOutputConfig::R2 {
                prefix: "gold/vector-tiles/artifacts".to_owned(),
            },
            min_zoom: 0,
            max_zoom: 12,
            expected_anchor_row_count: Some(39_862_470),
            max_input_object_count: 100,
            max_input_row_count: 40_000_000,
            input_concurrency: 4,
            summary_path: None,
        };

        let error = validate_aggregate_build_scope(&config, &manifest)
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected aggregate max zoom rejection"))?;
        assert!(error.to_string().contains("max zoom must be <= 11"));
        Ok(())
    }

    #[test]
    fn aggregate_tilejson_exposes_count_lineage_without_pnu_identity() -> anyhow::Result<()> {
        let body = build_aggregate_tilejson(
            "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001",
            "018f0000-0000-7000-8000-000000000001",
            0,
            11,
        )?;
        let json: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(json["format"].as_str(), Some("pbf"));
        assert_eq!(
            json["vector_layers"][0]["id"].as_str(),
            Some("parcel_anchor_aggregate")
        );
        assert_eq!(
            json["tiles"][0].as_str(),
            Some(
                "gold/vector-tiles/artifacts/018f0000-0000-7000-8000-000000000001/parcel_anchor_aggregate/{z}/{x}/{y}.pbf"
            )
        );
        let fields = json["vector_layers"][0]["fields"]
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("expected TileJSON fields object"))?;
        assert!(fields.contains_key("count"));
        assert!(fields.contains_key("source_snapshot_id"));
        assert!(!fields.contains_key("pnu"));
        assert!(!fields.contains_key("detail_ref"));
        Ok(())
    }

    #[test]
    fn aggregate_tile_keys_are_derived_from_one_max_zoom_projection() -> anyhow::Result<()> {
        let keys = aggregate_tile_keys(127.123_470_234_50, 36.123_456, 9, 11)?;
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0].z, 9);
        assert_eq!(keys[1].z, 10);
        assert_eq!(keys[2].z, 11);
        assert_eq!(keys[1].x, keys[2].x >> 1);
        assert_eq!(keys[1].y, keys[2].y >> 1);
        assert_eq!(keys[0].x, keys[2].x >> 2);
        assert_eq!(keys[0].y, keys[2].y >> 2);
        Ok(())
    }

    #[test]
    fn aggregate_object_results_merge_without_losing_counts_or_centroids() -> anyhow::Result<()> {
        let key = AggregateTileKey {
            z: 11,
            x: 1746,
            y: 794,
        };
        let mut target = std::collections::BTreeMap::from([(
            key,
            AggregateAccumulator {
                source_anchor_count: 2,
                source_anchor_count_f64: 2.0,
                lng_sum: 200.0,
                lat_sum: 70.0,
            },
        )]);
        let source = std::collections::BTreeMap::from([(
            key,
            AggregateAccumulator {
                source_anchor_count: 3,
                source_anchor_count_f64: 3.0,
                lng_sum: 390.0,
                lat_sum: 120.0,
            },
        )]);

        merge_aggregate_accumulators(&mut target, source)?;

        let merged = target
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("expected merged aggregate key"))?;
        assert_eq!(merged.source_anchor_count, 5);
        assert!((merged.source_anchor_count_f64 - 5.0).abs() < f64::EPSILON);
        assert!((merged.lng_sum - 590.0).abs() < f64::EPSILON);
        assert!((merged.lat_sum - 190.0).abs() < f64::EPSILON);
        Ok(())
    }

    #[test]
    fn aggregate_stage_sql_uses_streamed_rows_instead_of_exact_point_stage() {
        let sql = aggregate_stage_insert_sql();
        assert!(sql.contains("parcel_marker_anchor_aggregate_pbf_raw"));
        assert!(sql.contains("ST_MakePoint"));
        assert!(!sql.contains("parcel_marker_anchor_pbf_stage"));
        assert!(!sql.contains("ST_Centroid"));
        assert!(!sql.contains("count(*)"));
        assert!(!sql.contains("detail_ref"));
        assert!(!sql.contains("pnu"));
        assert!(!sql.contains("catalog.parcel_marker_anchor"));
    }

    #[test]
    fn aggregate_tile_sql_never_emits_individual_parcel_identity() {
        let sql = render_aggregate_tile_sql();
        assert!(sql.contains("parcel_marker_anchor_aggregate_pbf_stage"));
        assert!(sql.contains("ST_AsMVTGeom"));
        assert!(sql.contains("ST_AsMVT"));
        assert!(!sql.contains("detail_ref"));
        assert!(!sql.contains("pnu"));
        assert!(!sql.contains("catalog.parcel_marker_anchor"));
    }

    fn national_anchor_manifest() -> AnchorArtifactManifest {
        AnchorArtifactManifest {
            schema_version: "foundation-platform.parcel_marker_anchor_artifact_manifest.v1"
                .to_owned(),
            artifact_version: "018f0000-0000-7000-8000-000000000001".to_owned(),
            source_snapshot_id: "national-promotion:silver-parcel-boundaries-vworld".to_owned(),
            source_table: "silver.parcel_boundaries".to_owned(),
            anchor_srid: "EPSG:4326".to_owned(),
            algorithm: "polylabel".to_owned(),
            algorithm_version: "postgis-st_maximuminscribedcircle-v1".to_owned(),
            source_row_count: 39_862_472,
            artifact_object_count: 85,
            artifact_row_count: 39_862_470,
            rejected_object_count: 2,
            rejected_row_count: 2,
            checksum_sha256: "a".repeat(64),
            objects: Vec::new(),
            rejected_objects: Vec::new(),
        }
    }
}
