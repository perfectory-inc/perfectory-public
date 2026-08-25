//! Publishes the active industrial-complex boundary source as one immutable static release.
//!
//! ADR-0053 fixes the order: build from the active dynamic source, validate both archive formats,
//! create the derivative object once, rehash every stored byte, prove Martin can decode that exact
//! release-addressed source, then and only then record and promote the build.

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::Output,
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure, Context as _};
use async_trait::async_trait;
use catalog_application::{
    ports::{
        PromoteTileLayerStaticCommand, RecordVectorTileBuildResultCommand,
        RuntimeManifestPublicationCapability, StartVectorTileBuildCommand,
    },
    PromoteTileLayerStatic, VectorTileBuildLifecycle,
};
use catalog_domain::{
    static_file_asset_id_for_build, static_release_id_for_build, static_release_martin_source_id,
    static_release_pmtiles_object_key, BuildEvidenceDigest, CanonicalIcebergSnapshotId,
    PmtilesChecksum, RuntimeTilesUrlTemplate, ServingGeneration, ValidatedPmtilesArtifact,
    VectorTileBuildOutcome,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_outbox::errors::PublishError;
use foundation_outbox::object_storage::{
    ByteStream, FileObjectStorage, ObjectStorageStreamingService, ObjectWriteMode, R2ObjectStorage,
    StreamingObjectRehash, StreamingPutObjectRequest,
};
use foundation_shared_kernel::ids::{
    FileAssetId, StaffId, VectorTileBuildJobId, VectorTileDataRevisionId, VectorTileReleaseId,
};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Postgres, Row as _, Transaction};
use tokio::{io::AsyncReadExt as _, time};
use uuid::Uuid;

use crate::static_release_toolchain::VerifiedToolchain;
use crate::tile_derivative_object_storage::TileDerivativeR2Config;

const UNIT_KEY: &str = "complex";
const CONFIRM_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLISH_CONFIRM";
const DYNAMIC_MARTIN_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_DYNAMIC_MARTIN_BASE_URL";
const STATIC_MARTIN_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_STATIC_MARTIN_BASE_URL";
const PUBLIC_TILES_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PUBLIC_TILES_BASE_URL";
const MARTIN_CONFIG_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_MARTIN_CONFIG_PATH";
const WORK_ROOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_WORK_ROOT";
const OPERATOR_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_OPERATOR_STAFF_ID";
const BUILD_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_BUILD_IDEMPOTENCY_KEY";
const PROMOTE_KEY_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_PROMOTE_IDEMPOTENCY_KEY";
const TOOL_TIMEOUT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_TOOL_TIMEOUT_SECONDS";

#[derive(Clone)]
struct Config {
    dynamic_martin_base_url: String,
    static_martin_base_url: String,
    public_tiles_base_url: String,
    martin_config_path: PathBuf,
    work_root: PathBuf,
    operator_staff_id: StaffId,
    build_idempotency_key: String,
    promote_idempotency_key: String,
    tool_timeout: Duration,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        ensure!(
            required_env(CONFIRM_ENV)? == "1",
            "{CONFIRM_ENV}=1 is required; static publication moves the serving pointer"
        );
        let timeout_seconds = required_env(TOOL_TIMEOUT_ENV)?
            .parse::<u64>()
            .with_context(|| format!("{TOOL_TIMEOUT_ENV} must be an integer"))?;
        ensure!(
            (1..=3600).contains(&timeout_seconds),
            "{TOOL_TIMEOUT_ENV} must be in 1..=3600"
        );
        let operator = Uuid::parse_str(&required_env(OPERATOR_ENV)?)
            .with_context(|| format!("{OPERATOR_ENV} must be a UUID"))?;
        Ok(Self {
            dynamic_martin_base_url: base_url(&required_env(DYNAMIC_MARTIN_ENV)?)?,
            static_martin_base_url: base_url(&required_env(STATIC_MARTIN_ENV)?)?,
            public_tiles_base_url: base_url(&required_env(PUBLIC_TILES_ENV)?)?,
            martin_config_path: PathBuf::from(required_env(MARTIN_CONFIG_ENV)?),
            work_root: PathBuf::from(required_env(WORK_ROOT_ENV)?),
            operator_staff_id: StaffId::new(operator),
            build_idempotency_key: required_env(BUILD_KEY_ENV)?,
            promote_idempotency_key: required_env(PROMOTE_KEY_ENV)?,
            tool_timeout: Duration::from_secs(timeout_seconds),
        })
    }
}

#[derive(Deserialize)]
struct DynamicTileJson {
    minzoom: Option<u8>,
    maxzoom: Option<u8>,
    bounds: Option<[f64; 4]>,
    vector_layers: serde_json::Value,
}

struct ActiveDynamicRelease {
    release_id: VectorTileReleaseId,
    data_revision: VectorTileDataRevisionId,
    snapshot_id: CanonicalIcebergSnapshotId,
    serving_generation: ServingGeneration,
}

struct BuildFiles {
    mbtiles: PathBuf,
    pmtiles: PathBuf,
    unpacked: PathBuf,
}

struct RepresentativeTile {
    z: u32,
    x: u32,
    y: u32,
}

/// Runs the production static-release publisher.
pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let database_url = required_env("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to DATABASE_URL for static tile publication")?;
    let http = Client::builder()
        .timeout(config.tool_timeout)
        .build()
        .context("failed to build bounded Martin HTTP client")?;

    let active = read_active_dynamic_release(&pool).await?;
    let toolchain = crate::static_release_toolchain::verify(config.tool_timeout).await?;
    let projection_snapshot =
        lock_active_dynamic_projection(&pool, active.release_id, config.tool_timeout).await?;
    let tilejson = read_dynamic_tilejson(&http, &config).await?;
    validate_dynamic_build_conditions(&pool, &tilejson).await?;

    let uow = Arc::new(
        PgCatalogUnitOfWork::new(pool.clone())
            .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled()),
    );
    let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
    let build_job_id = lifecycle
        .start(StartVectorTileBuildCommand {
            unit_key: UNIT_KEY.to_owned(),
            input_release_id: active.release_id,
            input_data_revision: active.data_revision,
            frozen_source_snapshot_id: active.snapshot_id.clone(),
            idempotency_key: config.build_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
        })
        .await?;

    let publication = publish_candidate(&http, &config, &toolchain, &tilejson, build_job_id).await;
    projection_snapshot
        .rollback()
        .await
        .context("failed to release the frozen dynamic projection")?;
    let (release_id, file_asset_id, verified, evidence) = match publication {
        Ok(candidate) => candidate,
        Err(error) => {
            let reason = bounded_failure_reason(&error);
            lifecycle
                .record_result(RecordVectorTileBuildResultCommand {
                    build_job_id,
                    outcome: VectorTileBuildOutcome::Failed(reason),
                    operator_staff_id: config.operator_staff_id,
                })
                .await
                .context("static build failed and its failure could not be recorded")?;
            return Err(error);
        }
    };

    let source_id = static_release_martin_source_id(UNIT_KEY, release_id);
    let object_key = static_release_pmtiles_object_key(UNIT_KEY, release_id);
    lifecycle
        .record_result(RecordVectorTileBuildResultCommand {
            build_job_id,
            outcome: VectorTileBuildOutcome::Validated {
                evidence: BuildEvidenceDigest::new(evidence).map_err(anyhow::Error::msg)?,
                artifact: ValidatedPmtilesArtifact {
                    release_id,
                    file_asset_id,
                    object_key,
                    tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                        "{}/{source_id}/{{z}}/{{x}}/{{y}}",
                        config.public_tiles_base_url
                    ))
                    .map_err(anyhow::Error::msg)?,
                    checksum: PmtilesChecksum::new(verified.checksum_sha256.clone())
                        .map_err(anyhow::Error::msg)?,
                    size_bytes: verified.size_bytes,
                },
            },
            operator_staff_id: config.operator_staff_id,
        })
        .await?;

    let manifest = PromoteTileLayerStatic::new(uow)
        .execute(PromoteTileLayerStaticCommand {
            unit_key: UNIT_KEY.to_owned(),
            build_job_id,
            expected_active_release_id: active.release_id,
            expected_serving_generation: active.serving_generation,
            idempotency_key: config.promote_idempotency_key,
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    println!(
        "industrial complex boundary static release published build_job_id={} release_id={} manifest_id={} generation={} bytes={} sha256={}",
        build_job_id,
        release_id,
        manifest.current_version,
        manifest.manifest_generation.value(),
        verified.size_bytes,
        verified.checksum_sha256
    );
    Ok(())
}

/// Runs the destructive local-store experiments used by the disposable boundary proof.
///
/// This is deliberately a separate command from [`run`]: production publication has no local
/// storage mode, while the proof needs to demonstrate the negative paths without touching R2.
pub async fn run_local_mutation_guard_proof() -> anyhow::Result<()> {
    const CONFIRM: &str =
        "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_STATIC_RELEASE_LOCAL_PROOF_CONFIRM";
    ensure!(
        required_env(CONFIRM)? == "1",
        "{CONFIRM}=1 is required for the destructive local-store proof"
    );
    let root = std::env::temp_dir().join(format!(
        "perfectory-static-release-mutation-proof-{}",
        Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&root).await?;
    let result = async {
        prove_file_store_create_only_and_bypass(&root).await?;
        prove_missing_and_mismatched_readback_are_refused(&root).await?;
        prove_unreadable_martin_is_refused(&root).await
    }
    .await;
    tokio::fs::remove_dir_all(&root)
        .await
        .with_context(|| format!("failed to remove local proof directory {}", root.display()))?;
    result?;
    println!(
        "STATIC OBJECT GUARDS OK create_only=reject bypass=overwrite readback_missing=reject readback_mismatch=reject martin=reject"
    );
    Ok(())
}

async fn lock_active_dynamic_projection(
    pool: &PgPool,
    expected_release_id: VectorTileReleaseId,
    timeout: Duration,
) -> anyhow::Result<Transaction<'_, Postgres>> {
    let mut tx = pool.begin().await?;
    let timeout_ms = timeout.as_millis().to_string();
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{timeout_ms}ms"))
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query_scalar::<_, String>(
        "SELECT set_config('idle_in_transaction_session_timeout', '0', true)",
    )
    .fetch_one(&mut *tx)
    .await?;
    // The CAS gate takes SHARE ROW EXCLUSIVE on this table. SHARE is compatible with Martin's
    // reads but blocks that pointer change, freezing the exact append-only projection row set for
    // every external build/readback step. Promotion occurs only after this guard is released.
    sqlx::query("LOCK TABLE catalog.vector_tile_runtime_manifest_pointer IN SHARE MODE")
        .execute(&mut *tx)
        .await
        .context("could not freeze the active dynamic projection before the bounded deadline")?;
    let current_release_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT unit.active_release_id
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release ON release.id = unit.active_release_id
         WHERE unit.unit_key = 'complex' AND release.source_kind = 'dynamic_postgis'",
    )
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    ensure!(
        current_release_id == Some(expected_release_id.as_uuid()),
        "the active complex release changed before its PostGIS projection could be frozen"
    );
    Ok(tx)
}

async fn publish_candidate(
    http: &Client,
    config: &Config,
    toolchain: &VerifiedToolchain,
    tilejson: &DynamicTileJson,
    build_job_id: VectorTileBuildJobId,
) -> anyhow::Result<(
    VectorTileReleaseId,
    FileAssetId,
    StreamingObjectRehash,
    String,
)> {
    let release_id = static_release_id_for_build(build_job_id);
    let file_asset_id = static_file_asset_id_for_build(build_job_id);
    let source_id = static_release_martin_source_id(UNIT_KEY, release_id);
    let run_dir = config
        .work_root
        .join(format!("{build_job_id}-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&config.work_root).with_context(|| {
        format!(
            "failed to create static release work root {}",
            config.work_root.display()
        )
    })?;
    std::fs::create_dir(&run_dir).with_context(|| {
        format!(
            "static release work directory {} already exists or cannot be created",
            run_dir.display()
        )
    })?;
    let files = BuildFiles {
        mbtiles: run_dir.join("complex.mbtiles"),
        pmtiles: run_dir.join(format!("{source_id}.pmtiles")),
        unpacked: run_dir.join("unpacked"),
    };
    build_archives(config, toolchain, tilejson, &source_id, &files).await?;
    let representative = representative_tile(&files.unpacked)?;

    let storage_config = TileDerivativeR2Config::from_env()?;
    let object_key = storage_config.release_key(UNIT_KEY, &release_id.to_string())?;
    let reader = R2ObjectStorage::from_config(storage_config.reader_config());
    let writer = R2ObjectStorage::from_config(storage_config.writer);
    let verified =
        create_only_upload_and_rehash(&writer, &reader, &files.pmtiles, &object_key).await?;

    let dynamic_bytes = fetch_tile(
        http,
        &config.dynamic_martin_base_url,
        UNIT_KEY,
        &representative,
    )
    .await?;
    wait_for_static_source(http, config, &source_id).await?;
    let static_bytes = fetch_tile(
        http,
        &config.static_martin_base_url,
        &source_id,
        &representative,
    )
    .await?;
    ensure!(
        !dynamic_bytes.is_empty(),
        "representative dynamic tile is empty"
    );
    ensure!(
        dynamic_bytes == static_bytes,
        "Martin decoded the uploaded release but its representative MVT bytes differ from dynamic"
    );

    let evidence_json = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "build_job_id": build_job_id.to_string(),
        "release_id": release_id.to_string(),
        "object_key": object_key,
        "pmtiles_sha256": verified.checksum_sha256,
        "pmtiles_bytes": verified.size_bytes,
        "representative_tile": {"z": representative.z, "x": representative.x, "y": representative.y},
        "dynamic_and_static_mvt_sha256": format!("{:x}", Sha256::digest(&dynamic_bytes)),
    }))?;
    let evidence = format!("{:x}", Sha256::digest(evidence_json));
    Ok((release_id, file_asset_id, verified, evidence))
}

async fn read_active_dynamic_release(pool: &PgPool) -> anyhow::Result<ActiveDynamicRelease> {
    let row = sqlx::query(
        "SELECT unit.active_release_id, unit.serving_generation, release.data_revision,
                release.canonical_iceberg_snapshot_id, release.source_kind
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release ON release.id = unit.active_release_id
         WHERE unit.unit_key = 'complex'",
    )
    .fetch_optional(pool)
    .await?
    .context("publication unit complex has no active release")?;
    let source_kind: String = row.try_get("source_kind")?;
    ensure!(
        source_kind == "dynamic_postgis",
        "publication unit complex must be dynamic_postgis before static publication; got {source_kind}"
    );
    let generation = u64::try_from(row.try_get::<i64, _>("serving_generation")?)?;
    Ok(ActiveDynamicRelease {
        release_id: VectorTileReleaseId::new(row.try_get("active_release_id")?),
        data_revision: VectorTileDataRevisionId::new(row.try_get("data_revision")?),
        snapshot_id: CanonicalIcebergSnapshotId::new(row.try_get("canonical_iceberg_snapshot_id")?)
            .map_err(anyhow::Error::msg)?,
        serving_generation: ServingGeneration::new(generation).map_err(anyhow::Error::msg)?,
    })
}

async fn read_dynamic_tilejson(http: &Client, config: &Config) -> anyhow::Result<DynamicTileJson> {
    http.get(format!("{}/{UNIT_KEY}", config.dynamic_martin_base_url))
        .send()
        .await
        .context("failed to read active dynamic Martin TileJSON")?
        .error_for_status()
        .context("dynamic Martin TileJSON returned an error status")?
        .json()
        .await
        .context("dynamic Martin TileJSON does not match the build-condition contract")
}

async fn validate_dynamic_build_conditions(
    pool: &PgPool,
    tilejson: &DynamicTileJson,
) -> anyhow::Result<()> {
    let bounds = tilejson.bounds.context(
        "dynamic complex TileJSON has no bounds; Martin auto_bounds is not current enough to bake",
    )?;
    let minzoom = tilejson
        .minzoom
        .context("dynamic TileJSON has no minzoom")?;
    let maxzoom = tilejson
        .maxzoom
        .context("dynamic TileJSON has no maxzoom")?;
    ensure!(
        minzoom <= maxzoom,
        "dynamic TileJSON zoom range is reversed"
    );
    let covers: Option<bool> = sqlx::query_scalar(
        "SELECT ST_Covers(
             ST_Expand(ST_MakeEnvelope($1, $2, $3, $4, 4326), 1e-9),
             ST_SetSRID(ST_Extent(geom)::geometry, 4326))
         FROM serving_postgis.industrial_complex_boundary_current",
    )
    .bind(bounds[0])
    .bind(bounds[1])
    .bind(bounds[2])
    .bind(bounds[3])
    .fetch_one(pool)
    .await?;
    ensure!(
        covers == Some(true),
        "dynamic TileJSON bounds do not cover all currently served complex rows"
    );
    let zooms: Vec<(i16, i16)> = sqlx::query_as(
        "SELECT layer.tile_min_zoom, layer.tile_max_zoom
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release_layer AS layer
           ON layer.release_id = unit.active_release_id
         WHERE unit.unit_key = 'complex' ORDER BY layer.layer_id",
    )
    .fetch_all(pool)
    .await?;
    ensure!(!zooms.is_empty(), "active complex release has no layers");
    ensure!(
        zooms
            .iter()
            .all(|(min, max)| *min == i16::from(minzoom) && *max == i16::from(maxzoom)),
        "dynamic TileJSON zoom range disagrees with the active release layers"
    );
    ensure!(
        tilejson.vector_layers.is_array(),
        "dynamic TileJSON vector_layers is not an array"
    );
    Ok(())
}

async fn build_archives(
    config: &Config,
    toolchain: &VerifiedToolchain,
    tilejson: &DynamicTileJson,
    source_id: &str,
    files: &BuildFiles,
) -> anyhow::Result<()> {
    let bounds = tilejson
        .bounds
        .context("validated TileJSON bounds disappeared")?;
    let minzoom = tilejson
        .minzoom
        .context("validated TileJSON minzoom disappeared")?;
    let maxzoom = tilejson
        .maxzoom
        .context("validated TileJSON maxzoom disappeared")?;
    let bounds_text = bounds.map(|value| value.to_string()).join(",");
    let vector_layers = serde_json::to_string(&serde_json::json!({
        "vector_layers": tilejson.vector_layers.clone()
    }))?;
    let martin_args = vec![
        OsString::from("--config"),
        config.martin_config_path.as_os_str().to_owned(),
        OsString::from("--source"),
        OsString::from(UNIT_KEY),
        OsString::from("--output-file"),
        files.mbtiles.as_os_str().to_owned(),
        OsString::from("--encoding"),
        OsString::from("identity"),
        OsString::from("--bbox"),
        OsString::from(bounds_text),
        OsString::from("--min-zoom"),
        OsString::from(minzoom.to_string()),
        OsString::from("--max-zoom"),
        OsString::from(maxzoom.to_string()),
        OsString::from("--concurrency"),
        OsString::from("2"),
    ];
    ensure_tool_success(
        "martin-cp",
        &toolchain
            .run("martin-cp", martin_args, config.tool_timeout, None)
            .await?,
    )?;
    for args in [
        vec![
            OsString::from("meta-set"),
            files.mbtiles.as_os_str().to_owned(),
            OsString::from("json"),
            OsString::from(vector_layers),
        ],
        vec![
            OsString::from("meta-set"),
            files.mbtiles.as_os_str().to_owned(),
            OsString::from("name"),
            OsString::from(source_id),
        ],
        vec![
            OsString::from("validate"),
            files.mbtiles.as_os_str().to_owned(),
        ],
        vec![
            OsString::from("unpack"),
            files.mbtiles.as_os_str().to_owned(),
            files.unpacked.as_os_str().to_owned(),
        ],
    ] {
        ensure_tool_success(
            "mbtiles",
            &toolchain
                .run("mbtiles", args, config.tool_timeout, None)
                .await?,
        )?;
    }
    ensure_tool_success(
        "pmtiles convert",
        &toolchain
            .run(
                "pmtiles",
                [
                    OsString::from("convert"),
                    files.mbtiles.as_os_str().to_owned(),
                    files.pmtiles.as_os_str().to_owned(),
                ],
                config.tool_timeout,
                None,
            )
            .await?,
    )?;
    ensure_tool_success(
        "pmtiles verify",
        &toolchain
            .run(
                "pmtiles",
                [
                    OsString::from("verify"),
                    files.pmtiles.as_os_str().to_owned(),
                ],
                config.tool_timeout,
                None,
            )
            .await?,
    )
}

async fn create_only_upload_and_rehash(
    writer: &dyn ObjectStorageStreamingService,
    reader: &dyn ObjectStorageStreamingService,
    path: &Path,
    key: &str,
) -> anyhow::Result<StreamingObjectRehash> {
    let local = hash_file(path).await?;
    let body = ByteStream::from_path(path)
        .await
        .with_context(|| format!("failed to open PMTiles stream {}", path.display()))?;
    let upload = writer
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.to_owned(),
            content_type: "application/vnd.pmtiles".to_owned(),
            cache_control: "public, max-age=31536000, immutable".to_owned(),
            size_bytes: local.size_bytes,
            body,
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await;
    match upload {
        Ok(()) => {}
        Err(PublishError::ObjectAlreadyExists { key: collided }) if collided == key => {
            // A retry still performs the create-only request; storage rejects the second write.
            // Only a full GET rehash equal to this attempt's local archive may reconcile it.
        }
        Err(error) => {
            return Err(anyhow::Error::new(error))
                .with_context(|| format!("create-only PMTiles upload failed for {key}"));
        }
    }
    let readback = reader
        .read_object_sha256_and_size_by_rehash(key)
        .await
        .with_context(|| format!("exact GET rehash failed for uploaded PMTiles {key}"))?
        .with_context(|| format!("uploaded PMTiles {key} disappeared before registration"))?;
    ensure!(
        readback.checksum_sha256 == local.checksum_sha256
            && readback.size_bytes == local.size_bytes,
        "uploaded PMTiles readback mismatch: local sha256={} bytes={}, stored sha256={} bytes={}",
        local.checksum_sha256,
        local.size_bytes,
        readback.checksum_sha256,
        readback.size_bytes
    );
    Ok(readback)
}

struct TamperingFileStore {
    inner: FileObjectStorage,
}

#[async_trait]
impl ObjectStorageStreamingService for TamperingFileStore {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        let key = request.key.clone();
        self.inner.put_streaming_object(request).await?;
        self.inner
            .put_streaming_object(StreamingPutObjectRequest {
                key,
                content_type: "application/vnd.pmtiles".to_owned(),
                cache_control: "no-store".to_owned(),
                size_bytes: 8,
                body: ByteStream::from_static(b"tampered"),
                write_mode: ObjectWriteMode::OverwriteAllowed,
            })
            .await
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        self.inner.read_object_sha256_and_size_by_rehash(key).await
    }
}

struct MissingReadbackFileStore {
    inner: FileObjectStorage,
}

#[async_trait]
impl ObjectStorageStreamingService for MissingReadbackFileStore {
    async fn put_streaming_object(
        &self,
        request: StreamingPutObjectRequest,
    ) -> Result<(), PublishError> {
        self.inner.put_streaming_object(request).await
    }

    async fn read_object_sha256_and_size_by_rehash(
        &self,
        _key: &str,
    ) -> Result<Option<StreamingObjectRehash>, PublishError> {
        Ok(None)
    }
}

async fn prove_file_store_create_only_and_bypass(root: &Path) -> anyhow::Result<()> {
    let source = root.join("create-only-source.pmtiles");
    tokio::fs::write(&source, b"verified-pmtiles-bytes").await?;
    let store = FileObjectStorage::new(root.join("create-only-objects"))?;
    let key = "gold/vector-tiles/releases/complex-test.pmtiles";

    let verified = create_only_upload_and_rehash(&store, &store, &source, key).await?;
    ensure!(verified.size_bytes == 22, "local proof source size drifted");
    let second = store
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.to_owned(),
            content_type: "application/vnd.pmtiles".to_owned(),
            cache_control: "public, max-age=31536000, immutable".to_owned(),
            size_bytes: 22,
            body: ByteStream::from_static(b"verified-pmtiles-bytes"),
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await;
    ensure!(
        matches!(second, Err(PublishError::ObjectAlreadyExists { .. })),
        "second create-only write was not rejected"
    );

    // A rejected retry can reconcile only through the exact readback path.
    let reconciled = create_only_upload_and_rehash(&store, &store, &source, key).await?;
    ensure!(
        reconciled == verified,
        "create-only retry did not rehash exactly"
    );
    store
        .put_streaming_object(StreamingPutObjectRequest {
            key: key.to_owned(),
            content_type: "application/vnd.pmtiles".to_owned(),
            cache_control: "no-store".to_owned(),
            size_bytes: 17,
            body: ByteStream::from_static(b"replacement-bytes"),
            write_mode: ObjectWriteMode::OverwriteAllowed,
        })
        .await?;
    ensure!(
        store.get_object_bytes(key)? == b"replacement-bytes",
        "bypass control did not overwrite the object"
    );
    Ok(())
}

async fn prove_missing_and_mismatched_readback_are_refused(root: &Path) -> anyhow::Result<()> {
    let source = root.join("readback-source.pmtiles");
    tokio::fs::write(&source, b"different-bytes").await?;
    let missing_inner = FileObjectStorage::new(root.join("missing-readback-objects"))?;
    let missing = MissingReadbackFileStore {
        inner: missing_inner.clone(),
    };
    let missing_gate =
        create_only_upload_and_rehash(&missing_inner, &missing, &source, "missing-candidate").await;
    ensure!(
        missing_gate.as_ref().err().is_some_and(|error| error
            .to_string()
            .contains("disappeared before registration")),
        "missing exact readback did not refuse registration"
    );

    let tampering = TamperingFileStore {
        inner: FileObjectStorage::new(root.join("mismatched-readback-objects"))?,
    };
    let mismatch_gate = create_only_upload_and_rehash(
        &tampering,
        &tampering.inner,
        &source,
        "mismatched-candidate",
    )
    .await;
    ensure!(
        mismatch_gate
            .as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains("readback mismatch")),
        "mismatched exact readback did not refuse registration"
    );
    Ok(())
}

async fn prove_unreadable_martin_is_refused(root: &Path) -> anyhow::Result<()> {
    let source_id = "complex-00000000-0000-7000-8000-000000000001";
    let config = Config {
        dynamic_martin_base_url: "http://127.0.0.1:1".to_owned(),
        static_martin_base_url: "http://127.0.0.1:1".to_owned(),
        public_tiles_base_url: "http://127.0.0.1:1".to_owned(),
        martin_config_path: PathBuf::from("unused-martin.yaml"),
        work_root: root.join("unused-work-root"),
        operator_staff_id: StaffId::new(Uuid::now_v7()),
        build_idempotency_key: "unused-build-key".to_owned(),
        promote_idempotency_key: "unused-promote-key".to_owned(),
        tool_timeout: Duration::from_millis(50),
    };
    let store = FileObjectStorage::new(root.join("unreadable-martin-object"))?;
    store
        .put_streaming_object(StreamingPutObjectRequest {
            key: "gold/vector-tiles/releases/unreadable.pmtiles".to_owned(),
            content_type: "application/vnd.pmtiles".to_owned(),
            cache_control: "public, max-age=31536000, immutable".to_owned(),
            size_bytes: 15,
            body: ByteStream::from_static(b"verified-object"),
            write_mode: ObjectWriteMode::CreateOnly,
        })
        .await?;
    ensure!(
        store
            .read_object_sha256_and_size_by_rehash("gold/vector-tiles/releases/unreadable.pmtiles")
            .await?
            .is_some(),
        "local object was not present before the Martin negative control"
    );
    let http = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let gate = wait_for_static_source(&http, &config, source_id).await;
    ensure!(
        gate.as_ref()
            .err()
            .is_some_and(|error| error.to_string().contains("could not read release source")),
        "unreadable Martin source did not refuse registration"
    );
    Ok(())
}

async fn hash_file(path: &Path) -> anyhow::Result<StreamingObjectRehash> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {} for SHA-256", path.display()))?;
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size_bytes = size_bytes
            .checked_add(u64::try_from(read)?)
            .context("PMTiles byte count overflowed u64")?;
    }
    ensure!(size_bytes > 0, "PMTiles archive is empty");
    Ok(StreamingObjectRehash {
        checksum_sha256: format!("{:x}", hasher.finalize()),
        size_bytes,
        observed_e_tag: None,
        observed_last_modified: None,
    })
}

fn representative_tile(root: &Path) -> anyhow::Result<RepresentativeTile> {
    let mut directories = vec![root.to_owned()];
    let mut tiles = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("failed to read unpacked MBTiles {}", directory.display()))?
        {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension() == Some(OsStr::new("pbf")) {
                tiles.push(path);
            }
        }
    }
    tiles.sort();
    let path = tiles
        .first()
        .context("validated MBTiles archive unpacked no representative PBF tile")?;
    let relative = path.strip_prefix(root)?;
    let parts = relative.iter().collect::<Vec<_>>();
    ensure!(
        parts.len() == 3,
        "unpacked tile path is not z/x/y.pbf: {}",
        path.display()
    );
    let parse = |value: &OsStr, label: &str| -> anyhow::Result<u32> {
        value
            .to_str()
            .with_context(|| format!("tile {label} is not UTF-8"))?
            .parse::<u32>()
            .with_context(|| format!("tile {label} is not an integer"))
    };
    let y_stem = Path::new(parts[2])
        .file_stem()
        .context("representative tile has no y filename stem")?;
    Ok(RepresentativeTile {
        z: parse(parts[0], "z")?,
        x: parse(parts[1], "x")?,
        y: parse(y_stem, "y")?,
    })
}

async fn wait_for_static_source(
    http: &Client,
    config: &Config,
    source_id: &str,
) -> anyhow::Result<()> {
    let deadline = time::Instant::now() + config.tool_timeout;
    let url = format!("{}/{source_id}", config.static_martin_base_url);
    loop {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            bail!(
                "static Martin could not read release source {source_id} before the bounded deadline"
            );
        }
        if let Ok(Ok(response)) = time::timeout(remaining, http.get(&url).send()).await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            bail!(
                "static Martin could not read release source {source_id} before the bounded deadline"
            );
        }
        time::sleep(remaining.min(Duration::from_secs(1))).await;
    }
}

async fn fetch_tile(
    http: &Client,
    base_url: &str,
    source_id: &str,
    tile: &RepresentativeTile,
) -> anyhow::Result<Vec<u8>> {
    Ok(http
        .get(format!(
            "{base_url}/{source_id}/{}/{}/{}",
            tile.z, tile.x, tile.y
        ))
        .header("Accept-Encoding", "identity")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

fn ensure_tool_success(tool: &str, output: &Output) -> anyhow::Result<()> {
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "{tool} failed with status {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn required_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing required environment variable {name}"))
}

fn base_url(raw: &str) -> anyhow::Result<String> {
    let value = raw.trim_end_matches('/');
    ensure!(
        value.starts_with("http://") || value.starts_with("https://"),
        "Martin base URLs must use http or https"
    );
    Ok(value.to_owned())
}

fn bounded_failure_reason(error: &anyhow::Error) -> String {
    error.to_string().chars().take(2_000).collect::<String>()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("perfectory-{label}-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn file_store_upload_is_create_only_and_exactly_rehashed() -> anyhow::Result<()> {
        let root = temp_root("static-create-only");
        tokio::fs::create_dir_all(&root).await?;
        prove_file_store_create_only_and_bypass(&root).await?;
        std::fs::remove_dir_all(&root)?;
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_exact_readback_is_refused_before_registration() -> anyhow::Result<()> {
        let root = temp_root("static-readback-mismatch");
        tokio::fs::create_dir_all(&root).await?;
        prove_missing_and_mismatched_readback_are_refused(&root).await?;
        std::fs::remove_dir_all(&root)?;
        Ok(())
    }

    #[tokio::test]
    async fn unreadable_static_martin_source_is_refused_before_registration() -> anyhow::Result<()>
    {
        let root = temp_root("unreadable-martin");
        tokio::fs::create_dir_all(&root).await?;
        prove_unreadable_martin_is_refused(&root).await?;
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
