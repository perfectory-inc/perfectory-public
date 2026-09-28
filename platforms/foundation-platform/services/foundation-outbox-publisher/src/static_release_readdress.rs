//! Readdresses validated immutable PMTiles bytes without baking or editing their original release.
use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{ensure, Context as _};
use catalog_application::{
    ports::{
        PromoteTileLayerStaticCommand, RecordVectorTileBuildResultCommand,
        RuntimeManifestPublicationCapability, StartStaticReleaseReaddressCommand,
    },
    PromoteTileLayerStatic, VectorTileBuildLifecycle,
};
use catalog_domain::{
    is_publication_unit_key, static_file_asset_id_for_build, static_release_id_for_build,
    static_release_martin_source_id, static_release_pmtiles_object_key, BuildEvidenceDigest,
    PmtilesChecksum, RuntimeTilesUrlTemplate, ServingGeneration, ValidatedPmtilesArtifact,
    VectorTileBuildOutcome,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_outbox::object_storage::{
    create_only_copy_and_rehash, CreateOnlyCopyObjectRequest, R2ObjectStorage,
    StreamingObjectRehash, SINGLE_COPY_MAX_BYTES,
};
use foundation_shared_kernel::ids::{StaffId, VectorTileBuildJobId, VectorTileReleaseId};
use reqwest::Client;
use sha2::{Digest as _, Sha256};
use sqlx::{pool::PoolConnection, PgPool, Postgres, Row as _};
use uuid::Uuid;

use crate::{
    static_release_url::{base_url, public_tiles_base_url},
    tile_derivative_object_storage::TileDerivativeR2Config,
};

#[path = "static_release_readdress_proof.rs"]
mod proof;
use proof::{prove_tiles, RepresentativeTile};

const CONFIRM: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_CONFIRM";
const UNIT: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_UNIT_KEY";
const INPUT: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_EXPECTED_ACTIVE_RELEASE_ID";
const PUBLIC: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_PUBLIC_TILES_BASE_URL";
const VERIFY: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_VERIFY_TILES_BASE_URL";
const SOURCE: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_SOURCE_TILES_BASE_URL";
const TILE: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_REPRESENTATIVE_TILE";
const OPERATOR: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_OPERATOR_STAFF_ID";
const KEY: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_IDEMPOTENCY_KEY";
const PROMOTE_KEY: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_PROMOTE_IDEMPOTENCY_KEY";
const TIMEOUT: &str = "FOUNDATION_PLATFORM_STATIC_RELEASE_READDRESS_CONTROL_TIMEOUT_SECONDS";

struct Config {
    unit_key: String,
    input_release_id: VectorTileReleaseId,
    public_base: String,
    verify_base: String,
    source_base: String,
    tile: RepresentativeTile,
    operator: StaffId,
    key: String,
    promote_key: String,
    control_timeout: Duration,
}

impl Config {
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let required = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .with_context(|| format!("{name} is required"))
        };
        ensure!(
            required(CONFIRM)? == "1",
            "{CONFIRM}=1 is required; readdressing moves the serving pointer"
        );
        let unit_key = required(UNIT)?;
        ensure!(
            is_publication_unit_key(&unit_key),
            "{UNIT} is not a publication unit key"
        );
        let parse_id = |name| {
            Uuid::parse_str(&required(name)?).with_context(|| format!("{name} must be a UUID"))
        };
        let timeout = required(TIMEOUT)?
            .parse::<u64>()
            .with_context(|| format!("{TIMEOUT} must be an integer"))?;
        ensure!((1..=600).contains(&timeout), "{TIMEOUT} must be in 1..=600");
        Ok(Self {
            unit_key,
            input_release_id: VectorTileReleaseId::new(parse_id(INPUT)?),
            public_base: public_tiles_base_url(&required(PUBLIC)?)
                .with_context(|| format!("{PUBLIC} is not a public tile address"))?,
            verify_base: proof_base_url(&required(VERIFY)?)?,
            source_base: proof_base_url(&required(SOURCE)?)?,
            tile: RepresentativeTile::parse(&required(TILE)?)
                .with_context(|| format!("{TILE} must be z/x/y"))?,
            operator: StaffId::new(parse_id(OPERATOR)?),
            key: required(KEY)?,
            promote_key: required(PROMOTE_KEY)?,
            control_timeout: Duration::from_secs(timeout),
        })
    }
}

fn proof_base_url(raw: &str) -> anyhow::Result<String> {
    let base = base_url(raw)?;
    let url = reqwest::Url::parse(&base)?;
    ensure!(
        url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "proof base URL must have a host and no credentials, query or fragment"
    );
    Ok(base)
}

struct Input {
    status: String,
    input_release_id: VectorTileReleaseId,
    generation: ServingGeneration,
    snapshot: String,
    revision: Uuid,
    source_key: String,
    expected: StreamingObjectRehash,
}

/// All SQL/control waits have an independent deadline; archive copy/rehash never inherits it.
async fn control<T>(
    timeout: Duration,
    operation: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::time::timeout(timeout, operation)
        .await
        .context("readdress control operation exceeded its deadline")?
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let config = Config::from_lookup(|name| std::env::var(name).ok())?;
    let storage = TileDerivativeR2Config::from_env()?;
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = control(config.control_timeout, async {
        Ok(PgPool::connect(&database_url).await?)
    })
    .await?;
    let uow = Arc::new(
        PgCatalogUnitOfWork::new(pool.clone())
            .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled()),
    );
    let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
    // Start resolves its idempotency key before observing the active pointer. After a successful
    // promotion, replay must still find this original S1 and its original CAS generation.
    let build_id = control(config.control_timeout, async {
        Ok(lifecycle
            .start_readdress(StartStaticReleaseReaddressCommand {
                unit_key: config.unit_key.clone(),
                input_release_id: config.input_release_id,
                public_tiles_base_url: config.public_base.clone(),
                idempotency_key: config.key.clone(),
                operator_staff_id: config.operator,
            })
            .await?)
    })
    .await?;
    // Same-build retries share one destination. Serialize them even when an object-store
    // provider needs the explicitly supported HEAD-absent conditional-copy fallback.
    let mut lease = control(config.control_timeout, acquire_build_lease(&pool, build_id)).await?;
    let result = async {
        let input = control(config.control_timeout, read_input(&pool, build_id, &config)).await?;
        let release_id = static_release_id_for_build(build_id);
        let source_id = static_release_martin_source_id(&config.unit_key, release_id);
        let template = format!("{}/{source_id}/{{z}}/{{x}}/{{y}}", config.public_base);

        if should_record(&input.status)? {
            let destination_key = storage.release_key(&config.unit_key, &release_id.to_string())?;
            let reader = R2ObjectStorage::from_config(storage.reader_config());
            let writer = R2ObjectStorage::from_config(storage.writer);
            // Do not turn transient copy/readback/HTTP failures into a terminal failed build. A crash
            // after create is recovered only by exact authenticated full-object rehash on the retry.
            let verified = create_only_copy_and_rehash(
                &writer,
                &reader,
                CreateOnlyCopyObjectRequest {
                    source_key: input.source_key.clone(),
                    destination_key: destination_key.clone(),
                    size_bytes: input.expected.size_bytes,
                    single_copy_max_bytes: SINGLE_COPY_MAX_BYTES,
                },
                &input.expected,
            )
            .await?;
            let http = Client::builder()
                .timeout(config.control_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()?;
            let source_id_s1 =
                static_release_martin_source_id(&config.unit_key, input.input_release_id);
            let tile_bytes = control(
                config.control_timeout,
                prove_tiles(
                    &http,
                    [
                        &config.source_base,
                        &config.verify_base,
                        &config.public_base,
                    ],
                    &source_id_s1,
                    &source_id,
                    &config.tile,
                ),
            )
            .await?;
            let evidence = evidence_digest(
                &config,
                &input,
                build_id,
                release_id,
                &destination_key,
                &tile_bytes,
            )?;
            control(config.control_timeout, verify_build_lease(&mut lease)).await?;
            control(config.control_timeout, async {
                Ok(lifecycle
                    .record_result(RecordVectorTileBuildResultCommand {
                        build_job_id: build_id,
                        outcome: VectorTileBuildOutcome::Validated {
                            evidence: BuildEvidenceDigest::new(evidence)
                                .map_err(anyhow::Error::msg)?,
                            artifact: ValidatedPmtilesArtifact {
                                release_id,
                                file_asset_id: static_file_asset_id_for_build(build_id),
                                object_key: destination_key,
                                tiles_url_template: RuntimeTilesUrlTemplate::new(template)
                                    .map_err(anyhow::Error::msg)?,
                                checksum: PmtilesChecksum::new(verified.checksum_sha256)
                                    .map_err(anyhow::Error::msg)?,
                                size_bytes: verified.size_bytes,
                            },
                        },
                        operator_staff_id: config.operator,
                    })
                    .await?)
            })
            .await?;
        }
        control(config.control_timeout, verify_build_lease(&mut lease)).await?;
        let manifest = control(config.control_timeout, async {
            Ok(PromoteTileLayerStatic::new(uow)
                .execute(PromoteTileLayerStaticCommand {
                    unit_key: config.unit_key.clone(),
                    build_job_id: build_id,
                    expected_active_release_id: input.input_release_id,
                    expected_serving_generation: input.generation,
                    idempotency_key: config.promote_key.clone(),
                    operator_staff_id: config.operator,
                })
                .await?)
        })
        .await?;
        Ok::<_, anyhow::Error>((manifest, input, release_id))
    }
    .await;
    let unlock = control(
        config.control_timeout,
        release_build_lease(&mut lease, build_id),
    )
    .await;
    let (manifest, input, release_id) = result?;
    unlock?;
    println!("static release readdressed build_job_id={} input_release_id={} release_id={} manifest_id={} generation={} bytes={} sha256={}",
        build_id, input.input_release_id, release_id, manifest.current_version,
        manifest.manifest_generation.value(), input.expected.size_bytes, input.expected.checksum_sha256);
    Ok(())
}

async fn acquire_build_lease(
    pool: &PgPool,
    build: VectorTileBuildJobId,
) -> anyhow::Result<PoolConnection<Postgres>> {
    let mut connection = pool.acquire().await?;
    // Every failure/cancellation closes the session rather than returning an advisory lock to
    // the pool. Explicit unlock below handles the normal and recoverable-error paths.
    connection.close_on_drop();
    sqlx::query("SELECT set_config('idle_session_timeout', '0', false)")
        .execute(&mut *connection)
        .await?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
        .bind(build_lease_key(build))
        .execute(&mut *connection)
        .await?;
    Ok(connection)
}

fn build_lease_key(build: VectorTileBuildJobId) -> String {
    format!("foundation-static-release-readdress:{build}")
}

async fn verify_build_lease(connection: &mut PoolConnection<Postgres>) -> anyhow::Result<()> {
    // A PoolConnection never silently reconnects. Losing this session loses its lock, so a
    // failed heartbeat refuses registration/promotion after lengthy external copy/readback.
    sqlx::query("SELECT 1")
        .execute(&mut **connection)
        .await
        .context("readdress build lease was lost before registration or promotion")?;
    Ok(())
}

async fn release_build_lease(
    connection: &mut PoolConnection<Postgres>,
    build: VectorTileBuildJobId,
) -> anyhow::Result<()> {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
        .bind(build_lease_key(build))
        .fetch_one(&mut **connection)
        .await?;
    ensure!(released, "readdress build lease was not held at completion");
    Ok(())
}

fn should_record(status: &str) -> anyhow::Result<bool> {
    match status {
        "running" => Ok(true),
        // Promotion consumes the recorded artifact. Repeating proof after validation could
        // produce different evidence and must never prevent a promotion retry.
        "validated" | "promoted" => Ok(false),
        _ => anyhow::bail!("readdress build status {status} is terminal and cannot be resumed"),
    }
}

async fn read_input(
    pool: &PgPool,
    build_id: VectorTileBuildJobId,
    config: &Config,
) -> anyhow::Result<Input> {
    let row = sqlx::query(
        "SELECT build.status, build.input_release_id, build.input_serving_generation,
                build.readdress_tiles_base_url, input.data_revision,
                input.canonical_iceberg_snapshot_id, input.pmtiles_object_key,
                input.pmtiles_sha256, input.pmtiles_bytes
         FROM catalog.vector_tile_build_job AS build
         JOIN catalog.vector_tile_release AS input ON input.id = build.input_release_id
         JOIN catalog.vector_tile_publication_unit AS unit ON unit.id = build.publication_unit_id
         WHERE build.id = $1 AND build.kind = 'readdress' AND unit.unit_key = $2
           AND input.source_kind = 'static_pmtiles' AND input.validated_at IS NOT NULL",
    )
    .bind(build_id.as_uuid())
    .bind(&config.unit_key)
    .fetch_one(pool)
    .await?;
    let input_release_id = VectorTileReleaseId::new(row.try_get("input_release_id")?);
    ensure!(
        input_release_id == config.input_release_id,
        "readdress retry changed the input release"
    );
    ensure!(
        row.try_get::<String, _>("readdress_tiles_base_url")? == config.public_base,
        "readdress retry changed the public base URL"
    );
    let source_key: String = row.try_get("pmtiles_object_key")?;
    ensure!(
        source_key == static_release_pmtiles_object_key(&config.unit_key, input_release_id),
        "input object key is not release-addressed"
    );
    let size_bytes = u64::try_from(row.try_get::<i64, _>("pmtiles_bytes")?)?;
    ensure!(size_bytes > 0, "input archive is empty");
    let checksum_sha256: String = row.try_get("pmtiles_sha256")?;
    PmtilesChecksum::new(checksum_sha256.clone()).map_err(anyhow::Error::msg)?;
    Ok(Input {
        status: row.try_get("status")?,
        input_release_id,
        generation: ServingGeneration::new(u64::try_from(
            row.try_get::<i64, _>("input_serving_generation")?,
        )?)
        .map_err(anyhow::Error::msg)?,
        snapshot: row.try_get("canonical_iceberg_snapshot_id")?,
        revision: row.try_get("data_revision")?,
        source_key,
        expected: StreamingObjectRehash {
            checksum_sha256,
            size_bytes,
            observed_e_tag: None,
            observed_last_modified: None,
        },
    })
}

fn evidence_digest(
    config: &Config,
    input: &Input,
    build: VectorTileBuildJobId,
    release: VectorTileReleaseId,
    destination: &str,
    tile: &[u8],
) -> anyhow::Result<String> {
    let evidence = serde_json::to_string(&serde_json::json!({
        "schema_version": 1, "kind": "readdress", "unit_key": config.unit_key,
        "build_job_id": build.to_string(), "input_release_id": input.input_release_id.to_string(),
        "release_id": release.to_string(), "input_serving_generation": input.generation.value(),
        "data_revision": input.revision.to_string(), "canonical_iceberg_snapshot_id": input.snapshot,
        "source_object_key": input.source_key, "destination_object_key": destination,
        "pmtiles_sha256": input.expected.checksum_sha256, "pmtiles_bytes": input.expected.size_bytes,
        "source_tiles_base_url": config.source_base, "verify_tiles_base_url": config.verify_base,
        "public_tiles_base_url": config.public_base, "representative_tile": config.tile.path(),
        "decoded_mvt_sha256": format!("{:x}", Sha256::digest(tile)), "decoded_mvt_bytes": tile.len(),
    }))?;
    let digest = format!("{:x}", Sha256::digest(evidence.as_bytes()));
    tracing::info!(evidence_sha256 = %digest, evidence_json = %evidence, "static release readdress verification evidence");
    Ok(digest)
}

#[cfg(test)]
#[path = "static_release_readdress_tests.rs"]
mod tests;
