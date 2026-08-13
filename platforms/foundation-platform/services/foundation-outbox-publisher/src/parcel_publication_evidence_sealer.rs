//! Seals one verified R2 execution-evidence object to one immutable parcel source tuple.

use std::{env, path::PathBuf};

use anyhow::{bail, Context};
use foundation_outbox::{EvidenceByteReader, R2ObjectStorage};
use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_infrastructure::IcebergRestCatalog;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{
    parcel_projection_digest::{stream_projection_digest, ProjectionRows},
    parcel_publication_contract::{
        verify_quality_report, ParcelPublicationExecutionEvidence, ParcelPublicationQuality,
        PARCEL_LOGICAL_TABLE, QUALITY_SCHEMA_VERSION,
    },
    r2_command_support::{
        env_path, evidence_sealer_database_url_from_env_file,
        lakehouse_catalog_config_from_env_file, r2_config_from_env_file,
    },
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_EVIDENCE_SEAL_CONFIRM";
const OBJECT_KEY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_EVIDENCE_OBJECT_KEY";
const ENV_FILE_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_EVIDENCE_ENV_FILE";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let storage = R2ObjectStorage::from_config(r2_config_from_env_file(&config.env_file)?);
    let catalog =
        IcebergRestCatalog::new(lakehouse_catalog_config_from_env_file(&config.env_file)?)
            .context("failed to initialise Iceberg REST catalog for parcel evidence sealing")?;
    let database_url = evidence_sealer_database_url_from_env_file(&config.env_file)?;

    let bytes = storage
        .read_evidence_bytes(&config.object_key)
        .await
        .with_context(|| {
            format!(
                "failed to read execution evidence object {}",
                config.object_key
            )
        })?;
    let execution_sha256 = format!("{:x}", Sha256::digest(&bytes));
    let execution: ParcelPublicationExecutionEvidence = serde_json::from_slice(&bytes)
        .context("parcel publication execution evidence is not its strict v1 JSON contract")?;
    execution.validate_publication_claims()?;

    let table = catalog
        .get_table_metadata(PARCEL_LOGICAL_TABLE)
        .await
        .context("failed to load parcel table identity from Iceberg REST catalog")?
        .context("Iceberg REST catalog does not contain silver.parcel_boundaries")?;
    if table.table_name != execution.iceberg_commit.logical_table
        || table.table_uuid != execution.iceberg_commit.table_uuid
        || !table
            .snapshot_ids
            .contains(&execution.iceberg_commit.snapshot_id)
    {
        bail!(
            "execution evidence table UUID/snapshot is not present in the loaded Iceberg catalog metadata"
        );
    }

    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to DATABASE_URL for parcel evidence sealing")?;
    let sealed = seal(&pool, &config.object_key, &execution_sha256, &execution).await?;
    println!(
        "parcel-publication-evidence-seal-ok evidence_id={} mirror_rebuild_run_id={} outcome={}",
        sealed.id,
        execution.mirror_rebuild_run_id,
        if sealed.created { "created" } else { "reused" }
    );
    Ok(())
}

struct Config {
    env_file: PathBuf,
    object_key: String,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if env::var(CONFIRM_ENV).ok().as_deref() != Some("1") {
            bail!("{CONFIRM_ENV}=1 is required before sealing parcel publication evidence");
        }
        let object_key = env::var(OBJECT_KEY_ENV)
            .with_context(|| format!("Missing required environment variable: {OBJECT_KEY_ENV}"))?;
        validate_object_key(&object_key)?;
        Ok(Self {
            env_file: env_path(ENV_FILE_ENV, ".env.local")?,
            object_key,
        })
    }
}

fn validate_object_key(key: &str) -> anyhow::Result<()> {
    if key.is_empty()
        || key.starts_with('/')
        || key.contains("//")
        || key.contains('\\')
        || key.split('/').any(|part| part == "." || part == "..")
    {
        bail!("{OBJECT_KEY_ENV} must be a canonical relative R2 object key");
    }
    Ok(())
}

struct SealResult {
    id: Uuid,
    created: bool,
}

struct RunBinding {
    status: String,
    source_snapshot_id: String,
    source_table: String,
    source_record_id: Option<Uuid>,
    source_file_asset_id: Option<Uuid>,
    loaded_row_count: i64,
    rejected_row_count: i64,
    srid: i32,
    finished: bool,
    quality_report: JsonValue,
}

async fn seal(
    pool: &PgPool,
    object_key: &str,
    execution_sha256: &str,
    execution: &ParcelPublicationExecutionEvidence,
) -> anyhow::Result<SealResult> {
    let mut transaction = pool.begin().await?;
    let run = lock_run(&mut transaction, execution.mirror_rebuild_run_id).await?;
    verify_run_binding(&run, execution)?;

    let quality: ParcelPublicationQuality = serde_json::from_value(run.quality_report.clone())
        .context("parcel mirror run quality_report is not the complete typed v1 contract")?;
    verify_quality_report(QUALITY_SCHEMA_VERSION, run.loaded_row_count, &quality)?;
    let (source_row_count, projection_content_sha256) = stream_projection_digest(
        &mut transaction,
        ProjectionRows::Source(execution.mirror_rebuild_run_id),
    )
    .await?;
    if source_row_count != run.loaded_row_count {
        bail!("parcel mirror source bytes do not match the terminal run row count");
    }

    sqlx::query("SELECT set_config('foundation.parcel_publication_evidence_sealer', 'on', true)")
        .execute(&mut *transaction)
        .await?;
    let candidate_id = Uuid::new_v4();
    let inserted = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO catalog.parcel_publication_source_evidence
            (id, mirror_rebuild_run_id, iceberg_table_uuid, iceberg_logical_table,
             iceberg_snapshot_id, source_record_id, source_file_asset_id,
             execution_evidence_schema_version, execution_evidence_object_key,
             execution_evidence_sha256, source_row_count, projection_content_sha256,
             quality_schema_version)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (mirror_rebuild_run_id) DO NOTHING
         RETURNING id",
    )
    .bind(candidate_id)
    .bind(execution.mirror_rebuild_run_id)
    .bind(execution.iceberg_commit.table_uuid)
    .bind(&execution.iceberg_commit.logical_table)
    .bind(execution.iceberg_commit.snapshot_id)
    .bind(execution.source_record_id)
    .bind(execution.source_file_asset_id)
    .bind(&execution.schema_version)
    .bind(object_key)
    .bind(execution_sha256)
    .bind(source_row_count)
    .bind(&projection_content_sha256)
    .bind(QUALITY_SCHEMA_VERSION)
    .fetch_optional(&mut *transaction)
    .await?;

    let stored = read_stored_evidence(&mut transaction, execution.mirror_rebuild_run_id).await?;
    verify_exact_idempotent_match(
        &stored,
        object_key,
        execution_sha256,
        source_row_count,
        &projection_content_sha256,
        execution,
    )?;
    transaction.commit().await?;
    Ok(SealResult {
        id: stored.id,
        created: inserted.is_some(),
    })
}

async fn lock_run(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> anyhow::Result<RunBinding> {
    let row = sqlx::query(
        "SELECT status, source_snapshot_id, source_table, source_record_id,
                source_file_asset_id, loaded_row_count, rejected_row_count, srid,
                finished_at IS NOT NULL AS finished, quality_report
           FROM serving_postgis.parcel_boundary_mirror_rebuild_run
          WHERE id = $1
          FOR UPDATE",
    )
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .with_context(|| format!("execution evidence names unknown mirror rebuild run {run_id}"))?;
    Ok(RunBinding {
        status: row.try_get("status")?,
        source_snapshot_id: row.try_get("source_snapshot_id")?,
        source_table: row.try_get("source_table")?,
        source_record_id: row.try_get("source_record_id")?,
        source_file_asset_id: row.try_get("source_file_asset_id")?,
        loaded_row_count: row.try_get("loaded_row_count")?,
        rejected_row_count: row.try_get("rejected_row_count")?,
        srid: row.try_get("srid")?,
        finished: row.try_get("finished")?,
        quality_report: row.try_get("quality_report")?,
    })
}

fn verify_run_binding(
    run: &RunBinding,
    execution: &ParcelPublicationExecutionEvidence,
) -> anyhow::Result<()> {
    let snapshot = format!("iceberg:{}", execution.iceberg_commit.snapshot_id);
    if run.status != "succeeded"
        || !run.finished
        || run.loaded_row_count <= 0
        || run.rejected_row_count != 0
        || run.srid != 5179
        || run.source_snapshot_id != snapshot
        || run.source_table != execution.iceberg_commit.logical_table
        || run.source_record_id != Some(execution.source_record_id)
        || run.source_file_asset_id != Some(execution.source_file_asset_id)
    {
        bail!("execution evidence does not match one complete terminal parcel mirror run tuple");
    }
    Ok(())
}

struct StoredEvidence {
    id: Uuid,
    iceberg_table_uuid: Uuid,
    iceberg_logical_table: String,
    iceberg_snapshot_id: i64,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    execution_schema: String,
    execution_object_key: String,
    execution_sha256: String,
    source_row_count: i64,
    projection_sha256: String,
    quality_schema: String,
}

async fn read_stored_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> anyhow::Result<StoredEvidence> {
    let row = sqlx::query(
        "SELECT id, iceberg_table_uuid, iceberg_logical_table, iceberg_snapshot_id,
                source_record_id, source_file_asset_id, execution_evidence_schema_version,
                execution_evidence_object_key, execution_evidence_sha256::text,
                source_row_count, projection_content_sha256::text, quality_schema_version
           FROM catalog.parcel_publication_source_evidence
          WHERE mirror_rebuild_run_id = $1
          FOR KEY SHARE",
    )
    .bind(run_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(StoredEvidence {
        id: row.try_get("id")?,
        iceberg_table_uuid: row.try_get("iceberg_table_uuid")?,
        iceberg_logical_table: row.try_get("iceberg_logical_table")?,
        iceberg_snapshot_id: row.try_get("iceberg_snapshot_id")?,
        source_record_id: row.try_get("source_record_id")?,
        source_file_asset_id: row.try_get("source_file_asset_id")?,
        execution_schema: row.try_get("execution_evidence_schema_version")?,
        execution_object_key: row.try_get("execution_evidence_object_key")?,
        execution_sha256: row.try_get("execution_evidence_sha256")?,
        source_row_count: row.try_get("source_row_count")?,
        projection_sha256: row.try_get("projection_content_sha256")?,
        quality_schema: row.try_get("quality_schema_version")?,
    })
}

fn verify_exact_idempotent_match(
    stored: &StoredEvidence,
    object_key: &str,
    execution_sha256: &str,
    source_row_count: i64,
    projection_sha256: &str,
    execution: &ParcelPublicationExecutionEvidence,
) -> anyhow::Result<()> {
    if stored.iceberg_table_uuid != execution.iceberg_commit.table_uuid
        || stored.iceberg_logical_table != execution.iceberg_commit.logical_table
        || stored.iceberg_snapshot_id != execution.iceberg_commit.snapshot_id
        || stored.source_record_id != execution.source_record_id
        || stored.source_file_asset_id != execution.source_file_asset_id
        || stored.execution_schema != execution.schema_version
        || stored.execution_object_key != object_key
        || stored.execution_sha256 != execution_sha256
        || stored.source_row_count != source_row_count
        || stored.projection_sha256 != projection_sha256
        || stored.quality_schema != QUALITY_SCHEMA_VERSION
    {
        bail!(
            "mirror rebuild run is already sealed to different evidence; idempotent reuse requires an exact tuple match"
        );
    }
    Ok(())
}
