//! Produces one strict parcel-publication execution-evidence object from one immutable terminal run.

use std::{env, path::PathBuf};

use anyhow::{bail, Context};
use foundation_outbox::{
    object_storage::{ObjectWriteMode, PutObjectRequest},
    EvidenceByteReader, ObjectStorageService, PublishError, R2ObjectStorage,
};
use lakehouse_application::ports::LakehouseCatalog;
use lakehouse_infrastructure::IcebergRestCatalog;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    national_data_collection_rollout_approval_check::validate_parcel_publication_national_approval_check,
    parcel_publication_contract::{
        verify_quality_report, IcebergCommit, ParcelPublicationExecutionEvidence,
        ParcelPublicationQuality, PublicationLimits, PublicationScope, EXECUTION_SCHEMA_VERSION,
        PARCEL_LOGICAL_TABLE, QUALITY_SCHEMA_VERSION,
    },
    r2_command_support::{
        env_path, lakehouse_catalog_config_from_env_file,
        parcel_publication_evidence_database_url_from_env_file,
        parcel_publication_evidence_writer_config_from_env_file, read_json,
    },
    r2_layout::parcel_publication_execution_evidence_key,
};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_PRODUCTION_CUTOVER_CONFIRM";
const RUN_ID_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_EVIDENCE_MIRROR_REBUILD_RUN_ID";
const APPROVAL_CHECK_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_NATIONAL_ROLLOUT_APPROVAL_CHECK_PATH";
const ENV_FILE_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_PUBLICATION_EVIDENCE_ENV_FILE";
const DEFAULT_APPROVAL_CHECK_PATH: &str =
    "target/audit/national-data-collection-rollout-approval-check.json";
const JSON_CONTENT_TYPE: &str = "application/json";
const EVIDENCE_CACHE_CONTROL: &str = "private, max-age=31536000, immutable";

pub async fn run() -> anyhow::Result<()> {
    // Approval gates deliberately run before any database, catalog, or R2 client is constructed.
    let config = Config::from_env()?;
    let pool = PgPool::connect(&parcel_publication_evidence_database_url_from_env_file(
        &config.env_file,
    )?)
    .await
    .context("failed to connect to DATABASE_URL for parcel publication evidence writing")?;
    let catalog =
        IcebergRestCatalog::new(lakehouse_catalog_config_from_env_file(&config.env_file)?)
            .context("failed to initialise Iceberg REST catalog for parcel evidence writing")?;
    let storage = R2ObjectStorage::from_config(
        parcel_publication_evidence_writer_config_from_env_file(&config.env_file)?,
    );

    let result = write_from_terminal_run(&pool, &storage, &catalog, config.run_id).await?;
    println!(
        "parcel-publication-evidence-write-ok mirror_rebuild_run_id={} object_key={} sha256={} outcome={}",
        config.run_id,
        result.object_key,
        result.sha256,
        if result.created { "created" } else { "reused" }
    );
    Ok(())
}

struct Config {
    env_file: PathBuf,
    run_id: Uuid,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if env::var(CONFIRM_ENV).ok().as_deref() != Some("1") {
            bail!(
                "{CONFIRM_ENV}=1 is required because no production cutover approval artifact SSOT exists"
            );
        }
        let approval_path = env_path(APPROVAL_CHECK_PATH_ENV, DEFAULT_APPROVAL_CHECK_PATH)?;
        let approval = read_json(&approval_path, "national rollout approval check").with_context(
            || {
                format!(
                    "{APPROVAL_CHECK_PATH_ENV} must name an existing national rollout approval-check JSON"
                )
            },
        )?;
        validate_parcel_publication_national_approval_check(&approval).with_context(|| {
            format!("{APPROVAL_CHECK_PATH_ENV} does not authorise national parcel publication")
        })?;

        let raw_run_id = env::var(RUN_ID_ENV)
            .with_context(|| format!("Missing required environment variable: {RUN_ID_ENV}"))?;
        let run_id = Uuid::parse_str(raw_run_id.trim())
            .with_context(|| format!("{RUN_ID_ENV} must be a UUID"))?;
        Ok(Self {
            env_file: env_path(ENV_FILE_ENV, ".env.local")?,
            run_id,
        })
    }
}

struct TerminalRun {
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
    publication_scope: JsonValue,
    publication_limits: JsonValue,
}

#[derive(Debug)]
struct WriteResult {
    object_key: String,
    sha256: String,
    created: bool,
}

async fn write_from_terminal_run<S, C>(
    pool: &PgPool,
    storage: &S,
    catalog: &C,
    run_id: Uuid,
) -> anyhow::Result<WriteResult>
where
    S: ObjectStorageService + EvidenceByteReader,
    C: LakehouseCatalog,
{
    let run = read_terminal_run(pool, run_id).await?;
    let (snapshot_id, source_record_id, source_file_asset_id, scope, limits) =
        verify_terminal_run(&run)?;

    let table = catalog
        .get_table_metadata(PARCEL_LOGICAL_TABLE)
        .await
        .context("failed to load parcel table identity from Iceberg REST catalog")?
        .context("Iceberg REST catalog does not contain silver.parcel_boundaries")?;
    if table.table_name != PARCEL_LOGICAL_TABLE || !table.snapshot_ids.contains(&snapshot_id) {
        bail!(
            "terminal mirror run snapshot {snapshot_id} is not present in silver.parcel_boundaries Iceberg metadata"
        );
    }

    let execution = ParcelPublicationExecutionEvidence {
        schema_version: EXECUTION_SCHEMA_VERSION.to_owned(),
        status: "succeeded".to_owned(),
        mirror_rebuild_run_id: run_id,
        source_record_id,
        source_file_asset_id,
        scope,
        limits,
        iceberg_commit: IcebergCommit {
            committed: true,
            logical_table: PARCEL_LOGICAL_TABLE.to_owned(),
            table_uuid: table.table_uuid,
            snapshot_id,
        },
        production_cutover_allowed: true,
        national_rollout_allowed: true,
    };
    execution.validate_publication_claims()?;
    let bytes = serde_json::to_vec(&execution)
        .context("failed to serialize strict parcel publication execution evidence")?;
    write_create_only(storage, &bytes).await
}

async fn read_terminal_run(pool: &PgPool, run_id: Uuid) -> anyhow::Result<TerminalRun> {
    let row = sqlx::query(
        "SELECT status, source_snapshot_id, source_table, source_record_id,
                source_file_asset_id, loaded_row_count, rejected_row_count, srid,
                finished_at IS NOT NULL AS finished, quality_report,
                publication_scope, publication_limits
           FROM serving_postgis.parcel_boundary_mirror_rebuild_run
          WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?
    .with_context(|| format!("unknown parcel mirror rebuild run {run_id}"))?;
    Ok(TerminalRun {
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
        publication_scope: row.try_get("publication_scope")?,
        publication_limits: row.try_get("publication_limits")?,
    })
}

fn verify_terminal_run(
    run: &TerminalRun,
) -> anyhow::Result<(i64, Uuid, Uuid, PublicationScope, PublicationLimits)> {
    if run.status != "succeeded" {
        bail!(
            "parcel mirror rebuild run status must be succeeded, got {}",
            run.status
        );
    }
    if !run.finished {
        bail!("parcel mirror rebuild run finished_at must be present");
    }
    if run.loaded_row_count <= 0 {
        bail!("parcel mirror rebuild run loaded_row_count must be positive");
    }
    if run.rejected_row_count != 0 {
        bail!("parcel mirror rebuild run rejected_row_count must be zero");
    }
    if run.srid != 5179 {
        bail!("parcel mirror rebuild run srid must be 5179");
    }
    if run.source_table != PARCEL_LOGICAL_TABLE {
        bail!("parcel mirror rebuild run source_table must be {PARCEL_LOGICAL_TABLE}");
    }
    let snapshot_text = run
        .source_snapshot_id
        .strip_prefix("iceberg:")
        .context("parcel mirror rebuild run source_snapshot_id must be iceberg:<positive id>")?;
    let snapshot_id = snapshot_text
        .parse::<i64>()
        .context("parcel mirror rebuild run source_snapshot_id must contain a numeric snapshot")?;
    if snapshot_id <= 0 || run.source_snapshot_id != format!("iceberg:{snapshot_id}") {
        bail!(
            "parcel mirror rebuild run source_snapshot_id must be canonical iceberg:<positive id>"
        );
    }
    let source_record_id = run
        .source_record_id
        .context("parcel mirror rebuild run source_record_id must be present")?;
    let source_file_asset_id = run
        .source_file_asset_id
        .context("parcel mirror rebuild run source_file_asset_id must be present")?;
    let quality: ParcelPublicationQuality = serde_json::from_value(run.quality_report.clone())
        .context("parcel mirror rebuild run quality_report is not the strict typed v1 contract")?;
    verify_quality_report(QUALITY_SCHEMA_VERSION, run.loaded_row_count, &quality)?;
    let scope: PublicationScope = serde_json::from_value(run.publication_scope.clone())
        .context("parcel mirror rebuild run publication_scope is not the strict contract")?;
    let limits: PublicationLimits = serde_json::from_value(run.publication_limits.clone())
        .context("parcel mirror rebuild run publication_limits is not the strict contract")?;
    if scope.kind != "national"
        || !scope.complete
        || !limits.object_limit.is_null()
        || !limits.row_limit.is_null()
        || !limits.shard_limit.is_null()
    {
        bail!(
            "parcel mirror rebuild run must record complete national scope with no object, row, or shard limits"
        );
    }
    Ok((
        snapshot_id,
        source_record_id,
        source_file_asset_id,
        scope,
        limits,
    ))
}

async fn write_create_only<S>(storage: &S, bytes: &[u8]) -> anyhow::Result<WriteResult>
where
    S: ObjectStorageService + EvidenceByteReader,
{
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    let object_key = parcel_publication_execution_evidence_key(&sha256)?;
    let request = PutObjectRequest {
        key: object_key.clone(),
        body: bytes.to_vec(),
        content_type: JSON_CONTENT_TYPE.to_owned(),
        cache_control: EVIDENCE_CACHE_CONTROL.to_owned(),
        write_mode: ObjectWriteMode::CreateOnly,
        sha256: Some(sha256.clone()),
    };
    let created = match storage.put_object(request).await {
        Ok(()) => true,
        Err(PublishError::ObjectAlreadyExists { key }) if key == object_key => {
            let stored = storage
                .read_evidence_bytes(&object_key)
                .await
                .context("failed to read colliding parcel publication evidence object")?;
            if stored != bytes {
                bail!(
                    "content-addressed parcel publication evidence collision contains different exact bytes"
                );
            }
            false
        }
        Err(error) => {
            return Err(error).context("failed to write parcel publication execution evidence")
        }
    };
    Ok(WriteResult {
        object_key,
        sha256,
        created,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use foundation_outbox::{
        object_storage::{ObjectWriteMode, PutObjectRequest},
        EvidenceByteReader, ObjectStorageService, PublishError,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::{verify_terminal_run, write_create_only, TerminalRun};
    use crate::parcel_publication_contract::GEOMETRY_REPAIR_STRATEGY;

    fn terminal_run() -> TerminalRun {
        TerminalRun {
            status: "succeeded".to_owned(),
            source_snapshot_id: "iceberg:841361364657368626".to_owned(),
            source_table: "silver.parcel_boundaries".to_owned(),
            source_record_id: Some(Uuid::nil()),
            source_file_asset_id: Some(Uuid::max()),
            loaded_row_count: 2,
            rejected_row_count: 0,
            srid: 5179,
            finished: true,
            quality_report: json!({
                "schema_version": "foundation-platform.parcel_publication_quality.v1",
                "object_count": 1,
                "expected_row_count": 2,
                "loaded_row_count": 2,
                "invalid_srid_count": 0,
                "invalid_geometry_count": 0,
                "empty_geometry_count": 0,
                "nonpositive_area_count": 0,
                "source_srid": "EPSG:4326",
                "target_srid": "EPSG:5179",
                "geometry_repair_strategy": GEOMETRY_REPAIR_STRATEGY
            }),
            publication_scope: json!({"kind": "national", "complete": true}),
            publication_limits: json!({
                "object_limit": null,
                "row_limit": null,
                "shard_limit": null
            }),
        }
    }

    #[test]
    fn terminal_run_is_the_only_source_of_publication_provenance() -> anyhow::Result<()> {
        let run = terminal_run();
        let (snapshot_id, record_id, asset_id, scope, limits) = verify_terminal_run(&run)?;
        assert_eq!(snapshot_id, 841_361_364_657_368_626);
        assert_eq!(record_id, Uuid::nil());
        assert_eq!(asset_id, Uuid::max());
        assert_eq!(scope.kind, "national");
        assert!(limits.object_limit.is_null());
        Ok(())
    }

    #[test]
    fn non_success_or_noncanonical_terminal_run_is_rejected() {
        let mut failed = terminal_run();
        failed.status = "failed".to_owned();
        assert!(verify_terminal_run(&failed)
            .expect_err("failed run")
            .to_string()
            .contains("status must be succeeded"));

        let mut live_alias = terminal_run();
        live_alias.source_snapshot_id = "iceberg:latest".to_owned();
        assert!(verify_terminal_run(&live_alias)
            .expect_err("live alias")
            .to_string()
            .contains("numeric snapshot"));

        let mut bounded = terminal_run();
        bounded.publication_scope = json!({"kind": "bounded", "complete": false});
        bounded.publication_limits =
            json!({"object_limit": 1, "row_limit": 1000, "shard_limit": 1});
        assert!(verify_terminal_run(&bounded)
            .expect_err("bounded run")
            .to_string()
            .contains("complete national scope"));
    }

    #[test]
    fn null_publication_scope_is_rejected_instead_of_inferred() {
        let mut missing_scope = terminal_run();
        missing_scope.publication_scope = json!(null);

        let error = verify_terminal_run(&missing_scope)
            .expect_err("a run without recorded scope must not produce publication evidence");

        assert!(
            error
                .to_string()
                .contains("publication_scope is not the strict contract"),
            "unexpected rejection: {error:#}"
        );
    }

    struct CollisionStorage {
        existing: Vec<u8>,
        requests: Mutex<Vec<PutObjectRequest>>,
    }

    #[async_trait]
    impl ObjectStorageService for CollisionStorage {
        async fn put_object(&self, request: PutObjectRequest) -> Result<(), PublishError> {
            self.requests
                .lock()
                .expect("recording mutex")
                .push(request.clone());
            Err(PublishError::ObjectAlreadyExists { key: request.key })
        }

        async fn read_object_sha256(&self, _key: &str) -> Result<Option<String>, PublishError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl EvidenceByteReader for CollisionStorage {
        async fn read_evidence_bytes(&self, _key: &str) -> Result<Vec<u8>, PublishError> {
            Ok(self.existing.clone())
        }
    }

    #[tokio::test]
    async fn create_only_collision_reuses_only_exact_stored_bytes() -> anyhow::Result<()> {
        let bytes = br#"{"schema_version":"fixture"}"#;
        let exact = CollisionStorage {
            existing: bytes.to_vec(),
            requests: Mutex::new(Vec::new()),
        };
        let reused = write_create_only(&exact, bytes).await?;
        assert!(!reused.created);
        {
            let requests = exact.requests.lock().expect("recording mutex");
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].write_mode, ObjectWriteMode::CreateOnly);
        }

        let conflict = CollisionStorage {
            existing: b"different exact bytes".to_vec(),
            requests: Mutex::new(Vec::new()),
        };
        let error = write_create_only(&conflict, bytes)
            .await
            .expect_err("same content-addressed key with different bytes must fail");
        assert!(error.to_string().contains("different exact bytes"));
        Ok(())
    }
}
