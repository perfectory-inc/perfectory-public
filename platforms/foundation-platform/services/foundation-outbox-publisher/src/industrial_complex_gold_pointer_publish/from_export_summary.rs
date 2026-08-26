//! Publishes every Gold pointer an export summary describes.
//!
//! Root ADR-0036 decision 7 made the export summary the input of the pointer publish, naming the
//! fields "그 이름 그대로". Nothing read it. Filling the 1,442-row pointer table therefore meant
//! invoking the single-complex command 1,442 times with thirteen values copied by hand out of a
//! JSON document each time — which is a machine for producing exactly the invented lineage this
//! repository blocks in three places. This command is the reader that was missing.
//!
//! Each entry is checked against its stored object before its pointer is written, through the same
//! `artifact_verification` the single-complex command uses.
//!
//! Re-runs are safe. A complex whose pointer already names the version being published is counted
//! and skipped, because the artifact id is a pure function of the Gold snapshot and the complex
//! (root ADR-0036 decision 5) — already being at that version *is* the publication having happened.
//! A pointer sitting at some other version is advanced with that version as the expected one, so a
//! concurrent publish is detected rather than overwritten.

use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use foundation_shared_kernel::ids::{ComplexId, LakehouseComplexId};
use lakehouse_application::ports::IndustrialComplexGoldPointerReader as _;
use lakehouse_application::PublishIndustrialComplexGoldPointer;
use lakehouse_domain::LakehouseError;
use lakehouse_infrastructure::{
    PgIndustrialComplexGoldPointerReader, PgLakehousePublicationUnitOfWork,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    artifact_verification::{
        ClaimedGoldProfileArtifact, PointerPublication, VerifiedGoldProfileArtifact,
    },
    resolve_catalog_complex_ids,
};
use crate::industrial_complex_gold_profile_store::{
    local_root, ProfileObjectStore, ProfileStoreConfig,
};
use catalog_infrastructure::PgCatalogRepository;

const RUN_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_gold_pointer_publish_run.v1";
const EXPORT_SUMMARY_SCHEMA_VERSION: &str =
    "foundation-platform.industrial_complex_gold_profile_export_summary.v1";

const DATABASE_URL_ENV: &str = "DATABASE_URL";
const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_CONFIRM_PUBLISH";
const EXPORT_SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_EXPORT_SUMMARY_PATH";
const PROFILE_STORAGE_DRIVER_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_PROFILE_STORAGE_DRIVER";
const PROFILE_LOCAL_ROOT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_PROFILE_LOCAL_ROOT";
const PROFILE_URL_TEMPLATE_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_PROFILE_URL_TEMPLATE";
const SOURCE_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_SOURCE";
const SOURCE_URL_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_SOURCE_URL";
const SOURCE_EXTERNAL_ID_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_SOURCE_EXTERNAL_ID";
const EXPECTED_ARTIFACT_COUNT_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_EXPECTED_ARTIFACT_COUNT";
const RUN_SUMMARY_PATH_ENV: &str =
    "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_GOLD_POINTERS_RUN_SUMMARY_PATH";

/// The export summary fields this command reads.
///
/// A narrow view of `ProfileExportSummary` on purpose: the pointer publish consumes the artifact
/// inventory and the address, and re-declaring the export's tallies here would make this file a
/// second, silently drifting copy of that schema.
#[derive(Debug, Deserialize)]
struct ExportSummary {
    schema_version: String,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    profile_url_template: Option<String>,
    output_storage_driver: String,
    output_bucket: Option<String>,
    artifacts: Vec<ExportedArtifact>,
}

/// One exported profile, named exactly as the export wrote it.
#[derive(Debug, Deserialize)]
struct ExportedArtifact {
    #[serde(rename = "complex_id")]
    lakehouse_complex_id: LakehouseComplexId,
    current_version: String,
    profile_object_key: String,
    profile_size_bytes: u64,
    profile_row_count: u64,
    profile_checksum_sha256: String,
    source_snapshot_id: String,
    iceberg_snapshot_id: String,
    published_at_utc: String,
}

/// What one run of this command did.
#[derive(Debug, Serialize)]
struct PointerPublishRunSummary {
    schema_version: &'static str,
    generated_at_utc: String,
    publish_run_id: Uuid,
    export_summary_path: String,
    gold_table: String,
    gold_iceberg_snapshot_id: String,
    profile_url_template: String,
    profile_storage_driver: &'static str,
    profile_bucket: Option<String>,
    artifact_count: u64,
    published_count: u64,
    already_current_count: u64,
    published_complex_ids: Vec<String>,
}

#[derive(Debug)]
struct Config {
    database_url: String,
    export_summary_path: PathBuf,
    store: ProfileStoreConfig,
    profile_url_template_override: Option<String>,
    source: String,
    source_url: Option<String>,
    source_external_id: Option<String>,
    expected_artifact_count: Option<u64>,
    run_summary_path: Option<PathBuf>,
}

/// Publishes the Gold pointers described by an export summary.
pub(crate) async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let summary = read_export_summary(&config.export_summary_path)?;

    ensure!(
        summary.schema_version == EXPORT_SUMMARY_SCHEMA_VERSION,
        "{} declares schema {} but this command reads {EXPORT_SUMMARY_SCHEMA_VERSION}",
        config.export_summary_path.display(),
        summary.schema_version
    );

    let profile_url_template = resolve_profile_url_template(
        config.profile_url_template_override.as_deref(),
        summary.profile_url_template.as_deref(),
    )
    .with_context(|| {
        format!(
            "neither {} nor {PROFILE_URL_TEMPLATE_ENV} states an address template; a pointer \
                 without one leaves consumers holding an object key they cannot fetch",
            config.export_summary_path.display()
        )
    })?;

    let artifact_count =
        u64::try_from(summary.artifacts.len()).context("artifact count overflow")?;
    if let Some(expected) = config.expected_artifact_count {
        ensure!(
            expected == artifact_count,
            "{} carries {artifact_count} artifacts but {expected} were expected",
            config.export_summary_path.display()
        );
    }

    let store = ProfileObjectStore::open(&config.store)?;
    ensure!(
        store.storage_driver() == summary.output_storage_driver,
        "the export wrote with the '{}' driver but this run reads with '{}'",
        summary.output_storage_driver,
        store.storage_driver()
    );
    // A run pointed at a different bucket than the export wrote to would verify objects that are
    // not the published ones. The export records the bucket it reached for exactly this comparison
    // (root ADR-0039 decision 5).
    ensure!(
        store.bucket() == summary.output_bucket.as_deref(),
        "the export wrote to bucket {:?} but this run reads bucket {:?}",
        summary.output_bucket,
        store.bucket()
    );

    let pool = PgPool::connect(config.database_url.as_str())
        .await
        .context("failed to connect to database for industrial-complex Gold pointer publish")?;
    let catalog_repository = PgCatalogRepository::new(pool.clone());
    let lakehouse_complex_ids = summary
        .artifacts
        .iter()
        .map(|artifact| artifact.lakehouse_complex_id)
        .collect::<Vec<_>>();
    let complex_ids =
        resolve_catalog_complex_ids(&catalog_repository, &lakehouse_complex_ids).await?;
    let reader = PgIndustrialComplexGoldPointerReader::new(pool.clone());
    let use_case = PublishIndustrialComplexGoldPointer::new(Arc::new(
        PgLakehousePublicationUnitOfWork::new(pool),
    ));

    let mut published_complex_ids = Vec::new();
    let mut already_current_count = 0_u64;
    for (index, (artifact, complex_id)) in summary.artifacts.iter().zip(complex_ids).enumerate() {
        let existing_version = reader
            .find_industrial_complex_gold_pointer(complex_id)
            .await
            .with_context(|| {
                format!(
                    "failed to read the existing pointer for complex {}",
                    artifact.lakehouse_complex_id
                )
            })?
            .map(|pointer| pointer.current_version);

        if existing_version.as_deref() == Some(artifact.current_version.as_str()) {
            already_current_count += 1;
            continue;
        }

        publish_one(
            &use_case,
            &store,
            artifact,
            complex_id,
            existing_version,
            &config,
            &profile_url_template,
        )
        .await
        .with_context(|| {
            format!(
                "failed to publish the Gold pointer for complex {} (artifacts[{index}])",
                artifact.lakehouse_complex_id
            )
        })?;
        published_complex_ids.push(artifact.lakehouse_complex_id.to_string());
    }

    let published_count =
        u64::try_from(published_complex_ids.len()).context("published count overflow")?;
    let run_summary = PointerPublishRunSummary {
        schema_version: RUN_SCHEMA_VERSION,
        generated_at_utc: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        publish_run_id: Uuid::now_v7(),
        export_summary_path: config.export_summary_path.display().to_string(),
        gold_table: summary.gold_table.clone(),
        gold_iceberg_snapshot_id: summary.gold_iceberg_snapshot_id.clone(),
        profile_url_template,
        profile_storage_driver: store.storage_driver(),
        profile_bucket: store.bucket().map(ToOwned::to_owned),
        artifact_count,
        published_count,
        already_current_count,
        published_complex_ids,
    };
    if let Some(path) = &config.run_summary_path {
        write_run_summary(path, &run_summary)?;
    }

    tracing::info!(
        gold_table = %run_summary.gold_table,
        gold_iceberg_snapshot_id = %run_summary.gold_iceberg_snapshot_id,
        profile_storage_driver = run_summary.profile_storage_driver,
        profile_bucket = run_summary.profile_bucket.as_deref().unwrap_or("(local)"),
        artifact_count = run_summary.artifact_count,
        published_count = run_summary.published_count,
        already_current_count = run_summary.already_current_count,
        "industrial-complex Gold pointer publish run succeeded"
    );
    Ok(())
}

/// Verifies one artifact against its stored object and publishes its pointer.
async fn publish_one(
    use_case: &PublishIndustrialComplexGoldPointer,
    store: &ProfileObjectStore,
    artifact: &ExportedArtifact,
    complex_id: ComplexId,
    expected_current_version: Option<String>,
    config: &Config,
    profile_url_template: &str,
) -> anyhow::Result<()> {
    let verified = VerifiedGoldProfileArtifact::verify(
        store,
        ClaimedGoldProfileArtifact {
            lakehouse_complex_id: artifact.lakehouse_complex_id,
            current_version: artifact.current_version.clone(),
            object_key: artifact.profile_object_key.clone(),
            checksum_sha256: artifact.profile_checksum_sha256.clone(),
            size_bytes: artifact.profile_size_bytes,
        },
    )
    .await?;

    let published_at = DateTime::parse_from_rfc3339(artifact.published_at_utc.as_str())
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| {
            format!(
                "published_at_utc {} is not an RFC3339 timestamp",
                artifact.published_at_utc
            )
        })?;

    let result = use_case
        .execute(verified.into_publish_input(
            complex_id,
            PointerPublication {
                expected_current_version,
                profile_url_template: profile_url_template.to_owned(),
                spatial_locator_object_key: None,
                source: config.source.clone(),
                source_url: config.source_url.clone(),
                source_external_id: config.source_external_id.clone(),
                source_snapshot_id: artifact.source_snapshot_id.clone(),
                iceberg_snapshot_id: artifact.iceberg_snapshot_id.clone(),
                profile_row_count: artifact.profile_row_count,
                spatial_locator_size_bytes: None,
                published_at,
            },
        ))
        .await;

    match result {
        Ok(_) => Ok(()),
        // Another run advanced this pointer between the read above and this write. Reporting it as
        // a conflict is the point of the expected-version field; retrying here would be the
        // overwrite it exists to prevent.
        Err(LakehouseError::IndustrialComplexGoldPointerVersionConflict { expected, current }) => {
            bail!(
                "the pointer moved during this run: expected {expected:?} but found {current:?}. \
                 Re-run to publish against the pointer's new version."
            )
        }
        Err(error) => Err(error.into()),
    }
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = optional_env(CONFIRM_ENV)?.unwrap_or_default();
        ensure!(
            confirm.eq_ignore_ascii_case("true"),
            "{CONFIRM_ENV} must be true"
        );
        Ok(Self {
            database_url: required_env(DATABASE_URL_ENV)?,
            export_summary_path: PathBuf::from(required_env(EXPORT_SUMMARY_PATH_ENV)?),
            store: ProfileStoreConfig::parse(
                required_env(PROFILE_STORAGE_DRIVER_ENV)?.as_str(),
                local_root(optional_env(PROFILE_LOCAL_ROOT_ENV)?),
            )
            .with_context(|| format!("{PROFILE_STORAGE_DRIVER_ENV}/{PROFILE_LOCAL_ROOT_ENV}"))?,
            profile_url_template_override: optional_env(PROFILE_URL_TEMPLATE_ENV)?,
            source: required_env(SOURCE_ENV)?,
            source_url: optional_env(SOURCE_URL_ENV)?,
            source_external_id: optional_env(SOURCE_EXTERNAL_ID_ENV)?,
            expected_artifact_count: optional_env(EXPECTED_ARTIFACT_COUNT_ENV)?
                .map(|value| {
                    value.parse::<u64>().with_context(|| {
                        format!("{EXPECTED_ARTIFACT_COUNT_ENV} must be an unsigned integer")
                    })
                })
                .transpose()?,
            run_summary_path: optional_env(RUN_SUMMARY_PATH_ENV)?.map(PathBuf::from),
        })
    }
}

/// Chooses the address template a pointer will carry.
///
/// Root ADR-0037: writing an object is recording a fact and pointing consumers at it is a
/// publication, so the address may be stated at export time or at publish time — but a publication
/// without one is refused rather than guessed. `None` here is what makes the run stop.
///
/// The publish-time value wins, so an export made before a serving host existed can be published
/// once one does, without rewriting immutable objects to change an address they never carried.
fn resolve_profile_url_template(
    publish_time: Option<&str>,
    export_time: Option<&str>,
) -> Option<String> {
    publish_time.or(export_time).map(ToOwned::to_owned)
}

fn read_export_summary(path: &Path) -> anyhow::Result<ExportSummary> {
    let raw = std::fs::read(path)
        .with_context(|| format!("failed to read the export summary {}", path.display()))?;
    serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse the export summary {}", path.display()))
}

fn write_run_summary(path: &Path, summary: &PointerPublishRunSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create summary directory {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(summary)
        .context("failed to serialize the Gold pointer publish run summary")?;
    std::fs::write(path, payload)
        .with_context(|| format!("failed to write the run summary {}", path.display()))
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.trim().to_owned())),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

#[cfg(test)]
mod tests;
