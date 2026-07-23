use std::{fs, path::PathBuf};

use anyhow::{bail, Context as _, Result};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use collection_application::bronze_catalog_recovery::{
    BronzeCatalogRecoveryCatalogWriter, BronzeCatalogRecoveryInput, BronzeCatalogRecoveryMode,
    BronzeCatalogRecoveryObjectReader, BronzeCatalogRecoveryService,
    BronzeCatalogRecoveryStorageError, ExistingBronzeObject,
};
use collection_infrastructure::PgBronzeIngestUnitOfWork;
use foundation_outbox::{object_storage::R2ObjectStorageConfig, R2ObjectStorage};
use futures_util::{stream, StreamExt as _};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use sqlx::PgPool;

use crate::{
    bronze_catalog_recovery_evidence::seal_recovery_manifest,
    bronze_catalog_recovery_manifest::BronzeCatalogRecoveryManifest,
    bronze_object_storage::BronzeCatalogRecoveryObjectStorageReader,
    r2_command_support::{canonical_path, env_path, optional_env, write_json_file},
};

const DEFAULT_MANIFEST_PATH: &str = "target/audit/vworld-bronze-catalog-recovery-manifest.json";
const DEFAULT_REPORT_PATH: &str = "target/audit/bronze-catalog-recovery-execution-report.json";
const DEFAULT_VERIFICATION_CONCURRENCY: usize = 32;
const MAX_VERIFICATION_CONCURRENCY: usize = 64;
const EXECUTION_REPORT_SCHEMA_VERSION: &str =
    "foundation-platform.bronze_catalog_recovery_execution_report.v1";

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryExecutionPolicy {
    mode: BronzeCatalogRecoveryMode,
    selected_source_slug: Option<String>,
    max_candidates: Option<usize>,
    database_url: Option<String>,
}

impl RecoveryExecutionPolicy {
    fn from_values(
        mode: Option<&str>,
        apply_confirmation: Option<&str>,
        selected_source_slug: Option<&str>,
        max_candidates: Option<&str>,
        database_url: Option<&str>,
    ) -> Result<Self> {
        let mode = match mode.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("dry_run" | "dry-run") => BronzeCatalogRecoveryMode::DryRun,
            Some("apply") => BronzeCatalogRecoveryMode::Apply,
            Some(value) => bail!("invalid Bronze Catalog recovery mode {value:?}"),
        };
        let selected_source_slug = selected_source_slug
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let max_candidates = max_candidates
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::parse::<usize>)
            .transpose()
            .map_err(|error| anyhow::anyhow!("invalid MAX_CANDIDATES: {error}"))?;
        if max_candidates == Some(0) {
            bail!("MAX_CANDIDATES must be greater than zero");
        }

        if mode == BronzeCatalogRecoveryMode::DryRun {
            return Ok(Self {
                mode,
                selected_source_slug,
                max_candidates,
                database_url: None,
            });
        }

        if apply_confirmation != Some("APPLY") {
            bail!("apply mode requires explicit APPLY confirmation");
        }
        if max_candidates.is_some() {
            bail!("apply mode forbids MAX_CANDIDATES partial recovery");
        }
        if selected_source_slug.is_none() {
            bail!("apply mode requires one explicit source slug");
        }
        let database_url = database_url
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("apply mode requires DATABASE_URL"))?;

        Ok(Self {
            mode,
            selected_source_slug,
            max_candidates,
            database_url: Some(database_url),
        })
    }
}

#[derive(Clone, Debug)]
struct RecoveryExecutionConfig {
    manifest_path: PathBuf,
    manifest_uri: Option<String>,
    report_path: PathBuf,
    verification_concurrency: usize,
    policy: RecoveryExecutionPolicy,
}

struct BoundedBronzeCatalogRecoveryReader<'a, Reader: ?Sized> {
    inner: &'a Reader,
    max_in_flight: usize,
}

impl<'a, Reader: ?Sized> BoundedBronzeCatalogRecoveryReader<'a, Reader> {
    const fn new(inner: &'a Reader, max_in_flight: usize) -> Self {
        debug_assert!(max_in_flight > 0);
        Self {
            inner,
            max_in_flight,
        }
    }
}

#[async_trait]
impl<Reader> BronzeCatalogRecoveryObjectReader for BoundedBronzeCatalogRecoveryReader<'_, Reader>
where
    Reader: BronzeCatalogRecoveryObjectReader + ?Sized,
{
    async fn read_existing_object(
        &self,
        key: &str,
    ) -> Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError> {
        self.inner.read_existing_object(key).await
    }

    async fn read_existing_objects(
        &self,
        keys: &[String],
    ) -> Vec<Result<Option<ExistingBronzeObject>, BronzeCatalogRecoveryStorageError>> {
        let indexed_keys = keys.iter().cloned().enumerate().collect::<Vec<_>>();
        let mut indexed_results = stream::iter(indexed_keys)
            .map(|(index, key)| async move { (index, self.inner.read_existing_object(&key).await) })
            .buffer_unordered(self.max_in_flight)
            .collect::<Vec<_>>()
            .await;
        indexed_results.sort_unstable_by_key(|(index, _)| *index);
        indexed_results
            .into_iter()
            .map(|(_, result)| result)
            .collect()
    }
}

impl RecoveryExecutionConfig {
    fn from_env() -> Result<Self> {
        let mode = optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_MODE")?;
        let apply_confirmation =
            optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_CONFIRM")?;
        let selected_source_slug =
            optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_SOURCE_SLUG")?;
        let max_candidates =
            optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_MAX_CANDIDATES")?;
        let database_url = optional_env("DATABASE_URL")?;
        let verification_concurrency = parse_verification_concurrency(
            optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_VERIFY_CONCURRENCY")?
                .as_deref(),
        )?;

        Ok(Self {
            manifest_path: env_path(
                "FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_MANIFEST_PATH",
                DEFAULT_MANIFEST_PATH,
            )?,
            manifest_uri: optional_env("FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_MANIFEST_URI")?,
            report_path: env_path(
                "FOUNDATION_PLATFORM_BRONZE_CATALOG_RECOVERY_REPORT_PATH",
                DEFAULT_REPORT_PATH,
            )?,
            verification_concurrency,
            policy: RecoveryExecutionPolicy::from_values(
                mode.as_deref(),
                apply_confirmation.as_deref(),
                selected_source_slug.as_deref(),
                max_candidates.as_deref(),
                database_url.as_deref(),
            )?,
        })
    }
}

#[derive(Debug, Serialize)]
struct RecoveryExecutionSourceReport {
    source_slug: String,
    validated_object_count: u64,
    applied_object_count: u64,
    total_size_bytes: u64,
    excluded_unresolved_object_count: u64,
    ingestion_run_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RecoveryExecutionReport {
    schema_version: &'static str,
    manifest_uri: String,
    manifest_sha256: String,
    manifest_size_bytes: u64,
    verification_concurrency: usize,
    mode: &'static str,
    selected_source_slug: Option<String>,
    max_candidates: Option<usize>,
    started_at_utc: String,
    finished_at_utc: String,
    sources: Vec<RecoveryExecutionSourceReport>,
}

pub(crate) async fn run() -> Result<()> {
    let config = RecoveryExecutionConfig::from_env()?;
    let manifest_bytes = fs::read(&config.manifest_path).with_context(|| {
        format!(
            "failed to read Bronze Catalog recovery manifest {}",
            config.manifest_path.display()
        )
    })?;
    let manifest: BronzeCatalogRecoveryManifest =
        serde_json::from_slice(strip_utf8_bom(&manifest_bytes)).with_context(|| {
            format!(
                "failed to parse Bronze Catalog recovery manifest {}",
                config.manifest_path.display()
            )
        })?;
    validate_manifest_uri_policy(config.policy.mode, config.manifest_uri.as_deref())?;
    let storage_config = R2ObjectStorageConfig::from_env()
        .context("failed to configure R2 for Bronze Catalog recovery")?;
    let bucket_name = storage_config.bucket_name.clone();
    let storage = R2ObjectStorage::from_config(storage_config);
    let (manifest, manifest_uri, manifest_sha256, manifest_size_bytes) =
        if config.policy.mode == BronzeCatalogRecoveryMode::Apply {
            let sealed = seal_recovery_manifest(&storage, &bucket_name, manifest).await?;
            let size_bytes = u64::try_from(sealed.bytes.len())
                .context("sealed recovery manifest size does not fit u64")?;
            (sealed.manifest, sealed.uri, sealed.sha256, size_bytes)
        } else {
            let size_bytes = u64::try_from(manifest_bytes.len())
                .context("recovery manifest size does not fit u64")?;
            (
                manifest,
                config
                    .manifest_uri
                    .clone()
                    .unwrap_or_else(|| canonical_path(&config.manifest_path)),
                sha256_hex(&manifest_bytes),
                size_bytes,
            )
        };
    let started_at = Utc::now();
    let mut inputs = manifest.to_recovery_inputs(
        config.policy.mode,
        &manifest_uri,
        &manifest_sha256,
        started_at,
    )?;
    select_execution_scope(&mut inputs, &config.policy)?;

    let storage_reader = BronzeCatalogRecoveryObjectStorageReader::new(&storage);
    let reader =
        BoundedBronzeCatalogRecoveryReader::new(&storage_reader, config.verification_concurrency);
    let writer = recovery_writer(&config.policy).await?;
    let writer_ref = writer
        .as_ref()
        .map(|writer| writer as &dyn BronzeCatalogRecoveryCatalogWriter);
    let service = BronzeCatalogRecoveryService::new();
    let mut source_reports = Vec::with_capacity(inputs.len());

    for input in inputs {
        let source_slug = input.source.slug.clone();
        let excluded_unresolved_object_count = input.excluded_unresolved_object_count;
        let report = service
            .execute(&reader, writer_ref, input)
            .await
            .with_context(|| format!("Bronze Catalog recovery failed for source {source_slug}"))?;
        source_reports.push(RecoveryExecutionSourceReport {
            source_slug,
            validated_object_count: report.validated_object_count,
            applied_object_count: report.applied_object_count,
            total_size_bytes: report.total_size_bytes,
            excluded_unresolved_object_count,
            ingestion_run_id: report.ingestion_run_id.map(|id| id.as_uuid().to_string()),
        });
    }

    let report = RecoveryExecutionReport {
        schema_version: EXECUTION_REPORT_SCHEMA_VERSION,
        manifest_uri,
        manifest_sha256,
        manifest_size_bytes,
        verification_concurrency: config.verification_concurrency,
        mode: mode_name(config.policy.mode),
        selected_source_slug: config.policy.selected_source_slug.clone(),
        max_candidates: config.policy.max_candidates,
        started_at_utc: timestamp(started_at),
        finished_at_utc: timestamp(Utc::now()),
        sources: source_reports,
    };
    write_json_file(&config.report_path, &report)?;
    tracing::info!(
        mode = report.mode,
        source_count = report.sources.len(),
        report_path = %config.report_path.display(),
        "Bronze Catalog recovery execution completed"
    );
    Ok(())
}

fn validate_manifest_uri_policy(
    mode: BronzeCatalogRecoveryMode,
    manifest_uri: Option<&str>,
) -> Result<()> {
    if mode == BronzeCatalogRecoveryMode::Apply && manifest_uri.is_some() {
        bail!("apply mode seals recovery evidence automatically and forbids MANIFEST_URI override");
    }
    Ok(())
}

fn parse_verification_concurrency(raw: Option<&str>) -> Result<usize> {
    let concurrency = raw
        .map(str::parse::<usize>)
        .transpose()
        .context("recovery verification concurrency must be an integer")?
        .unwrap_or(DEFAULT_VERIFICATION_CONCURRENCY);
    if !(1..=MAX_VERIFICATION_CONCURRENCY).contains(&concurrency) {
        bail!(
            "recovery verification concurrency must be between 1 and {MAX_VERIFICATION_CONCURRENCY}"
        );
    }
    Ok(concurrency)
}

fn select_execution_scope(
    inputs: &mut Vec<BronzeCatalogRecoveryInput>,
    policy: &RecoveryExecutionPolicy,
) -> Result<()> {
    if let Some(source_slug) = &policy.selected_source_slug {
        inputs.retain(|input| input.source.slug == *source_slug);
        if inputs.is_empty() {
            bail!("selected recovery source {source_slug:?} is absent from the manifest");
        }
    }

    if let Some(max_candidates) = policy.max_candidates {
        let mut remaining = max_candidates;
        for input in inputs.iter_mut() {
            input.candidates.truncate(remaining);
            remaining = remaining.saturating_sub(input.candidates.len());
        }
        inputs.retain(|input| !input.candidates.is_empty());
    }
    if inputs.is_empty() {
        bail!("Bronze Catalog recovery execution scope is empty");
    }
    Ok(())
}

async fn recovery_writer(
    policy: &RecoveryExecutionPolicy,
) -> Result<Option<PgBronzeIngestUnitOfWork>> {
    let Some(database_url) = policy.database_url.as_deref() else {
        return Ok(None);
    };
    let pool = PgPool::connect(database_url)
        .await
        .context("failed to connect to Postgres for Bronze Catalog recovery apply")?;
    Ok(Some(PgBronzeIngestUnitOfWork::new(pool)))
}

const fn mode_name(mode: BronzeCatalogRecoveryMode) -> &'static str {
    match mode {
        BronzeCatalogRecoveryMode::DryRun => "dry_run",
        BronzeCatalogRecoveryMode::Apply => "apply",
    }
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn strip_utf8_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes)
}

#[cfg(test)]
mod tests;
