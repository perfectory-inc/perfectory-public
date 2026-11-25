//! Host scheduling adapter: select committed inputs, then gate, export and load them.
//!
//! The export runs in the pinned control container (it reads the database through its network);
//! the history gate and the `DuckDB` load run here, where the release's `DuckDB` extensions are
//! (root ADR-0599). Their work directory is on the system disk (`DUCKDB_ROOT_ENV`), never on the
//! data disk the handoff is on.
use std::{
    fs::File,
    io::Read as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use crate::building_register_floor_silver_export::{
    floor_load, history_gate_run, select_inputs, FloorInputSelection,
};
use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};

use super::{floor_source::FloorSource, FloorPipelineConfig};

mod native_runtime;

use super::invocation_cleanup::{self, Job};

const MAX_OUTCOME_BYTES: u64 = 256 * 1024;
/// Where the cycle's `DuckDB` stages work: the system disk, as the Silver refresh lanes'
/// (`silver-refresh.sh`).
const DUCKDB_ROOT_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_DUCKDB_ROOT";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LocalExecution {
    pub state_root: PathBuf,
    pub duckdb_root: PathBuf,
    pub image: String,
    pub database_network: String,
    pub database_endpoint: String,
    pub project: String,
    /// This attempt's own directory: the gate's outcome and the Compose override.
    pub attempt: PathBuf,
}

impl LocalExecution {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
        release: &Path,
    ) -> anyhow::Result<Self> {
        let state_root = absolute_directory(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT")?;
        let duckdb_root = absolute_directory(lookup, DUCKDB_ROOT_ENV)?;
        ensure!(
            !state_root.starts_with(release) && !duckdb_root.starts_with(release),
            "FLOOR work directories must be outside the release"
        );
        ensure!(
            state_root != duckdb_root,
            "FLOOR state and DuckDB work must be separate directories"
        );
        let image = super::required_lookup(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE")?;
        validate_image(&image)?;
        let (database_network, database_endpoint) = native_runtime::configuration(lookup)?;
        Ok(Self {
            state_root,
            duckdb_root,
            image,
            database_network,
            database_endpoint,
            project: invocation_cleanup::project_id(Job::Floor, lookup)?,
            attempt: PathBuf::new(),
        })
    }
}

fn absolute_directory(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(super::required_lookup(lookup, key)?);
    ensure!(
        path.is_absolute() && path.is_dir(),
        "{key} must be an existing absolute directory"
    );
    path.canonicalize()
        .with_context(|| format!("cannot resolve {key}"))
}

fn validate_image(image: &str) -> anyhow::Result<()> {
    let digest = image
        .strip_prefix("sha256:")
        .or_else(|| {
            image
                .rsplit_once("@sha256:")
                .filter(|(name, _)| {
                    !name.is_empty()
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b))
                })
                .map(|(_, digest)| digest)
        })
        .context("scheduled FLOOR requires a pinned image ID or digest")?;
    ensure!(
        !image.chars().any(char::is_whitespace)
            && digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid pinned FLOOR image"
    );
    Ok(())
}

fn source(
    selection: &FloorInputSelection,
    derivation: Option<String>,
    now: DateTime<Utc>,
) -> anyhow::Result<FloorSource> {
    let ingested_at = execution_time(
        selection.retained_ingested_at_utc,
        derivation.is_some(),
        now,
    )?;
    Ok(FloorSource {
        floor: selection.floor_source_object.clone(),
        title: selection.title_source_object.clone(),
        ingested_at,
        derivation,
        history: selection.committed_inputs.history().clone(),
    })
}

pub(super) fn stop() -> anyhow::Result<()> {
    invocation_cleanup::run(Job::Floor)
}

pub(super) async fn run() -> anyhow::Result<()> {
    invocation_cleanup::project_id(Job::Floor, &mut |key| std::env::var(key).ok())?;
    let result = run_inner().await;
    let stopped = stop();
    match (result, stopped) {
        (Err(error), Err(cleanup)) => {
            Err(error.context(format!("FLOOR cleanup also failed: {cleanup}")))
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(()), stopped) => stopped,
    }
}

/// Runs one container step of the cycle; logs stream to systemd.
fn run_script(script: &str, native_database_url: &str, step: &str) -> anyhow::Result<()> {
    let status = Command::new("/bin/bash")
        .args(["-c", script])
        // Only this child uses Docker DNS. Keep the host URL and audit connection unchanged.
        .env("DATABASE_URL", native_database_url)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("cannot run the FLOOR {step}"))?;
    ensure!(status.success(), "the FLOOR {step} failed: {status}");
    Ok(())
}

async fn run_inner() -> anyhow::Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "scheduled FLOOR execution requires the Linux data server"
    );
    let mut lookup = |name: &str| std::env::var(name).ok();
    for key in [
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_FLOOR_SOURCE_OBJECT",
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_BUILDING_REGISTER_TITLE_SOURCE_OBJECT",
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC",
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_PATH",
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_INPUT_FILE_BATCH_SIZE",
    ] {
        ensure!(
            lookup(key).is_none(),
            "scheduled FLOOR selects its own inputs and time; remove {key}"
        );
    }
    let release = absolute_directory(&mut lookup, "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_ROOT")?;
    let mut local = LocalExecution::from_lookup(&mut lookup, &release)?;
    let native_database_url = native_runtime::database_url(
        &super::required_lookup(&mut lookup, "DATABASE_URL")?,
        &local.database_endpoint,
    )?;
    let audit = super::RemoteLakehouseAuditConfig::from_lookup(&mut lookup)?;
    let derivation = super::optional_lookup(
        &mut lookup,
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_DERIVATION",
    );
    let selection = select_inputs().await?;
    let floor_source = source(&selection, derivation, Utc::now())?;
    // This directory carries only the current attempt's files, never the next run's skip decision.
    let attempt = tempfile::Builder::new()
        .prefix("floor-cycle-")
        .tempdir_in(&local.state_root)?;
    attempt.path().clone_into(&mut local.attempt);
    native_runtime::write_override(&local, &floor_source.history)?;
    let config = FloorPipelineConfig {
        release_root: release
            .to_str()
            .context("release path is not UTF-8")?
            .to_owned(),
        floor_source,
        local,
        audit,
        native_execution_profile: crate::lakehouse_engine_contract::execution_profile()?,
    };
    let summary = gate_export_and_load(&config, &native_database_url).await?;
    let value: serde_json::Value = serde_json::from_str(&summary)?;
    validate_selected_outcome(&value, &selection.source_snapshot_id)?;
    super::finish_job(&config, summary).await
}

/// The cycle's three steps: the inspected receipt goes through the history gate; an edition the
/// table already retains ends there, any other is exported and appended whole. Returns the
/// outcome the cycle finishes with: the gate's, or the load's run summary.
async fn gate_export_and_load(
    config: &FloorPipelineConfig,
    native_database_url: &str,
) -> anyhow::Result<String> {
    let source = &config.floor_source;
    let run_root = source.root();
    let host_root = PathBuf::from(host_path(config, &run_root));
    std::fs::create_dir_all(&host_root)
        .with_context(|| format!("cannot create {}", host_root.display()))?;
    let check = tempfile::Builder::new()
        .prefix("source-check.")
        .tempdir_in(&host_root)?;
    let check_name = check
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .context("the source check directory is not UTF-8")?;
    let receipt = format!("{run_root}/{check_name}/source.json");
    run_script(
        &super::build_floor_inspection_script(config, &receipt),
        native_database_url,
        "input staging and source inspection",
    )?;
    let outcome = config.local.attempt.join("history.json");
    let gate = history_gate_run::run(
        &source.history,
        &check.path().join("source.json"),
        &outcome,
        source.derivation.as_deref(),
        &config.local.duckdb_root,
    )
    .await?;
    if gate == 0 {
        return read_outcome(&outcome);
    }
    ensure!(
        gate == history_gate_run::EXPORT_REQUIRED,
        "the FLOOR history gate ended with {gate}"
    );
    run_script(
        &super::build_floor_export_script(config),
        native_database_url,
        "export",
    )?;
    let export_summary = PathBuf::from(host_path(
        config,
        &format!("{run_root}/export-summary.json"),
    ));
    let edition =
        floor_load::ready_edition(&serde_json::from_str(&read_outcome(&export_summary)?)?)?;
    let load = floor_load::load(
        Path::new(&host_path(config, &format!("{run_root}/handoff"))),
        &config.local.duckdb_root,
        &edition.source_snapshot_id,
        source.derivation.as_deref(),
        Some(edition.row_count),
        true,
    )
    .await?;
    ensure!(
        load.appended,
        "the FLOOR load committed nothing for {}; the history gate retains a recorded edition \
         before its export runs",
        edition.source_snapshot_id
    );
    Ok(load.summary.to_string())
}

fn read_outcome(path: &Path) -> anyhow::Result<String> {
    ensure!(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "FLOOR outcome must be a regular file"
    );
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_OUTCOME_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_OUTCOME_BYTES,
        "FLOOR outcome exceeds its byte limit"
    );
    String::from_utf8(bytes).context("FLOOR outcome is not UTF-8")
}

/// The host path of a run file the container names under `target/lakehouse/`.
pub(super) fn host_path(config: &FloorPipelineConfig, relative: &str) -> String {
    // Only generated FloorSource paths enter this formatter: the cycle accepts no input path.
    relative.replacen(
        "target/lakehouse/",
        &format!("{}/", config.local.state_root.display()),
        1,
    )
}

fn validate_selected_outcome(value: &serde_json::Value, selected: &str) -> anyhow::Result<()> {
    let identity_matches =
        if value["schema_version"] == "foundation-platform.floor-history-check.v1" {
            value["source_snapshot_id"] == selected
        } else {
            value["source_snapshot_ids"] == serde_json::json!([selected])
        };
    ensure!(
        identity_matches,
        "FLOOR outcome differs from selected ledger evidence"
    );
    Ok(())
}

pub(super) fn native_compose(config: &FloorPipelineConfig) -> String {
    format!(
        "{} -f compose.lakehouse-native.yml -f {}",
        super::LAKEHOUSE_COMPOSE_COMMAND,
        super::shell_quote(&native_runtime::override_path(&config.local).to_string_lossy())
    )
}

fn execution_time(
    retained: Option<DateTime<Utc>>,
    derived: bool,
    now: DateTime<Utc>,
) -> anyhow::Result<DateTime<Utc>> {
    let time = if derived { None } else { retained }.unwrap_or(now);
    // Match the microsecond precision Parquet and Iceberg persist.
    DateTime::from_timestamp_micros(time.timestamp_micros()).context("invalid FLOOR execution time")
}

#[cfg(test)]
mod tests;
