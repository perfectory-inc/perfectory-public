//! Host scheduling adapter: select committed inputs and run the existing full pipeline.
use std::{
    fs::File,
    io::Read as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_outbox_publisher::building_register_floor_silver_export::{
    select_inputs, FloorInputSelection,
};

use super::{floor_source::FloorSource, RemoteLakehouseJob, RemoteLakehouseJobConfig};

mod cleanup;
mod native_runtime;

const MAX_OUTCOME_BYTES: u64 = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LocalExecution {
    pub state_root: PathBuf,
    pub ivy_cache: PathBuf,
    pub image: String,
    pub database_network: String,
    pub database_endpoint: String,
    pub project: String,
    pub outcome: PathBuf,
}

impl LocalExecution {
    fn from_lookup(
        lookup: &mut impl FnMut(&str) -> Option<String>,
        release: &Path,
    ) -> anyhow::Result<Self> {
        let state_root = absolute_directory(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT")?;
        let ivy_cache = absolute_directory(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE")?;
        ensure!(
            !state_root.starts_with(release) && !ivy_cache.starts_with(release),
            "FLOOR work directories must be outside the release"
        );
        ensure!(
            state_root != ivy_cache,
            "FLOOR state and dependency cache must be separate directories"
        );
        let image = super::required_lookup(lookup, "FOUNDATION_PLATFORM_LAKEHOUSE_CONTROL_IMAGE")?;
        validate_image(&image)?;
        let (database_network, database_endpoint) = native_runtime::configuration(lookup)?;
        Ok(Self {
            state_root,
            ivy_cache,
            image,
            database_network,
            database_endpoint,
            project: cleanup::project_id(lookup)?,
            outcome: PathBuf::new(),
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
    cleanup::run()
}

pub(super) async fn run() -> anyhow::Result<()> {
    cleanup::project_id(&mut |key| std::env::var(key).ok())?;
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
    // This file carries only the current subprocess result, never the next run's skip decision.
    let attempt = tempfile::Builder::new()
        .prefix("floor-cycle-")
        .tempdir_in(&local.state_root)?;
    local.outcome = attempt.path().join("outcome.json");
    native_runtime::write_override(&local, &floor_source.history)?;
    let config = RemoteLakehouseJobConfig {
        ssh_target: String::new(),
        ssh_path: String::new(),
        env_file: String::new(),
        remote_root: release
            .to_str()
            .context("release path is not UTF-8")?
            .to_owned(),
        execute: true,
        job: RemoteLakehouseJob::PipelineFull,
        input_path_override: None,
        input_file_batch_size_override: None,
        source_snapshot: super::BuildingRegisterSourceSnapshotConfig::NotRequired,
        floor_source: Some(floor_source),
        local: Some(local.clone()),
        audit,
        iceberg_packages: crate::lakehouse_engine_contract::iceberg_packages()?.to_owned(),
        native_execution_profile: crate::lakehouse_engine_contract::execution_profile()?,
    };
    let script = super::build_remote_script(&config);
    // Logs stream to systemd, so an hours-long Spark run cannot accumulate output in Rust RAM.
    let status = Command::new("/bin/bash")
        .args(["-c", &script])
        // Only this child uses Docker DNS. Keep the host URL and audit connection unchanged.
        .env("DATABASE_URL", native_database_url)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .context("cannot run scheduled FLOOR pipeline")?;
    ensure!(
        status.success(),
        "scheduled FLOOR pipeline failed: {status}"
    );
    let summary = read_outcome(&local.outcome)?;
    let value: serde_json::Value = serde_json::from_str(&summary)?;
    validate_selected_outcome(&value, &selection.source_snapshot_id)?;
    super::finish_job(&config, summary).await
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

pub(super) fn host_path(config: &RemoteLakehouseJobConfig, relative: &str) -> String {
    config.local.as_ref().map_or_else(
        || relative.to_owned(),
        |local| {
            // Only generated FloorSource paths enter this formatter. Local execution rejects
            // externally supplied input paths before constructing its configuration.
            relative.replacen(
                "target/lakehouse/",
                &format!("{}/", local.state_root.display()),
                1,
            )
        },
    )
}

pub(super) fn environment_script(config: &RemoteLakehouseJobConfig) -> String {
    if let Some(local) = &config.local {
        format!("export COMPOSE_PROJECT_NAME={}\nexport FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT={}\nexport FOUNDATION_PLATFORM_LAKEHOUSE_IVY_CACHE={}\nexport FOUNDATION_PLATFORM_LAKEHOUSE_UID=185\nexport FOUNDATION_PLATFORM_LAKEHOUSE_GID=\"$(id -g)\"\n",
            super::shell_quote(&local.project), super::shell_quote(&local.state_root.to_string_lossy()), super::shell_quote(&local.ivy_cache.to_string_lossy()))
    } else {
        let env_file = super::shell_quote(&config.env_file);
        format!("if [ ! -f {env_file} ]; then\n  echo 'missing remote lakehouse env file' >&2\n  exit 2\nfi\nset -a\n. {env_file}\nset +a\nexport FOUNDATION_PLATFORM_LAKEHOUSE_GID=\"$(id -g)\"\n")
    }
}

pub(super) fn capture_outcome(config: &RemoteLakehouseJobConfig, quoted_source: &str) -> String {
    config.local.as_ref().map_or_else(String::new, |local| {
        format!(
            "(set -C; cat {quoted_source} > {})\n",
            super::shell_quote(&local.outcome.to_string_lossy())
        )
    })
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

pub(super) fn runtime_setup(config: &RemoteLakehouseJobConfig) -> String {
    if let Some(local) = &config.local {
        let image = super::shell_quote(&local.image);
        let remote_state = super::shell_quote(&local.state_root.join("remote").to_string_lossy());
        format!("export FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT={remote_state}\nmkdir -p {remote_state}\ndocker image inspect {image} >/dev/null\n{compose} --profile lakehouse-batch config --images | while IFS= read -r image; do docker image inspect \"$image\" >/dev/null; done\n{compose} --profile lakehouse-batch run --rm --no-deps --pull never lakehouse-target-init\n", compose=super::LAKEHOUSE_COMPOSE_COMMAND)
    } else {
        format!("mkdir -p 'target/remote-lakehouse/ai' 'target/remote-lakehouse/summaries'\n{} -f compose.lakehouse-native.yml --profile lakehouse-control build lakehouse-control\n", super::LAKEHOUSE_COMPOSE_COMMAND)
    }
}

pub(super) fn native_compose(config: &RemoteLakehouseJobConfig) -> String {
    let base = format!(
        "{} -f compose.lakehouse-native.yml",
        super::LAKEHOUSE_COMPOSE_COMMAND
    );
    config.local.as_ref().map_or(base.clone(), |local| {
        format!(
            "{base} -f {}",
            super::shell_quote(&native_runtime::override_path(local).to_string_lossy())
        )
    })
}

pub(super) fn spark_compose(config: &RemoteLakehouseJobConfig) -> String {
    if config.local.is_some() {
        native_compose(config)
    } else {
        super::LAKEHOUSE_COMPOSE_COMMAND.to_owned()
    }
}

fn execution_time(
    retained: Option<DateTime<Utc>>,
    derived: bool,
    now: DateTime<Utc>,
) -> anyhow::Result<DateTime<Utc>> {
    let time = if derived { None } else { retained }.unwrap_or(now);
    // Match the precision that Parquet and Spark persist.
    DateTime::from_timestamp_micros(time.timestamp_micros()).context("invalid FLOOR execution time")
}

#[cfg(test)]
mod tests;
