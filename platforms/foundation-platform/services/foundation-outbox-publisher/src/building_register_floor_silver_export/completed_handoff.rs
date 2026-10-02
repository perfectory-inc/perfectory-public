//! Authenticated reuse of one published FLOOR handoff. The ready summary is the sole authority.
use std::{
    fs,
    fs::File,
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use super::blocking::Cancellation;
use crate::bounded_bytes::BoundedBytes;
use anyhow::{bail, ensure, Context};
use serde_json::{json, Value};
use std::io::Read as _;

use super::{
    input_evidence::{self, FileEvidence},
    ExportConfig, ExportReport,
};

// Completion metadata protocol bounds, shared by initial publication and retries.
const MAX_SUMMARY_BYTES: usize = 4 * 1024 * 1024;
const MAX_ARTIFACT_FILES: usize = 4096;
const READY_SCHEMA: &str = "foundation-platform.building_register_floor_silver_handoff_export.v1";
const EVIDENCE_SCHEMA: &str = "foundation-platform.floor_completed_handoff.v1";

pub(super) fn reuse_flag() -> anyhow::Result<bool> {
    const NAME: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_REUSE_COMPLETED_HANDOFF";
    match std::env::var(NAME) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(value) if value == "1" => Ok(true),
        _ => bail!("{NAME} must be 1 when supplied"),
    }
}

pub(super) fn validate_flag(config: &ExportConfig) -> anyhow::Result<()> {
    if config.reuse_completed {
        ensure!(
            config.committed_inputs.is_some()
                && config.exact_inputs.is_some()
                && config.summary_path.is_some()
                && config.max_rows.is_none(),
            "completed FLOOR reuse requires committed full exact inputs and a summary path"
        );
    }
    Ok(())
}

pub(super) fn lock_path(summary: &Path) -> PathBuf {
    let mut name = summary.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

pub(super) fn lock(summary: &Path) -> anyhow::Result<File> {
    let path = lock_path(summary);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_file(),
            "floor lock must be a regular file"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir_all(path.parent().context("floor lock has no parent")?)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("cannot open floor export lock {}", path.display()))?;
    file.try_lock().with_context(|| {
        format!(
            "floor export already active or lock unavailable: {}",
            path.display()
        )
    })?;
    // The inode must remain stable across invocations. Dropping the file unlocks it.
    Ok(file)
}

fn configuration(config: &ExportConfig) -> Value {
    json!({
        "bronze_local_object_root": config.bronze_local_object_root,
        "source_selector": config.source_selector.source_summary(),
        "exact_floor_object": config.exact_inputs.as_ref().map(|inputs| &inputs.floor),
        "exact_title_object": config.exact_inputs.as_ref().map(|inputs| &inputs.title),
        "committed_inputs": config.committed_inputs,
        "source_snapshot_id": config.source_snapshot_id,
        "valid_from_utc": config.valid_from_utc,
        "ingested_at_utc": config.ingested_at_utc,
        "max_rows": config.max_rows,
        "chunk_rows": config.chunk_rows,
        "output_format": config.output_format.wire_name(),
        "title_source_slug": config.title_source_slug,
        "output_path": config.output_path,
        "proposal_input_path": config.proposal_input_path,
        "summary_path": config.summary_path,
    })
}

fn inventory(
    path: &Path,
    limit: usize,
    cancellation: &Cancellation,
) -> anyhow::Result<Vec<PathBuf>> {
    cancellation.check()?;
    let metadata = fs::symlink_metadata(path)?;
    let mut paths = Vec::new();
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            cancellation.check()?;
            let entry = entry?;
            ensure!(
                entry.file_type()?.is_file(),
                "non-file in completed FLOOR output: {}",
                entry.path().display()
            );
            ensure!(
                paths.len() < limit,
                "completed FLOOR artifact file count exceeds bound"
            );
            paths.push(entry.path());
        }
    } else {
        ensure!(
            metadata.file_type().is_file(),
            "completed FLOOR artifact must be a regular file"
        );
        paths.push(path.to_owned());
    }
    paths.sort();
    Ok(paths)
}

fn files(path: &Path, cancellation: &Cancellation) -> anyhow::Result<Vec<FileEvidence>> {
    inventory(path, MAX_ARTIFACT_FILES, cancellation)?
        .iter()
        .map(|part| input_evidence::capture(part, cancellation))
        .collect()
}

fn verify_membership(
    config: &ExportConfig,
    stored: &Value,
    cancellation: &Cancellation,
) -> anyhow::Result<()> {
    let paths = |value: &Value| -> anyhow::Result<Vec<PathBuf>> {
        value
            .as_array()
            .context("completed FLOOR file inventory missing")?
            .iter()
            .map(|entry| {
                entry["path"]
                    .as_str()
                    .map(PathBuf::from)
                    .context("completed FLOOR file path missing")
            })
            .collect()
    };
    ensure!(
        inventory(&config.output_path, MAX_ARTIFACT_FILES, cancellation)?
            == paths(&stored["output_files"])?,
        "completed FLOOR output file membership differs"
    );
    if let Some(path) = &config.proposal_input_path {
        ensure!(
            inventory(path, MAX_ARTIFACT_FILES, cancellation)? == paths(&stored["proposal_files"])?,
            "completed FLOOR proposal file membership differs"
        );
    } else {
        ensure!(
            stored["proposal_files"].is_null(),
            "completed FLOOR has unexpected proposal files"
        );
    }
    Ok(())
}

pub(super) fn serialize_summary(summary: &Value) -> anyhow::Result<Vec<u8>> {
    let mut payload = BoundedBytes::with_error(
        MAX_SUMMARY_BYTES,
        "completed FLOOR summary exceeds byte bound",
    );
    serde_json::to_writer_pretty(&mut payload, summary)?;
    Ok(payload.into_inner())
}

pub(super) fn evidence(
    config: &ExportConfig,
    selected: &[FileEvidence],
    report: &ExportReport,
    cancellation: &Cancellation,
) -> anyhow::Result<Value> {
    let executable =
        std::env::current_exe().context("cannot identify FLOOR producer executable")?;
    Ok(json!({
        "schema_version": EVIDENCE_SCHEMA,
        "producer": input_evidence::capture(&executable, cancellation)?,
        "configuration": configuration(config),
        "selected_input_evidence": selected,
        "report": report,
        "output_files": files(&config.output_path, cancellation)?,
        "proposal_files": config.proposal_input_path.as_ref().map(|path| files(path, cancellation)).transpose()?,
    }))
}

fn read_summary(path: &Path) -> anyhow::Result<Value> {
    let metadata = fs::symlink_metadata(path).context("cannot inspect completed FLOOR summary")?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() <= MAX_SUMMARY_BYTES as u64,
        "completed FLOOR summary must be a bounded regular file"
    );
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_SUMMARY_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_SUMMARY_BYTES,
        "completed FLOOR summary exceeds byte bound"
    );
    serde_json::from_slice(&bytes).context("invalid completed FLOOR summary")
}

/// Recover only the original execution time from this handoff's existing ready evidence.
/// Full byte/producer authentication still runs under the export lock in `reuse`.
pub(super) fn restore_time(config: &mut ExportConfig) -> anyhow::Result<()> {
    if !config.reuse_completed {
        return Ok(());
    }
    let path = config
        .summary_path
        .as_ref()
        .context("FLOOR reuse requires a summary path")?;
    if !summary_exists(path)? {
        return Ok(());
    }
    let summary = read_summary(path)?;
    ensure!(
        summary["schema_version"] == READY_SCHEMA && summary["status"] == "ready",
        "FLOOR summary is not ready"
    );
    let time: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(summary["ingested_at_utc"].clone())?;
    let mut retained = config.clone();
    retained.ingested_at_utc = time;
    ensure!(
        summary["reuse_evidence"]["schema_version"] == EVIDENCE_SCHEMA
            && summary["reuse_evidence"]["configuration"] == configuration(&retained),
        "completed FLOOR retry configuration differs"
    );
    config.ingested_at_utc = time;
    Ok(())
}

pub(super) fn reuse(
    config: &ExportConfig,
    selected: &[FileEvidence],
    summary_path: &Path,
    cancellation: &Cancellation,
) -> anyhow::Result<ExportReport> {
    ensure!(
        config.committed_inputs.is_some()
            && config.exact_inputs.is_some()
            && config.max_rows.is_none(),
        "completed FLOOR reuse requires committed full exact inputs"
    );
    let summary = read_summary(summary_path)?;
    ensure!(
        summary["schema_version"] == READY_SCHEMA && summary["status"] == "ready",
        "FLOOR summary is not ready"
    );
    ensure!(
        summary["completion_claim_allowed"] == false
            && summary["production_cutover_allowed"] == false
            && summary["national_rollout_allowed"] == false,
        "FLOOR summary claims unexpected authority"
    );
    ensure!(
        summary["selected_input_evidence"] == json!(selected)
            && summary["committed_inputs"] == json!(config.committed_inputs),
        "completed FLOOR input evidence differs"
    );
    let stored = summary
        .get("reuse_evidence")
        .context("old FLOOR summary has no reuse evidence")?;
    ensure!(
        stored["configuration"] == configuration(config),
        "completed FLOOR settings differ"
    );
    let report: ExportReport = serde_json::from_value(stored["report"].clone())
        .context("completed FLOOR report evidence missing")?;
    verify_membership(config, stored, cancellation)?;
    let expected = evidence(config, selected, &report, cancellation)?;
    ensure!(summary.get("reuse_evidence") == Some(&expected),
        "completed FLOOR producer, settings or output evidence differs (old summaries cannot be reused)");
    let source = &summary["source"];
    let output = &summary["output"];
    ensure!(
        source["source_snapshot_id"] == config.source_snapshot_id
            && source["selector"] == config.source_selector.source_summary()
            && source["input_object_count"] == 1
            && source["max_rows"] == json!(config.max_rows)
            && source["chunk_rows"] == json!(config.chunk_rows)
            && source["output_format"] == config.output_format.wire_name()
            && source["bronze_local_object_root"]
                == config.bronze_local_object_root.display().to_string()
            && summary["ingested_at_utc"] == json!(config.ingested_at_utc)
            && summary["valid_from_utc"] == json!(config.valid_from_utc)
            && output["path"] == config.output_path.display().to_string()
            && output["format"] == config.output_format.wire_name()
            && output["contract"] == "silver.building_register_floors",
        "completed FLOOR summary bindings differ"
    );
    validate_counts(config, output, &report)?;
    let ids = output["source_snapshot_ids"]
        .as_array()
        .context("completed FLOOR source IDs missing")?;
    ensure!(
        ids.iter()
            .all(|id| id == &Value::String(config.source_snapshot_id.clone()))
            && (report.row_count == 0 || ids.len() == 1),
        "completed FLOOR source IDs differ"
    );
    Ok(report)
}

fn validate_counts(
    config: &ExportConfig,
    output: &Value,
    report: &ExportReport,
) -> anyhow::Result<()> {
    ensure!(
        report.input_object_count == 1
            && output["row_count"] == json!(report.row_count)
            && output["proposal_required_count"] == json!(report.proposal_required_count)
            && report.proposal_required_count <= report.row_count as u64
            && report.normalization_proposal_count <= report.row_count as u64,
        "completed FLOOR report counts differ"
    );
    let proposal = &output["floor_entity_context_pack_input"];
    if let Some(path) = &config.proposal_input_path {
        ensure!(
            proposal["path"] == path.display().to_string()
                && proposal["proposal_count"] == json!(report.normalization_proposal_count),
            "completed FLOOR proposal binding differs"
        );
    } else {
        ensure!(
            proposal.is_null(),
            "completed FLOOR has unexpected proposal"
        );
    }
    Ok(())
}

pub(super) fn summary_exists(path: &Path) -> anyhow::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => bail!(
            "cannot inspect FLOOR ready summary {}: {error}",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_and_read_share_the_summary_bound() -> anyhow::Result<()> {
        let large = json!({"value": "x".repeat(MAX_SUMMARY_BYTES)});
        assert!(serialize_summary(&large).is_err());
        let root = tempfile::tempdir()?;
        let path = root.path().join("large.json");
        let file = File::create(&path)?;
        file.set_len(MAX_SUMMARY_BYTES as u64 + 1)?;
        assert!(read_summary(&path).is_err());
        Ok(())
    }

    #[test]
    fn inventory_is_bounded_before_hashing_files() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "c"] {
            fs::write(root.path().join(name), b"x")?;
        }
        assert!(inventory(root.path(), 2, &Cancellation::default()).is_err());
        Ok(())
    }

    #[test]
    fn stable_lock_excludes_a_second_writer() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let summary = root.path().join("ready.json");
        let first = lock(&summary)?;
        assert!(lock(&summary).is_err());
        drop(first);
        lock(&summary)?;
        assert!(lock_path(&summary).exists());
        Ok(())
    }

    #[test]
    fn output_inventory_catches_extra_and_same_size_changed_part() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let part = root.path().join("part-000001.parquet");
        fs::write(&part, b"first")?;
        let original = files(root.path(), &Cancellation::default())?;
        fs::write(&part, b"other")?;
        assert_ne!(files(root.path(), &Cancellation::default())?, original);
        fs::write(&part, b"first")?;
        fs::write(root.path().join("part-000002.parquet"), b"extra")?;
        assert_ne!(files(root.path(), &Cancellation::default())?, original);
        Ok(())
    }
}
