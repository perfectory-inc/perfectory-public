//! `load-building-register-floor-silver`: appends the FLOOR edition the export just wrote (root
//! ADR-0559, ADR-0599); it was `silver_scalar_handoff_to_lakehouse.py` (Spark). The load is
//! `foundation_publisher_silver::building_register_floor_silver_export::floor_load`.
//!
//! The scheduled cycle calls the load in its process; this command runs the same code for a
//! verdict or an operator. It reads the edition and its row count from the export's ready summary
//! (`..._EXPORT_SUMMARY_PATH`) and the rows from `..._HANDOFF_PATH`, works in `..._WORK_DIR`, and
//! writes the run summary to `..._OUTPUT_PATH`. With `..._LIVE_WRITE=1` it commits; without, it
//! checks and stages only, with the read-only credentials when the environment holds them.

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use foundation_publisher_silver::building_register_floor_silver_export::floor_load;
use serde_json::Value;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SILVER_LOAD";
const MAX_SUMMARY_BYTES: u64 = 4 * 1024 * 1024;

fn read_summary(path: &Path) -> anyhow::Result<Value> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    ensure!(
        u64::try_from(bytes.len())? <= MAX_SUMMARY_BYTES,
        "the FLOOR export summary exceeds its byte limit"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

/// Runs the load and writes its run summary.
///
/// # Errors
/// The export summary is not a ready FLOOR edition, the edition is already recorded (the history
/// gate retains a recorded edition before an export runs), a check refuses the rows, or the commit
/// fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let export_summary = PathBuf::from(required_env_value(&format!(
        "{PREFIX}_EXPORT_SUMMARY_PATH"
    ))?);
    let output = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_PATH"))?);
    let work = PathBuf::from(required_env_value(&format!("{PREFIX}_WORK_DIR"))?);
    let derivation = optional_env_value(&format!("{PREFIX}_DERIVATION"))?;
    let live = optional_env_value(&format!("{PREFIX}_LIVE_WRITE"))?.as_deref() == Some("1");
    let handoff = PathBuf::from(required_env_value(&format!("{PREFIX}_HANDOFF_PATH"))?);
    let edition = floor_load::ready_edition(&read_summary(&export_summary)?)?;
    let load = floor_load::load(
        &handoff,
        &work,
        &edition.source_snapshot_id,
        derivation.as_deref(),
        Some(edition.row_count),
        live,
    )
    .await?;
    if load.summary["status"] == "already_appended" {
        bail!(
            "{} is already recorded in {}; the history gate retains a recorded edition before \
             its export runs",
            edition.source_snapshot_id,
            floor_load::TABLE
        );
    }
    ensure!(
        !live || load.appended,
        "the FLOOR load committed nothing for {}",
        edition.source_snapshot_id
    );
    eprintln!(
        "building-register-floor-silver-load staged_files={} appended={} snapshot_id={}",
        load.staged_files,
        load.appended,
        load.snapshot_id
            .map_or_else(|| "none".to_owned(), |id| id.to_string())
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .with_context(|| format!("cannot create {}", output.display()))?;
    std::io::Write::write_all(&mut file, format!("{}\n", load.summary).as_bytes())?;
    Ok(())
}
