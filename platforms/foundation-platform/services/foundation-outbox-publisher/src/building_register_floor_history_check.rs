//! `check-building-register-floor-history`: the FLOOR history gate (root ADR-0338) as a command of
//! its own, for a verdict or an operator; the scheduled cycle calls the same
//! `foundation_publisher_silver::building_register_floor_silver_export::history_gate_run` in its
//! process. It was the Spark job `building_register_floor_history.py`. Exit 0: the source is
//! already retained; exit 10: the export must run.

use std::path::PathBuf;

use foundation_publisher_silver::building_register_floor_silver_export::{
    history_gate_run, HistoryWitness,
};

use crate::public_data_control_support::{optional_env_value, required_env_value};

const PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_CHECK";

/// Runs the gate on the witness the FLOOR unit names (path and SHA-256 pin) with `DuckDB` working
/// in `..._WORK_DIR`; exits with [`history_gate_run::EXPORT_REQUIRED`] when the export must run.
///
/// # Errors
/// The gate refuses the witness, the receipt or the table's history, or a read fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let witness = HistoryWitness::from_lookup(&mut |key| std::env::var(key).ok(), true)?;
    let receipt = PathBuf::from(required_env_value(&format!("{PREFIX}_RECEIPT_PATH"))?);
    let outcome = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTCOME_PATH"))?);
    let work = PathBuf::from(required_env_value(&format!("{PREFIX}_WORK_DIR"))?);
    let derivation = optional_env_value(&format!("{PREFIX}_DERIVATION"))?;
    let code =
        history_gate_run::run(&witness, &receipt, &outcome, derivation.as_deref(), &work).await?;
    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}
