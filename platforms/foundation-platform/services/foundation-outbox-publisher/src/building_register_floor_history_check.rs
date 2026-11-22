//! `check-building-register-floor-history`: the FLOOR history gate before an export (root
//! ADR-0338), in the publisher; it was the Spark job `building_register_floor_history.py`. The
//! decisions and reads are `foundation_publisher_silver::building_register_floor_silver_export::
//! history_gate_run`. Exit 0: the source is already retained; exit 10: run the export.

use std::path::PathBuf;

use foundation_publisher_silver::building_register_floor_silver_export::history_gate_run;

use crate::public_data_control_support::{optional_env_value, required_env_value};

const PREFIX: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_HISTORY_CHECK";

/// Runs the gate; exits with [`history_gate_run::EXPORT_REQUIRED`] when the export must run.
///
/// # Errors
/// The gate refuses the witness, the receipt or the table's history, or a read fails.
pub(crate) async fn run() -> anyhow::Result<()> {
    let receipt = PathBuf::from(required_env_value(&format!("{PREFIX}_RECEIPT_PATH"))?);
    let outcome = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTCOME_PATH"))?);
    let derivation = optional_env_value(&format!("{PREFIX}_DERIVATION"))?;
    let code = history_gate_run::run(&receipt, &outcome, derivation.as_deref()).await?;
    if code != 0 {
        // The pipeline reads this exit code as "export required", as it read the Spark job's.
        std::process::exit(code);
    }
    Ok(())
}
