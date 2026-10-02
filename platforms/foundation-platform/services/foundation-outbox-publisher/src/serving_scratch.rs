//! Shared row, transport, and temporary spill budgets, independent of the staging engine.
use crate::bounded_bytes::MAX_ROW_BYTES;
use anyhow::Context;

/// Default spill ceiling; not a reservation or evidence of free disk capacity.
pub const DEFAULT_MAX_DATABASE_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const SCRATCH_BYTES_ENV: &str = "FOUNDATION_PLATFORM_SERVING_SCRATCH_MAX_BYTES";
/// Maximum number of rows in a bounded ZIP batch.
pub const CURSOR_ROWS: usize = 32;
/// Shared raw/serialized row and transport byte bound.
pub const CURSOR_BYTES: usize = MAX_ROW_BYTES;

/// Read the existing scratch ceiling environment contract.
/// # Errors
/// Rejects values that are not an unsigned UTF-8 byte count.
pub fn configured_maximum_bytes() -> anyhow::Result<u64> {
    let maximum_bytes = match std::env::var(SCRATCH_BYTES_ENV) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("{SCRATCH_BYTES_ENV} must be an unsigned byte count"))?,
        Err(std::env::VarError::NotPresent) => DEFAULT_MAX_DATABASE_BYTES,
        Err(error) => return Err(error.into()),
    };
    Ok(maximum_bytes)
}
