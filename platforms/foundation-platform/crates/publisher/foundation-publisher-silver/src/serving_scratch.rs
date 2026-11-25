//! Shared row and transport budgets, independent of the staging engine.
//!
//! The spill has no budget here: every `DuckDB` takes `DuckDB`'s own ceiling on the disk it spills
//! to (`duckdb_parquet_stage::engine_config`).
use crate::bounded_bytes::MAX_ROW_BYTES;

/// Maximum number of rows in a bounded ZIP batch.
pub const CURSOR_ROWS: usize = 32;
/// Shared raw/serialized row and transport byte bound.
pub const CURSOR_BYTES: usize = MAX_ROW_BYTES;
