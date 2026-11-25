//! Shared row and transport budgets, independent of the staging engine.
//!
//! The spill has no budget here: every `DuckDB` spills at most a share of what its disk has free
//! (`duckdb_parquet_stage::DISK_SHARE`, `open_engine`).
use crate::bounded_bytes::MAX_ROW_BYTES;

/// Maximum number of rows in a bounded ZIP batch.
pub const CURSOR_ROWS: usize = 32;
/// Shared raw/serialized row and transport byte bound.
pub const CURSOR_BYTES: usize = MAX_ROW_BYTES;
