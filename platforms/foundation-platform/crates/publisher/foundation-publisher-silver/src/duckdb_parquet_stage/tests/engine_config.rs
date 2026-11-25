//! Every `DuckDB` the publisher opens holds the one share of what its process may still take, and
//! counts its threads itself.

use super::super::{memory_limit, DuckDbParquetStage, MEMORY_SHARE};

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn the_limit_is_the_share_of_what_the_process_may_still_take() {
    assert_eq!(MEMORY_SHARE, (3, 5));
    assert_eq!(memory_limit(Some(5 * GIB)), Some(3 * GIB));
    // A platform that publishes no limit keeps DuckDB's own default.
    assert_eq!(memory_limit(None), None);
}

#[test]
fn a_stage_holds_the_share_and_takes_duckdbs_own_thread_count() -> anyhow::Result<()> {
    let before = crate::process_memory::available_bytes();
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let after = crate::process_memory::available_bytes();
    let limit = stage.memory_limit_bytes()?;
    if let (Some(before), Some(after)) = (before, after) {
        // What the process may still take moves while other processes run; the limit lies within
        // the share of the two readings, give or take DuckDB's rounding of the figure it reports.
        let low = memory_limit(Some(before.min(after))).unwrap_or(0) / 10 * 9;
        let high = memory_limit(Some(before.max(after))).unwrap_or(0) / 10 * 11;
        assert!(
            (low..=high).contains(&limit),
            "the stage holds {limit} bytes, not the share of {before}..{after}"
        );
    }
    let threads = |connection: &duckdb::Connection| -> anyhow::Result<i64> {
        Ok(
            connection.query_row("SELECT current_setting('threads')::BIGINT", [], |row| {
                row.get(0)
            })?,
        )
    };
    assert_eq!(
        threads(stage.connection())?,
        threads(&duckdb::Connection::open_in_memory()?)?,
        "a stage takes the thread count DuckDB reads from the cgroup, not one of its own"
    );
    Ok(())
}
