//! Every `DuckDB` the publisher opens holds the one share of what its process may still take,
//! spills at most the one share of what its disk has free, and counts its threads itself.

use super::super::{disk_share, memory_limit, DuckDbParquetStage, DISK_SHARE, MEMORY_SHARE};

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

#[test]
fn the_spill_cap_is_the_share_of_what_the_disk_has_free() -> anyhow::Result<()> {
    assert_eq!(DISK_SHARE, (1, 3));
    assert_eq!(disk_share(9 * GIB), 3 * GIB);
    if !cfg!(unix) {
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let before = foundation_publisher_core::free_space::free_bytes(directory.path())?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let after = foundation_publisher_core::free_space::free_bytes(directory.path())?;
    let text: String = stage.connection().query_row(
        "SELECT current_setting('max_temp_directory_size')",
        [],
        |row| row.get(0),
    )?;
    let cap = reported_bytes(&text)?;
    // What the disk has free moves while other processes write; DuckDB reports the cap rounded.
    let low = disk_share(before.min(after)) / 10 * 9;
    let high = disk_share(before.max(after)) / 10 * 11;
    assert!(
        (low..=high).contains(&cap),
        "the stage may spill {text}, not the share of {before}..{after} bytes free"
    );
    Ok(())
}

/// A byte count as `DuckDB` reports a setting (`'12.3 GiB'`, `'512.0 MiB'`, `'1.2 GB'`).
fn reported_bytes(text: &str) -> anyhow::Result<u64> {
    let (number, unit) = text
        .trim()
        .split_once(' ')
        .ok_or_else(|| anyhow::anyhow!("{text:?} is no byte count"))?;
    let scale: f64 = match unit {
        "bytes" | "B" => 1.0,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "KB" | "kB" => 1e3,
        "MB" => 1e6,
        "GB" => 1e9,
        "TB" => 1e12,
        other => anyhow::bail!("{other:?} is no unit of {text:?}"),
    };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok((number.parse::<f64>()? * scale) as u64)
}
