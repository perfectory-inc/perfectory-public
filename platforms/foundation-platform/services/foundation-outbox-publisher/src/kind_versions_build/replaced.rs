//! What a shadow reads of the replaced table's build: the instant it was built as of, the
//! edition it served.

use super::*;

/// Whether `text` is a UTC instant as the builds write them (`YYYY-MM-DDTHH:MM:SSZ`, with an
/// optional fraction): safe to write into SQL as a literal.
pub(super) fn is_instant(text: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(text).is_ok()
        && text.ends_with('Z')
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'-' | b':' | b'T' | b'Z' | b'.'))
}

/// The instant the replaced table's build commit was built as of: its rows' newest
/// `published_at_utc` (`onbid_public_auction_silver_to_gold.previous_build_time`; a merge that
/// rebuilt some PNUs leaves its own instant on those and the older one on the rest).
///
/// # Errors
/// Returns an error when the table cannot be read at `build`, holds no row, or its instant is not one.
pub(super) fn replaced_as_of(work: &Path, replaced: &str, build: i64) -> anyhow::Result<String> {
    let instant = replaced_value(
        work,
        replaced,
        build,
        "SELECT CAST(max(published_at_utc) AS VARCHAR) FROM @rows@",
    )?
    .with_context(|| format!("{replaced} holds no row at {build}"))?;
    ensure!(
        is_instant(&instant),
        "{replaced} at {build} was built as of {instant:?}, not a UTC instant"
    );
    Ok(instant)
}

/// The served edition the replaced table's build `build` served, when that build records none:
/// what the stage's rule reads of its rows ([`KindVersionsStage::replaced_edition_sql`], the one
/// edition the served boundaries' source rows carry).
///
/// # Errors
/// Returns an error when the table cannot be read at `build`, or its rows carry no one plain
/// edition.
pub(super) fn replaced_edition(
    work: &Path,
    replaced: &str,
    build: i64,
    sql: &str,
) -> anyhow::Result<String> {
    let editions = replaced_value(work, replaced, build, sql)?
        .with_context(|| format!("{replaced} build {build} holds no row of an edition"))?;
    ensure!(
        !editions.contains(',')
            && !editions.is_empty()
            && editions
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte)),
        "{replaced} build {build} holds the editions {editions:?}, not one"
    );
    Ok(editions)
}

/// One text value of the replaced table's build `build`: `select` over `@rows@`, its rows then.
fn replaced_value(
    work: &Path,
    replaced: &str,
    build: i64,
    select: &str,
) -> anyhow::Result<Option<String>> {
    ensure!(
        replaced
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || matches!(byte, b'_' | b'.')),
        "{replaced} is not a plain table name"
    );
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let rows = foundation_publisher_silver::duckdb_parquet_stage::table_at(replaced, build);
    Ok(duck
        .connection()
        .query_row(&select.replace("@rows@", &rows), [], |row| row.get(0))?)
}
