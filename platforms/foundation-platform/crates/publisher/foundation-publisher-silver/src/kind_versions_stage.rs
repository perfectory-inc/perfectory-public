//! What every parcel kind Gold built as a table of appended row versions shares (root ADR-0564).
//!
//! A kind's stage reads its own Silver at pins, keeps the served parcel edition's PNUs, reads a
//! row under a code a reorganization renumbered under the served code (root ADR-0329 §3), names
//! the PNUs a partial run rebuilds and folds them into `built`. The publisher's one build command
//! (`kind_versions_build`) plans, diffs and appends whatever stage it is handed, so a new lane is a
//! stage, a kind line and a job: no build command of its own (ADR-0564 §6, "레인마다 다른 것은 자기
//! 단계와 빌드 명령뿐").

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{ensure, Context};
use duckdb::Connection;

use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};
use crate::parcel_nearby_stage::{current_rows, Source};

/// The parcels; the served edition's are every kind's universe.
pub const PARCEL_SOURCE: &str = crate::parcel_nearby_stage::PARCEL_SOURCE;
/// The code change table a renumbering is read from.
pub const CODE_CHANGE_SOURCE: &str = crate::parcel_nearby_stage::CODE_CHANGE_SOURCE;

/// What one read left behind: the served parcels, the kind's rows, and each input's read time.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct KindInputs {
    /// PNUs of the served edition.
    pub served_parcels: u64,
    /// The kind's rows on served PNUs.
    pub rows: u64,
    /// Milliseconds each input took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
}

/// One parcel kind's Gold in `DuckDB`.
pub trait KindVersionsStage: Sync {
    /// The column that holds the document.
    fn section_column(&self) -> &'static str;
    /// Every input, in the order the summary names them.
    fn inputs(&self) -> Vec<&'static str>;
    /// The inputs whose change may move any parcel's document.
    fn whole_table_inputs(&self) -> Vec<&'static str> {
        vec![CODE_CHANGE_SOURCE]
    }
    /// The build's code version: a change of it rebuilds every PNU.
    fn code_version(&self) -> String;
    /// Reads the inputs at `source` for the served `edition`; `suffix` (`_old`) reads the base's pins
    /// beside the new ones for [`Self::changed_pnus`]. With an empty suffix it leaves an empty `built`.
    ///
    /// # Errors
    /// Returns an error when an input cannot be read or the edition holds no served parcel.
    fn load_inputs(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        edition: &str,
        suffix: &str,
    ) -> anyhow::Result<KindInputs>;
    /// Leaves `targets(pnu)`, the PNUs whose rows or served parcel changed between the two reads,
    /// and returns how many.
    ///
    /// # Errors
    /// Returns an error when a side has not been read.
    fn changed_pnus(&self, connection: &Connection) -> anyhow::Result<u64>;
    /// Builds the documents of every PNU (`all`) or of `targets` into `built`.
    ///
    /// # Errors
    /// Returns an error when the stamps are not plain ids or `DuckDB` refuses the statement.
    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()>;
}

/// Whether `text` is safe to write into SQL as a literal or a name part.
pub(crate) fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// `template` with each `@name@` in it replaced by its value.
pub(crate) fn fill(template: &str, values: &[(&str, &str)]) -> String {
    values
        .iter()
        .fold(template.to_owned(), |text, (name, value)| {
            text.replace(&format!("@{name}@"), value)
        })
}

pub(crate) fn elapsed(at: Instant) -> u64 {
    u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// The PNU of `alias`'s row under the served code: a renumbered 동·리 code is translated, the lot kept.
pub(crate) fn served_pnu(alias: &str) -> String {
    format!("CASE WHEN r.dst IS NULL THEN {alias}.pnu ELSE r.dst || substr({alias}.pnu, 11, 9) END")
}

/// The join that [`served_pnu`] reads `r` from.
pub(crate) fn renumbering_join(alias: &str, suffix: &str) -> String {
    format!("LEFT JOIN renumbering{suffix} r ON substr({alias}.pnu, 1, 10) = r.src")
}

/// Checks the suffix and the edition, and leaves `served<suffix>(pnu)` of the served `edition` and
/// `renumbering<suffix>(src, dst)` of the code change table at `source`; returns the served count.
///
/// # Errors
/// Returns an error when the edition or suffix is not plain, an input cannot be read, or the
/// edition holds no served parcel.
pub(crate) fn load_served_and_renumbering(
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
    suffix: &str,
    read_millis: &mut BTreeMap<String, u64>,
) -> anyhow::Result<u64> {
    ensure!(
        is_plain(edition),
        "the edition {edition:?} is not a plain snapshot id"
    );
    ensure!(
        suffix.is_empty()
            || suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
        "the suffix {suffix:?} is not a plain name"
    );
    let at = Instant::now();
    let parcels = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == PARCEL_SOURCE)
        .context("no lakehouse contract for the parcels")?;
    let predicate = parcels
        .current_row_predicate
        .context("silver.parcel_boundaries names its current rows by a predicate")?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE served{suffix} AS SELECT DISTINCT pnu FROM {table} \
         WHERE source_snapshot_id = '{edition}' AND ({predicate}) AND pnu IS NOT NULL;",
        table = source.of(PARCEL_SOURCE)?
    ))?;
    read_millis.insert(PARCEL_SOURCE.to_owned(), elapsed(at));
    let served: i64 =
        connection.query_row(&format!("SELECT count(*) FROM served{suffix}"), [], |row| {
            row.get(0)
        })?;
    ensure!(served > 0, "edition {edition} holds no served parcel");

    // The code change table's pairs in force, and the codes the served edition holds of them.
    let at = Instant::now();
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE code_changes AS \
         SELECT kind, old_code, new_code, level, effective_date, source \
         FROM {changes} WHERE kind IN ('pair', 'supersession');",
        changes = source.of(CODE_CHANGE_SOURCE)?
    ))?;
    read_millis.insert(CODE_CHANGE_SOURCE.to_owned(), elapsed(at));
    connection.execute_batch(PAIR_ROWS_SQL)?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE served_codes AS \
         SELECT DISTINCT substr(pnu, 1, 10) AS code FROM served{suffix} WHERE substr(pnu, 1, 10) IN ( \
           SELECT old_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri') \
           UNION SELECT new_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri'));"
    ))?;
    connection.execute_batch(RENUMBERED_INTO_SQL)?;
    if !suffix.is_empty() {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE renumbering{suffix} AS SELECT src, dst FROM renumbering;"
        ))?;
    }
    Ok(u64::try_from(served)?)
}

/// The current rows of `table` at `source`, projecting `columns`, in the temporary table `into`
/// (the contract's one rendering, root ADR-0564 §4); returns the time it took.
///
/// # Errors
/// Returns an error when the table cannot be read or ties where its contract forbids it.
pub(crate) fn load_current(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    into: &str,
) -> anyhow::Result<u64> {
    let at = Instant::now();
    let rows = current_rows(connection, source, table, columns, into)?;
    ensure!(
        rows == format!("SELECT * FROM {into}"),
        "the current rows of {table} were not left in {into}"
    );
    Ok(elapsed(at))
}

/// The tables the publisher's commit (`gold_versions_commit`) creates on the stage's connection.
///
/// A stage must not leave a table of these names: a `TEMP` one would shadow the commit's own (`DuckDB`
/// reads `temp` before `main`), as the first lane-1 shadow build met (2026-11-22: the 총괄표제부
/// Silver rows read as the head's current rows).
pub const COMMIT_TABLES: [&str; 2] = ["current_rows", "appended"];

/// Errors when `connection` holds a table [`COMMIT_TABLES`] names.
///
/// # Errors
/// Returns an error naming the table, or when `DuckDB` cannot list its tables.
pub fn ensure_no_commit_tables(connection: &Connection) -> anyhow::Result<()> {
    let names = COMMIT_TABLES.map(|name| format!("'{name}'")).join(", ");
    let held: Option<String> = connection.query_row(
        &format!(
            "SELECT string_agg(schema_name || '.' || table_name, ', ') FROM duckdb_tables()              WHERE table_name IN ({names})"
        ),
        [],
        |row| row.get(0),
    )?;
    ensure!(
        held.is_none(),
        "the stage left {} on the connection, a name the commit creates",
        held.unwrap_or_default()
    );
    Ok(())
}

/// The SQL texts every kind's code version folds in beside its own: the code views.
pub(crate) const SHARED_SQL: [&str; 2] = [PAIR_ROWS_SQL, RENUMBERED_INTO_SQL];
