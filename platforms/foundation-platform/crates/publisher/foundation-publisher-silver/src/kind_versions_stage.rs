//! What every parcel kind Gold built as a table of appended row versions shares (root ADR-0564).
//!
//! A kind's stage declares its row sets (its Silver, the columns it keeps) and folds one 시도's rows
//! into `built`. The reading is shared and regional (rules.txt "DuckDB ports are regional, one
//! pass"; root ADR-0559 §7): [`spool`] reads each input at its pin once, the served parcel edition's
//! PNUs and every row set re-keyed under the served code (a code a reorganization renumbered, root
//! ADR-0329 §3), into local Parquet split by the served PNU's 시도; [`load_region`] then loads one
//! 시도 of them, the build folds it, and the next 시도 replaces it. The nation is never one table. The publisher's one build command
//! (`kind_versions_build`) plans, diffs and appends whatever stage it is handed, so a new lane is a
//! stage, a kind line and a job: no build command of its own (ADR-0564 §6, "레인마다 다른 것은 자기
//! 단계와 빌드 명령뿐").

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use anyhow::{ensure, Context};
use duckdb::Connection;

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};
use crate::parcel_nearby_stage::Source;

/// The parcels; the served edition's are every kind's universe.
pub const PARCEL_SOURCE: &str = crate::parcel_nearby_stage::PARCEL_SOURCE;
/// The code change table a renumbering is read from.
pub const CODE_CHANGE_SOURCE: &str = crate::parcel_nearby_stage::CODE_CHANGE_SOURCE;

/// What one [`spool`] left behind: the served parcels, the 시도 they lie in, and each input's read
/// time.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Spooled {
    /// PNUs of the served edition.
    pub served_parcels: u64,
    /// The 시도 (two-digit PNU prefixes) the served edition holds.
    pub sidos: BTreeSet<String>,
    /// Milliseconds each input took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
}

/// One table of a kind's rows: a Silver's current rows, each re-keyed to the served PNU.
#[derive(Clone, Debug)]
pub struct RowSet {
    /// The table a region load leaves (`<name><suffix>`).
    pub name: &'static str,
    /// The Silver it reads, at its pin, by its contract's current-row rule; with `prepared`, the
    /// table the stage's [`KindVersionsStage::prepare`] left (`<source><suffix>`), read as it is.
    pub source: &'static str,
    /// The column that names the parcel a row is on: the row's 시도 and its served PNU are this
    /// column's (`pnu` for a row of one parcel; a 부속지번 row also lies on its `sub_pnu`).
    pub pnu: &'static str,
    /// The Silver's columns the expressions read (as `m.<column>`), beside [`Self::pnu`].
    pub reads: Vec<&'static str>,
    /// `(expression over the current row m and the joins, column)`, in the table's order after
    /// `pnu`. `@suffix@` in it names the read's suffix (`renumbering@suffix@`).
    pub columns: Vec<(String, &'static str)>,
    /// `LEFT JOIN`s the expressions read beside `m` (a view the stage prepared, or the read's
    /// `renumbering@suffix@` for a second PNU of the row); `@suffix@` as in the columns.
    pub joins: Vec<String>,
    /// Whether `source` is a table the stage prepared rather than a Silver at its pin.
    pub prepared: bool,
    /// Whether the table keeps a row once (the kind's rows are distinct on their columns).
    pub distinct: bool,
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
    /// The tables of the kind's rows (each loaded per 시도 by [`load_region`]).
    fn row_sets(&self) -> Vec<RowSet>;
    /// Leaves what a row set reads beside its Silver, from the inputs at `source`: the tables a
    /// `prepared` row set names and the views a row set's joins read (each `<name><suffix>`).
    /// [`spool`] calls it after the read's renumbering and before the row sets. A prepared table
    /// holds the whole nation, so a stage prepares only what is small, states its size and refuses
    /// a larger one; a view is read by the one row set that joins it, in its own spool. Returns
    /// each input's read time.
    ///
    /// # Errors
    /// Returns an error when an input cannot be read or a prepared table is larger than its bound.
    fn prepare(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        suffix: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let _ = (connection, source, suffix);
        Ok(BTreeMap::new())
    }
    /// Whether a row set's current rows are chosen per 시도.
    ///
    /// They are chosen in each 시도's [`load_region`] rather than over
    /// the nation in [`spool`]: the spool then copies every version of the rows, with the columns
    /// the contract's rule ranks by, and the rule (its one rendering, root ADR-0564 §4) runs on one
    /// 시도 at a time. It holds only for a Silver whose key names the row set's parcel column, so
    /// that every version of a key lies in one 시도; [`spool`] refuses any other. A stage whose
    /// inputs are hundreds of millions of rows answers `true` (the parcel panel: 402,659,127 land
    /// use rows, 2026-11-22), so no read sorts the nation's rows at once.
    fn current_rows_per_region(&self) -> bool {
        false
    }
    /// Leaves an empty `built`, the table [`Self::build`] inserts into.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` refuses the statement.
    fn create_built(&self, connection: &Connection) -> anyhow::Result<()>;
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

/// The PNU `column` under the served code: a renumbered 동·리 code is translated through the join
/// `alias` ([`renumbering_join`]), the lot kept.
pub(crate) fn served_pnu(column: &str, alias: &str) -> String {
    format!(
        "CASE WHEN {alias}.dst IS NULL THEN {column} ELSE {alias}.dst || substr({column}, 11, 9) END"
    )
}

/// The join that [`served_pnu`] reads `alias` from: the renumbering of the read `suffix` names.
pub(crate) fn renumbering_join(column: &str, alias: &str, suffix: &str) -> String {
    format!("LEFT JOIN renumbering{suffix} {alias} ON substr({column}, 1, 10) = {alias}.src")
}

/// A spool directory as `DuckDB` SQL text.
fn sql_dir(dir: &Path) -> anyhow::Result<String> {
    let text = dir
        .to_str()
        .context("the spool directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the spool directory must not contain a quote"
    );
    Ok(text)
}

/// `COPY` of `query` (which has a `pnu` column) to `<dir>/<name>`, one directory per 시도 of `pnu`.
const SPOOL_SQL: &str =
    "COPY (SELECT *, substr(pnu, 1, 2) AS sido FROM (@query@)) TO '@dir@/@name@' \
    (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (sido), OVERWRITE_OR_IGNORE)";

/// The spool query of a row set whose current rows a 시도's load chooses
/// ([`KindVersionsStage::current_rows_per_region`]): the served PNU, the set's columns, and every
/// column the contract's rule reads (key, order, removal) as `rule_<n>`, of every version.
const REGIONAL_SPOOL_QUERY: &str = "SELECT @pnu@ AS pnu, @kept@, @rule@ \
    FROM (SELECT * FROM @read@) m @join@ @joins@ WHERE @column@ IS NOT NULL";
/// A 시도 of that spool as the relation the contract's rule reads: the rule's columns under their
/// own names, the served PNU and the set's columns apart from them (`foundation_served_pnu`,
/// `foundation_kept_<n>`), so a set column named like a key column does not shadow it.
const REGIONAL_LOAD_SOURCE: &str =
    "(SELECT pnu AS foundation_served_pnu, @kept@, @rule@ FROM @part@)";
/// The texts the per-시도 rule adds to a stage's code version beside [`SHARED_SQL`].
pub(crate) const REGIONAL_SQL: [&str; 2] = [REGIONAL_SPOOL_QUERY, REGIONAL_LOAD_SOURCE];

/// Reads every input of `stage` at `source` once, for the served `edition`, into `dir`.
///
/// It writes the served
/// PNUs (`served<suffix>/sido=<시도>`) and each row set re-keyed to the served code
/// (`<name><suffix>/sido=<시도>`), and leaves `renumbering<suffix>(src, dst)`. `suffix` (`_old`)
/// reads the base's pins beside the new ones for a partial run.
///
/// # Errors
/// Returns an error when the edition or suffix is not plain, an input cannot be read or ties where
/// its contract forbids it, or the edition holds no served parcel.
pub fn spool(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
    suffix: &str,
    dir: &Path,
) -> anyhow::Result<Spooled> {
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
    let dir_text = sql_dir(dir)?;
    let mut read_millis = BTreeMap::new();

    // The served parcels, once.
    let at = Instant::now();
    let parcels = contract(PARCEL_SOURCE)?;
    let predicate = parcels
        .current_row_predicate
        .context("silver.parcel_boundaries names its current rows by a predicate")?;
    connection.execute_batch(&fill(
        SPOOL_SQL,
        &[
            (
                "query",
                &format!(
                    "SELECT DISTINCT pnu FROM {table} WHERE source_snapshot_id = '{edition}' \
                     AND ({predicate}) AND pnu IS NOT NULL",
                    table = source.of(PARCEL_SOURCE)?
                ),
            ),
            ("dir", &dir_text),
            ("name", &format!("served{suffix}")),
        ],
    ))?;
    read_millis.insert(PARCEL_SOURCE.to_owned(), elapsed(at));
    let served_glob = format!("'{dir_text}/served{suffix}/*/*.parquet'");
    let served: i64 = connection.query_row(
        &format!("SELECT count(*) FROM read_parquet({served_glob})"),
        [],
        |row| row.get(0),
    )?;
    ensure!(served > 0, "edition {edition} holds no served parcel");
    let sidos = connection
        .prepare(&format!(
            "SELECT DISTINCT substr(pnu, 1, 2) FROM read_parquet({served_glob}) ORDER BY 1"
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<BTreeSet<_>, _>>()?;

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
         SELECT DISTINCT substr(pnu, 1, 10) AS code FROM read_parquet({served_glob}) \
         WHERE substr(pnu, 1, 10) IN ( \
           SELECT old_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri') \
           UNION SELECT new_code FROM pair_rows WHERE level IN ('eupmyeondong', 'ri'));"
    ))?;
    connection.execute_batch(RENUMBERED_INTO_SQL)?;
    if !suffix.is_empty() {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE renumbering{suffix} AS SELECT src, dst FROM renumbering;"
        ))?;
    }

    // What the row sets read beside their Silver, then each row set, once, re-keyed to the served
    // code.
    read_millis.extend(stage.prepare(connection, source, suffix)?);
    for set in stage.row_sets() {
        let at = Instant::now();
        if stage.current_rows_per_region() && !set.prepared {
            spool_regional_row_set(connection, source, &set, suffix, &dir_text)?;
        } else {
            spool_row_set(connection, source, &set, suffix, &dir_text)?;
        }
        if !set.prepared {
            read_millis.insert(set.source.to_owned(), elapsed(at));
        }
    }
    Ok(Spooled {
        served_parcels: u64::try_from(served)?,
        sidos,
        read_millis,
    })
}

fn contract(table: &str) -> anyhow::Result<&'static lakehouse_domain::LakehouseTableContract> {
    lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The current rows of `table` at `source` as SQL text, by its contract's one rendering (root
/// ADR-0564 §4), projecting `columns`: what a stage's [`KindVersionsStage::prepare`] reads. `filter`
/// keeps rows before the rule ranks them, so it must keep or drop a key's rows together (it reads
/// the key's columns only). A table whose key must not tie is refused: only a row set's spool
/// checks ties.
///
/// # Errors
/// Returns an error when `table` has no newest-per-key contract, forbids ties, or is not pinned.
pub(crate) fn current_rows_query(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
    columns: &[&str],
    filter: &str,
) -> anyhow::Result<String> {
    let contract = contract(table)?;
    ensure!(
        contract.current_row_predicate.is_none(),
        "{table} selects its current rows by a predicate too; this reader applies only the newest-per-key rule"
    );
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{table} keeps no versions per key"))?;
    ensure!(
        latest.keep_ties,
        "{table} forbids ties on its key; read it as a row set, whose spool checks them"
    );
    let read = with_contract_columns(connection, contract, &source.of(table)?)?;
    Ok(latest.current_rows_sql(&read, columns, filter))
}

/// The key columns a row set's spool carries for the tie check (`<column>` as `tie_<n>`).
const TIE_PREFIX: &str = "tie_";

fn spool_row_set(
    connection: &Connection,
    source: &Source<'_>,
    set: &RowSet,
    suffix: &str,
    dir_text: &str,
) -> anyhow::Result<()> {
    let mut projected: Vec<&str> = vec![set.pnu];
    projected.extend(set.reads.iter().copied().filter(|name| *name != set.pnu));
    // A prepared table is already the rows: the stage read its inputs by their current-row rule.
    let (rows, ties, pinned): (String, &[&str], String) = if set.prepared {
        ensure!(
            set.source
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "the prepared table {} is not a plain name",
            set.source
        );
        let table = format!("{}{suffix}", set.source);
        let none: &[&str] = &[];
        (format!("SELECT * FROM {table}"), none, table)
    } else {
        let contract = contract(set.source)?;
        ensure!(
            contract.current_row_predicate.is_none(),
            "{} selects its current rows by a predicate too; this reader applies only the newest-per-key rule",
            set.source
        );
        let latest = contract
            .current_row_latest()
            .with_context(|| format!("{} keeps no versions per key", set.source))?;
        let pinned = source.of(set.source)?;
        // A column the table gained after the pinned snapshot reads as NULL there.
        let read = with_contract_columns(connection, contract, &pinned)?;
        let ties: &[&str] = if latest.keep_ties { &[] } else { latest.key };
        for key in ties {
            if !projected.contains(key) {
                projected.push(key);
            }
        }
        (
            latest.current_rows_sql(&read, &projected, "TRUE"),
            ties,
            pinned,
        )
    };
    let at_suffix = |text: &str| fill(text, &[("suffix", suffix)]);
    let joins = set
        .joins
        .iter()
        .map(|join| at_suffix(join.as_str()))
        .collect::<Vec<_>>()
        .join(" ");
    let kept = set
        .columns
        .iter()
        .map(|(expression, column)| format!("{} AS {column}", at_suffix(expression.as_str())))
        .chain(
            ties.iter()
                .enumerate()
                .map(|(index, key)| format!("m.{key} AS {TIE_PREFIX}{index}")),
        )
        .collect::<Vec<_>>()
        .join(", ");
    let name = format!("{}{suffix}", set.name);
    let column = format!("m.{}", set.pnu);
    connection.execute_batch(&fill(
        SPOOL_SQL,
        &[
            (
                "query",
                &format!(
                    "SELECT {pnu} AS pnu, {kept} FROM ({rows}) m {join} {joins} \
                     WHERE {column} IS NOT NULL",
                    pnu = served_pnu(&column, "r"),
                    join = renumbering_join(&column, "r", suffix),
                ),
            ),
            ("dir", dir_text),
            ("name", &name),
        ],
    ))?;
    if !ties.is_empty() {
        let glob = format!("'{dir_text}/{name}/*/*.parquet'");
        let keys = (0..ties.len())
            .map(|index| format!("{TIE_PREFIX}{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let (rows, distinct): (i64, i64) = connection.query_row(
            &format!("SELECT count(*), count(DISTINCT ({keys})) FROM read_parquet({glob})"),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == distinct,
            "{} ({pinned}) holds {} current rows tied on their version order",
            set.source,
            rows - distinct
        );
    }
    Ok(())
}

/// The rule of a row set whose current rows a 시도's load chooses, and the columns it reads in the
/// order the spool carries them (`rule_<n>`): the key, then the order, then the removal column.
///
/// # Errors
/// Refuses a source with no newest-per-key rule, one that also selects by a predicate, and one
/// whose key does not name the row set's parcel column: its versions could lie in two 시도.
fn regional_rule(
    set: &RowSet,
) -> anyhow::Result<(
    lakehouse_domain::LakehouseCurrentRowLatest,
    Vec<&'static str>,
)> {
    let contract = contract(set.source)?;
    ensure!(
        contract.current_row_predicate.is_none(),
        "{} selects its current rows by a predicate too; this reader applies only the newest-per-key rule",
        set.source
    );
    let latest = contract
        .current_row_latest()
        .with_context(|| format!("{} keeps no versions per key", set.source))?;
    ensure!(
        latest.key.contains(&set.pnu),
        "{}'s key {:?} does not name {}: its versions could lie in two 시도, so its current rows \
         cannot be chosen per 시도",
        set.source,
        latest.key,
        set.pnu
    );
    let mut rule: Vec<&'static str> = Vec::new();
    for column in latest
        .key
        .iter()
        .chain(latest.newest_first)
        .copied()
        .chain(latest.removal_column)
    {
        if !rule.contains(&column) {
            rule.push(column);
        }
    }
    Ok((latest, rule))
}

/// Spools every version of `set`'s rows, re-keyed to the served code, with the columns its rule
/// reads ([`REGIONAL_SPOOL_QUERY`]); [`load_region`] chooses the current ones per 시도.
fn spool_regional_row_set(
    connection: &Connection,
    source: &Source<'_>,
    set: &RowSet,
    suffix: &str,
    dir_text: &str,
) -> anyhow::Result<()> {
    let (_, rule) = regional_rule(set)?;
    let contract = contract(set.source)?;
    // A column the table gained after the pinned snapshot reads as NULL there.
    let read = with_contract_columns(connection, contract, &source.of(set.source)?)?;
    let at_suffix = |text: &str| fill(text, &[("suffix", suffix)]);
    let kept = set
        .columns
        .iter()
        .map(|(expression, column)| format!("{} AS {column}", at_suffix(expression.as_str())))
        .collect::<Vec<_>>()
        .join(", ");
    let rule = rule
        .iter()
        .enumerate()
        .map(|(index, column)| format!("m.{column} AS rule_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let joins = set
        .joins
        .iter()
        .map(|join| at_suffix(join.as_str()))
        .collect::<Vec<_>>()
        .join(" ");
    let column = format!("m.{}", set.pnu);
    let query = fill(
        REGIONAL_SPOOL_QUERY,
        &[
            ("pnu", &served_pnu(&column, "r")),
            ("kept", &kept),
            ("rule", &rule),
            ("read", &read),
            ("join", &renumbering_join(&column, "r", suffix)),
            ("joins", &joins),
            ("column", &column),
        ],
    );
    connection.execute_batch(&fill(
        SPOOL_SQL,
        &[
            ("query", &query),
            ("dir", dir_text),
            ("name", &format!("{}{suffix}", set.name)),
        ],
    ))?;
    Ok(())
}

/// One 시도 of a regionally spooled row set: its current rows by the contract's rule, on served
/// PNUs only, as `pnu` and the set's columns. A source that forbids ties is refused when a key of
/// the 시도 ties.
fn regional_rows(
    connection: &Connection,
    set: &RowSet,
    part: &str,
    served: &str,
    distinct: &str,
) -> anyhow::Result<String> {
    let (latest, rule) = regional_rule(set)?;
    let kept = set
        .columns
        .iter()
        .enumerate()
        .map(|(index, (_, column))| format!("{column} AS foundation_kept_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let renamed = rule
        .iter()
        .enumerate()
        .map(|(index, column)| format!("rule_{index} AS {column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let relation = fill(
        REGIONAL_LOAD_SOURCE,
        &[("kept", &kept), ("rule", &renamed), ("part", part)],
    );
    if !latest.keep_ties {
        let keys = latest.key.join(", ");
        let (rows, keyed): (i64, i64) = connection.query_row(
            &format!(
                "SELECT count(*), count(DISTINCT ({keys})) FROM ({})",
                latest.current_rows_sql(&relation, latest.key, "TRUE")
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            rows == keyed,
            "{} holds {} current rows tied on their version order",
            set.source,
            rows - keyed
        );
    }
    let mut projected = vec!["foundation_served_pnu".to_owned()];
    projected.extend((0..set.columns.len()).map(|index| format!("foundation_kept_{index}")));
    let projected = projected.iter().map(String::as_str).collect::<Vec<_>>();
    let columns = set
        .columns
        .iter()
        .enumerate()
        .map(|(index, (_, column))| format!("foundation_kept_{index} AS {column}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT {distinct}foundation_served_pnu AS pnu, {columns} FROM ({}) \
         WHERE foundation_served_pnu IN (SELECT pnu FROM {served})",
        latest.current_rows_sql(&relation, &projected, "TRUE")
    ))
}

/// Loads `sido` of what [`spool`] left in `dir` under `suffix`.
///
/// It leaves `served<suffix>(pnu)` and each row
/// set as `<name><suffix>`, its rows on served PNUs only; with an empty suffix also an empty
/// `built`. Returns the row sets' rows. A 시도 the spool holds nothing of loads empty.
///
/// # Errors
/// Returns an error when `sido` is not two digits or `DuckDB` cannot read the spool.
pub fn load_region(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    dir: &Path,
    suffix: &str,
    sido: &str,
) -> anyhow::Result<u64> {
    ensure!(
        sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()),
        "the 시도 {sido:?} is not two digits"
    );
    let dir_text = sql_dir(dir)?;
    let part = |name: &str| -> Option<String> {
        let path = dir.join(name).join(format!("sido={sido}"));
        path.is_dir()
            .then(|| format!("read_parquet('{dir_text}/{name}/sido={sido}/*.parquet', hive_partitioning = false)"))
    };
    let served = part(&format!("served{suffix}")).map_or_else(
        || "SELECT CAST(NULL AS VARCHAR) AS pnu WHERE FALSE".to_owned(),
        |read| format!("SELECT pnu FROM {read}"),
    );
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE served{suffix} AS {served};"
    ))?;
    let mut rows = 0;
    for set in stage.row_sets() {
        let name = format!("{}{suffix}", set.name);
        let columns = std::iter::once("pnu")
            .chain(set.columns.iter().map(|(_, column)| *column))
            .collect::<Vec<_>>()
            .join(", ");
        let distinct = if set.distinct { "DISTINCT " } else { "" };
        let body = match part(&name) {
            None => format!(
                "SELECT {} WHERE FALSE",
                std::iter::once("CAST(NULL AS VARCHAR) AS pnu".to_owned())
                    .chain(
                        set.columns
                            .iter()
                            .map(|(_, column)| format!("CAST(NULL AS VARCHAR) AS {column}"))
                    )
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Some(read) if stage.current_rows_per_region() && !set.prepared => regional_rows(
                connection,
                &set,
                &read,
                &format!("served{suffix}"),
                distinct,
            )?,
            Some(read) => format!(
                "SELECT {distinct}{columns} FROM {read} WHERE pnu IN (SELECT pnu FROM served{suffix})"
            ),
        };
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {name} AS {body};"))?;
        let count: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        rows += u64::try_from(count)?;
    }
    if suffix.is_empty() {
        stage.create_built(connection)?;
    }
    Ok(rows)
}

/// What a test reads back: the served parcels and the kind's rows of its one 시도.
#[cfg(test)]
pub(crate) struct TestRead {
    pub served_parcels: u64,
    pub rows: u64,
}

/// Spools the connection's local tables to a temporary directory and loads their one 시도, as the
/// build does for each.
#[cfg(test)]
pub(crate) fn read_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    edition: &str,
    suffix: &str,
) -> anyhow::Result<TestRead> {
    let dir = tempfile::tempdir()?;
    let spooled = spool(
        stage,
        connection,
        &Source::Local,
        edition,
        suffix,
        dir.path(),
    )?;
    ensure!(
        spooled.sidos.len() == 1,
        "a test's served parcels lie in one 시도, not {:?}",
        spooled.sidos
    );
    let sido = spooled.sidos.iter().next().context("one 시도")?;
    let rows = load_region(stage, connection, dir.path(), suffix, sido)?;
    Ok(TestRead {
        served_parcels: spooled.served_parcels,
        rows,
    })
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

/// The SQL texts every kind's code version folds in beside its own: the code views and the spool.
pub(crate) const SHARED_SQL: [&str; 3] = [PAIR_ROWS_SQL, RENUMBERED_INTO_SQL, SPOOL_SQL];
