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

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use lakehouse_domain::LakehouseRowVersions;
use lakehouse_infrastructure::IngestCommitLog;

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::{place_ids_sql, BRIDGE_SOURCE};

pub use crate::kind_versions_parts::{build_in_parts, BUILD_PART, PART_DIGITS};

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

/// A check of one 시도's result, run before that 시도 is appended (root ADR-0615 §7).
///
/// `sql` counts the rows that break the rule, over the 시도's `built` (the documents the run built),
/// `appended` (the rows it would append) and `current_rows` (the head's rows it diffed against).
/// A `blocking` check that counts any row refuses the 시도: nothing of it is appended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultCheck {
    /// The check's name, as the orchestrator records it.
    pub name: &'static str,
    /// One statement returning one count.
    pub sql: String,
    /// Whether a breach refuses the 시도.
    pub blocking: bool,
    /// One statement returning one text column, the breaching keys: the driver records the first
    /// few, so a failure names what to look at without a second query. `None` when the count says
    /// it all.
    pub samples_sql: Option<String>,
}

/// A check of the whole build, held before anything is appended (root ADR-0615 §7): a rule over
/// every 시도's result together, which no 시도's own [`ResultCheck`] can judge (a share of the
/// nation's units; a unit that moved 시도 meets its new row in another 시도's result).
///
/// It belongs to a table committed whole: one that commits a 시도 at a time has appended its first
/// 시도 before the last is built, and the driver refuses the two together. It judges the 시도 the
/// run built: every 시도, for a lane whose every run is whole.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NationalCheck {
    /// The check's name, as the orchestrator records it.
    pub name: &'static str,
    /// Whether a breach past the bound refuses the build: nothing is appended.
    pub blocking: bool,
    /// Statements run once, before the first 시도: the tables the check gathers into. Empty when it
    /// reads what the stage's read left.
    pub setup_sql: String,
    /// Statements run after each 시도's diff, over its `built`, `appended` and `current_rows`. Empty
    /// when nothing is gathered.
    pub gather_sql: String,
    /// One statement returning `(breaches, of)`: the rows that break the rule, and how many it was
    /// judged over.
    pub count_sql: String,
    /// The key of the replaced table's entry in `gold-panel-rebuild.contract.json` that names the
    /// share of `of` allowed to break; `None`: any breach is past the bound.
    pub bound: Option<&'static str>,
    /// Why the rule holds: what a refusal says beside the counts.
    pub why: &'static str,
}

/// Why `breaches` of `of` refuse a build under `check`, when the share of them `bound` allows to
/// break is passed (any breach, without a bound); `None` otherwise.
#[must_use]
pub fn national_verdict(
    check: &NationalCheck,
    breaches: u64,
    of: u64,
    bound: Option<f64>,
) -> Option<String> {
    #[allow(clippy::cast_precision_loss)]
    let (breaches_f, of_f) = (breaches as f64, of as f64);
    if breaches == 0 || bound.is_some_and(|share| breaches_f <= of_f * share) {
        return None;
    }
    let allowed = bound.map_or_else(String::new, |share| {
        format!(
            "; at most {:.4}% may, gold-panel-rebuild.contract.json {}",
            100.0 * share,
            check.bound.unwrap_or("-")
        )
    });
    Some(format!(
        "{breaches} of {of} break {} ({:.4}%{allowed}): {}",
        check.name,
        100.0 * breaches_f / of_f.max(1.0),
        check.why
    ))
}

/// An input the stage reads one batch of, rather than its current rows (root ADR-0615 §7): the unit
/// prices read the newest vintage's batch, a vintage's ties broken by the order the table appended
/// them (root ADR-0182).
///
/// The driver reads the input's ingest log before the session opens (the session cannot await the
/// catalog), has [`Self::choose`] pick the batch at the pins once it opens, and leaves the batch as
/// [`INPUT_BATCH`] before [`spool`]. Every commit records the batch under `property`, and a batch
/// that moved from the base commit's explains a change of any size, as a moved code version does
/// (root ADR-0346). The rows are the batch's, so the input's pin stamps their `source_snapshot_id`.
#[derive(Clone, Copy)]
pub struct BatchInput {
    /// The input.
    pub input: &'static str,
    /// The commit property that records the batch a commit read.
    pub property: &'static str,
    /// The batch to read, of those the input holds at the source, by the input's ingest log.
    pub choose: fn(&Connection, &Source<'_>, &IngestCommitLog) -> anyhow::Result<String>,
}

/// The table (`id`) the driver leaves the batch of a [`BatchInput`] in, before [`spool`].
pub const INPUT_BATCH: &str = "input_batch";

/// Leaves `id` as [`INPUT_BATCH`].
///
/// # Errors
/// Returns an error when `DuckDB` refuses the statements.
pub fn leave_input_batch(connection: &Connection, id: &str) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {INPUT_BATCH} (id VARCHAR);"
    ))?;
    connection.execute(&format!("INSERT INTO {INPUT_BATCH} VALUES (?)"), [id])?;
    Ok(())
}

/// The batch [`INPUT_BATCH`] names, for the stage's [`KindVersionsStage::spool_more`].
///
/// # Errors
/// Returns an error when the driver left none: the stage declares no [`BatchInput`].
pub fn input_batch(connection: &Connection) -> anyhow::Result<String> {
    connection
        .query_row(&format!("SELECT id FROM {INPUT_BATCH}"), [], |row| {
            row.get(0)
        })
        .with_context(|| {
            format!("the driver left no {INPUT_BATCH}: the stage declares no batch input")
        })
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
    /// Whether each row carries its parcel's place id (the kind line's `place_identity`, root
    /// ADR-0483): [`spool`] then also writes the place ids of the PNUs the row sets hold, and
    /// [`load_region`] loads them as `place_ids<suffix>(pnu, place_id)`.
    fn carries_place_id(&self) -> bool {
        false
    }
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
    /// Whether the kind's documents depend on the instant they are built as of, not only on its
    /// inputs: an auction serves the rounds still open then (root ADR-0287 §7), so a round leaves its
    /// document when it closes with no input moving. The build then stamps `published_at_utc` with
    /// that instant (`as_of`, the Spark producers' `published_at_utc`), records it on the commit
    /// (`foundation.as-of`), builds a shadow as of the instant the replaced build was, leaves
    /// `as_of(instant_us)` (microseconds since the epoch) on the connection before [`spool`], and
    /// builds every PNU on every run (root ADR-0326): no partial run is planned for it.
    fn as_of_dependent(&self) -> bool {
        false
    }
    /// Reads what the kind places by its own rule rather than as a row set re-keyed to the served
    /// code (an auction's rounds: located, carried by the lineage, root ADR-0287 §7), once, into
    /// spools under `dir_text` split by the served PNU's 시도 (`<name><suffix>/sido=<시도>`, a `pnu`
    /// column each), after the served parcels (`served<suffix>`) and the row sets; returns each
    /// input's read time by table name. Nothing by default.
    ///
    /// # Errors
    /// Returns an error when an input cannot be read or `DuckDB` refuses a statement.
    fn spool_more(
        &self,
        _connection: &Connection,
        _source: &Source<'_>,
        _suffix: &str,
        _dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        Ok(BTreeMap::new())
    }
    /// Loads one 시도 of what [`Self::spool_more`] left (`read` gives the reader of a spool's
    /// `sido=<시도>` part, `None` when the 시도 has none); returns its rows. Nothing by default.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` cannot read the spool.
    fn load_more(
        &self,
        _connection: &Connection,
        _read: &dyn Fn(&str) -> Option<String>,
        _suffix: &str,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }
    /// The tables [`Self::load_more`] leaves that carry their part in a `part` column rather than
    /// in a PNU (root ADR-0615 §7): rows a document reads by another key (a deal by its address
    /// code), and rows a part reads from another part.
    ///
    /// The stage spools such a row once for every part that reads it, with that part in `part` (and
    /// under that part's 시도), so each part's build sees what it reads and nothing else; the build
    /// writes only the documents whose PNU lies in the part ([`BUILD_PART`] names it). A document
    /// built for another part's PNU would be built twice, which the diff refuses. None by default.
    fn part_tables(&self) -> Vec<&'static str> {
        Vec::new()
    }
    /// The spools whose `pnu` column names the PNUs the place ids are read for: the row sets', by
    /// default.
    fn pnu_spools(&self) -> Vec<&'static str> {
        self.row_sets().iter().map(|set| set.name).collect()
    }
    /// The checks of a 시도's result the kind adds to the ones every table has: its domain's
    /// refusals. None by default.
    fn result_checks(&self) -> Vec<ResultCheck> {
        Vec::new()
    }
    /// The checks of the whole build the kind holds before anything is appended: its domain's
    /// refusals over every 시도 together, and what its read dropped. None by default.
    fn national_checks(&self) -> Vec<NationalCheck> {
        Vec::new()
    }
    /// The input the kind reads one batch of. `None` by default.
    fn batch_input(&self) -> Option<BatchInput> {
        None
    }
    /// Why the kind's documents cannot be built one 시군구 at a time, when they cannot.
    ///
    /// [`build_in_parts`] shows [`Self::build`] one 시군구 of the loaded 시도 at a time, which holds
    /// when a PNU's document reads only rows under PNUs of its own 시군구 (its own rows, as a rule).
    /// A kind whose document reads another 시군구's rows names the rows here and is built a 시도 at
    /// once. `None` by default.
    fn whole_region_reason(&self) -> Option<&'static str> {
        None
    }
    /// Leaves an empty `built`, the table [`Self::build`] inserts into.
    ///
    /// # Errors
    /// Returns an error when `DuckDB` refuses the statement.
    fn create_built(&self, connection: &Connection) -> anyhow::Result<()>;
    /// PNUs a partial run rebuilds beside the ones [`targets_from_spools`] finds by itself (the rows
    /// of a row set or the served parcels that differ between the two reads): one `SELECT` of a
    /// `pnu` column, repeats allowed. `@dir@` in it is the spool directory; it reads the spools of
    /// both sides (`<name>` and `<name>_old`) nationally and the tables [`Self::prepare`] left for
    /// both. A stage that gives one has its whole base side read again ([`spool_base`]). `None` by
    /// default.
    fn changed_keys_sql(&self) -> Option<String> {
        None
    }
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
pub(crate) fn sql_dir(dir: &Path) -> anyhow::Result<String> {
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
pub(crate) const SPOOL_SQL: &str =
    "COPY (SELECT *, substr(pnu, 1, 2) AS sido FROM (@query@)) TO '@dir@/@name@' \
    (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (sido), OVERWRITE_OR_IGNORE)";

/// Writes `query` (which has a `pnu` column) as the spool `name` under `dir_text`, one directory per
/// 시도 of its PNU (`SPOOL_SQL`, the one split of every spool): what a stage's
/// [`KindVersionsStage::spool_more`] reads by its own rule. A row without a PNU is written under no
/// 시도 (`DuckDB` 2.0 names its directory `sido=__HIVE_DEFAULT_PARTITION__`): no region load reads
/// it, and a stage that counts or refuses such rows reads them back by `pnu IS NULL`.
///
/// # Errors
/// Returns an error when `name` is not plain or `DuckDB` refuses the statement.
pub fn spool_by_sido(
    connection: &Connection,
    query: &str,
    dir_text: &str,
    name: &str,
) -> anyhow::Result<()> {
    ensure!(is_plain(name), "{name:?} is not a plain spool name");
    connection
        .execute_batch(&fill(
            SPOOL_SQL,
            &[("query", query), ("dir", dir_text), ("name", name)],
        ))
        .with_context(|| format!("spooling {name} by the 시도 of its PNU"))
}

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
    read_millis.extend(stage.spool_more(connection, source, suffix, &dir_text)?);
    // The place ids of the PNUs the kind's rows hold, once (`place_id_join`): the others have no
    // document to carry one.
    // A spool no row was written to has no directory, and none of its PNUs to ask about.
    let held = stage
        .pnu_spools()
        .iter()
        .filter(|name| dir.join(format!("{name}{suffix}")).is_dir())
        .map(|name| {
            format!("SELECT pnu FROM read_parquet('{dir_text}/{name}{suffix}/*/*.parquet')")
        })
        .collect::<Vec<_>>()
        .join(" UNION ");
    if stage.carries_place_id() && !held.is_empty() {
        let at = Instant::now();
        connection.execute_batch(&fill(
            SPOOL_SQL,
            &[
                (
                    "query",
                    &place_ids_sql(source, &format!("code IN ({held})"))?,
                ),
                ("dir", &dir_text),
                ("name", &format!("{PLACE_IDS}{suffix}")),
            ],
        ))?;
        read_millis.insert(BRIDGE_SOURCE.to_owned(), elapsed(at));
    }
    Ok(Spooled {
        served_parcels: u64::try_from(served)?,
        sidos,
        read_millis,
    })
}

/// What [`spool_base`] read of the base commit's pins: the spools a partial run compares.
#[derive(Clone, Debug, Default)]
pub struct BaseSpool {
    /// The spools read at both pins (`<name>` and `<name>_old`): the only ones that can differ.
    pub compared: Vec<String>,
    /// The 시도 the base's served edition holds, when its served parcels were read again.
    pub sidos: BTreeSet<String>,
    /// Milliseconds each input took to read, by table name.
    pub read_millis: BTreeMap<String, u64>,
}

/// The suffix of the base commit's side of a partial run.
pub const BASE_SUFFIX: &str = "_old";

/// The read's own renumbering in a row set's SQL: the one name a set may carry `@suffix@` in and
/// still be read alone at the base's pins.
const RENUMBERING_OF_READ: &str = "renumbering@suffix@";

/// Reads the base commit's side of a partial run into `dir`, after [`spool`] read the new side.
///
/// Only what can differ is read again. When every input in `moved` is the Silver of a row set, and
/// no row set reads what the stage prepared, only those row sets are spooled at `source` (the
/// base's pins, as `<name>_old`): the served parcels, the code change table and so the renumbering
/// are the new side's, and a row set whose Silver did not move holds the same rows. Any other move
/// (the parcels, a table the stage prepares from, a stage whose `pnu_spools` names a spool that is
/// no row set) reads the whole base side, as [`spool`] does.
///
/// On 2026-11-24 the 총괄표제부 lane's first partial run read all three inputs twice, the 39,861,511
/// served parcels among them, and loaded both sides of each of 17 시도 to find 2,155 changed keys
/// in 323 seconds, longer than its whole first build; only its Silver had moved.
///
/// # Errors
/// As [`spool`].
pub fn spool_base(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    source: &Source<'_>,
    edition: &str,
    dir: &Path,
    moved: &BTreeSet<String>,
) -> anyhow::Result<BaseSpool> {
    let sets = stage.row_sets();
    let reads_itself_only = |set: &RowSet| {
        !set.prepared
            && set
                .joins
                .iter()
                .chain(set.columns.iter().map(|(expression, _)| expression))
                .all(|text| !text.replace(RENUMBERING_OF_READ, "").contains("@suffix@"))
    };
    let narrow = !moved.is_empty()
        && stage.changed_keys_sql().is_none()
        && stage
            .pnu_spools()
            .iter()
            .all(|name| sets.iter().any(|set| set.name == *name))
        && sets.iter().all(reads_itself_only)
        && moved
            .iter()
            .all(|input| sets.iter().any(|set| set.source == input.as_str()));
    if !narrow {
        let read = spool(stage, connection, source, edition, BASE_SUFFIX, dir)?;
        return Ok(BaseSpool {
            compared: every_spool(stage),
            sidos: read.sidos,
            read_millis: read.read_millis,
        });
    }
    let dir_text = sql_dir(dir)?;
    // The parcels and the code change table stand where the new side read them.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE renumbering{BASE_SUFFIX} AS SELECT src, dst FROM renumbering;"
    ))?;
    let mut base = BaseSpool::default();
    for set in sets.iter().filter(|set| moved.contains(set.source)) {
        let at = Instant::now();
        if stage.current_rows_per_region() {
            spool_regional_row_set(connection, source, set, BASE_SUFFIX, &dir_text)?;
        } else {
            spool_row_set(connection, source, set, BASE_SUFFIX, &dir_text)?;
        }
        base.read_millis.insert(set.source.to_owned(), elapsed(at));
        base.compared.push(set.name.to_owned());
    }
    Ok(base)
}

/// Every spool a read of `stage` may write: the served parcels, the row sets, the stage's own
/// spools and the place ids.
fn every_spool(stage: &dyn KindVersionsStage) -> Vec<String> {
    let mut names = vec!["served".to_owned()];
    names.extend(stage.row_sets().iter().map(|set| set.name.to_owned()));
    names.extend(stage.pnu_spools().iter().map(|name| (*name).to_owned()));
    names.push(PLACE_IDS.to_owned());
    names.sort();
    names.dedup();
    names
}

/// The 시도 (`sido=<시도>` directories) a spool `name` holds under `dir`.
fn spool_sidos(dir: &Path, name: &str) -> anyhow::Result<BTreeSet<String>> {
    let path = dir.join(name);
    let mut sidos = BTreeSet::new();
    if !path.is_dir() {
        // A spool no row was written to has no directory.
        return Ok(sidos);
    }
    for entry in
        std::fs::read_dir(&path).with_context(|| format!("cannot list {}", path.display()))?
    {
        let file_name = entry?.file_name();
        let sido = file_name
            .to_str()
            .and_then(|text| text.strip_prefix("sido="));
        if let Some(sido) = sido {
            if sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()) {
                sidos.insert(sido.to_owned());
            }
        }
    }
    Ok(sidos)
}

/// Leaves `targets(pnu)`: the PNUs a partial run rebuilds, found once from the two sides' spools.
///
/// For each spool of `compared` (`<name>` and `<name>_old` under `dir`) and each 시도 either side
/// holds, the PNUs of the rows one side has and the other has not, as a multiset (a row twice is
/// another total); a 시도 only one side holds gives every PNU of it. A 시도 whose two sides hold the
/// same rows gives none, and the build never loads it. The stage's own rule
/// ([`KindVersionsStage::changed_keys_sql`]) adds its PNUs. Only PNUs a side serves are kept (the
/// spools hold rows of unserved PNUs too, which have no document on either side). For a kind whose
/// current rows a 시도's load chooses the spools hold every version, so the set may be more than
/// the changed PNUs: a PNU named for nothing builds the document it had and appends nothing.
/// Returns the PNUs named.
///
/// # Errors
/// Returns an error when a spool cannot be listed or `DuckDB` cannot read it.
pub fn targets_from_spools(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    dir: &Path,
    compared: &[String],
) -> anyhow::Result<u64> {
    let dir_text = sql_dir(dir)?;
    connection.execute_batch("CREATE OR REPLACE TABLE changed_row_pnus (pnu VARCHAR);")?;
    for name in compared {
        ensure!(is_plain(name), "{name:?} is not a plain spool name");
        let base_name = format!("{name}{BASE_SUFFIX}");
        let (new, base) = (spool_sidos(dir, name)?, spool_sidos(dir, &base_name)?);
        let read = |side: &str, sido: &str| {
            format!(
                "read_parquet('{dir_text}/{side}/sido={sido}/*.parquet', hive_partitioning = false)"
            )
        };
        for sido in new.union(&base) {
            let pnus = match (new.contains(sido), base.contains(sido)) {
                (true, true) => {
                    let (a, b) = (read(name, sido), read(&base_name, sido));
                    format!(
                        "SELECT pnu FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}) \
                         UNION ALL SELECT pnu FROM (SELECT * FROM {b} EXCEPT ALL SELECT * FROM {a})"
                    )
                }
                (true, false) => format!("SELECT pnu FROM {}", read(name, sido)),
                _ => format!("SELECT pnu FROM {}", read(&base_name, sido)),
            };
            connection
                .execute_batch(&format!(
                    "INSERT INTO changed_row_pnus SELECT DISTINCT pnu FROM ({pnus});"
                ))
                .with_context(|| format!("comparing the two reads of {name} in 시도 {sido}"))?;
        }
    }
    if let Some(more) = stage.changed_keys_sql() {
        connection
            .execute_batch(&format!(
                "INSERT INTO changed_row_pnus SELECT DISTINCT pnu FROM ({});",
                fill(&more, &[("dir", &dir_text)])
            ))
            .context("the stage's own rule of changed keys")?;
    }
    // The served parcels of either side, read once and probed by the few PNUs named.
    let served = ["served".to_owned(), format!("served{BASE_SUFFIX}")]
        .iter()
        .filter(|name| dir.join(name.as_str()).is_dir())
        .map(|name| format!("'{dir_text}/{name}/*/*.parquet'"))
        .collect::<Vec<_>>();
    ensure!(
        !served.is_empty(),
        "no served parcels were spooled under {}",
        dir.display()
    );
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE targets AS \
         SELECT DISTINCT s.pnu FROM read_parquet([{}], hive_partitioning = false) s \
         WHERE s.pnu IN (SELECT pnu FROM changed_row_pnus); \
         DROP TABLE changed_row_pnus;",
        served.join(", ")
    ))?;
    let count: i64 = connection.query_row("SELECT count(*) FROM targets", [], |row| row.get(0))?;
    Ok(u64::try_from(count)?)
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
/// The spool (and the region's table) of the place ids a kind that carries them reads.
pub const PLACE_IDS: &str = "place_ids";

/// How a row set's Silver names its current rows (its contract's rule: the newest row per key, or
/// the newest edition per group), and the columns that ride along so the spool can be checked for
/// a tie SQL keeps.
struct CurrentRule {
    /// Columns that ride along for the check.
    carried: Vec<&'static str>,
    /// How many of `carried` (the first ones) name a group a duplicate is counted in: the key of a
    /// newest-per-key table (each key once), the group of an editions table (each group's
    /// newest edition once).
    group: usize,
    /// Whether the check counts distinct `carried` tuples against distinct groups (editions) or
    /// rows against distinct keys (newest per key).
    editions: bool,
}

impl CurrentRule {
    fn of(contract: &lakehouse_domain::LakehouseTableContract) -> anyhow::Result<Self> {
        let table = contract.table_name;
        match contract.row_versions {
            LakehouseRowVersions::LatestPerKey(latest) => {
                ensure!(
                    contract.current_row_predicate.is_none(),
                    "{table} selects its current rows by a predicate too; this reader applies only \
                     the newest-per-key rule"
                );
                let carried = if latest.keep_ties {
                    Vec::new()
                } else {
                    latest.key.to_vec()
                };
                Ok(Self {
                    group: carried.len(),
                    carried,
                    editions: false,
                })
            }
            LakehouseRowVersions::Editions {
                edition_column,
                per,
                ..
            } => {
                ensure!(
                    contract.newest_edition_sql("t", &["pnu"], "TRUE").is_some(),
                    "{table} is read one pinned edition at a time, not by its newest edition"
                );
                Ok(Self {
                    carried: per.iter().copied().chain([edition_column]).collect(),
                    group: per.len(),
                    editions: true,
                })
            }
            _ => bail!("{table} keeps neither versions per key nor editions read newest first"),
        }
    }

    /// The current rows of `read`, projecting `columns`: the contract's one rendering.
    fn query(
        &self,
        contract: &lakehouse_domain::LakehouseTableContract,
        read: &str,
        columns: &[&str],
    ) -> anyhow::Result<String> {
        if self.editions {
            contract
                .newest_edition_sql(read, columns, "TRUE")
                .with_context(|| format!("{} has no newest edition", contract.table_name))
        } else {
            Ok(contract
                .current_row_latest()
                .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
                .current_rows_sql(read, columns, "TRUE"))
        }
    }

    /// `(found, allowed)`: kept rows and their distinct keys, or kept editions and their groups.
    fn check_sql(&self, glob: &str) -> String {
        let tuple = |count: usize| {
            (0..count)
                .map(|index| format!("{TIE_PREFIX}{index}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let all = tuple(self.carried.len());
        if self.editions {
            let groups = if self.group == 0 {
                "CAST(count(*) > 0 AS BIGINT)".to_owned()
            } else {
                format!("count(DISTINCT ({}))", tuple(self.group))
            };
            format!(
                "SELECT (SELECT count(*) FROM (SELECT DISTINCT {all} FROM read_parquet({glob}))), \
                 (SELECT {groups} FROM read_parquet({glob}))"
            )
        } else {
            format!("SELECT count(*), count(DISTINCT ({all})) FROM read_parquet({glob})")
        }
    }
}

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
    let (rows, rule, pinned): (String, Option<CurrentRule>, String) = if set.prepared {
        ensure!(
            set.source
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "the prepared table {} is not a plain name",
            set.source
        );
        let table = format!("{}{suffix}", set.source);
        (format!("SELECT * FROM {table}"), None, table)
    } else {
        let contract = contract(set.source)?;
        // The contract's one current-row rule (root ADR-0267, ADR-0564 §4). SQL keeps a tie, so the
        // columns that tell one ride along and the spool is checked: a key read twice, or two
        // editions of one group chosen.
        let rule = CurrentRule::of(contract)?;
        let pinned = source.of(set.source)?;
        // A column the table gained after the pinned snapshot reads as NULL there.
        let read = with_contract_columns(connection, contract, &pinned)?;
        for column in &rule.carried {
            if !projected.contains(column) {
                projected.push(column);
            }
        }
        (rule.query(contract, &read, &projected)?, Some(rule), pinned)
    };
    let ties: &[&str] = match &rule {
        Some(rule) => &rule.carried,
        None => &[],
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
    if let Some(rule) = rule.as_ref().filter(|rule| !rule.carried.is_empty()) {
        let glob = format!("'{dir_text}/{name}/*/*.parquet'");
        let (found, allowed): (i64, i64) =
            connection.query_row(&rule.check_sql(&glob), [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        ensure!(
            found == allowed,
            "{} ({pinned}) holds {} current rows or editions tied on their version order",
            set.source,
            found - allowed
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
    rows += stage.load_more(connection, &part, suffix)?;
    if stage.carries_place_id() {
        let name = format!("{PLACE_IDS}{suffix}");
        let body = part(&name).map_or_else(
            || {
                "SELECT CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS VARCHAR) AS place_id WHERE FALSE"
                    .to_owned()
            },
            |read| format!("SELECT pnu, place_id FROM {read}"),
        );
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {name} AS {body};"))?;
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
    let dir = test_spool_dir()?;
    // This side's spools of an earlier read go: a spool no row is written to leaves no directory.
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        let of_base = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.ends_with(BASE_SUFFIX));
        if of_base == (suffix == BASE_SUFFIX) {
            std::fs::remove_dir_all(&path)?;
        }
    }
    let spooled = spool(stage, connection, &Source::Local, edition, suffix, &dir)?;
    ensure!(
        spooled.sidos.len() == 1,
        "a test's served parcels lie in one 시도, not {:?}",
        spooled.sidos
    );
    let sido = spooled.sidos.iter().next().context("one 시도")?;
    let rows = load_region(stage, connection, &dir, suffix, sido)?;
    Ok(TestRead {
        served_parcels: spooled.served_parcels,
        rows,
    })
}

#[cfg(test)]
thread_local! {
    /// The spool directory of a test's reads ([`read_for_test`]): both sides in one, as a run's.
    static TEST_SPOOL: std::cell::RefCell<Option<tempfile::TempDir>> =
        const { std::cell::RefCell::new(None) };
}

/// The test thread's spool directory, made on first use and removed with the thread.
#[cfg(test)]
fn test_spool_dir() -> anyhow::Result<std::path::PathBuf> {
    TEST_SPOOL.with(|kept| {
        let mut kept = kept.borrow_mut();
        if kept.is_none() {
            *kept = Some(tempfile::tempdir()?);
        }
        kept.as_ref()
            .map(|dir| dir.path().to_path_buf())
            .context("the test's spool directory")
    })
}

/// Leaves `targets` from the two sides [`read_for_test`] last read (`_old` and none), comparing
/// every spool as a run whose whole base side was read does; returns the PNUs named.
#[cfg(test)]
pub(crate) fn changed_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
) -> anyhow::Result<u64> {
    targets_from_spools(stage, connection, &test_spool_dir()?, &every_spool(stage))
}

/// Reads two states of the connection's local tables as a partial run does, and loads the new one.
///
/// `new` leaves the tables as they are now and `base` as the base commit read them; `moved` names
/// the inputs that differ. Returns what the base read compared and how many PNUs `targets` names.
#[cfg(test)]
pub(crate) fn partial_for_test(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    edition: &str,
    moved: &[&str],
    base: &dyn Fn() -> anyhow::Result<()>,
    new: &dyn Fn() -> anyhow::Result<()>,
) -> anyhow::Result<(BaseSpool, u64)> {
    let dir = tempfile::tempdir()?;
    new()?;
    let spooled = spool(stage, connection, &Source::Local, edition, "", dir.path())?;
    base()?;
    let moved = moved.iter().map(|input| (*input).to_owned()).collect();
    let read = spool_base(
        stage,
        connection,
        &Source::Local,
        edition,
        dir.path(),
        &moved,
    )?;
    let targets = targets_from_spools(stage, connection, dir.path(), &read.compared)?;
    let sido = spooled.sidos.iter().next().context("one 시도")?;
    load_region(stage, connection, dir.path(), "", sido)?;
    Ok((read, targets))
}

/// The tables the publisher's commit (`gold_versions_commit`) creates on the stage's connection.
///
/// A stage must not leave a table of these names: a `TEMP` one would shadow the commit's own (`DuckDB`
/// reads `temp` before `main`), as the first lane-1 shadow build met (2026-11-22: the 총괄표제부
/// Silver rows read as the head's current rows).
pub const COMMIT_TABLES: [&str; 2] = ["current_rows", "appended"];

/// Errors when `connection` holds a `TEMP` table [`COMMIT_TABLES`] names.
///
/// Only a `TEMP` one shadows the commit's. The commit's own tables of the previous 시도 stay in
/// `main` and the next diff replaces them; refusing those stopped every build at its second 시도.
///
/// # Errors
/// Returns an error naming the table, or when `DuckDB` cannot list its tables.
pub fn ensure_no_commit_tables(connection: &Connection) -> anyhow::Result<()> {
    let names = COMMIT_TABLES.map(|name| format!("'{name}'")).join(", ");
    let held: Option<String> = connection.query_row(
        &format!(
            "SELECT string_agg(schema_name || '.' || table_name, ', ') FROM duckdb_tables() \
             WHERE temporary AND table_name IN ({names})"
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

#[cfg(test)]
mod commit_table_tests {
    use super::*;

    /// The previous 시도's commit tables pass; a `TEMP` table of their names, which would shadow the
    /// next commit's, is refused.
    #[test]
    fn only_a_temp_table_of_the_commit_s_names_is_refused() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE OR REPLACE TABLE current_rows AS SELECT 1 AS pnu; \
             CREATE OR REPLACE TABLE appended AS SELECT 1 AS pnu;",
        )?;
        ensure_no_commit_tables(&connection)?;
        connection.execute_batch("CREATE TEMP TABLE appended AS SELECT 2 AS pnu;")?;
        let refused = ensure_no_commit_tables(&connection)
            .err()
            .map(|error| error.to_string());
        assert!(refused.is_some_and(|text| text.contains("appended")));
        Ok(())
    }
}

/// The current-row rule a row set's spool is read and checked by, on synthetic rows: an editions
/// table refuses two editions tied on its order, per group; a newest-per-key table refuses a key
/// read twice.
#[cfg(test)]
mod rule_tests {
    use super::*;

    fn found_and_allowed(rule: &CurrentRule, rows: &str) -> anyhow::Result<(i64, i64)> {
        let connection = Connection::open_in_memory()?;
        let dir = tempfile::tempdir()?;
        let dir_text = sql_dir(dir.path())?;
        let carried = (0..rule.carried.len())
            .map(|index| format!("{TIE_PREFIX}{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        connection.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES {rows}) v({carried})) TO '{dir_text}/set.parquet' (FORMAT parquet);"
        ))?;
        Ok(connection.query_row(
            &rule.check_sql(&format!("'{dir_text}/set.parquet'")),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }

    #[test]
    fn two_editions_tied_on_the_order_are_refused() -> anyhow::Result<()> {
        let boundaries = contract("silver.administrative_boundaries")?;
        let rule = CurrentRule::of(boundaries)?;
        assert_eq!(
            (rule.carried.as_slice(), rule.group, rule.editions),
            (&["source_snapshot_id"][..], 0, true)
        );
        // Two rounds loaded at one instant: both are the newest, and the check refuses them.
        assert_eq!(
            found_and_allowed(&rule, "('round-a'), ('round-b')")?,
            (2, 1)
        );
        assert_eq!(
            found_and_allowed(&rule, "('round-b'), ('round-b')")?,
            (1, 1)
        );
        Ok(())
    }

    #[test]
    fn each_group_holds_one_newest_edition() -> anyhow::Result<()> {
        let schedule = contract("silver.court_auction_item_schedule")?;
        let rule = CurrentRule::of(schedule)?;
        assert_eq!((rule.carried.len(), rule.group), (4, 3));
        let one_each = "('c', '1', '1', '2099-01-02'), ('c', '2', '1', '2099-01-01')";
        assert_eq!(found_and_allowed(&rule, one_each)?, (2, 2));
        let tied = "('c', '1', '1', '2099-01-02'), ('c', '1', '1', '2099-01-03')";
        assert_eq!(found_and_allowed(&rule, tied)?, (2, 1));
        Ok(())
    }

    #[test]
    fn a_key_read_twice_is_refused_and_a_pinned_edition_table_is_not_a_row_set(
    ) -> anyhow::Result<()> {
        let rule = CurrentRule::of(contract("silver.onbid_item_bid_info")?)?;
        assert!(!rule.editions);
        assert_eq!(found_and_allowed(&rule, "('k1'), ('k1')")?, (2, 1));
        assert_eq!(found_and_allowed(&rule, "('k1'), ('k2')")?, (2, 2));
        // The parcels are read by the edition the caller names, never as a row set's newest.
        assert!(CurrentRule::of(contract(PARCEL_SOURCE)?).is_err());
        Ok(())
    }
}
