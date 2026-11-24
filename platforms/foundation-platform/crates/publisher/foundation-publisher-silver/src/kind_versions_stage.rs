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

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::legal_dong_code_change_stage::{PAIR_ROWS_SQL, RENUMBERED_INTO_SQL};
use crate::parcel_nearby_stage::Source;
use crate::place_id_join::{place_ids_sql, BRIDGE_SOURCE};

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
    /// builds every PNU on every run (root ADR-0326): [`Self::changed_pnus`] is never asked.
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
    /// The spools whose `pnu` column names the PNUs the place ids are read for: the row sets', by
    /// default.
    fn pnu_spools(&self) -> Vec<&'static str> {
        self.row_sets().iter().map(|set| set.name).collect()
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

/// The leading digits of a PNU one part of a 시도's build shares: its 시군구.
pub const PART_DIGITS: usize = 5;

/// The suffix a 시도's table carries while a view of one part has its name.
const WHOLE_REGION: &str = "__region";

/// [`KindVersionsStage::build`] over the loaded 시도, one 시군구 at a time; returns the parts built.
///
/// A statement over a whole 시도 holds what does not spill (an ordered aggregate, a list) in
/// memory at once: on 2026-11-23 the ownership shadow passed its 4.3 GiB four times out of four
/// (15,345,613 owner rows in one 시도; its largest 시군구 has 1,299,963) and never had a first
/// commit. So every kind is built in parts, here, and no stage carries its own split.
///
/// # Errors
/// Returns the stage's refusal, or an error when `DuckDB` refuses a statement.
pub fn build_in_parts(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
) -> anyhow::Result<u64> {
    build_by_prefix(
        stage,
        connection,
        all,
        source_snapshot_id,
        published_at_utc,
        PART_DIGITS,
    )
}

/// [`build_in_parts`] with parts of `digits` leading digits (a test's PNUs share their first five).
///
/// The stage reads the same names: each table of the 시도 that has a `pnu` column (the served
/// parcels, the row sets, what `load_more` left, the place ids) is renamed aside and a view of one
/// part's rows takes its name, then the tables take their names back, also when a part fails.
/// A partial run builds only the parts `targets` names.
pub(crate) fn build_by_prefix(
    stage: &dyn KindVersionsStage,
    connection: &Connection,
    all: bool,
    source_snapshot_id: &str,
    published_at_utc: &str,
    digits: usize,
) -> anyhow::Result<u64> {
    if stage.whole_region_reason().is_some() {
        stage.build(connection, all, source_snapshot_id, published_at_utc)?;
        return Ok(1);
    }
    let mut names = vec!["served".to_owned()];
    names.extend(stage.row_sets().iter().map(|set| set.name.to_owned()));
    names.extend(stage.pnu_spools().iter().map(|name| (*name).to_owned()));
    names.push(PLACE_IDS.to_owned());
    names.sort();
    names.dedup();
    let mut tables = Vec::new();
    for name in names {
        ensure!(is_plain(&name), "{name:?} is not a plain table name");
        let held: i64 = connection.query_row(
            "SELECT count(*) FROM information_schema.columns c \
             JOIN information_schema.tables t USING (table_schema, table_name) \
             WHERE t.table_schema = 'main' AND t.table_type = 'BASE TABLE' \
               AND c.table_name = ? AND c.column_name = 'pnu'",
            [name.as_str()],
            |row| row.get(0),
        )?;
        if held == 1 {
            tables.push(name);
        }
    }
    let prefixes = tables
        .iter()
        .map(|name| format!("SELECT substr(pnu, 1, {digits}) AS part FROM {name}"))
        .collect::<Vec<_>>()
        .join(" UNION ");
    let named = if all {
        String::new()
    } else {
        format!(" WHERE part IN (SELECT substr(pnu, 1, {digits}) FROM targets)")
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE build_parts AS \
         SELECT part, row_number() OVER (ORDER BY part NULLS FIRST) AS part_no \
         FROM ({prefixes}){named};"
    ))?;
    let parts: i64 =
        connection.query_row("SELECT count(*) FROM build_parts", [], |row| row.get(0))?;
    for name in &tables {
        connection.execute_batch(&format!(
            "ALTER TABLE {name} RENAME TO {name}{WHOLE_REGION};"
        ))?;
    }
    let built = (1..=parts).try_for_each(|part| {
        for name in &tables {
            connection.execute_batch(&format!(
                "CREATE OR REPLACE VIEW {name} AS SELECT * FROM {name}{WHOLE_REGION} \
                 WHERE substr(pnu, 1, {digits}) IS NOT DISTINCT FROM \
                   (SELECT part FROM build_parts WHERE part_no = {part});"
            ))?;
        }
        stage
            .build(connection, all, source_snapshot_id, published_at_utc)
            .with_context(|| format!("part {part} of {parts} of the 시도"))
    });
    for name in &tables {
        connection.execute_batch(&format!(
            "DROP VIEW IF EXISTS {name}; ALTER TABLE {name}{WHOLE_REGION} RENAME TO {name};"
        ))?;
    }
    connection.execute_batch("DROP TABLE build_parts;")?;
    built?;
    Ok(u64::try_from(parts)?)
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
