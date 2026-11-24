// one-off: until the last served Gold has moved to its `_versions` table (no lane serves the table a
// versions table replaces, so no takeover is left to prove): this module and its three commands go.
//! The two sides of a takeover verdict (root ADR-0564 §6, ADR-0559 §5): the served Gold a Spark
//! build replaced rows of, and the `_versions` table `DuckDB` built from the same input pins as its
//! shadow.
//!
//! `export-gold-takeover-sides` checks the whole of both sides by aggregates `DuckDB` computes over
//! the table scan, bringing no row out (`checks.json`: rows, unique keys, the key hash sum, and of
//! the columns that are not documents a hash sum and the non-null counts); renders the candidate's
//! documents of one 시도 and holds each to the schema registry (`gold_documents_check`); and
//! writes both sides' rows of that 시도, a 시군구 a Parquet file, with the columns they share and a
//! `sides.json` naming them (root ADR-0559, ADR-0564 revisions of 2026-11-22 and 2026-11-24).
//! Document content (`*_json`) is judged in that 시도 only: the switch's full bake is its national
//! check.
//! `query-takeover-sides` runs one SQL statement over the two files and prints its rows as JSON, so
//! `engine_equality.py` reaches its verdict through this `DuckDB` (the release images have no Python
//! `duckdb`).
//!
//! Both sides are compared as the values they say, not as the bytes a build wrote (root ADR-0564,
//! Revision 2026-11-22 (둘째): a port does not reproduce Spark's `to_json`): every `*_json` cell is
//! read by `foundation_publisher_core::json_values`, the reading the switch's content check holds
//! the served documents to (a null member absent, a number its value, an instant its time)
//! ([`values`]). `row_digest`, a hash of the bytes, is not compared; the content it stands for is,
//! column by column.
//!
//! `show-gold-takeover-switch` names that pair for the switch, whose full bake holds every
//! document to the served one (`section_packs::takeover` of the by-PNU publisher).

mod values;

use std::{io::Read as _, path::Path};

use anyhow::{ensure, Context};
use foundation_publisher_silver::{
    duckdb::{Config, Connection},
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LakehouseRead},
};
use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, LakehouseTableContract, GOLD_VERSIONS_TABLE_SUFFIX,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use foundation_publisher_by_pnu::building_by_pnu_serving_export::section_packs::sections::PackProvenance;

use crate::{
    gold_documents_check::{DocumentCheck, DocumentRules},
    gold_versions_commit::{shadow_of, Shadow, PER_COMMIT_STAMPS, SOURCE_SNAPSHOTS_PROPERTY},
    public_data_control_support::{optional_env_value, required_env_value},
};

const PREFIX: &str = "FOUNDATION_PLATFORM_GOLD_TAKEOVER_";
// one-off: each line with its table's switch, which deletes its Spark job.
/// The lineage a replaced table's build stamps on the rows it writes besides [`PER_COMMIT_STAMPS`]:
/// the Spark served-boundary job writes the Silver snapshot and the ledger position it read on every
/// row of a full build, while its versions table keeps a row's stamps from the build that last
/// changed the row's content (`row_digest`: the PNU, the geometry, its CRS and its origin). They name
/// a build, not the parcel, so after the first shadow build they differ on every row neither build
/// changed.
const BUILD_STAMPS: &[(&str, &[&str])] = &[
    (
        "gold.parcel_boundary_served",
        &["silver_iceberg_snapshot_id", "edits_through_change_seq"],
    ),
    // The place-id release's run (`parcel-<run>`, `admin-<run>`): the versions tables stamp their own.
    ("gold.place_id_registry", &["release_id"]),
    ("gold.place_id_bridge", &["release_id"]),
];
/// The row digest (root ADR-0099): a hash of the bytes a build wrote, which two engines need not
/// write alike. The verdict compares the content it stands for, as values, and not the digest.
const DIGEST: &str = "row_digest";
/// The 시도 compared when the verdict names none: 경기도, the largest (ADR-0559's measurements).
pub(crate) const DEFAULT_SIDO: &str = "41";
/// The key columns a 시도 is read from, in order: both begin with the 시도 code (a PNU; a region
/// code of every level the region stats hold, `legal_dong` and `sigungu`).
const SIDO_COLUMNS: [&str; 2] = ["pnu", "region_code"];

/// The first of [`SIDO_COLUMNS`] the key holds: the column a side's rows are read by.
fn sido_column(keys: &[&str]) -> anyhow::Result<&'static str> {
    SIDO_COLUMNS
        .into_iter()
        .find(|column| keys.contains(column))
        .with_context(|| format!("no key of {keys:?} holds a 시도 code ({SIDO_COLUMNS:?})"))
}

/// The predicate that keeps the rows of `sido` (two digits) by [`sido_column`], as a range so
/// `DuckDB` skips the files whose bounds lie outside it.
fn sido_predicate(keys: &[&str], sido: &str) -> anyhow::Result<String> {
    let number: u8 = sido
        .parse()
        .ok()
        .filter(|_| sido.len() == 2 && sido.bytes().all(|byte| byte.is_ascii_digit()))
        .with_context(|| format!("the 시도 {sido:?} is not a two-digit code"))?;
    ensure!(number < 99, "the 시도 {sido} has no next code to bound it");
    let column = sido_column(keys)?;
    Ok(format!(
        "({column} >= '{sido}' AND {column} < '{:02}')",
        number + 1
    ))
}

/// What the whole of one side holds, by aggregates alone, in one pass of `DuckDB` over the scan.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct SideCounts {
    rows: i64,
    keys: i64,
    /// The sum of a hash of each row's key: the same keys on both sides, not only as many.
    key_hash_sum: String,
    /// The sum of a hash of each row's columns that are not documents, the key among them, each
    /// as its text: the same values under the same keys on both sides. A document (`*_json`) is
    /// not in it: two engines write equal documents as different bytes, and they are judged as
    /// values in the 시도 ([`values::write_chunks`]).
    plain_hash_sum: String,
    /// Non-null values per column that is not a document, in the columns' order.
    non_null: Vec<(String, i64)>,
}

/// The columns the national aggregates read: every compared column that is not a document.
fn plain_columns<'a>(columns: &[&'a str]) -> Vec<&'a str> {
    columns
        .iter()
        .copied()
        .filter(|column| !column.ends_with(values::JSON_SUFFIX))
        .collect()
}

/// The rows, distinct keys, key hash sum, hash sum of `columns` and non-null count of each of
/// `columns` that `query` returns, in one aggregate.
fn side_counts(
    connection: &Connection,
    query: &str,
    keys: &[&str],
    columns: &[&str],
) -> anyhow::Result<SideCounts> {
    let key_list = keys.join(", ");
    let distinct = if keys.len() == 1 {
        key_list.clone()
    } else {
        format!("({key_list})")
    };
    let counted = columns
        .iter()
        .map(|column| format!("count({column})"))
        .collect::<Vec<_>>()
        .join(", ");
    let hash_sum =
        |of: &str| format!("CAST(coalesce(sum(CAST(hash({of}) AS HUGEINT)), 0) AS VARCHAR)");
    let sql = format!(
        "SELECT count(*), count(DISTINCT {distinct}), {}, {}, {counted} FROM ({query})",
        hash_sum(&key_list),
        hash_sum(&values::as_text_values(columns)),
    );
    let width = columns.len();
    connection
        .query_row(&sql, [], |row| {
            let mut non_null = Vec::with_capacity(width);
            for (index, column) in columns.iter().enumerate() {
                non_null.push(((*column).to_owned(), row.get(index + 4)?));
            }
            Ok(SideCounts {
                rows: row.get(0)?,
                keys: row.get(1)?,
                key_hash_sum: row.get(2)?,
                plain_hash_sum: row.get(3)?,
                non_null,
            })
        })
        .context("DuckDB failed to count a side")
}

/// The national result checks: each side's keys unique, and both sides the same rows, the same
/// keys (hash sum), the same values under the same keys in the columns that are not documents
/// (hash sum) and the same non-null count in each of those columns.
/// Returns what failed, empty when every check holds.
fn failed_checks(reference: &SideCounts, candidate: &SideCounts) -> Vec<String> {
    let mut failed = Vec::new();
    for (name, side) in [("reference", reference), ("candidate", candidate)] {
        if side.rows != side.keys {
            failed.push(format!(
                "{name} holds {} rows on {} keys",
                side.rows, side.keys
            ));
        }
    }
    let mut differ = |what: &str, left: &dyn std::fmt::Debug, right: &dyn std::fmt::Debug| {
        failed.push(format!("{what}: reference {left:?} candidate {right:?}"));
    };
    if reference.rows != candidate.rows {
        differ("rows", &reference.rows, &candidate.rows);
    }
    if reference.key_hash_sum != candidate.key_hash_sum {
        differ(
            "key hash sum",
            &reference.key_hash_sum,
            &candidate.key_hash_sum,
        );
    }
    if reference.plain_hash_sum != candidate.plain_hash_sum {
        differ(
            "plain column hash sum",
            &reference.plain_hash_sum,
            &candidate.plain_hash_sum,
        );
    }
    for ((column, left), (_, right)) in reference.non_null.iter().zip(&candidate.non_null) {
        if left != right {
            differ(&format!("{column} non-null"), left, right);
        }
    }
    failed
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The columns both sides are compared on: the replaced table's that the versions table keeps
/// (every one: it adds `version_seq` and `row_state`, which the replaced table has no counterpart
/// of), less the per-commit stamps, the build's ([`BUILD_STAMPS`]) and the digest of the bytes
/// ([`DIGEST`]).
fn compared_columns(
    replaced: &LakehouseTableContract,
    versions: &LakehouseTableContract,
) -> Vec<&'static str> {
    let build_stamps = BUILD_STAMPS
        .iter()
        .find(|(table, _)| *table == replaced.table_name)
        .map_or(&[][..], |(_, stamps)| *stamps);
    replaced
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| {
            !PER_COMMIT_STAMPS.contains(name)
                && !build_stamps.contains(name)
                && *name != DIGEST
                && versions.columns.iter().any(|column| column.name == *name)
        })
        .collect()
}

/// A path in the work directory as `DuckDB` SQL text.
pub(crate) fn sql_path(path: &Path) -> anyhow::Result<String> {
    let text = path
        .to_str()
        .context("the work directory must be UTF-8")?
        .replace('\\', "/");
    ensure!(
        !text.contains('\''),
        "the work directory must not contain a quote"
    );
    Ok(text)
}

/// The commit of `table` whose input pins are the shadow's: the build the verdict compares.
async fn shadow_commit(
    catalog: &IcebergRestCatalog,
    table: &str,
    shadow: &Shadow,
) -> anyhow::Result<i64> {
    let history = catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    history
        .iter()
        .find(|snapshot| snapshot.source_snapshots() == shadow.pins)
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| {
            format!(
                "the shadow has not built {}'s build {} inputs yet: no commit of {table} records \
                 those {SOURCE_SNAPSHOTS_PROPERTY}",
                shadow.replaced, shadow.build
            )
        })
}

/// Writes `checks.json`, then the 시도's two sides (`reference/<시군구>.parquet`,
/// `candidate/<시군구>.parquet`) and `sides.json` for the versions table the environment names;
/// fails after writing them when a result check failed (a national aggregate, or a document of the
/// 시도), so the 시도's comparison is still made.
#[allow(clippy::too_many_lines)]
pub(crate) async fn export() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}TABLE"))?;
    let work = required_env_value(&format!("{PREFIX}WORK_DIR"))?;
    let memory_limit = optional_env_value(&format!("{PREFIX}MEMORY_LIMIT"))?;
    let sido =
        optional_env_value(&format!("{PREFIX}SIDO"))?.unwrap_or_else(|| DEFAULT_SIDO.to_owned());
    let replaced_name = table
        .strip_suffix(GOLD_VERSIONS_TABLE_SUFFIX)
        .with_context(|| format!("{table} is not a versions table (gold.<x>_versions)"))?;
    let (versions, replaced) = (contract(&table)?, contract(replaced_name)?);
    let latest = versions
        .current_row_latest()
        .with_context(|| format!("{table} is no table of appended row versions"))?;

    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    // Asked as if the lane served the replaced table: the verdict is about that table's build.
    let shadow = shadow_of(&catalog, &table, replaced_name)
        .await?
        .with_context(|| format!("{table} is not a shadow of {replaced_name}"))?;
    let candidate = shadow_commit(&catalog, &table, &shadow).await?;

    let columns = compared_columns(replaced, versions);
    ensure!(
        latest.key.iter().all(|key| columns.contains(key)),
        "{replaced_name}'s columns lack its key {:?}",
        latest.key
    );
    let column = sido_column(latest.key)?;
    // A 시도 that is not two digits, or the last code, is refused before anything is read.
    sido_predicate(latest.key, &sido)?;
    // Loaded before the DuckDB session opens: the session is not `Send` across an await.
    let rules = DocumentRules::load(replaced_name).await?;
    let work = Path::new(&work);
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)?;
    let connection = stage.connection();
    if let Some(limit) = &memory_limit {
        connection.execute("SET memory_limit = ?", [limit.as_str()])?;
    }
    let reference_source = with_contract_columns(
        connection,
        replaced,
        &foundation_publisher_silver::duckdb_parquet_stage::table_at(replaced_name, shadow.build),
    )?;
    let candidate_source = with_contract_columns(
        connection,
        versions,
        &foundation_publisher_silver::duckdb_parquet_stage::table_at(&table, candidate),
    )?;
    let reference_rows = |columns: &[&str], filter: &str| {
        format!(
            "SELECT {} FROM {reference_source} WHERE {filter}",
            columns.join(", ")
        )
    };
    let candidate_rows = |columns: &[&str], filter: &str| {
        latest.current_rows_sql(&candidate_source, columns, filter)
    };

    // The whole of both sides: aggregates DuckDB computes over the scan. No row leaves the engine
    // and no document column is read (2026-11-23: the passes that brought every row of both sides
    // through the publisher took 26.5 minutes on the served parcel boundaries and were estimated
    // at 80 to 100 on the building permits' 8.69 GB of documents a side).
    let started = std::time::Instant::now();
    let plain = plain_columns(&columns);
    let reference_counts = side_counts(
        connection,
        &reference_rows(&plain, "TRUE"),
        latest.key,
        &plain,
    )?;
    println!(
        "gold-takeover-sides national reference: {} rows in {:.1}s",
        reference_counts.rows,
        started.elapsed().as_secs_f64()
    );
    let started = std::time::Instant::now();
    let candidate_counts = side_counts(
        connection,
        &candidate_rows(&plain, "TRUE"),
        latest.key,
        &plain,
    )?;
    println!(
        "gold-takeover-sides national candidate: {} rows in {:.1}s",
        candidate_counts.rows,
        started.elapsed().as_secs_f64()
    );

    // The judged 시도, a 시군구 at a time: the only rows that are brought out, as values.
    let in_sido = sido_predicate(latest.key, &sido)?;
    let sido_prefixes = values::prefixes(
        connection,
        &[
            format!("({})", reference_rows(latest.key, &in_sido)).as_str(),
            format!("({})", candidate_rows(latest.key, &in_sido)).as_str(),
        ],
        column,
    )?;
    ensure!(
        !sido_prefixes.is_empty(),
        "neither side holds a row in 시도 {sido}: name a 시도 the table has"
    );
    let sido_rows = values::write_chunks(
        connection,
        &|filter: &str| reference_rows(&columns, filter),
        column,
        &sido_prefixes,
        &columns,
        "reference",
        &work.join("reference"),
    )?;
    ensure!(
        sido_rows > 0,
        "the reference holds no row in 시도 {sido}: name a 시도 the table has"
    );
    values::write_chunks(
        connection,
        &|filter: &str| candidate_rows(&columns, filter),
        column,
        &sido_prefixes,
        &columns,
        "candidate",
        &work.join("candidate"),
    )?;
    let mut failed = failed_checks(&reference_counts, &candidate_counts);

    // Every document the candidate serves in the 시도, rendered as the bake renders it and held to
    // the registry's schema: the comparison says the tables agree, this that the documents keep
    // the registry's promise.
    let mut documents = DocumentCheck::default();
    match &rules {
        Ok(rules) => {
            let every_column = versions
                .columns
                .iter()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            let provenance = PackProvenance {
                table: table.clone(),
                iceberg_snapshot_id: candidate.to_string(),
                metadata_location: "-".to_owned(),
                manifest_list_location: "-".to_owned(),
            };
            for prefix in &sido_prefixes {
                rules.check(
                    connection,
                    &latest.current_rows_sql(
                        &candidate_source,
                        &every_column,
                        &values::prefix_filter(column, prefix)?,
                    ),
                    &provenance,
                    &mut documents,
                )?;
            }
            if documents.refused > 0 {
                failed.push(format!(
                    "documents: {} of the {} candidate documents of 시도 {sido} are refused by the \
                     renderer or the {} lane's schema",
                    documents.refused, documents.rendered, documents.lane
                ));
            }
        }
        Err(why) => documents.skipped = Some(why.clone()),
    }
    let checks_path = work.join("checks.json");
    let checks = json!({
        "reference": reference_counts, "candidate": candidate_counts, "documents": documents,
        "failed": failed,
    });
    std::fs::write(&checks_path, serde_json::to_vec_pretty(&checks)?)
        .with_context(|| format!("cannot write {}", checks_path.display()))?;

    drop(stage);
    let keys = latest.key;
    let content: Vec<&str> = columns
        .iter()
        .copied()
        .filter(|name| !keys.contains(name))
        .collect();
    let sides = json!({
        "table": table, "replaced": replaced_name,
        "reference_snapshot": shadow.build.to_string(), "candidate_snapshot": candidate.to_string(),
        "pins": shadow.pins, "keys": keys, "content": content,
        // No digest: the content is compared as values, and a digest is of bytes ([`DIGEST`]).
        "digest": serde_json::Value::Null,
        "sido": sido, "sido_rows": sido_rows,
        "rows_nationally": reference_counts.rows,
        // The chunks the two sides were written in, a file each: the comparison joins one at a
        // time (2026-11-23 17:38Z: the join of 시도 41 of the served parcel boundaries whole,
        // 5,211,798 geometries a side, ran out of the verdict's 4 GB).
        "chunks": {
            "column": column, "length": values::CHUNK_PREFIX, "prefixes": sido_prefixes,
        },
    });
    let path = work.join("sides.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&sides)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    ensure!(
        failed.is_empty(),
        "a result check failed ({}): {}",
        checks_path.display(),
        failed.join("; ")
    );
    println!(
        "gold-takeover-sides {table}: reference {replaced_name}@{} candidate {table}@{candidate} \
         keys={keys:?} content={} sido={sido} ({} rows) national checks passed on {} rows; \
         documents rendered {} {}",
        shadow.build,
        content.len(),
        sido_rows,
        reference_counts.rows,
        documents.rendered,
        documents.skipped.as_deref().map_or_else(
            || format!("held to the {} lane's schema", documents.lane),
            |why| { format!("(not checked: {why})") }
        )
    );
    Ok(())
}

/// Prints the build pair a takeover's switch is baked from (root ADR-0564 §6-5) as the last stdout
/// line: the replaced table's build-tag commit (`replaced_build`) and the versions table's shadow
/// commit of the same input pins (`shadow_build`), the pair the verdict compares. The bake script
/// pins the switch's full bake to `shadow_build` and refuses unless the packs reflect
/// `replaced_build`; the bake then refuses any document that differs from the served one.
///
/// # Errors
/// Refuses a table that is no shadow of a replaced one, and a shadow that has not built the
/// replaced build's pins (the replaced table built again after the shadow's last build: stop its
/// job, build the shadow and run the verdict again before the switch).
pub(crate) async fn switch() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}TABLE"))?;
    let replaced_name = table
        .strip_suffix(GOLD_VERSIONS_TABLE_SUFFIX)
        .with_context(|| format!("{table} is not a versions table (gold.<x>_versions)"))?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let shadow = shadow_of(&catalog, &table, replaced_name)
        .await?
        .with_context(|| format!("{table} is not a shadow of {replaced_name}"))?;
    let shadow_build = shadow_commit(&catalog, &table, &shadow).await?;
    println!(
        "{}",
        json!({
            "table": table, "replaced": replaced_name,
            "replaced_build": shadow.build.to_string(), "shadow_build": shadow_build.to_string(),
        })
    );
    Ok(())
}

/// The rows `sql` answers as one JSON array of objects, each as `DuckDB`'s `to_json` writes it:
/// numbers as JSON numbers at full width (a `HUGEINT` sum included, which Python's `json` reads
/// exactly), text as strings, NULL as null.
fn json_array(connection: &Connection, sql: &str) -> anyhow::Result<String> {
    let rows = connection
        .prepare(&format!("SELECT to_json(q)::VARCHAR FROM ({sql}) AS q"))
        .context("DuckDB refused the query")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to run the query")?;
    Ok(format!("[{}]", rows.join(",")))
}

/// Runs the SQL statement on stdin over the views `reference` and `candidate` of the work
/// directory's two sides (a file a chunk: a statement over one chunk's range reads that chunk's
/// file) and prints its rows as the last stdout line.
pub(crate) fn query() -> anyhow::Result<()> {
    let work = required_env_value(&format!("{PREFIX}WORK_DIR"))?;
    let mut sql = String::new();
    std::io::stdin()
        .read_to_string(&mut sql)
        .context("cannot read the SQL statement from stdin")?;
    let sql = sql.trim().trim_end_matches(';');
    ensure!(!sql.is_empty(), "no SQL statement on stdin");
    let connection = Connection::open_in_memory_with_flags(
        Config::default()
            .with("autoload_known_extensions", "false")?
            .with("autoinstall_known_extensions", "false")?,
    )?;
    for (name, value) in [
        (
            "memory_limit",
            optional_env_value(&format!("{PREFIX}MEMORY_LIMIT"))?,
        ),
        (
            "temp_directory",
            optional_env_value(&format!("{PREFIX}TEMP_DIRECTORY"))?,
        ),
    ] {
        if let Some(value) = value {
            connection.execute(&format!("SET {name} = ?"), [value])?;
        }
    }
    for name in ["reference", "candidate"] {
        let path = sql_path(&Path::new(&work).join(name).join("*.parquet"))?;
        connection.execute_batch(&format!(
            "CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')"
        ))?;
    }
    println!("{}", json_array(&connection, sql)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sides_share_the_replaced_columns_less_the_stamps() -> anyhow::Result<()> {
        let columns = compared_columns(
            contract("gold.parcel_nearby")?,
            contract("gold.parcel_nearby_versions")?,
        );
        assert!(columns.contains(&"pnu"));
        for absent in [
            DIGEST,
            "source_snapshot_id",
            "published_at_utc",
            "version_seq",
            "row_state",
        ] {
            assert!(!columns.contains(&absent), "{absent} is compared");
        }
        Ok(())
    }

    #[test]
    fn the_served_parcel_boundaries_compare_their_content_and_not_the_build_stamps(
    ) -> anyhow::Result<()> {
        let columns = compared_columns(
            contract("gold.parcel_boundary_served")?,
            contract("gold.parcel_boundary_served_versions")?,
        );
        assert_eq!(
            columns,
            [
                "pnu",
                "geometry_wkb",
                "geometry_srid",
                "geometry_checksum_sha256",
                "origin"
            ],
            "the replaced table has no digest, so none is compared"
        );
        Ok(())
    }

    #[test]
    fn a_verdict_reads_one_sido_by_the_key_that_holds_it() -> anyhow::Result<()> {
        assert_eq!(
            sido_predicate(&["pnu"], "41")?,
            "(pnu >= '41' AND pnu < '42')"
        );
        assert_eq!(
            sido_predicate(&["dataset_slug", "region_level", "region_code"], "09")?,
            "(region_code >= '09' AND region_code < '10')"
        );
        assert!(sido_predicate(&["complex_id"], "41").is_err());
        for bad in ["4", "411", "4'", "ab", "+1", "99"] {
            assert!(sido_predicate(&["pnu"], bad).is_err(), "{bad} was taken");
        }
        Ok(())
    }

    /// The contract's current-row rule with the 시도 range, on the region stats' five-column key:
    /// only the 시도's keys, each at its newest version, a removed one gone.
    #[test]
    fn the_current_rows_of_a_multi_column_key_are_read_for_one_sido() -> anyhow::Result<()> {
        let versions = contract("gold.real_transaction_region_stats_versions")?;
        let latest = versions
            .current_row_latest()
            .context("a versions table has a current-row rule")?;
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            "CREATE TABLE v (dataset_slug VARCHAR, region_level VARCHAR, region_code VARCHAR, \
               period_kind VARCHAR, period VARCHAR, n BIGINT, version_seq BIGINT, row_state VARCHAR); \
             INSERT INTO v VALUES \
               ('d', 'sigungu', '41999', 'month', '209901', 1, 1, NULL), \
               ('d', 'sigungu', '41999', 'month', '209901', 2, 2, NULL), \
               ('d', 'legal_dong', '4199900000', 'month', '209901', 3, 1, NULL), \
               ('d', 'legal_dong', '4199900000', 'month', '209901', 3, 2, 'removed'), \
               ('d', 'sigungu', '11999', 'month', '209901', 4, 1, NULL);",
        )?;
        let predicate = sido_predicate(latest.key, "41")?;
        let mut columns: Vec<&str> = latest.key.to_vec();
        columns.push("n");
        let query = latest.current_rows_sql("v", &columns, &predicate);
        let kept: Vec<(String, i64)> = connection
            .prepare(&format!("SELECT region_code, n FROM ({query}) ORDER BY 1"))?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        assert_eq!(kept, [("41999".to_owned(), 2)]);
        assert_eq!(sido_column(latest.key)?, "region_code");
        Ok(())
    }

    /// The national checks, computed in `DuckDB`: the same rows agree whatever their documents'
    /// bytes; a changed value, a value that moved to another key, another key and a tied key are
    /// each seen; a document column is not read.
    #[test]
    fn national_checks_see_counts_keys_and_plain_columns() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(
            r#"CREATE TABLE a (pnu VARCHAR, n BIGINT, v_json VARCHAR);
               INSERT INTO a VALUES ('9999910100100010000', 1, '{"area_m2":1.0E7,"note":null}'),
                 ('9999920100100010000', 2, '[{"seq":2}]');
               CREATE TABLE same (pnu VARCHAR, n INTEGER, v_json VARCHAR);
               INSERT INTO same VALUES ('9999910100100010000', 1, '{"area_m2":10000000.0}'),
                 ('9999920100100010000', 2, NULL);
               CREATE TABLE changed AS SELECT pnu, n + 2 AS n, v_json FROM a;
               CREATE TABLE moved (pnu VARCHAR, n BIGINT, v_json VARCHAR);
               INSERT INTO moved VALUES ('9999910100100010000', 2, NULL),
                 ('9999920100100010000', 1, NULL);
               CREATE TABLE swapped AS
                 SELECT replace(pnu, '9999920', '9999930') AS pnu, n, v_json FROM a;
               CREATE TABLE tied (pnu VARCHAR, n BIGINT, v_json VARCHAR);
               INSERT INTO tied VALUES ('9999910100100010000', 1, '[]'),
                 ('9999910100100010000', NULL, NULL);"#,
        )?;
        let plain = plain_columns(&["pnu", "n", "v_json"]);
        assert_eq!(plain, ["pnu", "n"]);
        let count = |table: &str| {
            side_counts(
                &connection,
                &format!("SELECT pnu, n FROM {table}"),
                &["pnu"],
                &plain,
            )
        };
        let a = count("a")?;
        assert_eq!(
            a.non_null,
            [("pnu".to_owned(), 2), ("n".to_owned(), 2)],
            "the document column is not counted"
        );
        assert!(failed_checks(&a, &count("same")?).is_empty());
        for (table, seen) in [
            ("changed", "plain column hash sum"),
            ("moved", "plain column hash sum"),
            ("swapped", "key hash sum"),
        ] {
            let found = failed_checks(&a, &count(table)?);
            assert!(
                found.iter().any(|line| line.starts_with(seen)),
                "{table}: {found:?}"
            );
        }
        let tied = failed_checks(&a, &count("tied")?);
        assert!(tied
            .iter()
            .any(|line| line.starts_with("candidate holds 2 rows on 1 keys")));
        assert!(tied.iter().any(|line| line.starts_with("n non-null")));
        Ok(())
    }

    #[test]
    fn rows_print_as_json_with_wide_numbers_and_nulls() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        let text = json_array(
            &connection,
            "SELECT 1 AS n, 'a' AS s, NULL AS z, CAST('170141183460469231731687303715884105727' AS HUGEINT) AS h",
        )?;
        let rows: serde_json::Value = serde_json::from_str(&text)?;
        assert_eq!(rows[0]["n"], 1);
        assert_eq!(rows[0]["s"], "a");
        assert!(rows[0]["z"].is_null());
        assert!(text.contains("170141183460469231731687303715884105727"));
        assert_eq!(json_array(&connection, "SELECT 1 AS n WHERE FALSE")?, "[]");
        Ok(())
    }
}
