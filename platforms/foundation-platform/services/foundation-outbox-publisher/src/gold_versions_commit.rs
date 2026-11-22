//! The write path of a Gold of appended row versions (root ADR-0564 §3): plan from the newest commit
//! that records `foundation.version-seq`, diff the recomputed keys against the head's current rows,
//! check, append only the keys that changed (a removal marker for a key that left) while `main` is
//! still the head the diff read, and pin what the next run plans from.
//!
//! The table's producer computes its rows into a `DuckDB` table (`built`: the replaced table's
//! columns, one row per key) for the keys it recomputed; everything after that is here, so every
//! versions table is written the same way. No Spark session.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, HeadMoved, LakehouseDataCommitter, StagedFile,
};
use foundation_publisher_silver::duckdb::Connection;
use foundation_publisher_silver::duckdb_parquet_stage::{
    with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG,
};
use lakehouse_domain::{LakehouseTableContract, GOLD_VERSIONS_PNU_PARTITION};
use lakehouse_infrastructure::{IcebergRestCatalog, IcebergSnapshotSummary};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// The table's commit number (root ADR-0564 §2).
pub(crate) const VERSION_SEQ_PROPERTY: &str = "foundation.version-seq";
/// The current rows after the commit.
pub(crate) const CURRENT_RECORDS_PROPERTY: &str = "foundation.current-records";
/// The input pins, as `gold_rebuild.pins_property_value` writes them.
pub(crate) const SOURCE_SNAPSHOTS_PROPERTY: &str = "foundation.source-iceberg-snapshots";
/// The producer's code version (`gold_code_version.PROPERTY`).
pub(crate) const CODE_VERSION_PROPERTY: &str = "foundation.build-source-sha256";
/// The served parcel edition the rows were built from (`gold_rebuild.SERVED_EDITION_PROPERTY`).
pub(crate) const SERVED_EDITION_PROPERTY: &str = "foundation.served-parcel-edition";
/// The current rows' fingerprint (`gold_fingerprint.PROPERTY`).
pub(crate) const FINGERPRINT_PROPERTY: &str = "foundation.content-fingerprint";
/// The keys this commit appended (`gold_incremental.KEYS_CHANGED_PROPERTY`).
pub(crate) const KEYS_CHANGED_PROPERTY: &str = "foundation.keys-changed";
/// A record the producer checked that explains a commit over the bound on changed keys, which the
/// bake's change set accepts too (root ADR-0346, ADR-0596); absent on every other commit.
pub(crate) const RECORDED_EXPLANATION_PROPERTY: &str = "foundation.recorded-explanation";
/// The tag on the commit the next run plans from (`gold_incremental.BUILD_TAG`, root ADR-0338).
pub(crate) const BUILD_TAG: &str = "foundation-gold-build";
/// The tag that keeps, on each input, the snapshot the table was last built from
/// (`gold_incremental.INPUT_TAG_PREFIX`).
pub(crate) const INPUT_TAG_PREFIX: &str = "foundation-gold-input-";
/// `gold_fingerprint.HEX_DIGITS`.
const FINGERPRINT_HEX_DIGITS: usize = 15;

/// The commit the next run plans from: the newest in `main`'s ancestry that records the version
/// sequence; only compactions follow it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Base {
    /// Its snapshot.
    pub snapshot_id: i64,
    /// Its `foundation.version-seq`.
    pub version_seq: i64,
    /// The input pins it records.
    pub pins: BTreeMap<String, i64>,
    /// Its code version.
    pub code_version: String,
    /// The served parcel edition it was built from, if it records one.
    pub served_edition: Option<String>,
    /// The current rows after it.
    pub current_records: u64,
    /// The fingerprint of the current rows after it: (rows, digest sum).
    pub fingerprint: (u64, u128),
}

/// `rows:<count>;digest_sum:<sum>`.
fn parse_fingerprint(text: &str) -> anyhow::Result<(u64, u128)> {
    let (rows, sum) = text
        .strip_prefix("rows:")
        .and_then(|rest| rest.split_once(";digest_sum:"))
        .with_context(|| format!("{text:?} is not a content fingerprint"))?;
    Ok((rows.parse()?, sum.parse()?))
}

pub(crate) fn fingerprint_text((rows, sum): (u64, u128)) -> String {
    format!("rows:{rows};digest_sum:{sum}")
}

/// The base of `history` (`main`'s lineage, newest first), or `None` for a table with no row yet.
///
/// # Errors
/// Refuses a table that holds rows and no commit that records the version sequence: its order
/// would start again at 1 (root ADR-0564 §3-1).
pub(crate) fn base(history: &[IcebergSnapshotSummary]) -> anyhow::Result<Option<Base>> {
    for snapshot in history {
        let Some(sequence) = snapshot.summary.get(VERSION_SEQ_PROPERTY) else {
            continue;
        };
        let summary = &snapshot.summary;
        let required = |key: &str| {
            summary.get(key).with_context(|| {
                format!(
                    "snapshot {} records {VERSION_SEQ_PROPERTY} and no {key}",
                    snapshot.snapshot_id
                )
            })
        };
        return Ok(Some(Base {
            snapshot_id: snapshot.snapshot_id,
            version_seq: sequence
                .parse()
                .context("foundation.version-seq is not a number")?,
            pins: snapshot.source_snapshots(),
            code_version: required(CODE_VERSION_PROPERTY)?.clone(),
            served_edition: summary.get(SERVED_EDITION_PROPERTY).cloned(),
            current_records: required(CURRENT_RECORDS_PROPERTY)?.parse()?,
            fingerprint: parse_fingerprint(required(FINGERPRINT_PROPERTY)?)?,
        }));
    }
    let rows = history
        .first()
        .and_then(|head| head.summary.get("total-records"))
        .map_or(Ok(0), |text| text.parse::<u64>())?;
    ensure!(
        rows == 0,
        "the table holds {rows} rows and no commit in main's ancestry records {VERSION_SEQ_PROPERTY}: \
         its version order cannot be continued"
    );
    Ok(None)
}

/// The run's identity: the table, its inputs, the code and the edition. A rerun of the same build
/// names the same batch, so the append's re-run guard refuses a second copy.
pub(crate) fn run_id(
    table: &str,
    pins: &BTreeMap<String, i64>,
    code_version: &str,
    served_edition: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(table.as_bytes());
    hash.update([0]);
    for (input, snapshot) in pins {
        hash.update(format!("{input}={snapshot};").as_bytes());
    }
    hash.update([0]);
    hash.update(code_version.as_bytes());
    hash.update([0]);
    hash.update(served_edition.as_bytes());
    let digest = format!("{:x}", hash.finalize());
    format!("{}-{}", table.replace('.', "-"), &digest[..40])
}

/// The pins as the summary property holds them: a JSON object of table to snapshot id text,
/// sorted, without spaces (`gold_rebuild.pins_property_value`).
pub(crate) fn pins_property(pins: &BTreeMap<String, i64>) -> anyhow::Result<String> {
    Ok(serde_json::to_string(
        &pins
            .iter()
            .map(|(table, snapshot)| (table.clone(), snapshot.to_string()))
            .collect::<BTreeMap<_, _>>(),
    )?)
}

/// What one commit appends: the diff of `built` against the head's current rows.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub(crate) struct Diff {
    /// Keys `built` holds.
    pub built: u64,
    /// Keys new to the table.
    pub added: u64,
    /// Keys whose content changed.
    pub changed: u64,
    /// Keys the recomputed range no longer holds (a removal marker each).
    pub removed: u64,
    /// The head's current rows in the recomputed range.
    pub current_in_range: u64,
}

impl std::ops::AddAssign for Diff {
    fn add_assign(&mut self, other: Self) {
        self.built += other.built;
        self.added += other.added;
        self.changed += other.changed;
        self.removed += other.removed;
        self.current_in_range += other.current_in_range;
    }
}

impl Diff {
    pub(crate) const fn keys(&self) -> u64 {
        self.added + self.changed + self.removed
    }
}

/// How a run's rows reach the table.
pub(crate) struct Commit<'a> {
    pub table: &'a str,
    pub versions: &'a LakehouseTableContract,
    /// The replaced table's columns: what `built` holds, in the table's order.
    pub content_columns: Vec<&'static str>,
    /// `main`'s head the diff reads (`None`: the table has no snapshot).
    pub head: Option<i64>,
    pub base: Option<Base>,
    pub version_seq: i64,
    /// The stamps a removal marker carries (this commit's).
    pub source_snapshot_id: String,
    pub published_at_utc: String,
}

/// The columns each build stamps on the rows it writes: the replaced table's per-commit partition
/// and the run's start (`appended_sql` stamps both on a removal marker). They differ on every commit
/// and say nothing of the content, so neither a digest nor a takeover verdict reads them.
pub(crate) const PER_COMMIT_STAMPS: [&str; 2] = ["source_snapshot_id", "published_at_utc"];
/// The content digest a by-PNU Gold carries on every row (root ADR-0099).
const ROW_DIGEST: &str = "row_digest";

impl Commit<'_> {
    /// The key: the versions table's current-row key (`pnu` for a by-PNU table).
    ///
    /// # Errors
    /// The versions table keeps no versions per key.
    pub(crate) fn key(&self) -> anyhow::Result<&'static [&'static str]> {
        Ok(self
            .versions
            .current_row_latest()
            .context("the versions table has no current-row rule")?
            .key)
    }

    /// A row's content digest as SQL over its content columns: its `row_digest`, or for a table
    /// that carries none (a Gold outside the by-PNU lanes) the SHA-256 of `to_json` of its content
    /// columns but the per-commit stamps.
    pub(crate) fn digest_sql(&self) -> String {
        if self.content_columns.contains(&ROW_DIGEST) {
            return ROW_DIGEST.to_owned();
        }
        let fields = self
            .content_columns
            .iter()
            .filter(|column| !PER_COMMIT_STAMPS.contains(column))
            .map(|column| format!("'{column}': {column}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("sha256(to_json({{{fields}}})::VARCHAR)")
    }
}

/// Diffs `built` against the current rows of the head among the keys `range` keeps (a predicate
/// on the key columns) and leaves the rows to append in `appended`.
///
/// # Errors
/// Refuses two current rows of one key at the head (the SQL rule keeps ties; the write path must
/// not, root ADR-0564 §4) and a key `built` holds twice.
pub(crate) fn diff(
    connection: &Connection,
    commit: &Commit<'_>,
    range: &str,
) -> anyhow::Result<Diff> {
    let key = commit.key()?.join(", ");
    let content = commit.content_columns.join(", ");
    let current = match commit.head {
        Some(head) => commit
            .versions
            .current_row_latest()
            .context("the versions table has no current-row rule")?
            .current_rows_sql(
                &with_contract_columns(
                    connection,
                    commit.versions,
                    &format!(
                        "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
                        commit.table
                    ),
                )?,
                &commit.content_columns,
                range,
            ),
        None => format!("SELECT {content} FROM built WHERE FALSE"),
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE current_rows AS {current};"
    ))?;
    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    let (rows, keys) = (
        count("SELECT count(*) FROM current_rows")?,
        count(&format!("SELECT count(DISTINCT ({key})) FROM current_rows"))?,
    );
    ensure!(
        rows == keys,
        "the head holds {} current rows tied on version_seq",
        rows - keys
    );
    let (built, built_keys) = (
        count("SELECT count(*) FROM built")?,
        count(&format!("SELECT count(DISTINCT ({key})) FROM built"))?,
    );
    ensure!(
        built == built_keys,
        "the build holds {} keys more than once",
        built - built_keys
    );
    assert_coverage(connection, commit)?;
    connection.execute_batch(&appended_sql(commit)?)?;
    Ok(Diff {
        built,
        added: count("SELECT count(*) FROM appended WHERE change = 'added'")?,
        changed: count("SELECT count(*) FROM appended WHERE change = 'changed'")?,
        removed: count("SELECT count(*) FROM appended WHERE change = 'removed'")?,
        current_in_range: rows,
    })
}

/// `section_coverage` of `gold-panel-rebuild.contract.json` (root ADR-0272), the one bound for
/// every table that carries served sections.
const REBUILD_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/gold-panel-rebuild.contract.json");
/// A section column holding one of these is empty (`section_coverage.EMPTY_VALUES`).
const EMPTY_SECTION_VALUES: &str = "'[]', '{}', 'null', ''";
/// The whole range, beside its 시도.
const WHOLE: &str = "all";

/// The coverage bound: the share a section's PNU count may fall by, and the 시도 size below which
/// a 시도 is judged only inside the whole.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CoverageBound {
    pub max_drop_fraction: f64,
    pub min_region_pnus: u64,
}

fn coverage_bound() -> anyhow::Result<CoverageBound> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    let declared = &contract["section_coverage"];
    let max_drop_fraction = declared["max_share_drop_fraction"]
        .as_f64()
        .filter(|drop| (0.0..1.0).contains(drop))
        .context("section_coverage.max_share_drop_fraction must be a number from 0 up to 1")?;
    let min_region_pnus = declared["min_region_pnus"]
        .as_u64()
        .context("section_coverage.min_region_pnus must be a non-negative integer")?;
    Ok(CoverageBound {
        max_drop_fraction,
        min_region_pnus,
    })
}

/// (section, 시도) -> PNUs whose section is non-empty.
pub(crate) type Coverage = BTreeMap<(String, String), u64>;

/// The region `region` is compared as, following `parent` to its root.
fn root_of(parent: &BTreeMap<String, String>, region: &str) -> String {
    let mut region = region.to_owned();
    while let Some(up) = parent.get(&region).filter(|up| **up != region) {
        region.clone_from(up);
    }
    region
}

/// Region -> the region it is compared as (`section_coverage.merged_regions`): a recorded 시도 pair
/// whose old side only the served rows hold and whose new side the build holds (or the reverse)
/// joins the two, so moving PNUs to a new code is not a loss; every other region is itself.
fn merged_regions(
    served: &BTreeSet<String>,
    built: &BTreeSet<String>,
    pairs: &[(String, String)],
) -> BTreeMap<String, String> {
    let mut parent: BTreeMap<String, String> = served
        .union(built)
        .map(|region| (region.clone(), region.clone()))
        .collect();
    let only_served = |region: &String| served.contains(region) && !built.contains(region);
    for (old, new) in pairs {
        if old == new || !parent.contains_key(old) || !parent.contains_key(new) {
            continue;
        }
        if (only_served(old) && built.contains(new)) || (only_served(new) && built.contains(old)) {
            let (old_root, new_root) = (root_of(&parent, old), root_of(&parent, new));
            parent.insert(old_root, new_root);
        }
    }
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for region in parent.keys() {
        groups
            .entry(root_of(&parent, region))
            .or_default()
            .push(region.clone());
    }
    parent
        .keys()
        .map(|region| {
            let group = groups
                .get(&root_of(&parent, region))
                .map_or_else(|| region.clone(), |group| group.join("+"));
            (region.clone(), group)
        })
        .collect()
}

fn regrouped(coverage: &Coverage, regions: &BTreeMap<String, String>) -> Coverage {
    let mut out = Coverage::new();
    for ((section, region), present) in coverage {
        let grouped = regions.get(region).unwrap_or(region).clone();
        for slot in [grouped, WHOLE.to_owned()] {
            *out.entry((section.clone(), slot)).or_default() += present;
        }
    }
    out
}

/// Every (section, region) whose PNU count fell by more than the bound, as readable lines
/// (`section_coverage.breaches` of a kind table, whose rows are its PNUs).
pub(crate) fn coverage_breaches(
    served: &Coverage,
    built: &Coverage,
    pairs: &[(String, String)],
    bound: CoverageBound,
) -> Vec<String> {
    let regions_of =
        |coverage: &Coverage| coverage.keys().map(|(_, region)| region.clone()).collect();
    let regions = merged_regions(&regions_of(served), &regions_of(built), pairs);
    let (before, after) = (regrouped(served, &regions), regrouped(built, &regions));
    let mut found = Vec::new();
    for ((section, region), present) in &before {
        // Spark judged the whole table always; here the whole is only the recomputed range, so a
        // range too small for a share to mean anything is not judged either (a partial run of a few
        // PNUs may lose some of them by right).
        if *present < bound.min_region_pnus {
            continue;
        }
        let now = after
            .get(&(section.clone(), region.clone()))
            .copied()
            .unwrap_or(0);
        // PNU counts are far below 2^52, so the floats are exact.
        #[allow(clippy::cast_precision_loss)]
        let fell = (now as f64) < (*present as f64) * (1.0 - bound.max_drop_fraction);
        if *present > 0 && fell {
            let place = if region == WHOLE {
                "the whole recomputed range".to_owned()
            } else {
                format!("시도 {region}")
            };
            found.push(format!("{section} in {place}: {present} -> {now} PNUs"));
        }
    }
    found
}

/// (section, 시도) counts of `relation`'s rows whose section column is non-empty.
fn coverage_of(
    connection: &Connection,
    relation: &str,
    key: &str,
    sections: &[&str],
) -> anyhow::Result<Coverage> {
    let mut coverage = Coverage::new();
    for section in sections {
        let mut statement = connection.prepare(&format!(
            "SELECT substr({key}, 1, 2), count(*) FROM {relation} \
             WHERE coalesce({section}, '') NOT IN ({EMPTY_SECTION_VALUES}) GROUP BY 1"
        ))?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (region, present) = row?;
            coverage.insert(((*section).to_owned(), region), u64::try_from(present)?);
        }
    }
    Ok(coverage)
}

/// The 시도 pairs (old 시도, new 시도) of the code change table, when the stage read it: every
/// `DuckDB` stage that reads `reference.legal_dong_code_change` leaves its pairs in `pair_rows`
/// (`legal_dong_code_change_stage`); a stage that does not has none.
fn region_pairs(connection: &Connection) -> anyhow::Result<Vec<(String, String)>> {
    let present: i64 = connection.query_row(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'pair_rows'",
        [],
        |row| row.get(0),
    )?;
    if present == 0 {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
        "SELECT DISTINCT substr(old_code, 1, 2), substr(new_code, 1, 2) FROM pair_rows \
         WHERE substr(old_code, 1, 2) <> substr(new_code, 1, 2) ORDER BY 1, 2",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// Refuses, before anything is appended, a build whose served sections lost a 시도's coverage
/// against the head's current rows over the same recomputed range (root ADR-0272): on 2026-10-16 a
/// panel lost one 시도's section with every row still present. The sections are the content
/// columns that hold documents (`*_json`); a 시도 outside the range is unchanged by this commit.
fn assert_coverage(connection: &Connection, commit: &Commit<'_>) -> anyhow::Result<()> {
    let sections: Vec<&str> = commit
        .content_columns
        .iter()
        .copied()
        .filter(|column| column.ends_with("_json"))
        .collect();
    // The sections are a by-PNU table's: a table whose key does not start with the PNU serves no
    // parcel a section.
    if sections.is_empty() || commit.head.is_none() || commit.key()?.first() != Some(&"pnu") {
        return Ok(());
    }
    let served = coverage_of(connection, "current_rows", "pnu", &sections)?;
    let built = coverage_of(connection, "built", "pnu", &sections)?;
    let bound = coverage_bound()?;
    let found = coverage_breaches(&served, &built, &region_pairs(connection)?, bound);
    ensure!(
        found.is_empty(),
        "{}: section coverage fell by more than {:.0}% against the head \
         (contracts/gold-panel-rebuild.contract.json section_coverage): {}; nothing was appended",
        commit.table,
        bound.max_drop_fraction * 100.0,
        found.join("; ")
    );
    Ok(())
}

/// The statement that leaves in `appended` the rows `built` adds over `current_rows`: a new or
/// changed key's row with this commit's `version_seq`, and for a key `built` no longer holds its
/// last current row, marked `removed` and stamped with this commit's lineage (root ADR-0564 §2).
pub(crate) fn appended_sql(commit: &Commit<'_>) -> anyhow::Result<String> {
    let keys = commit.key()?;
    let (first, key) = (keys[0], keys.join(", "));
    let digest = commit.digest_sql();
    // A removal marker's stamp, as the column's type holds it.
    let stamped = |column: &str, value: &str| {
        let timestamp = commit
            .versions
            .columns
            .iter()
            .any(|known| known.name == column && known.logical_type == "timestamp");
        if timestamp {
            format!("CAST('{value}' AS TIMESTAMPTZ) AS {column}")
        } else {
            format!("'{value}' AS {column}")
        }
    };
    let stamps = [&commit.source_snapshot_id, &commit.published_at_utc];
    ensure!(
        stamps.iter().all(|stamp| !stamp.contains(['\'', '\\'])),
        "a stamp holds a quote"
    );
    let removed_columns = commit
        .content_columns
        .iter()
        .map(|column| match *column {
            "source_snapshot_id" => stamped("source_snapshot_id", &commit.source_snapshot_id),
            "published_at_utc" => stamped("published_at_utc", &commit.published_at_utc),
            other => format!("c.{other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let built_columns = commit
        .content_columns
        .iter()
        .map(|column| format!("b.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sequence = commit.version_seq;
    Ok(format!(
        "CREATE OR REPLACE TABLE appended AS \
         WITH b AS (SELECT *, {digest} AS foundation_digest FROM built), \
              c AS (SELECT *, {digest} AS foundation_digest FROM current_rows) \
         SELECT {built_columns}, CAST({sequence} AS BIGINT) AS version_seq, CAST(NULL AS VARCHAR) AS row_state, \
                CASE WHEN c.{first} IS NULL THEN 'added' ELSE 'changed' END AS change \
         FROM b LEFT JOIN c USING ({key}) \
         WHERE c.{first} IS NULL OR c.foundation_digest IS DISTINCT FROM b.foundation_digest \
         UNION ALL \
         SELECT {removed_columns}, CAST({sequence} AS BIGINT) AS version_seq, 'removed' AS row_state, 'removed' AS change \
         FROM c ANTI JOIN b USING ({key});"
    ))
}

/// The digest sums one range's diff moves: what its appended rows add and the head rows they hide.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Digests {
    pub added: u128,
    pub hidden: u128,
}

impl std::ops::AddAssign for Digests {
    fn add_assign(&mut self, other: Self) {
        self.added += other.added;
        self.hidden += other.hidden;
    }
}

/// The digest sums of the range [`diff`] last left in `appended` and `current_rows`.
pub(crate) fn digests(connection: &Connection, commit: &Commit<'_>) -> anyhow::Result<Digests> {
    let key = commit.key()?.join(", ");
    let number = format!(
        "CAST(('0x' || substr({}, 1, {FINGERPRINT_HEX_DIGITS})) AS HUGEINT)",
        commit.digest_sql()
    );
    let sum = |sql: &str| -> anyhow::Result<u128> {
        let value: String = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(value.parse()?)
    };
    Ok(Digests {
        added: sum(&format!(
            "SELECT CAST(coalesce(sum({number}), 0) AS VARCHAR) FROM appended WHERE change <> 'removed'"
        ))?,
        hidden: sum(&format!(
            "SELECT CAST(coalesce(sum({number}), 0) AS VARCHAR) FROM current_rows \
             SEMI JOIN (SELECT {key} FROM appended) USING ({key})"
        ))?,
    })
}

/// The fingerprint and the current rows after the commit, from the base's, the diff of every range
/// and their summed [`Digests`] (root ADR-0564 §5: the fingerprint sums, so it is updated by adding
/// and subtracting).
pub(crate) fn after_digests(
    commit: &Commit<'_>,
    diff: &Diff,
    digests: Digests,
) -> anyhow::Result<((u64, u128), u64)> {
    let (rows, total, current) = commit.base.as_ref().map_or((0, 0, 0), |base| {
        (base.fingerprint.0, base.fingerprint.1, base.current_records)
    });
    ensure!(
        rows == current,
        "the base records {current} current rows and a fingerprint of {rows}"
    );
    let rows = (rows + diff.added)
        .checked_sub(diff.removed)
        .context("the commit removes more keys than the table holds")?;
    let total = (total + digests.added)
        .checked_sub(digests.hidden)
        .context("the commit hides digests the base fingerprint does not hold")?;
    Ok(((rows, total), rows))
}

/// The fingerprint and the current rows after the commit of one range: [`after_digests`] for a
/// table that carries `row_digest`; for one whose digest is computed ([`Commit::digest_sql`]),
/// [`recomputed`] over every current row, so a change of its columns or of the formula never
/// leaves two formulas in one sum.
pub(crate) fn after(
    connection: &Connection,
    commit: &Commit<'_>,
    diff: &Diff,
) -> anyhow::Result<((u64, u128), u64)> {
    if commit.digest_sql() != ROW_DIGEST {
        return recomputed(connection, commit);
    }
    after_digests(commit, diff, digests(connection, commit)?)
}

/// The fingerprint and the current rows after the commit, counted over the head's current rows
/// that the commit does not touch and the rows it appends (removal markers aside). It reads the
/// head's current rows whole: only the Golds outside the lanes, whose digest is computed, use it.
fn recomputed(connection: &Connection, commit: &Commit<'_>) -> anyhow::Result<((u64, u128), u64)> {
    let key = commit.key()?.join(", ");
    let content = commit.content_columns.join(", ");
    let head = match commit.head {
        Some(head) => commit
            .versions
            .current_row_latest()
            .context("the versions table has no current-row rule")?
            .current_rows_sql(
                &with_contract_columns(
                    connection,
                    commit.versions,
                    &format!(
                        "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
                        commit.table
                    ),
                )?,
                &commit.content_columns,
                "TRUE",
            ),
        None => format!("SELECT {content} FROM built WHERE FALSE"),
    };
    let number = format!(
        "CAST(('0x' || substr({}, 1, {FINGERPRINT_HEX_DIGITS})) AS HUGEINT)",
        commit.digest_sql()
    );
    let (rows, sum): (i64, String) = connection.query_row(
        &format!(
            "WITH kept AS (SELECT {content} FROM ({head}) h ANTI JOIN (SELECT {key} FROM appended) a \
               USING ({key})), \
             after_commit AS (SELECT {content} FROM kept \
               UNION ALL SELECT {content} FROM appended WHERE change <> 'removed') \
             SELECT count(*), CAST(coalesce(sum({number}), 0) AS VARCHAR) FROM after_commit"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let rows = u64::try_from(rows)?;
    Ok(((rows, sum.parse()?), rows))
}

/// The SQL a partition entry of a versions table splits rows by: the 시도 of a table keyed by PNU
/// (`truncate(2, pnu)`), or an identity column of any other (root ADR-0564 §2).
fn partition_expression(spec: &str) -> anyhow::Result<String> {
    if spec == GOLD_VERSIONS_PNU_PARTITION {
        return Ok("substr(pnu, 1, 2)".to_owned());
    }
    ensure!(
        !spec.is_empty()
            && spec
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "the partition {spec:?} is neither the 시도 of the PNU nor an identity column"
    );
    Ok(spec.to_owned())
}

/// Stages `appended` as one file per value of the versions table's partition (root ADR-0564 §2: the
/// 시도 of a table keyed by PNU, the identity columns of any other), each sorted by the key.
pub(crate) fn stage(
    stage: &DuckDbParquetStage,
    shape: &foundation_publisher_core::lakehouse_data_commit::TableShape,
    run: &str,
    versions: &LakehouseTableContract,
) -> anyhow::Result<Vec<StagedFile>> {
    let connection = stage.connection();
    let key = versions
        .current_row_latest()
        .context("the versions table has no current-row rule")?
        .key
        .join(", ");
    let parts = versions
        .partition_spec
        .iter()
        .map(|spec| partition_expression(spec))
        .collect::<anyhow::Result<Vec<_>>>()?;
    ensure!(
        !parts.is_empty(),
        "{} has no partition to split its files by",
        versions.table_name
    );
    let select = parts
        .iter()
        .enumerate()
        .map(|(at, part)| format!("CAST({part} AS VARCHAR) AS part_{at}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = connection
        .prepare(&format!(
            "SELECT DISTINCT {select} FROM appended ORDER BY ALL"
        ))?
        .query_map([], |row| {
            (0..parts.len())
                .map(|at| row.get::<_, Option<String>>(at))
                .collect::<Result<Vec<_>, _>>()
        })?
        .collect::<Result<Vec<_>, _>>()?;
    values
        .iter()
        .map(|values| {
            let mut filter = Vec::with_capacity(parts.len());
            let mut name = Vec::with_capacity(parts.len());
            for ((part, spec), value) in parts.iter().zip(versions.partition_spec).zip(values) {
                let value = value
                    .as_deref()
                    .with_context(|| format!("a row of {} has no {spec}", versions.table_name))?;
                if *spec == GOLD_VERSIONS_PNU_PARTITION {
                    ensure!(
                        value.bytes().all(|byte| byte.is_ascii_digit()),
                        "a key starts with {value:?}, not a 시도"
                    );
                } else {
                    ensure!(
                        !value.is_empty()
                            && value.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                            }),
                        "the {spec} {value:?} of {} is not a plain value",
                        versions.table_name
                    );
                }
                filter.push(format!("{part} = '{value}'"));
                name.push(value);
            }
            stage.write(
                shape,
                &format!(
                    "SELECT * EXCLUDE (change) FROM appended WHERE {} ORDER BY {key}",
                    filter.join(" AND ")
                ),
                &format!("{run}-{}", name.join("-")),
            )
        })
        .collect()
}

/// Appends the staged files while `main` is still the diff's head. A response lost after the
/// commit landed is read back from the table: the head then records this run's batch.
pub(crate) async fn append(
    committer: &LakehouseDataCommitter,
    catalog: &IcebergRestCatalog,
    commit: &Commit<'_>,
    files: &[StagedFile],
    run: &str,
    summary: BTreeMap<String, String>,
) -> anyhow::Result<i64> {
    let already_ingested = catalog
        .load_ingested_batch_objects(commit.table)
        .await?
        .with_context(|| format!("{} does not exist", commit.table))?;
    let batch = [run.to_owned()];
    let outcome = committer
        .append_at_head(
            DataAppend {
                table_name: commit.table,
                contract: commit.versions,
                files,
                batch: &batch,
                already_ingested: &already_ingested,
                summary,
            },
            commit.head,
        )
        .await;
    match outcome {
        Ok(AppendOutcome::Appended { snapshot, .. }) => Ok(snapshot.snapshot_id),
        Ok(AppendOutcome::AlreadyIngested) => head_holding(catalog, commit.table, run).await,
        Err(error) if error.downcast_ref::<HeadMoved>().is_some() => {
            // Our own commit, whose answer was lost, moved the head: the table is the record.
            match head_holding(catalog, commit.table, run).await {
                Ok(snapshot) => Ok(snapshot),
                Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// The snapshot of `main`'s lineage that records batch `run`.
async fn head_holding(catalog: &IcebergRestCatalog, table: &str, run: &str) -> anyhow::Result<i64> {
    catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .iter()
        .find(|snapshot| {
            snapshot
                .ingested_objects()
                .iter()
                .any(|object| object == run)
        })
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| format!("no snapshot of {table} records batch {run}"))
}

/// Moves `tag` of `table` to `snapshot`: a tag is never moved in place (`create_tag`), so the old
/// one is removed while it still names its snapshot, then the new one made.
async fn move_tag(
    catalog: &IcebergRestCatalog,
    table: &str,
    tag: &str,
    snapshot: i64,
) -> anyhow::Result<()> {
    let refs = catalog
        .load_snapshot_refs(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    match refs.refs.get(tag) {
        Some(existing) if existing.kind == "tag" && existing.snapshot_id == snapshot => {
            return Ok(())
        }
        Some(existing) if existing.kind == "tag" => {
            catalog.remove_tag(table, tag, existing.snapshot_id).await?;
        }
        Some(existing) => bail!("{table} has a {} named {tag}", existing.kind),
        None => {}
    }
    catalog.create_tag(table, tag, snapshot).await?;
    Ok(())
}

/// Pins what the next run plans from (root ADR-0564 §3-8): the build tag on `build`, the input tag
/// on each input's pin. Every run does it after its plan as well, so a run that died between its
/// commit and its tags is mended by the next, even one with nothing to do.
pub(crate) async fn pin(
    catalog: &IcebergRestCatalog,
    table: &str,
    build: i64,
    pins: &BTreeMap<String, i64>,
) -> anyhow::Result<Value> {
    move_tag(catalog, table, BUILD_TAG, build).await?;
    let input_tag = format!("{INPUT_TAG_PREFIX}{}", table.replace('.', "-"));
    for (input, snapshot) in pins {
        move_tag(catalog, input, &input_tag, *snapshot).await?;
    }
    Ok(json!({"build": build, "input_tag": input_tag, "inputs": pins.len()}))
}

// one-off: until the last served Gold has moved to its `_versions` table (no lane line says
// `versions_table` while it serves the replaced table): `Shadow`, `shadow`, `shadow_of`,
// `UNTAGGED_BUILDS`, `untagged_shadow` and the shadow branch of `planned_inputs` go with it.
/// What a versions table is built from while its lane still serves the table it replaces (root
/// ADR-0564 §6): the inputs that table's newest build read, so the takeover verdict compares two
/// engines over one set of inputs, and so that every key is built the first time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Shadow {
    /// The served table it replaces (`gold.<x>` of `gold.<x>_versions`).
    pub replaced: String,
    /// The replaced table's commit its build tag names.
    pub build: i64,
    /// The input pins that commit records: the only inputs the shadow reads; an input that build
    /// did not read is absent here too.
    pub pins: BTreeMap<String, i64>,
    /// The served parcel edition that commit was built from, if it records one.
    pub served_edition: Option<String>,
}

/// The replaced table of a versions table.
fn replaced_of(versions: &str) -> anyhow::Result<&str> {
    versions
        .strip_suffix("_versions")
        .with_context(|| format!("{versions} is not a versions table (gold.<x>_versions)"))
}

/// The shadow of `versions` while its lane serves `served`, from the replaced table's build tag and
/// `main` lineage; `None` once the lane serves `versions` itself (its pins are the input heads).
///
/// # Errors
/// Refuses a lane that serves neither table, and a replaced table whose build tag names no commit
/// of its `main` lineage or one that records no input pins: there is nothing to compare with.
pub(crate) fn shadow(
    versions: &str,
    served: &str,
    build_tag: Option<&lakehouse_infrastructure::IcebergSnapshotRef>,
    history: &[IcebergSnapshotSummary],
) -> anyhow::Result<Option<Shadow>> {
    if served == versions {
        return Ok(None);
    }
    let replaced = replaced_of(versions)?;
    ensure!(
        served == replaced,
        "the lane serves {served}, neither {versions} nor the {replaced} it replaces"
    );
    let tag = build_tag
        .filter(|tag| tag.kind == "tag")
        .with_context(|| format!("{replaced} has no {BUILD_TAG} tag: no build to shadow"))?;
    let commit = history
        .iter()
        .find(|snapshot| snapshot.snapshot_id == tag.snapshot_id)
        .with_context(|| {
            format!(
                "{replaced}'s {BUILD_TAG} names snapshot {}, not in its main lineage",
                tag.snapshot_id
            )
        })?;
    let pins = commit.source_snapshots();
    ensure!(
        !pins.is_empty(),
        "{replaced} snapshot {} records no {SOURCE_SNAPSHOTS_PROPERTY}",
        commit.snapshot_id
    );
    Ok(Some(Shadow {
        replaced: replaced.to_owned(),
        build: commit.snapshot_id,
        pins,
        served_edition: commit.summary.get(SERVED_EDITION_PROPERTY).cloned(),
    }))
}

// one-off: until gold.parcel_boundary_served is replaced (its switch deletes the Spark job and this).
/// The replaced tables whose builds carry no build tag and record no input pins, with the inputs
/// their builds read: the Spark served-boundary job (root ADR-0133 §2) overwrites
/// `gold.parcel_boundary_served` with plain Iceberg commits. It reads each input at its head (the
/// ledger after appending the edits it was handed) and commits within the same run, under the bake's
/// lock, so the inputs it read are each input as of its commit: Iceberg's time travel by commit
/// time, taken here from the catalog's snapshot log.
const UNTAGGED_BUILDS: &[(&str, &[&str])] = &[(
    "gold.parcel_boundary_served",
    &["silver.parcel_boundaries", "silver.map_edit_ledger"],
)];

/// The commit operation of a compaction, which rewrites files and keeps the rows.
const COMPACTION_OPERATION: &str = "replace";

/// The shadow of `versions` whose replaced table's builds are untagged ([`UNTAGGED_BUILDS`]): the
/// build is the newest commit of the replaced table's `main` that is not a compaction, and each
/// input is pinned at its newest snapshot committed by then; an input that had no snapshot yet (an
/// empty ledger) is absent, as the build read none of it. The served edition is the build's rows'
/// (the caller reads it), so it is `None` here.
///
/// # Errors
/// Refuses a lane that serves neither table, a replaced table whose every kept commit is a
/// compaction or whose build has no commit time, and a build no input had a snapshot before.
pub(crate) fn untagged_shadow(
    versions: &str,
    served: &str,
    history: &[IcebergSnapshotSummary],
    inputs: &BTreeMap<String, Vec<IcebergSnapshotSummary>>,
) -> anyhow::Result<Shadow> {
    let replaced = replaced_of(versions)?;
    ensure!(
        served == replaced,
        "the lane serves {served}, neither {versions} nor the {replaced} it replaces"
    );
    let build = history
        .iter()
        .find(|snapshot| {
            snapshot.summary.get("operation").map(String::as_str) != Some(COMPACTION_OPERATION)
        })
        .with_context(|| {
            format!("{replaced} keeps no build in its main lineage, only compactions: no build to shadow")
        })?;
    let at = build.timestamp_ms.with_context(|| {
        format!(
            "{replaced} snapshot {} has no commit time",
            build.snapshot_id
        )
    })?;
    let pins: BTreeMap<String, i64> = inputs
        .iter()
        .filter_map(|(input, history)| {
            history
                .iter()
                .find(|snapshot| snapshot.timestamp_ms.is_some_and(|ms| ms <= at))
                .map(|snapshot| (input.clone(), snapshot.snapshot_id))
        })
        .collect();
    ensure!(
        !pins.is_empty(),
        "no input of {replaced} had a snapshot when its build {} committed",
        build.snapshot_id
    );
    Ok(Shadow {
        replaced: replaced.to_owned(),
        build: build.snapshot_id,
        pins,
        served_edition: None,
    })
}

/// [`shadow`], read from the catalog: the replaced table's refs and `main` lineage (and, for an
/// untagged build, its inputs' lineages, [`untagged_shadow`]).
pub(crate) async fn shadow_of(
    catalog: &IcebergRestCatalog,
    versions: &str,
    served: &str,
) -> anyhow::Result<Option<Shadow>> {
    if served == versions {
        return Ok(None);
    }
    let replaced = replaced_of(versions)?;
    if let Some((_, inputs)) = UNTAGGED_BUILDS.iter().find(|(table, _)| *table == replaced) {
        let history = catalog
            .load_main_snapshot_history(replaced)
            .await?
            .with_context(|| format!("{replaced} does not exist"))?;
        let mut lineages = BTreeMap::new();
        for input in *inputs {
            let lineage = catalog
                .load_main_snapshot_history(input)
                .await?
                .unwrap_or_default();
            lineages.insert((*input).to_owned(), lineage);
        }
        return untagged_shadow(versions, served, &history, &lineages).map(Some);
    }
    let refs = catalog
        .load_snapshot_refs(replaced)
        .await?
        .with_context(|| format!("{replaced} does not exist"))?;
    let history = catalog
        .load_main_snapshot_history(replaced)
        .await?
        .with_context(|| format!("{replaced} does not exist"))?;
    shadow(versions, served, refs.refs.get(BUILD_TAG), &history)
}

/// The pins and served edition one run of `versions` builds from, and its shadow, if any: while
/// its lane serves the replaced table, the replaced table's build-tag commit's (the edition when it
/// records none is `edition_now`); otherwise every input at its head and `edition_now`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Planned {
    pub pins: BTreeMap<String, i64>,
    pub served_edition: String,
    pub shadow: Option<Shadow>,
}

/// [`Planned`] of `versions` whose lane serves `served`.
pub(crate) async fn planned_inputs(
    catalog: &IcebergRestCatalog,
    versions: &str,
    served: &str,
    inputs: &[&str],
    edition_now: String,
) -> anyhow::Result<Planned> {
    if let Some(shadow) = shadow_of(catalog, versions, served).await? {
        return Ok(Planned {
            pins: shadow.pins.clone(),
            served_edition: shadow.served_edition.clone().unwrap_or(edition_now),
            shadow: Some(shadow),
        });
    }
    let mut pins = BTreeMap::new();
    for input in inputs {
        let head = catalog
            .load_main_snapshot_history(input)
            .await?
            .with_context(|| format!("{input} does not exist"))?
            .first()
            .map(|snapshot| snapshot.snapshot_id)
            .with_context(|| format!("{input} holds no snapshot"))?;
        pins.insert((*input).to_owned(), head);
    }
    Ok(Planned {
        pins,
        served_edition: edition_now,
        shadow: None,
    })
}

/// The inputs whose pin moved from `base`'s, and every input `base` does not pin.
pub(crate) fn moved(base: &Base, pins: &BTreeMap<String, i64>) -> BTreeSet<String> {
    pins.iter()
        .filter(|(input, snapshot)| base.pins.get(*input) != Some(*snapshot))
        .map(|(input, _)| input.clone())
        .collect()
}

#[cfg(test)]
mod tests;
