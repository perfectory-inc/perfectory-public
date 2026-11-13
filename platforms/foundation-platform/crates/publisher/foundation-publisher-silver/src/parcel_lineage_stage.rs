//! Derives `silver.parcel_lineage` for one pair of parcel editions and one group of sido prefixes.
//!
//! Which old parcel became which new parcel, and on what evidence (root ADR-0113 §1·§4·§5), in
//! `DuckDB` inside the publisher (root ADR-0559): it was the Spark job
//! `parcel_lineage_to_silver.py`, whose Python kernels read both editions of a group on the
//! driver. `inputs.sql` reads the pinned inputs from the lakehouse once; `derive.sql` holds every
//! rule the scheduled derivation applies (the 동 pairs of the code change table, lots carried
//! under a paired 동, what the land movement history states, and area and category that single
//! a pair out); the caller appends the staged file once
//! (`foundation_publisher_core::lakehouse_data_commit`).
//!
//! The run id is the digest of the inputs, as the Spark job computed it, so a group derived by
//! either engine from the same pins names the same run and the append's re-run guard skips it.

use std::fmt::Write as _;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::duckdb_parquet_stage::DuckDbParquetStage;

/// The table the derivation appends to.
pub const LINEAGE_TABLE: &str = "silver.parcel_lineage";
/// The rules a row was derived under; a group is derived once per pair, group and rules version.
pub const RULES_VERSION: &str = "parcel-lineage.v1";
/// The catalog name the Spark job's provenance spelled its inputs under; part of the run id.
const PROVENANCE_CATALOG: &str = "lakehouse";

const INPUTS_SQL: &str = include_str!("parcel_lineage_stage/inputs.sql");
const DERIVE_SQL: &str = include_str!("parcel_lineage_stage/derive.sql");
/// What Python's `str.strip()` and `\s` take for white space, as a regular expression class body:
/// the history texts were parsed with those, and RE2's `\s` is ASCII only.
const WHITE_SPACE_CLASS: &str = r"\s\x{0B}\x{1C}-\x{1F}\x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}";

/// One derivation: a pair of editions and the sido prefixes whose parcels pair with each other.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageGroup {
    /// `silver.parcel_boundaries.source_snapshot_id` of the earlier edition.
    pub from_snapshot_id: String,
    /// The later edition's.
    pub to_snapshot_id: String,
    /// `YYYY-MM-DD` the earlier edition reflects; history before it is already in it.
    pub from_date: String,
    /// `YYYY-MM-DD` the later edition reflects.
    pub to_date: String,
    /// Two-digit sido prefixes of the earlier edition's parcels.
    pub from_sido: Vec<String>,
    /// Two-digit sido prefixes of the later edition's parcels.
    pub to_sido: Vec<String>,
}

/// The Iceberg snapshots a pair's inputs are read at, pinned once per pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LineagePins {
    /// `silver.parcel_boundaries`, for both editions.
    pub boundaries: i64,
    /// `reference.legal_dong_code_change`.
    pub code_changes: i64,
    /// `silver.land_transfer_history`.
    pub history: i64,
}

/// A group whose code change table is behind: 동 codes that held parcels before and none after,
/// with no pair recorded in the window (root ADR-0145 §2). Nothing is derived for it.
#[derive(Debug)]
pub struct UnrecordedDongCodes {
    /// Every such code, sorted.
    pub codes: Vec<String>,
    from_date: String,
    to_date: String,
}

impl std::fmt::Display for UnrecordedDongCodes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} legal dong codes held parcels at {} and none at {}, and the code change table records no pair \
             for them effective in that window: {:?}. Record the change (legal_dong_code_change_pairs.py) \
             before deriving the lineage.",
            self.codes.len(),
            self.from_date,
            self.to_date,
            self.codes.iter().take(10).collect::<Vec<_>>()
        )
    }
}

impl std::error::Error for UnrecordedDongCodes {}

/// What one derivation found, for the run's summary and the Slack line.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LineageSummary {
    /// `before − vanished + appeared = after`, and how many changes a link explains.
    pub reconciliation: Reconciliation,
    /// Links per relation.
    pub links_by_relation: std::collections::BTreeMap<String, u64>,
    /// Links per grade.
    pub links_by_grade: std::collections::BTreeMap<String, u64>,
    /// Parcels whose last day before the window, or in it, holds more than one set of facts; the
    /// history's own sequence orders them.
    pub same_day_facts: u64,
}

/// The count reconciliation of one derivation (`parcel_lineage.reconcile`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Reconciliation {
    /// Well-formed parcels of the earlier edition.
    pub before: u64,
    /// Of the later edition.
    pub after: u64,
    /// Parcels whose lot survives, under its own or its paired 동.
    pub kept: u64,
    /// Parcels of the earlier edition that did not survive.
    pub vanished: u64,
    /// Parcels of the later edition no earlier lot reached.
    pub appeared: u64,
    /// Vanished parcels a non-pending link names as predecessor.
    pub vanished_explained: u64,
    /// Appeared parcels a non-pending link names as successor.
    pub appeared_explained: u64,
    /// Whether the counts balance.
    pub balanced: bool,
}

/// A derived group, staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedLineage {
    /// The run every row carries: the append's batch identity (the table's load unit is the run).
    pub derivation_run_id: String,
    /// How many rows the derivation made.
    pub rows: u64,
    /// The rows, as one Parquet file; `None` when the derivation made none.
    pub file: Option<StagedFile>,
    /// What it found.
    pub summary: LineageSummary,
}

/// The run id of a derivation: the Spark job's, byte for byte.
///
/// `parcel_lineage_to_silver.derivation_run_id` hashed `json.dumps(provenance, sort_keys=True,
/// separators=(",", ":"))`; this writes the same text. A run without building-register or
/// ownership inputs, the only kind the schedule ever made, has `null` for them.
///
/// # Errors
/// Refuses a value that is not what the job's parser admitted (a snapshot id, a date, a sido
/// prefix), since anything else would need escaping this does not do.
pub fn derivation_run_id(group: &LineageGroup, pins: LineagePins) -> anyhow::Result<String> {
    for text in [
        &group.from_snapshot_id,
        &group.to_snapshot_id,
        &group.from_date,
        &group.to_date,
    ] {
        ensure!(is_plain(text), "{text:?} is not a snapshot id or a date");
    }
    let sido = |prefixes: &[String]| -> anyhow::Result<String> {
        let mut sorted = prefixes.to_vec();
        sorted.sort();
        sorted.dedup();
        ensure!(
            !sorted.is_empty() && sorted.iter().all(|prefix| is_sido(prefix)),
            "sido prefixes must be two digits each: {prefixes:?}"
        );
        Ok(sorted
            .iter()
            .map(|prefix| format!("\"{prefix}\""))
            .collect::<Vec<_>>()
            .join(","))
    };
    let input = |table: &str, snapshot: i64| {
        format!("{{\"snapshot_id\":\"{snapshot}\",\"table\":\"{PROVENANCE_CATALOG}.{table}\"}}")
    };
    let mut key = String::new();
    write!(
        key,
        "{{\"building_source_snapshot_ids\":{{\"from\":null,\"to\":null}},\"from_date\":\"{}\",\"from_sido\":[{}],\
         \"from_snapshot_id\":\"{}\",\"iceberg_inputs\":{{\"boundaries_from\":{},\"boundaries_to\":{},\
         \"code_changes\":{},\"history\":{}}},\"ownership_sha256\":{{\"new\":null,\"old\":null}},\
         \"rules_version\":\"{RULES_VERSION}\",\"to_date\":\"{}\",\"to_sido\":[{}],\"to_snapshot_id\":\"{}\"}}",
        group.from_date,
        sido(&group.from_sido)?,
        group.from_snapshot_id,
        input("silver.parcel_boundaries", pins.boundaries),
        input("silver.parcel_boundaries", pins.boundaries),
        input("reference.legal_dong_code_change", pins.code_changes),
        input("silver.land_transfer_history", pins.history),
        group.to_date,
        sido(&group.to_sido)?,
        group.to_snapshot_id,
    )?;
    let digest = format!("{:x}", Sha256::digest(key.as_bytes()));
    Ok(format!("parcel-lineage-{}", &digest[..24]))
}

fn is_plain(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 200
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn is_sido(prefix: &str) -> bool {
    prefix.len() == 2 && prefix.bytes().all(|byte| byte.is_ascii_digit())
}

/// `column`'s rows of the given sido prefixes, as one range per prefix.
///
/// The same rows as `substr(column, 1, 2) IN (…)`, written so the scan can skip a file by the
/// bounds of `column`: read with the function alone, one group read both editions whole.
pub(crate) fn in_sido(column: &str, prefixes: &[String]) -> anyhow::Result<String> {
    let mut sorted = prefixes.to_vec();
    sorted.sort();
    sorted.dedup();
    ensure!(
        !sorted.is_empty() && sorted.iter().all(|prefix| is_sido(prefix)),
        "sido prefixes must be two digits each: {prefixes:?}"
    );
    // ':' sorts right after '9': every value that starts with the prefix and goes on in digits.
    let ranges = sorted
        .iter()
        .map(|prefix| {
            format!(
                "({column} >= '{prefix}' AND {column} < '{prefix}:' AND substr({column}, 1, 2) = '{prefix}')"
            )
        })
        .collect::<Vec<_>>();
    Ok(format!("({})", ranges.join(" OR ")))
}

/// Reads one group's inputs from the lakehouse attached to `stage` as `catalog`.
///
/// # Errors
/// Returns an error when a snapshot cannot be read or a value is not what the job's parser admitted.
pub fn load_inputs(
    stage: &DuckDbParquetStage,
    catalog: &str,
    group: &LineageGroup,
    pins: LineagePins,
) -> anyhow::Result<()> {
    ensure!(
        is_plain(&group.from_snapshot_id) && is_plain(&group.to_snapshot_id),
        "an edition's snapshot id has characters a snapshot id does not use"
    );
    ensure!(
        !catalog.is_empty()
            && catalog
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "the catalog must be a plain identifier"
    );
    let connection = stage.connection();
    // A snapshot from before the removal column has none; the contract's column reads NULL there.
    let has_row_state = connection
        .prepare(&format!(
            "DESCRIBE SELECT * FROM {catalog}.silver.land_transfer_history AT (VERSION => {})",
            pins.history
        ))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|column| column == "row_state");
    let mut all_sido = group.from_sido.clone();
    all_sido.extend(group.to_sido.iter().cloned());
    let sql = INPUTS_SQL
        .replace("{lakehouse}", catalog)
        .replace("{boundaries_from}", &pins.boundaries.to_string())
        .replace("{boundaries_to}", &pins.boundaries.to_string())
        .replace("{code_changes}", &pins.code_changes.to_string())
        .replace("{history}", &pins.history.to_string())
        .replace("{from_snapshot_id}", &group.from_snapshot_id)
        .replace("{to_snapshot_id}", &group.to_snapshot_id)
        .replace("{from_sido}", &in_sido("pnu", &group.from_sido)?)
        .replace("{to_sido}", &in_sido("pnu", &group.to_sido)?)
        .replace("{every_sido}", &in_sido("pnu", &all_sido)?)
        .replace(
            "{row_state}",
            if has_row_state {
                "row_state"
            } else {
                "CAST(NULL AS VARCHAR)"
            },
        );
    connection
        .execute_batch(&sql)
        .context("DuckDB failed to read the lineage inputs from the lakehouse")
}

/// What the derivation `connection` just ran found.
fn summarize(connection: &duckdb::Connection) -> anyhow::Result<LineageSummary> {
    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    let counted = |sql: &str| -> anyhow::Result<std::collections::BTreeMap<String, u64>> {
        connection
            .prepare(sql)?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .map(|pair| {
                let (name, count) = pair?;
                Ok((name, u64::try_from(count)?))
            })
            .collect()
    };
    let (before, after, vanished, appeared) = (
        count("SELECT count(*) FROM before_pnus")?,
        count("SELECT count(*) FROM after_pnus")?,
        count("SELECT count(*) FROM vanished")?,
        count("SELECT count(*) FROM appeared")?,
    );
    Ok(LineageSummary {
        reconciliation: Reconciliation {
            before,
            after,
            kept: before - vanished,
            vanished,
            appeared,
            vanished_explained: count(
                "SELECT count(*) FROM vanished WHERE pnu IN \
                 (SELECT predecessor_pnu FROM links WHERE grade <> 'pending' AND predecessor_pnu <> '')",
            )?,
            appeared_explained: count(
                "SELECT count(*) FROM appeared WHERE pnu IN (SELECT successor_pnu FROM links WHERE grade <> 'pending')",
            )?,
            balanced: before + appeared == after + vanished,
        },
        links_by_relation: counted("SELECT relation, count(*) FROM links GROUP BY relation")?,
        links_by_grade: counted("SELECT grade, count(*) FROM links GROUP BY grade")?,
        same_day_facts: count(
            "SELECT (SELECT count(*) FROM old_facts WHERE ties > 1) + (SELECT count(*) FROM new_facts WHERE ties > 1)",
        )?,
    })
}

/// Derives the group from the inputs `stage` holds (`load_inputs`, or a test's own tables) and
/// stages its rows, each stamped `derived_at` (whole seconds).
///
/// # Errors
/// [`UnrecordedDongCodes`] when the code change table is behind for the group; otherwise an error
/// when a date is not `YYYY-MM-DD`, one side holds no parcel, or `DuckDB` fails.
pub fn stage_group(
    stage: &DuckDbParquetStage,
    shape: &TableShape,
    group: &LineageGroup,
    pins: LineagePins,
    derived_at: DateTime<Utc>,
) -> anyhow::Result<StagedLineage> {
    for date in [&group.from_date, &group.to_date] {
        chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .with_context(|| format!("{date:?} must be YYYY-MM-DD"))?;
    }
    ensure!(
        group.from_date < group.to_date,
        "the earlier edition's date must be before the later one's"
    );
    let derivation_run_id = derivation_run_id(group, pins)?;
    let connection = stage.connection();
    let count = |sql: &str| -> anyhow::Result<u64> {
        let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
        Ok(u64::try_from(value)?)
    };
    let (before_raw, after_raw) = (
        count("SELECT count(*) FROM before_raw")?,
        count("SELECT count(*) FROM after_raw")?,
    );
    ensure!(
        before_raw > 0 && after_raw > 0,
        "no parcels for one side: before={before_raw} after={after_raw}"
    );
    let sql = DERIVE_SQL
        .replace("{from_date}", &group.from_date)
        .replace("{to_date}", &group.to_date)
        .replace("{from_day}", &group.from_date.replace('-', ""))
        .replace("{to_day}", &group.to_date.replace('-', ""))
        .replace("{ws}", WHITE_SPACE_CLASS);
    connection
        .execute_batch(crate::legal_dong_code_change_stage::PAIR_ROWS_SQL)
        .context("DuckDB failed to read the code change pairs in force")?;
    connection
        .execute_batch(&sql)
        .context("DuckDB failed to derive the lineage")?;
    let codes = connection
        .prepare("SELECT code FROM dong_unrecorded ORDER BY code")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if !codes.is_empty() {
        return Err(UnrecordedDongCodes {
            codes,
            from_date: group.from_date.clone(),
            to_date: group.to_date.clone(),
        }
        .into());
    }
    let summary = summarize(connection)?;
    let rows = count("SELECT count(*) FROM lineage_rows")?;
    let file = if rows == 0 {
        None
    } else {
        ensure!(
            is_plain(&derivation_run_id),
            "the run id is not a plain name"
        );
        let query = format!(
            "SELECT predecessor_pnu, successor_pnu, relation, cardinality, effective_date, grade, evidence_kind, \
             evidence_ref, '{}' AS from_snapshot_id, '{}' AS to_snapshot_id, '{RULES_VERSION}' AS rules_version, \
             '{derivation_run_id}' AS derivation_run_id, to_timestamp({}) AS derived_at_utc \
             FROM lineage_rows ORDER BY successor_pnu",
            group.from_snapshot_id,
            group.to_snapshot_id,
            derived_at.timestamp()
        );
        Some(stage.write(shape, &query, &derivation_run_id)?)
    };
    Ok(StagedLineage {
        derivation_run_id,
        rows,
        file,
        summary,
    })
}

#[cfg(test)]
mod tests;
