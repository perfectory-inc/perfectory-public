//! The one steward queue's items, computed by the publisher.
//!
//! Unit `parcel` from `silver.parcel_lineage` (the rules of `lineage_review_queue.review_queue`) and
//! unit `place_reference` from every parcel source Silver the resolver placed; they replaced the
//! Spark jobs `lineage_review_queue_to_gold` and `place_reference_queue_to_gold` on the queue's
//! takeover (root ADR-0564 §6-9).
//!
//! A parcel is on the queue when the best derived row of its newest derivations is `needs_review`
//! or `pending` and no standing steward decision closes it; its candidates and fingerprint are the
//! ones every lineage reader checks a decision against (`lineage_steward_read::fingerprint`). A
//! place reference item is one (source, reason, 법정동 code) of a source's current rows without a
//! `pnu`: its row count and at most [`SAMPLE_KEYS`] sample keys, as Python's `json.dumps(…,
//! ensure_ascii=False, sort_keys=True)` writes them, since the steward database recomputes the
//! fingerprint of those bytes (`stewardship_domain::evidence_etag`).

use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use duckdb::Connection;
use stewardship_domain::{evidence_etag, ReviewStatus};
use uuid::Uuid;

use crate::duckdb_parquet_stage::with_contract_columns;
use crate::lineage_steward_read::{
    fingerprint, newest_derivations, standing_decision, LineageRow, STEWARD,
};
use crate::parcel_nearby_stage::Source;

/// The parcel lineage the parcel unit reads.
pub const LINEAGE_SOURCE: &str = "silver.parcel_lineage";
/// The unit of a parcel's lineage question.
pub const PARCEL_UNIT: &str = "parcel";
/// The unit of a source row no resolver could place.
pub const PLACE_REFERENCE_UNIT: &str = "place_reference";
/// The column a placed source names its unresolved rows' reason in (root ADR-0288 §4).
pub const REASON: &str = "pnu_unresolved_reason";
/// The reasons the source withheld: the value is not in the row, so a steward cannot decide it.
pub const AWAITING_DATA_REASONS: [&str; 4] =
    ["jibun_masked", "no_address", "no_jibun", "no_source_code"];
/// Sample row keys an item carries at most.
pub const SAMPLE_KEYS: usize = 5;
/// A parcel's grades that put it in front of a person.
const REVIEW_GRADES: [&str; 2] = ["needs_review", "pending"];
/// The subject's code part of an item without a 법정동 code.
const NO_CODE: &str = "-";

/// One item of the queue, as `gold.lineage_review_queue` holds it (its stamp aside).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueItem {
    /// `parcel` or `place_reference`.
    pub unit: &'static str,
    /// The parcel, or `<source>|<reason>|<code>`.
    pub subject_code: String,
    /// `UUIDv5(URL, lineage-review:<unit>:<subject_code>)`.
    pub item_id: String,
    /// Why it is on the queue.
    pub status: ReviewStatus,
    /// The evidence a steward sees.
    pub candidates_json: String,
    /// `sha256(<status>\n<candidates_json>)`.
    pub evidence_etag: String,
    /// Parcel: the earlier edition of the latest derived row; place reference: the source snapshot
    /// the queue read before.
    pub from_snapshot_id: Option<String>,
    /// Parcel: the later edition of the latest derived row; place reference: the source snapshot
    /// read.
    pub to_snapshot_id: Option<String>,
}

/// Stable per subject: the question is the same whichever run asked it.
#[must_use]
pub fn item_id(unit: &str, subject_code: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("lineage-review:{unit}:{subject_code}").as_bytes(),
    )
    .to_string()
}

fn is_review(grade: &str) -> bool {
    REVIEW_GRADES.contains(&grade)
}

fn optional(text: &str) -> Option<String> {
    (!text.is_empty()).then(|| text.to_owned())
}

/// The parcel unit's open items and the counts behind them.
///
/// The counts are `open_<status>`, `closed_by_steward`, `closed_by_evidence` and
/// `reopened_by_new_evidence`; `rows` are every lineage row of the parcels that ever had a
/// review-grade row.
///
/// # Errors
/// A row has a grade the lineage does not use.
pub fn parcel_items(
    rows: &[LineageRow],
) -> anyhow::Result<(Vec<QueueItem>, BTreeMap<String, u64>)> {
    let mut by_successor: BTreeMap<&str, Vec<&LineageRow>> = BTreeMap::new();
    for row in newest_derivations(rows) {
        by_successor
            .entry(row.successor_pnu.as_str())
            .or_default()
            .push(row);
    }
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut count = |name: String| *counts.entry(name).or_default() += 1;
    let mut items = Vec::new();
    for (successor, group) in by_successor {
        let derived: Vec<&LineageRow> = group
            .iter()
            .copied()
            .filter(|row| row.evidence_kind != STEWARD)
            .collect();
        if derived.iter().all(|row| !is_review(&row.grade)) {
            continue;
        }
        let (status, candidates_json, etag) = fingerprint(&derived)?;
        // The best derived row is above review grade: stronger evidence closed it.
        if status == ReviewStatus::Sample {
            count("closed_by_evidence".to_owned());
            continue;
        }
        if standing_decision(&group)?.is_some() {
            count("closed_by_steward".to_owned());
            continue;
        }
        if group.iter().any(|row| row.evidence_kind == STEWARD) {
            count("reopened_by_new_evidence".to_owned());
        }
        // Python's `max` by (to, from): the first of the greatest.
        let mut latest = derived[0];
        for row in &derived[1..] {
            if (&row.to_snapshot_id, &row.from_snapshot_id)
                > (&latest.to_snapshot_id, &latest.from_snapshot_id)
            {
                latest = row;
            }
        }
        count(format!("open_{status}"));
        items.push(QueueItem {
            unit: PARCEL_UNIT,
            subject_code: successor.to_owned(),
            item_id: item_id(PARCEL_UNIT, successor),
            status,
            candidates_json,
            evidence_etag: etag,
            from_snapshot_id: optional(&latest.from_snapshot_id),
            to_snapshot_id: optional(&latest.to_snapshot_id),
        });
    }
    Ok((items, counts))
}

/// Every row of the parcels that ever had a review-grade row in the lineage at `source`, in a
/// fixed order (the successor, then every column).
///
/// # Errors
/// Returns an error when the lineage cannot be read.
pub fn load_review_lineage(
    connection: &Connection,
    source: &Source<'_>,
) -> anyhow::Result<Vec<LineageRow>> {
    let table = source.of(LINEAGE_SOURCE)?;
    let grades = REVIEW_GRADES
        .iter()
        .map(|grade| format!("'{grade}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH subjects AS (SELECT DISTINCT successor_pnu FROM {table} WHERE grade IN ({grades})) \
         SELECT coalesce(predecessor_pnu, ''), successor_pnu, relation, grade, \
           coalesce(evidence_kind, ''), coalesce(evidence_ref, ''), coalesce(from_snapshot_id, ''), \
           coalesce(to_snapshot_id, ''), lpad(CAST(epoch_us(derived_at_utc) AS VARCHAR), 20, '0') \
         FROM {table} SEMI JOIN subjects USING (successor_pnu) \
         ORDER BY successor_pnu, to_snapshot_id, from_snapshot_id, predecessor_pnu, relation, grade, \
           evidence_kind, evidence_ref, derived_at_utc"
    );
    let rows = connection
        .prepare(&sql)
        .with_context(|| format!("DuckDB could not read {LINEAGE_SOURCE}"))?
        .query_map([], |row| {
            Ok(LineageRow {
                predecessor_pnu: row.get(0)?,
                successor_pnu: row.get(1)?,
                relation: row.get(2)?,
                grade: row.get(3)?,
                evidence_kind: row.get(4)?,
                evidence_ref: row.get(5)?,
                from_snapshot_id: row.get(6)?,
                to_snapshot_id: row.get(7)?,
                derived_at_utc: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// One (reason, 법정동 code) of a source's unresolved current rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedGroup {
    /// The resolver's reason, `unknown` when it wrote none.
    pub reason: String,
    /// The first ten characters of the row's 법정동 code, when the source has one.
    pub legal_dong_code: Option<String>,
    /// How many rows.
    pub rows: i64,
    /// The smallest distinct row keys, at most [`SAMPLE_KEYS`].
    pub sample_row_keys: Vec<String>,
}

/// The groups of `table`'s current rows at `source` that hold no `pnu`.
///
/// As the Spark job `place_reference_queue_to_gold` read them. A row's key is its contract key (or
/// `row_key`), each part as text joined by `|` with a null part left out.
///
/// # Errors
/// Returns an error when `table` has no contract or cannot be read.
pub fn load_unresolved(
    connection: &Connection,
    source: &Source<'_>,
    table: &str,
) -> anyhow::Result<Vec<UnresolvedGroup>> {
    let contract = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))?;
    let has = |name: &str| contract.columns.iter().any(|column| column.name == name);
    ensure!(
        has("pnu") && has(REASON),
        "{table} names no pnu and {REASON}: no resolver placed it"
    );
    let latest = contract.current_row_latest();
    let key: Vec<&str> = match &latest {
        Some(latest) => latest.key.to_vec(),
        None if has("row_key") => vec!["row_key"],
        None => Vec::new(),
    };
    let code = if has("legal_dong_code") {
        "substr(legal_dong_code, 1, 10)"
    } else {
        "CAST(NULL AS VARCHAR)"
    };
    let sample = if key.is_empty() {
        "CAST(NULL AS VARCHAR)".to_owned()
    } else {
        format!(
            "concat_ws('|', {})",
            key.iter()
                .map(|column| format!("CAST({column} AS VARCHAR)"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let read = with_contract_columns(connection, contract, &source.of(table)?)?;
    let current = latest.as_ref().map_or_else(
        || format!("SELECT * FROM {read}"),
        |latest| {
            let mut columns = vec!["pnu", REASON];
            if has("legal_dong_code") {
                columns.push("legal_dong_code");
            }
            for column in &key {
                if !columns.contains(column) {
                    columns.push(column);
                }
            }
            latest.current_rows_sql(&read, &columns, "TRUE")
        },
    );
    let sql = format!(
        "SELECT reason, legal_dong_code, count(*) AS rows, \
           to_json(list_slice(list_sort(list_distinct(list(sample_key))), 1, {SAMPLE_KEYS}))::VARCHAR \
             AS samples \
         FROM (SELECT coalesce({REASON}, 'unknown') AS reason, {code} AS legal_dong_code, \
                 {sample} AS sample_key \
               FROM ({current}) WHERE pnu IS NULL) \
         GROUP BY reason, legal_dong_code ORDER BY reason, legal_dong_code NULLS FIRST"
    );
    let groups = connection
        .prepare(&sql)
        .with_context(|| format!("DuckDB could not group the unresolved rows of {table}"))?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<(String, Option<String>, i64, Option<String>)>, _>>()?;
    groups
        .into_iter()
        .map(|(reason, legal_dong_code, rows, samples)| {
            let sample_row_keys: Vec<String> = match samples {
                Some(samples) => serde_json::from_str(&samples).with_context(|| {
                    format!("the sample keys of {table} are not a list of text: {samples}")
                })?,
                None => Vec::new(),
            };
            Ok(UnresolvedGroup {
                reason,
                legal_dong_code,
                rows,
                sample_row_keys,
            })
        })
        .collect()
}

/// `table`'s place reference items, one per group, without their
/// snapshot pair (the caller's: the snapshot read before and the one read now).
///
/// # Errors
/// Returns an error when a text cannot be written as JSON.
pub fn place_reference_items(
    table: &str,
    groups: &[UnresolvedGroup],
) -> anyhow::Result<Vec<QueueItem>> {
    let text = |value: &str| serde_json::to_string(value);
    let mut sorted: Vec<&UnresolvedGroup> = groups.iter().collect();
    sorted.sort_by(|a, b| {
        (&a.reason, a.legal_dong_code.as_deref().unwrap_or(""))
            .cmp(&(&b.reason, b.legal_dong_code.as_deref().unwrap_or("")))
    });
    let mut items = Vec::with_capacity(sorted.len());
    for group in sorted {
        // Python's `str(reason or "unknown")` and `code or None`: an empty value reads as absent.
        let reason = if group.reason.is_empty() {
            "unknown"
        } else {
            group.reason.as_str()
        };
        let code = group
            .legal_dong_code
            .as_deref()
            .filter(|code| !code.is_empty());
        let mut samples = group.sample_row_keys.clone();
        samples.sort();
        samples.truncate(SAMPLE_KEYS);
        let samples = samples
            .iter()
            .map(|sample| text(sample))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        // `json.dumps(body, ensure_ascii=False, sort_keys=True)`: keys in order, ", " and ": ".
        let body = format!(
            "{{\"legal_dong_code\": {}, \"reason\": {}, \"rows\": {}, \"sample_row_keys\": [{samples}], \
             \"source\": {}}}",
            code.map_or_else(|| Ok("null".to_owned()), text)?,
            text(reason)?,
            group.rows,
            text(table)?,
        );
        let subject_code = format!("{table}|{reason}|{}", code.unwrap_or(NO_CODE));
        let status = if AWAITING_DATA_REASONS.contains(&reason) {
            ReviewStatus::AwaitingData
        } else {
            ReviewStatus::Steward
        };
        items.push(QueueItem {
            unit: PLACE_REFERENCE_UNIT,
            item_id: item_id(PLACE_REFERENCE_UNIT, &subject_code),
            subject_code,
            status,
            evidence_etag: evidence_etag(status, &body),
            candidates_json: body,
            from_snapshot_id: None,
            to_snapshot_id: None,
        });
    }
    Ok(items)
}

/// The rules' code version: the text of this module and of the lineage reader it folds through,
/// so any change of either rebuilds the whole queue (some thousands of items).
#[must_use]
pub fn code_version() -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for text in [
        include_str!("lineage_review_queue_stage.rs"),
        include_str!("lineage_steward_read.rs"),
    ] {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// What the queue reads: the lineage (the parcel unit), then each place reference source.
#[must_use]
pub fn inputs() -> Vec<&'static str> {
    std::iter::once(LINEAGE_SOURCE)
        .chain(place_reference_sources())
        .collect()
}

/// The Silver tables whose rows the resolver placed: those whose contract carries a `pnu` and its
/// reason, by name.
#[must_use]
pub fn place_reference_sources() -> Vec<&'static str> {
    let mut found: Vec<&'static str> = lakehouse_domain::industrial_complex_lakehouse_contracts()
        .iter()
        .filter(|contract| {
            contract.table_name.starts_with("silver.")
                && ["pnu", REASON]
                    .iter()
                    .all(|name| contract.columns.iter().any(|column| column.name == *name))
        })
        .map(|contract| contract.table_name)
        .collect();
    found.sort_unstable();
    found
}

#[cfg(test)]
mod tests;
