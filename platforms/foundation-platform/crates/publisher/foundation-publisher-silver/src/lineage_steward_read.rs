//! The parcel lineage as every consumer reads it (root ADR-0115 §3·§8·§9, ADR-0329 §3).
//!
//! Ported once from `lineage_review_queue.steward_resolved` and `parcel_lineage.drop_outranked_code_changes`
//! (W81c): a parcel with a standing steward decision reads that decision instead of its derived rows; a
//! lapsed decision is not read; only the newest derivation run of a key counts; and a code change link a
//! better-graded 동 pair replaced is dropped. The walk (`lineage_carry`) runs over what this returns.

use std::collections::BTreeMap;

use serde_json::Value;
use stewardship_domain::{evidence_etag, ReviewStatus};

use crate::lineage_carry::GRADES;

/// The `evidence_kind` of a row a steward's decision wrote.
pub const STEWARD: &str = "steward";
/// The `evidence_kind` of a code change link carried by a paired 동.
pub const SAME_LOT_PAIRED_DONG: &str = "same_lot_paired_dong";
/// A pair's evidence grade by its `source` prefix, strongest first
/// (`legal_dong_code_change_views.EVIDENCE_RANKS`, root ADR-0329 §1).
pub const EVIDENCE_RANKS: [(&str, usize); 5] = [
    ("steward:", 0),
    ("official:", 1),
    ("derived:code-go-kr:date+name:", 2),
    ("derived:parcel-jibun:", 3),
    ("derived:children", 4),
];

/// One `silver.parcel_lineage` row, the columns the read needs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LineageRow {
    /// The earlier parcel; empty for a row that links nothing (`pending`).
    pub predecessor_pnu: String,
    /// The later parcel.
    pub successor_pnu: String,
    /// The lineage relation.
    pub relation: String,
    /// The lineage grade (`lineage_carry::GRADES`).
    pub grade: String,
    /// What evidence derived it, or `steward`.
    pub evidence_kind: String,
    /// The evidence; a steward row's decision as JSON.
    pub evidence_ref: String,
    /// The edition pair the derivation compared.
    pub from_snapshot_id: String,
    /// The edition pair the derivation compared.
    pub to_snapshot_id: String,
    /// The derivation run's instant; `None` for rows written without one.
    pub derived_at_utc: Option<String>,
}

fn grade_rank(grade: &str) -> anyhow::Result<usize> {
    GRADES
        .iter()
        .position(|known| *known == grade)
        .ok_or_else(|| anyhow::anyhow!("lineage grade {grade:?} is not one of {GRADES:?}"))
}

/// A pair source's evidence rank (lower is stronger); an unknown source ranks last.
#[must_use]
pub fn evidence_rank(source: &str) -> usize {
    EVIDENCE_RANKS
        .iter()
        .find(|(prefix, _)| source.starts_with(prefix))
        .map_or(EVIDENCE_RANKS.len(), |(_, rank)| *rank)
}

fn is_steward(row: &LineageRow) -> bool {
    row.evidence_kind == STEWARD
}

/// Of the derived rows, only the newest run's for each (edition pair, successor); steward rows and rows
/// without a run instant are kept as they are (root ADR-0286 §6).
#[must_use]
pub fn newest_derivations(rows: &[LineageRow]) -> Vec<&LineageRow> {
    let key = |row: &LineageRow| {
        (
            row.from_snapshot_id.clone(),
            row.to_snapshot_id.clone(),
            row.successor_pnu.clone(),
        )
    };
    let mut newest: BTreeMap<(String, String, String), &str> = BTreeMap::new();
    for row in rows {
        if let (false, Some(at)) = (is_steward(row), row.derived_at_utc.as_deref()) {
            let held = newest.entry(key(row)).or_insert(at);
            if at > *held {
                *held = at;
            }
        }
    }
    rows.iter()
        .filter(|row| {
            is_steward(row)
                || row
                    .derived_at_utc
                    .as_deref()
                    .is_none_or(|at| newest.get(&key(row)) == Some(&at))
        })
        .collect()
}

/// The rows without the `same_lot_paired_dong` links whose pair source is outranked, per predecessor.
#[must_use]
pub fn drop_outranked_code_changes<'a>(rows: &[&'a LineageRow]) -> Vec<&'a LineageRow> {
    let ranked = |row: &LineageRow| {
        row.evidence_kind == SAME_LOT_PAIRED_DONG && !row.predecessor_pnu.is_empty()
    };
    let mut best: BTreeMap<&str, usize> = BTreeMap::new();
    for row in rows.iter().filter(|row| ranked(row)) {
        let rank = evidence_rank(&row.evidence_ref);
        let held = best.entry(row.predecessor_pnu.as_str()).or_insert(rank);
        *held = (*held).min(rank);
    }
    rows.iter()
        .filter(|row| {
            !ranked(row)
                || best.get(row.predecessor_pnu.as_str()) == Some(&evidence_rank(&row.evidence_ref))
        })
        .copied()
        .collect()
}

/// `(status, candidates_json, evidence_etag)` of one parcel's derived rows: exactly what the review
/// queue shows a steward, so a decision is checked against the evidence it was made on.
///
/// `candidates_json` is byte-for-byte Python's `json.dumps(…, ensure_ascii=False, sort_keys=True)`
/// (`", "` and `": "` separators), since the stored decisions hold the etag of those bytes.
///
/// # Errors
/// No row, or a row whose grade is not a lineage grade.
pub fn fingerprint(derived: &[&LineageRow]) -> anyhow::Result<(ReviewStatus, String, String)> {
    let mut best: Option<(usize, usize)> = None;
    for (index, row) in derived.iter().enumerate() {
        let rank = grade_rank(&row.grade)?;
        if best.is_none_or(|(_, held)| rank < held) {
            best = Some((index, rank));
        }
    }
    let (best_index, _) =
        best.ok_or_else(|| anyhow::anyhow!("a fingerprint needs one derived row"))?;
    let status = match derived[best_index].grade.as_str() {
        "needs_review" => ReviewStatus::NeedsReview,
        "pending" => ReviewStatus::Pending,
        _ => ReviewStatus::Sample,
    };
    let mut candidates = Vec::new();
    for (index, row) in derived.iter().enumerate() {
        if row.predecessor_pnu.is_empty() {
            continue;
        }
        let in_effect = status == ReviewStatus::Sample && index == best_index;
        candidates.push((
            grade_rank(&row.grade)?,
            row.predecessor_pnu.as_str(),
            *row,
            in_effect,
        ));
    }
    candidates.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let text = |value: &str| serde_json::to_string(value);
    let mut items = Vec::new();
    for (_, _, row, in_effect) in candidates {
        items.push(format!(
            "{{\"evidence_kind\": {}, \"evidence_ref\": {}, \"grade\": {}, \"in_effect\": {in_effect}, \
             \"predecessor_pnu\": {}, \"relation\": {}}}",
            text(&row.evidence_kind)?,
            text(&row.evidence_ref)?,
            text(&row.grade)?,
            text(&row.predecessor_pnu)?,
            text(&row.relation)?,
        ));
    }
    let candidates_json = format!("[{}]", items.join(", "));
    let etag = evidence_etag(status, &candidates_json);
    Ok((status, candidates_json, etag))
}

fn decision_field(row: &LineageRow, field: &str) -> String {
    let parsed: Value = serde_json::from_str(if row.evidence_ref.is_empty() {
        "{}"
    } else {
        &row.evidence_ref
    })
    .unwrap_or(Value::Null);
    parsed
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The steward row in force for one parcel, or `None`: a decision stands only on the evidence it was
/// made on; among standing decisions the latest (`decided_at`, then `decision_id`) wins.
///
/// # Errors
/// As `fingerprint`.
pub fn standing_decision<'a>(group: &[&'a LineageRow]) -> anyhow::Result<Option<&'a LineageRow>> {
    let derived: Vec<&LineageRow> = group
        .iter()
        .copied()
        .filter(|row| !is_steward(row))
        .collect();
    let stewards: Vec<&LineageRow> = group
        .iter()
        .copied()
        .filter(|row| is_steward(row))
        .collect();
    if stewards.is_empty() || derived.is_empty() {
        return Ok(None);
    }
    let (_, _, current) = fingerprint(&derived)?;
    let mut latest: Option<(&LineageRow, (String, String))> = None;
    for row in stewards {
        if decision_field(row, "evidence_etag") != current {
            continue;
        }
        let order = (
            decision_field(row, "decided_at"),
            decision_field(row, "decision_id"),
        );
        if latest.as_ref().is_none_or(|(_, held)| order > *held) {
            latest = Some((row, order));
        }
    }
    Ok(latest.map(|(row, _)| row))
}

/// The lineage rows as every consumer must read them, by successor.
///
/// # Errors
/// As `fingerprint`.
pub fn steward_resolved(rows: &[LineageRow]) -> anyhow::Result<Vec<LineageRow>> {
    let newest = newest_derivations(rows);
    let mut by_successor: BTreeMap<&str, Vec<&LineageRow>> = BTreeMap::new();
    for row in drop_outranked_code_changes(&newest) {
        by_successor
            .entry(row.successor_pnu.as_str())
            .or_default()
            .push(row);
    }
    let mut resolved = Vec::new();
    for group in by_successor.values() {
        if let Some(standing) = standing_decision(group)? {
            resolved.push(standing.clone());
        } else {
            resolved.extend(
                group
                    .iter()
                    .filter(|row| !is_steward(row))
                    .map(|row| (*row).clone()),
            );
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    //! `tests/test_lineage_review_queue.py`'s steward cases, and a vector Python's `fingerprint` gave.
    use super::*;

    const OLD: &str = "9999910100";
    const NEW: &str = "9999930100";

    fn pnu(dong: &str, main: u32) -> String {
        format!("{dong}1{main:04}0000")
    }

    fn row(old: &str, new: &str, grade: &str, kind: &str) -> LineageRow {
        LineageRow {
            predecessor_pnu: old.to_owned(),
            successor_pnu: new.to_owned(),
            relation: "jurisdiction_transfer".to_owned(),
            grade: grade.to_owned(),
            evidence_kind: kind.to_owned(),
            from_snapshot_id: "s1".to_owned(),
            to_snapshot_id: "s2".to_owned(),
            ..LineageRow::default()
        }
    }

    fn steward(
        new: &str,
        derived: &[LineageRow],
        predecessor: &str,
        decided_at: &str,
    ) -> anyhow::Result<LineageRow> {
        let refs: Vec<&LineageRow> = derived.iter().collect();
        let (_, _, etag) = fingerprint(&refs)?;
        let reference = serde_json::json!({"decision_id": format!("d-{decided_at}"), "decided_at": decided_at,
                                           "evidence_etag": etag});
        Ok(LineageRow {
            predecessor_pnu: predecessor.to_owned(),
            successor_pnu: new.to_owned(),
            relation: if predecessor.is_empty() {
                "other"
            } else {
                "jurisdiction_transfer"
            }
            .to_owned(),
            grade: if predecessor.is_empty() {
                "pending"
            } else {
                "official"
            }
            .to_owned(),
            evidence_kind: STEWARD.to_owned(),
            evidence_ref: reference.to_string(),
            from_snapshot_id: "s1".to_owned(),
            to_snapshot_id: "s2".to_owned(),
            derived_at_utc: None,
        })
    }

    #[test]
    fn the_fingerprint_is_the_bytes_python_writes() -> anyhow::Result<()> {
        let successor = "9999930100100030000";
        let mut weak = row("9999910100100050000", successor, "evidence_weak", "area");
        weak.successor_pnu = successor.to_owned();
        let mut review = row(
            "9999910100100030000",
            successor,
            "needs_review",
            "area+category, ownership differs",
        );
        review.evidence_ref = "면적 \"같음\"".to_owned();
        let mut pending = row("", successor, "pending", "none");
        pending.relation = "other".to_owned();
        let (status, json, etag) = fingerprint(&[&review, &weak, &pending])?;
        assert_eq!(status, ReviewStatus::Sample);
        assert_eq!(
            json,
            "[{\"evidence_kind\": \"area\", \"evidence_ref\": \"\", \"grade\": \"evidence_weak\", \"in_effect\": true, \
             \"predecessor_pnu\": \"9999910100100050000\", \"relation\": \"jurisdiction_transfer\"}, \
             {\"evidence_kind\": \"area+category, ownership differs\", \"evidence_ref\": \"면적 \\\"같음\\\"\", \
             \"grade\": \"needs_review\", \"in_effect\": false, \"predecessor_pnu\": \"9999910100100030000\", \
             \"relation\": \"jurisdiction_transfer\"}]"
        );
        assert_eq!(
            etag,
            "3d691863a184ead152ed064da001bc3fbc3dff0713705793ff97a9bc64fc06cb"
        );
        Ok(())
    }

    #[test]
    fn a_lapsed_decision_is_not_read() -> anyhow::Result<()> {
        let before = vec![row(
            "",
            &pnu(NEW, 4),
            "pending",
            "no unique attribute match",
        )];
        let decision = steward(&pnu(NEW, 4), &before, "", "2099-10-01")?;
        let mut after = row(
            &pnu(OLD, 4),
            &pnu(NEW, 4),
            "needs_review",
            "area+category, ownership differs",
        );
        after.to_snapshot_id = "s3".to_owned();
        let every = vec![before[0].clone(), after.clone(), decision];
        assert_eq!(steward_resolved(&every)?, vec![before[0].clone(), after]);
        Ok(())
    }

    #[test]
    fn a_standing_decision_replaces_the_derived_rows_and_the_latest_wins() -> anyhow::Result<()> {
        let kind = "area+category, ownership differs";
        let derived = vec![
            row(&pnu(OLD, 3), &pnu(NEW, 3), "needs_review", kind),
            row(&pnu(OLD, 5), &pnu(NEW, 3), "needs_review", kind),
        ];
        let first = steward(&pnu(NEW, 3), &derived, &pnu(OLD, 3), "2099-10-01")?;
        let correction = steward(&pnu(NEW, 3), &derived, &pnu(OLD, 5), "2099-10-02")?;
        let with_first = [derived.clone(), vec![first.clone()]].concat();
        assert_eq!(steward_resolved(&with_first)?, vec![first.clone()]);
        let with_both = [derived.clone(), vec![first.clone(), correction.clone()]].concat();
        assert_eq!(steward_resolved(&with_both)?, vec![correction]);
        let untouched = row(&pnu(OLD, 1), &pnu(NEW, 1), "code_derived", "area+category");
        let mixed = [vec![untouched.clone()], derived, vec![first]].concat();
        assert!(steward_resolved(&mixed)?.contains(&untouched));
        Ok(())
    }

    #[test]
    fn only_the_newest_run_of_a_key_is_read() {
        let mut old = row(&pnu(OLD, 1), &pnu(NEW, 1), "code_derived", "area");
        old.derived_at_utc = Some("2026-11-01T00:00:00Z".to_owned());
        let mut new = old.clone();
        new.derived_at_utc = Some("2026-11-02T00:00:00Z".to_owned());
        let mut other = row(&pnu(OLD, 2), &pnu(NEW, 2), "code_derived", "area");
        other.derived_at_utc = Some("2026-11-01T00:00:00Z".to_owned());
        let rows = [old, new.clone(), other.clone()];
        assert_eq!(newest_derivations(&rows), vec![&new, &other]);
    }

    #[test]
    fn an_outranked_paired_dong_link_is_dropped() {
        let mut derived = row(
            &pnu(OLD, 1),
            &pnu(NEW, 1),
            "code_derived",
            SAME_LOT_PAIRED_DONG,
        );
        derived.evidence_ref = "derived:parcel-jibun:x".to_owned();
        let mut official = row(&pnu(OLD, 1), &pnu(NEW, 2), "official", SAME_LOT_PAIRED_DONG);
        official.evidence_ref = "official:parcel-history".to_owned();
        let other = row(&pnu(OLD, 2), &pnu(NEW, 3), "code_derived", "area");
        let rows = [&derived, &official, &other];
        assert_eq!(drop_outranked_code_changes(&rows), vec![&official, &other]);
    }
}
