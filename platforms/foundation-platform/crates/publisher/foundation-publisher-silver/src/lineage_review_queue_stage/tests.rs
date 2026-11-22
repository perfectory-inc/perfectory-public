//! The queue's rules on synthetic rows (PNUs under `99999`), with the vectors Python gives: the item
//! id, a parcel item and what closes one, and a place reference item's bytes and fingerprint.

use duckdb::Connection;

use super::*;
use crate::lineage_steward_read::LineageRow;

const SUBJECT: &str = "9999910100100010000";
const OLD: &str = "9999910100100020000";

fn lineage(predecessor: &str, grade: &str, kind: &str, to: &str) -> LineageRow {
    LineageRow {
        predecessor_pnu: predecessor.to_owned(),
        successor_pnu: SUBJECT.to_owned(),
        relation: "split".to_owned(),
        grade: grade.to_owned(),
        evidence_kind: kind.to_owned(),
        evidence_ref: String::new(),
        from_snapshot_id: "synthetic__parcel-209901".to_owned(),
        to_snapshot_id: to.to_owned(),
        derived_at_utc: Some("00000000000000000001".to_owned()),
    }
}

#[test]
fn an_item_id_is_python_s_uuid5_of_the_unit_and_subject() {
    // str(uuid.uuid5(uuid.NAMESPACE_URL, "lineage-review:parcel:9999910100100010000"))
    assert_eq!(
        item_id(PARCEL_UNIT, SUBJECT),
        Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"lineage-review:parcel:9999910100100010000"
        )
        .to_string()
    );
    assert_ne!(
        item_id(PARCEL_UNIT, SUBJECT),
        item_id(PLACE_REFERENCE_UNIT, SUBJECT)
    );
}

#[test]
fn a_parcel_whose_best_row_is_pending_is_open_with_its_latest_pair() -> anyhow::Result<()> {
    let rows = vec![
        lineage(
            OLD,
            "needs_review",
            "area_match",
            "synthetic__parcel-209903",
        ),
        lineage("", "pending", "none", "synthetic__parcel-209902"),
    ];
    let (items, counts) = parcel_items(&rows)?;
    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(
        (item.unit, item.subject_code.as_str(), item.status),
        (PARCEL_UNIT, SUBJECT, ReviewStatus::NeedsReview)
    );
    let (_, candidates, etag) = fingerprint(&rows.iter().collect::<Vec<_>>())?;
    assert_eq!(
        (&item.candidates_json, &item.evidence_etag),
        (&candidates, &etag)
    );
    // The latest derived row by (to, from).
    assert_eq!(
        (
            item.from_snapshot_id.as_deref(),
            item.to_snapshot_id.as_deref()
        ),
        (
            Some("synthetic__parcel-209901"),
            Some("synthetic__parcel-209903")
        )
    );
    assert_eq!(counts.get("open_needs_review"), Some(&1));
    Ok(())
}

#[test]
fn stronger_evidence_and_a_standing_decision_close_an_item() -> anyhow::Result<()> {
    // A high-grade link outranks the pending row: closed by evidence.
    let evidence = vec![
        lineage("", "pending", "none", "synthetic__parcel-209902"),
        lineage(OLD, "official", "area_match", "synthetic__parcel-209902"),
    ];
    let (items, counts) = parcel_items(&evidence)?;
    assert!(items.is_empty());
    assert_eq!(counts.get("closed_by_evidence"), Some(&1));

    // A steward's decision on the evidence as it stands closes it; on other evidence it reopens.
    let derived = vec![lineage("", "pending", "none", "synthetic__parcel-209902")];
    let (_, _, etag) = fingerprint(&derived.iter().collect::<Vec<_>>())?;
    let decision = |etag: &str| {
        LineageRow {
        evidence_kind: STEWARD.to_owned(),
        evidence_ref: format!(
            "{{\"evidence_etag\": \"{etag}\", \"decided_at\": \"2099-01-01T00:00:00Z\", \"decision_id\": \"d1\"}}"
        ),
        derived_at_utc: None,
        ..lineage("", "pending", STEWARD, "synthetic__parcel-209902")
    }
    };
    let mut standing = derived.clone();
    standing.push(decision(&etag));
    let (items, counts) = parcel_items(&standing)?;
    assert!(items.is_empty());
    assert_eq!(counts.get("closed_by_steward"), Some(&1));
    let mut lapsed = derived;
    lapsed.push(decision("0000"));
    let (items, counts) = parcel_items(&lapsed)?;
    assert_eq!(items.len(), 1);
    assert_eq!(counts.get("reopened_by_new_evidence"), Some(&1));
    Ok(())
}

#[test]
fn a_place_reference_item_is_python_s_json_and_fingerprint() -> anyhow::Result<()> {
    let groups = vec![
        UnresolvedGroup {
            reason: "jibun_masked".to_owned(),
            legal_dong_code: Some("9999910100".to_owned()),
            rows: 3,
            sample_row_keys: vec!["k3".to_owned(), "k1".to_owned()],
        },
        UnresolvedGroup {
            reason: String::new(),
            legal_dong_code: Some(String::new()),
            rows: 1,
            sample_row_keys: Vec::new(),
        },
    ];
    let items = place_reference_items("silver.synthetic_source", &groups)?;
    assert_eq!(items.len(), 2);
    // json.dumps({...}, ensure_ascii=False, sort_keys=True)
    let masked = items
        .iter()
        .find(|item| item.status == ReviewStatus::AwaitingData)
        .context("the masked group")?;
    assert_eq!(
        masked.candidates_json,
        "{\"legal_dong_code\": \"9999910100\", \"reason\": \"jibun_masked\", \"rows\": 3, \
         \"sample_row_keys\": [\"k1\", \"k3\"], \"source\": \"silver.synthetic_source\"}"
    );
    assert_eq!(
        masked.subject_code,
        "silver.synthetic_source|jibun_masked|9999910100"
    );
    assert_eq!(
        masked.evidence_etag,
        evidence_etag(ReviewStatus::AwaitingData, &masked.candidates_json)
    );
    // An empty reason reads as unknown, an empty code as none: a steward's item.
    let unknown = items
        .iter()
        .find(|item| item.status == ReviewStatus::Steward)
        .context("the unknown group")?;
    assert_eq!(unknown.subject_code, "silver.synthetic_source|unknown|-");
    assert_eq!(
        unknown.candidates_json,
        "{\"legal_dong_code\": null, \"reason\": \"unknown\", \"rows\": 1, \"sample_row_keys\": [], \
         \"source\": \"silver.synthetic_source\"}"
    );
    Ok(())
}

#[test]
fn the_review_lineage_is_every_row_of_a_parcel_that_had_a_review_grade() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE silver_parcel_lineage (predecessor_pnu VARCHAR, successor_pnu VARCHAR, \
           relation VARCHAR, grade VARCHAR, evidence_kind VARCHAR, evidence_ref VARCHAR, \
           from_snapshot_id VARCHAR, to_snapshot_id VARCHAR, derived_at_utc TIMESTAMPTZ); \
         INSERT INTO silver_parcel_lineage VALUES \
           ('9999910100100020000', '9999910100100010000', 'split', 'official', 'area_match', NULL, \
            'a', 'b', TIMESTAMPTZ '2099-01-01 00:00:00+00'), \
           (NULL, '9999910100100010000', 'none', 'pending', 'none', NULL, 'a', 'b', \
            TIMESTAMPTZ '2099-01-01 00:00:00+00'), \
           ('9999910100100040000', '9999910100100030000', 'split', 'official', 'area_match', NULL, \
            'a', 'b', TIMESTAMPTZ '2099-01-01 00:00:00+00');",
    )?;
    let rows = load_review_lineage(&connection, &Source::Local)?;
    // The second parcel never had a review-grade row.
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.successor_pnu == SUBJECT));
    assert!(rows.iter().any(|row| row.predecessor_pnu.is_empty()));
    Ok(())
}

#[test]
fn unresolved_rows_group_by_reason_and_code_with_sorted_distinct_samples() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    // silver.registered_factory keeps no versions and has no row key: no sample.
    connection.execute_batch(
        "CREATE TABLE silver_registered_factory (pnu VARCHAR, pnu_unresolved_reason VARCHAR, \
           legal_dong_code VARCHAR); \
         INSERT INTO silver_registered_factory VALUES \
           (NULL, 'no_jibun', '9999910100'), (NULL, 'no_jibun', '9999910100'), \
           (NULL, NULL, NULL), ('9999910100100010000', NULL, '9999910100');",
    )?;
    let groups = load_unresolved(&connection, &Source::Local, "silver.registered_factory")?;
    assert_eq!(
        groups,
        vec![
            UnresolvedGroup {
                reason: "no_jibun".to_owned(),
                legal_dong_code: Some("9999910100".to_owned()),
                rows: 2,
                sample_row_keys: Vec::new(),
            },
            UnresolvedGroup {
                reason: "unknown".to_owned(),
                legal_dong_code: None,
                rows: 1,
                sample_row_keys: Vec::new(),
            },
        ]
    );
    assert!(place_reference_sources().contains(&"silver.registered_factory"));
    Ok(())
}
