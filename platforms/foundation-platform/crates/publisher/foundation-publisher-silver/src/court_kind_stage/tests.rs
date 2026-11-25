//! The 경매 kind on synthetic inputs (PNUs under `99999`, dates in 2099): the items in progress as of
//! the build, soonest sale first, each with its case's photos and its detail (a code's name beside
//! it, the parties counted by kind); the sale results with their total; the registry's fields and no
//! other.

use std::collections::BTreeSet;

use duckdb::Connection;
use serde_json::Value;

use super::*;
use crate::kind_versions_stage::read_for_test;

const EDITION: &str = "synthetic__parcel-209906";
const PNU: &str = "9999910100100010000";

/// `CREATE TABLE` of `table` (named as `Source::Local` reads it) with every column of its contract.
fn create(connection: &Connection, table: &str) -> anyhow::Result<()> {
    let columns = contract(table)?
        .columns
        .iter()
        .map(|column| {
            let kind = match column.logical_type {
                "long" => "BIGINT",
                "int" => "INTEGER",
                "double" => "DOUBLE",
                "boolean" => "BOOLEAN",
                "date" => "DATE",
                "timestamp" => "TIMESTAMPTZ",
                _ => "VARCHAR",
            };
            format!("{} {kind}", column.name)
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {} ({columns});",
        table.replace('.', "_")
    ))?;
    Ok(())
}

fn insert(
    connection: &Connection,
    table: &str,
    columns: &str,
    rows: &[&str],
) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "INSERT INTO {} ({columns}) VALUES {};",
        table.replace('.', "_"),
        rows.join(", ")
    ))?;
    Ok(())
}

const ITEM_COLUMNS: &str =
    "court_office_code, case_no, property_seq, item_seq, pnu, source_code_pnu_raw, \
    item_in_progress_yn, sale_date, row_state, appraisal_amount_krw, source_snapshot_id, \
    source_record_id, source_line_number";

fn setup(connection: &Connection) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
           source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES ('{PNU}', NULL, '{EDITION}', NULL); \
         CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         CREATE OR REPLACE TABLE silver_parcel_lineage (predecessor_pnu VARCHAR, successor_pnu VARCHAR, \
           relation VARCHAR, grade VARCHAR, evidence_kind VARCHAR, evidence_ref VARCHAR); \
         CREATE OR REPLACE TABLE gold_place_id_bridge (unit VARCHAR, place_id VARCHAR, code VARCHAR, \
           valid_from VARCHAR, valid_to VARCHAR); \
         INSERT INTO gold_place_id_bridge VALUES ('parcel', 'P-1', '{PNU}', '2099-01-01', NULL); \
         CREATE OR REPLACE TABLE as_of AS \
           SELECT epoch_us(TIMESTAMPTZ '2099-01-10 00:00:00+00') AS instant_us;"
    ))?;
    for table in [ITEM_SOURCE, SALE_RESULT_SOURCE, PHOTO_SOURCE, CODE_SOURCE]
        .into_iter()
        .chain(DOCUMENTS.iter().map(|(table, _)| *table))
    {
        create(connection, table)?;
    }
    let pnu = format!("'{PNU}'");
    insert(
        connection,
        ITEM_SOURCE,
        ITEM_COLUMNS,
        &[
            // In progress, sold on the 20th.
            &format!("('B999999', '20990130000001', 1, 1, {pnu}, NULL, 'Y', DATE '2099-01-20', NULL, 100, '20990101T000000Z', 'r', 1)"),
            // Placed by its code PNU; sold sooner, so it is first.
            &format!("('B999999', '20990130000002', 1, 1, NULL, {pnu}, 'Y', DATE '2099-01-15', NULL, 200, '20990101T000000Z', 'r', 2)"),
            // Sold before the build's day, not in progress, and removed: none is served.
            &format!("('B999999', '20990130000003', 1, 1, {pnu}, NULL, 'Y', DATE '2099-01-05', NULL, 300, '20990101T000000Z', 'r', 3)"),
            &format!("('B999999', '20990130000004', 1, 1, {pnu}, NULL, 'N', DATE '2099-01-25', NULL, 400, '20990101T000000Z', 'r', 4)"),
            &format!("('B999999', '20990130000005', 1, 1, {pnu}, NULL, 'Y', DATE '2099-01-25', 'removed', 500, '20990101T000000Z', 'r', 5)"),
        ],
    )?;
    insert(
        connection,
        SALE_RESULT_SOURCE,
        "court_office_code, case_no, property_seq, item_seq, pnu, sale_date, sale_amount_krw, \
         source_snapshot_id, source_record_id, source_line_number",
        &[
            &format!("('B999999', '20980130000001', 1, 1, {pnu}, DATE '2098-12-01', 90, '20990101T000000Z', 'r', 1)"),
            &format!("('B999999', '20980130000002', 1, 1, {pnu}, DATE '2098-12-20', 80, '20990101T000000Z', 'r', 2)"),
        ],
    )?;
    insert(
        connection,
        PHOTO_SOURCE,
        "court_office_code, case_no, sha256, photo_kind_code, sequence, media_format, size_bytes, \
         source_snapshot_id, source_record_id, source_line_number",
        &["('B999999', '20990130000001', 'aa', '000241', 1, 'jpeg', 10, '20990101T000000Z', 'r', 1)"],
    )?;
    insert(
        connection,
        CODE_SOURCE,
        "system_code, owner_code, group_code, code, name, source_snapshot_id, source_record_id, \
         source_line_number",
        &["('PGJ', 'PGJ', 'AUCTN_GDS_USG_CD', '01', '토지', '20990101T000000Z', 'r', 1)"],
    )?;
    insert(
        connection,
        "silver.court_auction_case",
        "court_office_code, case_no, case_name, user_case_no, source_snapshot_id, source_record_id, \
         source_line_number",
        &["('B999999', '20990130000001', '부동산임의경매', '2099타경1', '20990101T000000Z', 'r', 1)"],
    )?;
    insert(
        connection,
        "silver.court_auction_item_spec",
        "court_office_code, case_no, property_seq, item_usage_code, source_snapshot_id, source_record_id, \
         source_line_number",
        &["('B999999', '20990130000001', 1, '01', '20990101T000000Z', 'r', 1)"],
    )?;
    insert(
        connection,
        "silver.court_auction_case_party",
        "court_office_code, case_no, party_seq, party_kind_name, source_snapshot_id, source_record_id, \
         source_line_number",
        &[
            "('B999999', '20990130000001', 1, '채권자', '20990101T000000Z', 'r', 1)",
            "('B999999', '20990130000001', 2, '채무자', '20990101T000000Z', 'r', 2)",
            "('B999999', '20990130000001', 3, '채권자', '20990101T000000Z', 'r', 3)",
        ],
    )?;
    Ok(())
}

fn documents(connection: &Connection) -> anyhow::Result<Vec<(String, Value, Option<String>)>> {
    connection
        .prepare("SELECT pnu, court_auction_json, place_id FROM built ORDER BY pnu")?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|(pnu, text, place_id)| Ok((pnu, serde_json::from_str(&text)?, place_id)))
        .collect::<anyhow::Result<_>>()
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, Value, Option<String>)>> {
    setup(connection)?;
    read_for_test(&COURT_AUCTION, connection, EDITION, "")?;
    COURT_AUCTION.build(
        connection,
        true,
        "999990000000000001",
        "2099-01-10T00:00:00Z",
    )?;
    crate::kind_versions_stage::ensure_no_commit_tables(connection)?;
    documents(connection)
}

#[test]
fn a_document_serves_the_items_in_progress_soonest_first_and_the_sale_results() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    let built = built(&connection)?;
    assert_eq!(built.len(), 1);
    let (pnu, document, place_id) = &built[0];
    assert_eq!((pnu.as_str(), place_id.as_deref()), (PNU, Some("P-1")));
    let rounds = document["rounds"].as_array().cloned().unwrap_or_default();
    let cases: Vec<&str> = rounds
        .iter()
        .map(|round| round["case_no"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(cases, ["20990130000002", "20990130000001"]);
    assert_eq!(rounds[1]["sale_date"], "2099-01-20");
    assert_eq!(rounds[1]["photos"][0]["sha256"], "aa");
    assert_eq!(rounds[1]["photos"][0]["media_kind"], "photo");
    assert_eq!(rounds[1]["photos"][0]["kind_code"], "000241");
    assert_eq!(rounds[0]["photos"], Value::Array(Vec::new()));
    // The property's 물건상세 was read: its detail, a code's name beside it, parties by kind.
    let detail = &rounds[1]["detail"];
    assert_eq!(detail["case"]["case_name"], "부동산임의경매");
    assert_eq!(detail["spec"]["item_usage_code"], "01");
    assert_eq!(detail["spec"]["item_usage_name"], "토지");
    assert_eq!(
        detail["parties"],
        serde_json::json!([{"kind_name": "채권자", "count": 2}, {"kind_name": "채무자", "count": 1}])
    );
    assert_eq!(detail["schedule"], Value::Array(Vec::new()));
    assert_eq!(detail["survey"], Value::Null);
    // The other was never read: no detail.
    assert_eq!(rounds[0]["detail"], Value::Null);
    let sales = document["sale_results"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let sold: Vec<&str> = sales
        .iter()
        .map(|sale| sale["case_no"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(sold, ["20980130000002", "20980130000001"]);
    assert_eq!(document["sale_result_total"], 2);
    assert_eq!(document["source_attribution"], SOURCE_ATTRIBUTION);
    assert_eq!(
        document["source_datasets"],
        serde_json::json!(SOURCE_DATASETS)
    );
    Ok(())
}

/// Every JSON path of `value` under `at` (`[]` for an array's elements).
fn json_paths(value: &Value, at: &str, paths: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                let path = format!("{at}.{key}");
                paths.insert(path.clone());
                json_paths(inner, &path, paths);
            }
        }
        Value::Array(items) => {
            for item in items {
                json_paths(item, &format!("{at}[]"), paths);
            }
        }
        _ => {}
    }
}

/// Every path the registry's schema names under `at`.
fn schema_paths(schema: &Value, at: &str, paths: &mut BTreeSet<String>) {
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, inner) in properties {
            let path = format!("{at}.{key}");
            paths.insert(path.clone());
            schema_paths(inner, &path, paths);
        }
    }
    if let Some(items) = schema.get("items") {
        schema_paths(items, &format!("{at}[]"), paths);
    }
}

#[test]
fn a_document_holds_only_the_registry_s_fields() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let built = built(&connection)?;
    let schema: Value = serde_json::from_str(SCHEMA)?;
    let mut recorded = BTreeSet::new();
    schema_paths(
        &schema["properties"]["court_auction"],
        "court_auction",
        &mut recorded,
    );
    let mut written = BTreeSet::new();
    json_paths(&built[0].1, "court_auction", &mut written);
    let unknown: Vec<&String> = written.difference(&recorded).collect();
    assert!(
        unknown.is_empty(),
        "fields the registry does not name: {unknown:?}"
    );
    Ok(())
}

/// The renderer reads the registry's revision 11 (`SCHEMA`): a revision added to the subject must
/// move it, or documents would miss its fields.
#[test]
fn the_registry_revision_read_is_the_subject_s_current_one() -> anyhow::Result<()> {
    let subject =
        foundation_publisher_core::schema_registry::subject_of_lane("parcel-court-auction")?
            .ok_or_else(|| anyhow::anyhow!("the registry lists the lane's documents"))?;
    assert_eq!(
        (subject.current().major, subject.current().revision),
        (1, 11)
    );
    Ok(())
}
