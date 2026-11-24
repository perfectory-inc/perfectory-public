//! The 공매 kind on synthetic inputs (PNUs under `99999`, instants in 2099): the rounds open as of
//! the build, soonest-closing first; a round placed by the source's own PNU text; the item and
//! announcement blocks once; a photo's original; the provider's lists in the document's keys; the
//! registry's fields and no other; and the document ceiling.

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
    rows: &[String],
) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "INSERT INTO {} ({columns}) VALUES {};",
        table.replace('.', "_"),
        rows.join(", ")
    ))?;
    Ok(())
}

/// A round: (item, round, PNU or NULL, the source's PNU text or NULL, closes at, name).
fn round(item: &str, number: i64, pnu: &str, raw: &str, ends: &str, name: &str) -> String {
    format!(
        "('{item}', {number}, {pnu}, {raw}, TIMESTAMPTZ '2099-01-15 00:00:00+00', TIMESTAMPTZ '{ends}', \
         '{name}', 7, 1, '20990101', '대지')"
    )
}

const ROUND_COLUMNS: &str =
    "cltr_mng_no, pbct_cdtn_no, pnu, ltno_pnu_raw, bid_begins_at, bid_ends_at, \
    item_name, onbid_pbanc_no, pbct_no, source_snapshot_id, cltr_usg_lcls_ctgr_nm";

fn setup(connection: &Connection, rounds: &[String]) -> anyhow::Result<()> {
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
           SELECT epoch_us(TIMESTAMPTZ '2099-01-01 00:00:00+00') AS instant_us;"
    ))?;
    for table in [
        ITEM_SOURCE,
        PHOTO_SOURCE,
        DETAIL_SOURCE,
        BID_INFO_SOURCE,
        RESULT_SOURCE,
        ANNOUNCEMENT_SOURCE,
        ANNOUNCEMENT_RESULT_SOURCE,
        ANNOUNCEMENT_DETAIL_SOURCE,
    ] {
        create(connection, table)?;
    }
    insert(connection, ITEM_SOURCE, ROUND_COLUMNS, rounds)?;
    insert(
        connection,
        DETAIL_SOURCE,
        "cltr_mng_no, sqms_list_json, paps_inf_json, zadr_nm, source_snapshot_id",
        &["('C1', '[{\"clandCont\":\"대\",\"sqmsCont\":100}]', '{\"dlgtOrgNm\":\"기관\"}', '주소', '20990101')"
            .to_owned()],
    )?;
    insert(
        connection,
        PHOTO_SOURCE,
        "cltr_mng_no, sha256, media_kind, sequence, media_format, size_bytes, source_snapshot_id, \
         source_line_number, ingested_at_utc",
        &[
            "('C1', 'aa', 'photo', 1, 'jpeg', 10, '20990101', 1, TIMESTAMPTZ '2099-01-01 00:00:00+00')"
                .to_owned(),
            "('C1', 'bb', 'photo_original', 1, 'jpeg', 99, '20990101', 2, TIMESTAMPTZ '2099-01-02 00:00:00+00')"
                .to_owned(),
            "('C1', 'cc', 'map', 1, 'png', 5, '20990101', 3, TIMESTAMPTZ '2099-01-01 00:00:00+00')"
                .to_owned(),
        ],
    )?;
    insert(
        connection,
        RESULT_SOURCE,
        "cltr_mng_no, pbct_cdtn_no, opened_at, winning_bid_amount_krw, source_snapshot_id",
        &["('C1', 0, TIMESTAMPTZ '2098-06-01 00:00:00+00', 1000, '20990101')".to_owned()],
    )?;
    insert(
        connection,
        ANNOUNCEMENT_SOURCE,
        "onbid_pbanc_no, pbct_no, onbid_pbanc_nm, source_snapshot_id",
        &["(7, 1, '공고', '20990101')".to_owned()],
    )?;
    insert(
        connection,
        ANNOUNCEMENT_DETAIL_SOURCE,
        "onbid_pbanc_no, pbct_no, pbanc_mng_no, atch_file_list_json, anncm_al_cont, source_snapshot_id",
        &[format!(
            "(7, 1, 'M7', '[{{\"btbdAtchFileNm\":\"공고문.pdf\",\"sha256\":\"dd\"}}]', '{}', '20990101')",
            "가".repeat(ANNOUNCEMENT_TEXT_LIMIT as usize + 1)
        )],
    )?;
    Ok(())
}

fn documents(connection: &Connection) -> anyhow::Result<Vec<(String, Value, Option<String>)>> {
    connection
        .prepare("SELECT pnu, public_auction_json, place_id FROM built ORDER BY pnu")?
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

fn build(connection: &Connection) -> anyhow::Result<()> {
    PUBLIC_AUCTION.build(
        connection,
        true,
        "999990000000000001",
        "2099-01-01T00:00:00Z",
    )
}

fn rounds() -> Vec<String> {
    let pnu = format!("'{PNU}'");
    vec![
        round("C1", 1, &pnu, "NULL", "2099-02-01 00:00:00+00", "공장"),
        // Closed before the instant the build serves as of: not served.
        round("C1", 2, &pnu, "NULL", "2098-12-01 00:00:00+00", "공장"),
        // Placed by the source's own PNU text; closes sooner, so it is first.
        round("C2", 1, "NULL", &pnu, "2099-01-20 00:00:00+00", "창고"),
    ]
}

#[test]
fn a_document_serves_the_open_rounds_soonest_first_with_their_blocks_once() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    setup(&connection, &rounds())?;
    read_for_test(&PUBLIC_AUCTION, &connection, EDITION, "")?;
    build(&connection)?;
    crate::kind_versions_stage::ensure_no_commit_tables(&connection)?;
    let built = documents(&connection)?;
    assert_eq!(built.len(), 1);
    let (pnu, document, place_id) = &built[0];
    assert_eq!((pnu.as_str(), place_id.as_deref()), (PNU, Some("P-1")));
    let rounds = document["rounds"].as_array().cloned().unwrap_or_default();
    let keys: Vec<(String, i64)> = rounds
        .iter()
        .map(|r| {
            (
                r["cltr_mng_no"].as_str().unwrap_or_default().to_owned(),
                r["pbct_cdtn_no"].as_i64().unwrap_or(-1),
            )
        })
        .collect();
    assert_eq!(keys, [("C2".to_owned(), 1), ("C1".to_owned(), 1)]);
    assert_eq!(rounds[1]["bid_ends_at"], "2099-02-01T00:00:00Z");
    assert_eq!(rounds[1]["usg_lcls_ctgr_name"], "대지");
    // The round's photo carries its latest original; the item's map is the item's media.
    let photos = rounds[1]["photos"].as_array().cloned().unwrap_or_default();
    assert_eq!(photos.len(), 1);
    assert_eq!(
        (
            photos[0]["sha256"].clone(),
            photos[0]["original"]["sha256"].clone()
        ),
        ("aa".into(), "bb".into())
    );
    assert_eq!(rounds[0]["photos"], Value::Array(Vec::new()));
    let items = document["items"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        items
            .iter()
            .map(|item| item["cltr_mng_no"].clone())
            .collect::<Vec<_>>(),
        [Value::from("C1"), Value::from("C2")]
    );
    let detail = &items[0]["detail"];
    assert_eq!(detail["sqms_list"][0]["cland_cont"], "대");
    // A number the provider wrote is text, as every list value.
    assert_eq!(detail["sqms_list"][0]["sqms_cont"], "100");
    // 공매재산명세 was an object before the provider's V2: read as an array of one.
    assert_eq!(detail["paps_inf"][0]["dlgt_org_nm"], "기관");
    assert_eq!(items[0]["media"][0]["sha256"], "cc");
    assert_eq!(items[0]["history"][0]["opened_at"], "2098-06-01T00:00:00Z");
    assert_eq!(items[1]["detail"], Value::Null);
    let announcements = document["announcements"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(announcements.len(), 1);
    assert_eq!(announcements[0]["onbid_pbanc_no"], 7);
    assert_eq!(announcements[0]["anncm_al_cont"], Value::Null);
    assert_eq!(announcements[0]["anncm_al_cont_omitted"], true);
    assert_eq!(
        announcements[0]["atch_file_list"][0]["btbd_atch_file_nm"],
        "공고문.pdf"
    );
    assert_eq!(
        document["source_datasets"].as_array().map(Vec::len),
        Some(SOURCE_DATASETS.len())
    );
    // The kind's result check: C1's item has a photo and its round carries it, so no round breaches.
    let checks = PUBLIC_AUCTION.result_checks();
    let check = checks
        .iter()
        .find(|check| check.name == "rounds_carry_their_media")
        .context("the media check")?;
    assert!(check.blocking);
    let breaches =
        |connection: &Connection| connection.query_row(&check.sql, [], |row| row.get::<_, i64>(0));
    assert_eq!(breaches(&connection)?, 0);
    // An item whose media the session holds and whose served round shows none breaches, by name.
    connection.execute_batch(
        "INSERT INTO photo_rows (cltr_mng_no, sha256, media_kind) VALUES ('C2', 'zz', 'photo');",
    )?;
    assert_eq!(breaches(&connection)?, 1);
    let samples = check.samples_sql.clone().unwrap_or_default();
    let item: String = connection.query_row(&samples, [], |row| row.get(0))?;
    assert_eq!(item, "C2");
    Ok(())
}

/// 2026-11-22, the takeover verdict of this lane: a null where the schema says a list, an
/// announcement round with a detail per revision, and the rounds those repeated.
#[test]
fn a_list_without_entries_is_empty_and_an_announcement_round_has_one_detail() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    setup(&connection, &rounds())?;
    // The list names revision M7. An earlier revision's detail landed in a later snapshot.
    connection.execute_batch(&format!(
        "UPDATE {} SET pbanc_mng_no = 'M7';",
        ANNOUNCEMENT_SOURCE.replace('.', "_")
    ))?;
    insert(
        &connection,
        ANNOUNCEMENT_DETAIL_SOURCE,
        "onbid_pbanc_no, pbct_no, pbanc_mng_no, rltn_pbanc_mng_no, source_snapshot_id",
        &["(7, 1, 'M6', 'M5', '20990102')".to_owned()],
    )?;
    read_for_test(&PUBLIC_AUCTION, &connection, EDITION, "")?;
    build(&connection)?;
    let built = documents(&connection)?;
    let document = &built[0].1;
    // Each open round once, though its announcement round has two details.
    assert_eq!(document["rounds"].as_array().map(Vec::len), Some(2));
    let announcements = document["announcements"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(announcements.len(), 1);
    // The named revision's detail, not the one that landed last.
    assert_eq!(
        announcements[0]["atch_file_list"][0]["btbd_atch_file_nm"],
        "공고문.pdf"
    );
    assert_eq!(announcements[0]["rltn_pbanc_mng_no"], Value::Null);
    // A list the provider sent empty (Silver keeps no text for it) is an empty list.
    let detail = &document["items"][0]["detail"];
    assert_eq!(detail["leas_inf_list"], Value::Array(Vec::new()));

    // No revision named: the detail that landed last. No detail at all: nothing was left out, and
    // its list is empty.
    let unnamed = Connection::open_in_memory()?;
    setup(&unnamed, &rounds())?;
    insert(
        &unnamed,
        ANNOUNCEMENT_DETAIL_SOURCE,
        "onbid_pbanc_no, pbct_no, pbanc_mng_no, rltn_pbanc_mng_no, source_snapshot_id",
        &["(7, 1, 'M8', 'M7', '20990102')".to_owned()],
    )?;
    read_for_test(&PUBLIC_AUCTION, &unnamed, EDITION, "")?;
    build(&unnamed)?;
    let announcement = documents(&unnamed)?[0].1["announcements"][0].clone();
    assert_eq!(announcement["rltn_pbanc_mng_no"], "M7");
    assert_eq!(announcement["atch_file_list"], Value::Array(Vec::new()));
    let bare = Connection::open_in_memory()?;
    setup(&bare, &rounds())?;
    bare.execute_batch(&format!(
        "DELETE FROM {};",
        ANNOUNCEMENT_DETAIL_SOURCE.replace('.', "_")
    ))?;
    read_for_test(&PUBLIC_AUCTION, &bare, EDITION, "")?;
    build(&bare)?;
    let announcement = documents(&bare)?[0].1["announcements"][0].clone();
    assert_eq!(announcement["anncm_al_cont_omitted"], false);
    assert_eq!(announcement["atch_file_list"], Value::Array(Vec::new()));

    // The block checks: one row per key passes; a second row of one key refuses the 시도, by name.
    let blocks = PUBLIC_AUCTION
        .result_checks()
        .into_iter()
        .filter(|check| check.name.contains("_once_per_"))
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 3);
    let breaches =
        |check: &ResultCheck| connection.query_row(&check.sql, [], |row| row.get::<_, i64>(0));
    for check in &blocks {
        assert!(check.blocking, "{}", check.name);
        assert_eq!(breaches(check)?, 0, "{}", check.name);
    }
    connection.execute_batch(
        "INSERT INTO announcement_blocks SELECT * FROM announcement_blocks LIMIT 1;",
    )?;
    let announcement_check = blocks
        .iter()
        .find(|check| check.name == "announcement_blocks_once_per_round")
        .context("the announcement block check")?;
    assert_eq!(breaches(announcement_check)?, 1);
    let samples = announcement_check.samples_sql.clone().unwrap_or_default();
    let key: String = connection.query_row(&samples, [], |row| row.get(0))?;
    assert_eq!(key, "7|1");
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
    setup(&connection, &rounds())?;
    read_for_test(&PUBLIC_AUCTION, &connection, EDITION, "")?;
    build(&connection)?;
    let schema: Value = serde_json::from_str(SCHEMA)?;
    let mut recorded = BTreeSet::new();
    schema_paths(
        &schema["properties"]["public_auction"],
        "public_auction",
        &mut recorded,
    );
    let mut written = BTreeSet::new();
    json_paths(
        &documents(&connection)?[0].1,
        "public_auction",
        &mut written,
    );
    let unknown: Vec<&String> = written.difference(&recorded).collect();
    assert!(
        unknown.is_empty(),
        "fields the registry does not name: {unknown:?}"
    );
    Ok(())
}

/// The renderer reads the registry's revision 4 (`SCHEMA`): a revision added to the subject must
/// move it, or documents would miss its fields.
#[test]
fn the_registry_revision_read_is_the_subject_s_current_one() -> anyhow::Result<()> {
    let subject =
        foundation_publisher_core::schema_registry::subject_of_lane("parcel-public-auction")?
            .ok_or_else(|| anyhow::anyhow!("the registry lists the lane's documents"))?;
    assert_eq!(
        (subject.current().major, subject.current().revision),
        (1, 4)
    );
    Ok(())
}

#[test]
fn rounds_past_the_document_ceiling_are_left_out_latest_closing_first() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let pnu = format!("'{PNU}'");
    let big = "가".repeat(100_000);
    let many: Vec<String> = (1..=4)
        .map(|day| {
            round(
                "C9",
                day,
                &pnu,
                "NULL",
                &format!("2099-02-0{day} 00:00:00+00"),
                &big,
            )
        })
        .collect();
    setup(&connection, &many)?;
    read_for_test(&PUBLIC_AUCTION, &connection, EDITION, "")?;
    build(&connection)?;
    let (_, document, _) = documents(&connection)?.remove(0);
    let kept: Vec<i64> = document["rounds"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|r| r["pbct_cdtn_no"].as_i64().unwrap_or(-1))
        .collect();
    assert_eq!(kept, [1, 2]);
    Ok(())
}
