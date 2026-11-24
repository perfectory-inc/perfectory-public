//! The stage's parts on the placement's own synthetic inputs (codes and PNUs of prefix 99999): a
//! deal written under a superseded code is built by the part its parcel lies in, each part's
//! documents are the ones one build of everything makes, and a row that names no part is counted
//! and left out. A test's codes share their first five digits, so its parts are their first eight.

use duckdb::Connection;

use super::*;
use crate::kind_versions_stage::build_by_prefix;
use crate::real_transaction_placement_stage::tests::{
    deal, inputs, pnu, NEW, NEW_NAME, OLD, OLD_NAME, OTHER, OTHER_NAME,
};

const TEST: RealTransactionKind = RealTransactionKind { part_digits: 8 };
const OLD_PART: &str = "99999101";
const NEW_PART: &str = "99999201";
const OTHER_PART: &str = "99999102";

fn pair(a: &str, b: &str) -> (String, String) {
    (a.to_owned(), b.to_owned())
}

/// `(pnu, transaction_total)` of `table`, by PNU.
fn documents(connection: &Connection, table: &str) -> anyhow::Result<Vec<(String, i64)>> {
    let rows = connection
        .prepare(&format!(
            "SELECT pnu, transaction_total FROM {table} ORDER BY pnu"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

#[test]
fn a_chain_of_links_is_one_group_whose_members_read_each_other() {
    // c -> b -> a: a's build must see c's rows, which no single link says.
    let found = readers(&[pair("c", "b"), pair("b", "a"), pair("x", "y")]);
    let expected: BTreeSet<(String, String)> = [
        ("a", "b"),
        ("a", "c"),
        ("b", "a"),
        ("b", "c"),
        ("c", "a"),
        ("c", "b"),
        ("x", "y"),
        ("y", "x"),
    ]
    .into_iter()
    .map(|(node, part)| pair(node, part))
    .collect();
    assert_eq!(found, expected);
    assert!(readers(&[]).is_empty());
}

#[test]
fn each_part_builds_the_documents_one_build_of_everything_makes() -> anyhow::Result<()> {
    // 가동 (OLD) became 신구 가동 (NEW): the served parcels carry the new code only, so the deal written
    // under the old name belongs to the new code's part. A recent day: the documents hold the
    // months before the build month.
    let day = chrono::Utc::now()
        .date_naive()
        .checked_sub_days(chrono::Days::new(40))
        .context("a day forty days ago")?
        .to_string();
    let (carried, own, other) = (pnu(NEW, 12, 0), pnu(NEW, 20, 0), pnu(OTHER, 30, 0));
    let deals = [
        deal("a-1", OLD_NAME, "12", &day, 1000.0),
        deal("b-1", NEW_NAME, "20", &day, 1000.0),
        deal("c-1", OTHER_NAME, "30", &day, 1000.0),
        // An address no 법정동 has: the deal names no part.
        deal("d-1", "합성도 합성시 없는동", "40", &day, 1000.0),
    ];
    let parcels = [carried, own, other];
    let land: Vec<(String, &str, f64)> = parcels
        .iter()
        .map(|parcel| (parcel.clone(), "공장용지", 1000.0))
        .collect();
    let connection = inputs(&deals, &[(OLD, NEW)], &parcels, &land, &[])?;
    connection.execute_batch(&format!(
        "CREATE TABLE {CODE_CHANGES} AS SELECT * FROM code_changes; \
         CREATE TABLE {CODE_NAMES} AS SELECT * FROM code_names; \
         CREATE TABLE {DEALS_READ} AS SELECT * FROM deals_raw; \
         CREATE TABLE {LINEAGE_READ} AS SELECT * FROM lineage; \
         CREATE TABLE served AS SELECT pnu FROM parcels; \
         CREATE TABLE place_ids AS SELECT pnu, 'P-' || pnu AS place_id FROM parcels;"
    ))?;
    RealTransactionKind::months(&connection, "")?;

    // Everything in one build.
    let as_of = build_month();
    placement::place(&connection, &rt_contract()?["place_inference"])?;
    gold::gold_rows(
        &connection,
        as_of,
        &RealTransactionKind::coverage(&connection, as_of)?,
    )?;
    let whole = documents(&connection, "gold_rows")?;
    let mut expected = parcels.to_vec();
    expected.sort();
    assert_eq!(
        whole.iter().map(|(pnu, _)| pnu.clone()).collect::<Vec<_>>(),
        expected
    );

    // The same inputs, spooled by the part that reads each row.
    let dir = tempfile::tempdir()?;
    let dir_text = dir
        .path()
        .to_str()
        .context("the spool directory is UTF-8")?
        .replace('\\', "/");
    RealTransactionKind::reference(&connection)?;
    TEST.links(&connection)?;
    for (query, name, unkeyed) in [
        (TEST.lineage_query(), "rt_lineage", 0),
        (TEST.deals_query(), "rt_deals", 1),
        (TEST.pnu_query("served"), "rt_parcels", 0),
        (TEST.pnu_query("land"), "rt_land", 0),
        (TEST.pnu_query("titles"), "rt_titles", 0),
        (TEST.pnu_query("ownership"), "rt_ownership", 0),
    ] {
        assert_eq!(
            spool_by_part(&connection, &query, name, "", &dir_text)?,
            unkeyed,
            "{name}"
        );
    }
    let read = |name: &str| {
        dir.path().join(name).join("sido=99").is_dir().then(|| {
            format!(
                "read_parquet('{dir_text}/{name}/sido=99/*.parquet', hive_partitioning = false)"
            )
        })
    };
    TEST.load_more(&connection, &read, "")?;
    // The deal under the old code is read by its own part and by the part the code became; the
    // deal without a part is nowhere.
    let copies = connection
        .prepare("SELECT row_key, part FROM rt_deals ORDER BY 1, 2")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        copies,
        [
            pair("a-1", OLD_PART),
            pair("a-1", NEW_PART),
            pair("b-1", OLD_PART),
            pair("b-1", NEW_PART),
            pair("c-1", OTHER_PART),
        ]
    );

    TEST.create_built(&connection)?;
    let parts = build_by_prefix(
        &TEST,
        &connection,
        true,
        "999990000000000001",
        "2099-03-01T00:00:00Z",
        8,
    )?;
    assert_eq!(parts, 3);
    assert_eq!(documents(&connection, "built")?, whole);
    Ok(())
}

#[test]
fn the_code_version_names_the_build_month_and_is_a_plain_id() {
    let code = REAL_TRANSACTION.code_version();
    let month = build_month();
    assert!(
        code.ends_with(&format!(".{:04}-{:02}", month.year(), month.month())),
        "{code}"
    );
    assert!(is_plain(&code), "{code}");
    assert_eq!(REAL_TRANSACTION.part_tables().len(), PART_TABLES.len());
}
