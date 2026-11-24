//! The sub-parcel rules on synthetic inputs (PNUs under `99999`): a link reaches both of its
//! parcels, an unserved end has no document, a row naming its own parcel is no link, a null PNU keeps
//! the provider's 지번, a row with no 대지 PNU still reaches its 부속 지번, only a register's newest
//! vintage counts, the bound, a renumbered code at either end, the bytes Spark writes, and the
//! partial diff that reaches a parcel a register left. The national judgement against the Spark job
//! is its takeover verdict.

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::read_for_test;

const EDITION: &str = "synthetic__parcel-209906";
const DONG: &str = "9999910100";
/// A served parcel under the code a reorganization renumbered 10200 into.
const MOVED: &str = "9999910300100040000";

fn pnu(main: u32) -> String {
    format!("{DONG}1{main:04}0000")
}

/// A 부속지번 row: the register's 대지 is lot 1 of `DONG`, the other parcel lot `sub_bon`.
fn row(site: Option<&str>, sub: Option<&str>, key: &str, sub_bon: &str, vintage: &str) -> String {
    let text = |value: Option<&str>| value.map_or_else(|| "NULL".to_owned(), |v| format!("'{v}'"));
    format!(
        "('{key}', '일반건축물', '99999', '10100', '0', '0001', '0000', '99999', '10100', '0', \
         '{sub_bon}', '0000', {}, {}, '{vintage}', TIMESTAMP '2099-01-02', NULL)",
        text(site),
        text(sub)
    )
}

fn load(connection: &Connection, rows: &[String]) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
           source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES \
           ('{site}', NULL, '{EDITION}', NULL), ('{annex}', NULL, '{EDITION}', NULL), \
           ('{MOVED}', NULL, '{EDITION}', NULL), ('{unserved}', NULL, 'synthetic__parcel-209903', NULL); \
         CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         INSERT INTO reference_legal_dong_code_change VALUES \
           ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x'); \
         CREATE OR REPLACE TABLE silver_building_register_sub_parcel (mgmt_key VARCHAR, \
           register_kind_name VARCHAR, sigungu_cd VARCHAR, bjdong_cd VARCHAR, san_gubun VARCHAR, \
           bonbeon VARCHAR, bubeon VARCHAR, sub_sigungu_cd VARCHAR, sub_bjdong_cd VARCHAR, \
           sub_san_gubun VARCHAR, sub_bonbeon VARCHAR, sub_bubeon VARCHAR, pnu VARCHAR, \
           sub_pnu VARCHAR, vintage VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
         INSERT INTO silver_building_register_sub_parcel VALUES {};",
        rows.join(", "),
        site = pnu(1),
        annex = pnu(2),
        unserved = pnu(3),
    ))?;
    Ok(())
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, String)>> {
    Ok(connection
        .prepare("SELECT pnu, sub_parcels_json, row_digest FROM built ORDER BY pnu")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?)
}

fn entry(key: &str, other: Option<&str>, jibun: &str) -> String {
    let other = other.map_or_else(|| "null".to_owned(), |pnu| format!("\"{pnu}\""));
    format!(
        "{{\"mgmt_key\":\"{key}\",\"register_kind_name\":\"일반건축물\",\"other_pnu\":{other},\
         \"other_jibun\":\"{jibun}\"}}"
    )
}

/// Reads `rows` as a new build does and builds every PNU.
fn build_all(
    connection: &Connection,
    rows: &[String],
) -> anyhow::Result<Vec<(String, String, String)>> {
    load(connection, rows)?;
    read_for_test(&SUB_PARCELS, connection, EDITION, "")?;
    SUB_PARCELS.build(
        connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    built(connection)
}

#[test]
fn a_link_reaches_both_parcels_in_spark_s_bytes() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let (site, annex, unserved) = (pnu(1), pnu(2), pnu(3));
    load(
        &connection,
        &[
            row(Some(&site), Some(&annex), "synthetic-1", "0002", "202610"),
            row(
                Some(&site),
                Some(&unserved),
                "synthetic-1",
                "0003",
                "202610",
            ),
            // Its own parcel: no link.
            row(Some(&site), Some(&site), "synthetic-1", "0001", "202610"),
            // The other parcel's PNU unresolved: its 지번 is still shown.
            row(Some(&site), None, "synthetic-2", "0099", "202610"),
            // An older vintage of synthetic-1: not its current set.
            row(Some(&site), Some(&pnu(7)), "synthetic-1", "0007", "202609"),
        ],
    )?;
    let read = read_for_test(&SUB_PARCELS, &connection, EDITION, "")?;
    // Served PNUs; the entries on them: four on the 대지 as the 대지 (the self link among them), and
    // two on a served 부속 지번 (the annex, and the self link's other end, the 대지 again). The
    // build leaves the self link out.
    assert_eq!((read.served_parcels, read.rows), (3, 6));
    SUB_PARCELS.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    crate::kind_versions_stage::ensure_no_commit_tables(&connection)?;
    let rows = built(&connection)?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [site.as_str(), annex.as_str()]);
    // Newest register key first, then the other parcel's PNU (a null one last).
    let site_document = format!(
        "{{\"on_other_parcels\":[{},{},{}],\"on_other_total\":3,\"from_other_parcels\":[],\
         \"from_other_total\":0}}",
        entry("synthetic-2", None, "99999-10100-0-0099-0000"),
        entry("synthetic-1", Some(&annex), "99999-10100-0-0002-0000"),
        entry("synthetic-1", Some(&unserved), "99999-10100-0-0003-0000"),
    );
    assert_eq!(rows[0].1, site_document);
    let annex_document = format!(
        "{{\"on_other_parcels\":[],\"on_other_total\":0,\"from_other_parcels\":[{}],\
         \"from_other_total\":1}}",
        entry("synthetic-1", Some(&site), "99999-10100-0-0001-0000"),
    );
    assert_eq!(rows[1].1, annex_document);
    // Spark's row_digest: to_json of the content columns, the null attached_via left out.
    let content = format!(
        "{{\"pnu\":\"{site}\",\"sub_parcels_json\":{}}}",
        serde_json::to_string(&site_document)?
    );
    assert_eq!(
        rows[0].2,
        format!("{:x}", Sha256::digest(content.as_bytes()))
    );
    Ok(())
}

/// A 시군구 the crosswalk refused leaves the 대지's PNU null; the row still lies on its 부속 지번,
/// which lists the register with the 대지's 지번 (Spark keeps it: its filter reads either end).
#[test]
fn a_row_without_a_site_pnu_still_reaches_its_annex() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let annex = pnu(2);
    let rows = build_all(
        &connection,
        &[row(None, Some(&annex), "synthetic-1", "0002", "202610")],
    )?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, annex);
    assert_eq!(
        rows[0].1,
        format!(
            "{{\"on_other_parcels\":[],\"on_other_total\":0,\"from_other_parcels\":[{}],\
             \"from_other_total\":1}}",
            entry("synthetic-1", None, "99999-10100-0-0001-0000"),
        )
    );
    Ok(())
}

#[test]
fn a_null_jibun_part_is_skipped_and_spaces_are_trimmed() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let (site, annex) = (pnu(1), pnu(2));
    let rows = build_all(
        &connection,
        &[
            row(Some(&site), Some(&annex), "synthetic-1", " 0002 ", "202610")
                .replace("'0', ' 0002 '", "NULL, ' 0002 '"),
        ],
    )?;
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    assert_eq!(
        document["on_other_parcels"][0]["other_jibun"],
        "99999-10100-0002-0000"
    );
    Ok(())
}

#[test]
fn more_links_than_the_bound_keep_the_bound_and_count_all() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let site = pnu(1);
    let many: Vec<String> = (0..ENTRY_BOUND + 3)
        .map(|n| {
            let other = format!("{DONG}1{:04}0000", 9000 + n);
            row(
                Some(&site),
                Some(&other),
                &format!("synthetic-{n:03}"),
                &format!("{}", 9000 + n),
                "202610",
            )
        })
        .collect();
    let rows = build_all(&connection, &many)?;
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    let entries = document["on_other_parcels"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(entries.len(), ENTRY_BOUND as usize);
    assert_eq!(document["on_other_total"], ENTRY_BOUND + 3);
    assert_eq!(
        entries[0]["mgmt_key"],
        format!("synthetic-{:03}", ENTRY_BOUND + 2)
    );
    Ok(())
}

/// Either end keyed under 10200 is read under the served 10300: the 부속 지번 here, the 대지 in the
/// second row (whose own 부속 지번 is the served 대지 of the first).
#[test]
fn both_ends_of_a_row_follow_a_renumbering() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let rows = build_all(
        &connection,
        &[
            row(
                Some(&pnu(1)),
                Some("9999910200100040000"),
                "synthetic-1",
                "0004",
                "202610",
            ),
            row(
                Some("9999910200100040000"),
                Some(&pnu(2)),
                "synthetic-2",
                "0002",
                "202610",
            ),
        ],
    )?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [pnu(1).as_str(), pnu(2).as_str(), MOVED]);
    let site: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    assert_eq!(site["on_other_parcels"][0]["other_pnu"], MOVED);
    let annex: serde_json::Value = serde_json::from_str(&rows[1].1)?;
    assert_eq!(annex["from_other_parcels"][0]["other_pnu"], MOVED);
    let moved: serde_json::Value = serde_json::from_str(&rows[2].1)?;
    assert_eq!(
        (
            moved["on_other_total"].clone(),
            moved["from_other_total"].clone()
        ),
        (serde_json::json!(1), serde_json::json!(1))
    );
    Ok(())
}

#[test]
fn a_partial_run_reaches_the_parcel_a_register_left() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let (site, annex) = (pnu(1), pnu(2));
    let first = [row(
        Some(&site),
        Some(&annex),
        "synthetic-1",
        "0002",
        "202610",
    )];
    load(&connection, &first)?;
    read_for_test(&SUB_PARCELS, &connection, EDITION, "_old")?;
    // The register's newest set names another parcel: the annex leaves it.
    load(
        &connection,
        &[
            first[0].clone(),
            row(Some(&site), Some(MOVED), "synthetic-1", "0004", "202611"),
        ],
    )?;
    read_for_test(&SUB_PARCELS, &connection, EDITION, "")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&SUB_PARCELS, &connection)?,
        3
    );
    SUB_PARCELS.build(
        &connection,
        false,
        "999990000000000002",
        "2099-01-04T00:00:00Z",
    )?;
    // The annex is rebuilt and has no document left: the diff marks it removed.
    let pnus: Vec<String> = built(&connection)?.into_iter().map(|row| row.0).collect();
    assert_eq!(pnus, [site, MOVED.to_owned()]);
    // Nothing changed: no PNU to rebuild.
    read_for_test(&SUB_PARCELS, &connection, EDITION, "_old")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&SUB_PARCELS, &connection)?,
        0
    );
    Ok(())
}

/// The base side of a partial run is the Silver that moved and nothing else (the served parcels and
/// the code changes are the new side's), and the changed PNUs are found once, from the two spools.
#[test]
fn a_partial_run_reads_the_base_of_the_moved_silver_only() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let (site, annex) = (pnu(1), pnu(2));
    let first = [row(
        Some(&site),
        Some(&annex),
        "synthetic-1",
        "0002",
        "202610",
    )];
    // The register's newest set names another parcel: the annex leaves it.
    let second = [
        first[0].clone(),
        row(Some(&site), Some(MOVED), "synthetic-1", "0004", "202611"),
    ];
    let base = || load(&connection, &first);
    let new = || load(&connection, &second);
    let (read, targets) = crate::kind_versions_stage::partial_for_test(
        &SUB_PARCELS,
        &connection,
        EDITION,
        &[SUB_PARCEL_SOURCE],
        &base,
        &new,
    )?;
    // Both directions of the one Silver, and no second read of the served parcels.
    assert_eq!(read.compared.len(), DIRECTIONS.len());
    assert!(!read.compared.iter().any(|name| name == "served"));
    assert!(read.sidos.is_empty());
    assert_eq!(targets, 3);
    SUB_PARCELS.build(
        &connection,
        false,
        "999990000000000002",
        "2099-01-04T00:00:00Z",
    )?;
    let pnus: Vec<String> = built(&connection)?.into_iter().map(|row| row.0).collect();
    assert_eq!(pnus, [site, MOVED.to_owned()]);
    // The same rows on both sides: no PNU is named.
    let (_, unchanged) = crate::kind_versions_stage::partial_for_test(
        &SUB_PARCELS,
        &connection,
        EDITION,
        &[SUB_PARCEL_SOURCE],
        &new,
        &new,
    )?;
    assert_eq!(unchanged, 0);
    // A move of the parcels reads the whole base side again.
    let (whole, _) = crate::kind_versions_stage::partial_for_test(
        &SUB_PARCELS,
        &connection,
        EDITION,
        &[crate::kind_versions_stage::PARCEL_SOURCE],
        &new,
        &new,
    )?;
    assert!(whole.compared.iter().any(|name| name == "served"));
    Ok(())
}
