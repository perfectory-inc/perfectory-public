//! The register kinds' rules on synthetic inputs (PNUs under `99999`): newest first, a tie once,
//! blanks null, a renumbered code on the served parcel, an unserved PNU left out, the bytes Spark
//! writes, the partial diff, and where the 총괄표제부 and the demolition reports trim. The national
//! judgement against each Spark job is its takeover verdict.

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::KindVersionsStage;

const RECORD_BOUND: u32 = REGISTER_MASTERS.bound;

const EDITION: &str = "synthetic__parcel-209906";
const SITE_PNU: &str = "9999910100100010000";
const MOVED_PNU: &str = "9999910300100010000";

fn site(key: &str, pnu: &str, area: &str, permit: &str, approval: &str, ingested: &str) -> String {
    format!(
        "('{key}', '{pnu}', '{area}', NULL, NULL, '공장', '1', '0', NULL, '{permit}', NULL, '{approval}', \
         '202610', TIMESTAMP '{ingested}', NULL)"
    )
}

fn load(connection: &Connection, sites: &[String]) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_parcel_boundaries (pnu VARCHAR, geometry_wkb BLOB, \
           source_snapshot_id VARCHAR, valid_to_utc TIMESTAMP); \
         INSERT INTO silver_parcel_boundaries VALUES \
           ('{SITE_PNU}', NULL, '{EDITION}', NULL), ('{MOVED_PNU}', NULL, '{EDITION}', NULL), \
           ('9999910100100020000', NULL, 'synthetic__parcel-209903', NULL); \
         CREATE OR REPLACE TABLE reference_legal_dong_code_change (kind VARCHAR, old_code VARCHAR, \
           new_code VARCHAR, level VARCHAR, effective_date VARCHAR, source VARCHAR); \
         INSERT INTO reference_legal_dong_code_change VALUES \
           ('pair', '9999910200', '9999910300', 'eupmyeondong', '2099-01-01', 'official:x'); \
         CREATE OR REPLACE TABLE silver_building_register_master (mgmt_key VARCHAR, pnu VARCHAR, \
           plot_area_m2 VARCHAR, building_area_m2 VARCHAR, total_floor_area_m2 VARCHAR, \
           main_purpose_name VARCHAR, main_building_count VARCHAR, annex_building_count VARCHAR, \
           parking_count VARCHAR, permit_date VARCHAR, start_date VARCHAR, use_approval_date VARCHAR, \
           vintage VARCHAR, ingested_at_utc TIMESTAMP, row_state VARCHAR); \
         INSERT INTO silver_building_register_master VALUES {};",
        sites.join(", ")
    ))?;
    Ok(())
}

fn sites() -> Vec<String> {
    vec![
        // An older version of s1: only the newest counts.
        site("s1", SITE_PNU, "1.0", "", "20200101", "2099-01-01"),
        site("s1", SITE_PNU, " 12.5 ", "", "20200101", "2099-01-02"),
        // The same current row twice (a tie the contract keeps): one site.
        site("s1", SITE_PNU, " 12.5 ", "", "20200101", "2099-01-02"),
        site("s2", SITE_PNU, "30", "20210101", "", "2099-01-02"),
        // Under the code a reorganization renumbered into the served 10300.
        site("s3", "9999910200100010000", "7", "", "", "2099-01-02"),
        // A PNU the served edition does not hold.
        site("s4", "9999910100100020000", "9", "", "", "2099-01-02"),
    ]
}

fn built(connection: &Connection) -> anyhow::Result<Vec<(String, String, i64, String)>> {
    Ok(connection
        .prepare(
            "SELECT pnu, register_masters_json, register_master_total, row_digest FROM built ORDER BY pnu",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn record(area: &str, permit: &str, approval: &str) -> String {
    let text = |value: &str| {
        if value.is_empty() {
            "null".to_owned()
        } else {
            format!("\"{value}\"")
        }
    };
    format!(
        "{{\"plot_area_m2\":{},\"building_area_m2\":null,\"total_floor_area_m2\":null,\
         \"main_purpose_name\":\"공장\",\"main_building_count\":\"1\",\"annex_building_count\":\"0\",\
         \"parking_count\":null,\"permit_date\":{},\"start_date\":null,\"use_approval_date\":{}}}",
        text(area),
        text(permit),
        text(approval)
    )
}

#[test]
fn a_document_is_spark_s_bytes_newest_first_with_blanks_null() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &sites())?;
    let read =
        crate::kind_versions_stage::read_for_test(&REGISTER_MASTERS, &connection, EDITION, "")?;
    assert_eq!((read.served_parcels, read.rows), (2, 3));
    REGISTER_MASTERS.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    crate::kind_versions_stage::ensure_no_commit_tables(&connection)?;
    let rows = built(&connection)?;
    // s4's PNU is not served; s3 lands on the served code.
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [SITE_PNU, MOVED_PNU]);
    let (_, document, total, digest) = &rows[0];
    // s2 (permit 2021) before s1 (approval 2020); s1's padded area trimmed, its blank permit null.
    let expected = format!(
        "{{\"records\":[{},{}]}}",
        record("30", "20210101", ""),
        record("12.5", "", "20200101")
    );
    assert_eq!(document, &expected);
    assert_eq!(*total, 2);
    // Spark's row_digest: to_json of the content columns, the null attached_via left out.
    let content = format!(
        "{{\"pnu\":\"{SITE_PNU}\",\"register_masters_json\":{},\"register_master_total\":2}}",
        serde_json::to_string(&expected)?
    );
    assert_eq!(digest, &format!("{:x}", Sha256::digest(content.as_bytes())));
    Ok(())
}

/// The build loads one 시도 of the spool at a time; a 시도 the spool holds nothing of loads empty
/// tables, so a 시도 only the head holds is diffed against an empty build.
#[test]
fn a_spool_is_loaded_one_sido_at_a_time_and_an_absent_sido_loads_empty() -> anyhow::Result<()> {
    use crate::kind_versions_stage::{load_region, spool};
    use crate::parcel_nearby_stage::Source;
    let connection = Connection::open_in_memory()?;
    load(&connection, &sites())?;
    let dir = tempfile::tempdir()?;
    let spooled = spool(
        &REGISTER_MASTERS,
        &connection,
        &Source::Local,
        EDITION,
        "",
        dir.path(),
    )?;
    assert_eq!(spooled.sidos.iter().collect::<Vec<_>>(), ["99"]);
    assert!(dir.path().join("served").join("sido=99").is_dir());
    assert_eq!(
        load_region(&REGISTER_MASTERS, &connection, dir.path(), "", "99")?,
        3
    );
    assert_eq!(
        load_region(&REGISTER_MASTERS, &connection, dir.path(), "", "98")?,
        0
    );
    let (served, built): (i64, i64) = connection.query_row(
        "SELECT (SELECT count(*) FROM served), (SELECT count(*) FROM built)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!((served, built), (0, 0));
    assert!(load_region(&REGISTER_MASTERS, &connection, dir.path(), "", "9'").is_err());
    Ok(())
}

#[test]
fn a_parcel_keeps_the_newest_sites_up_to_the_bound_and_counts_all() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let many: Vec<String> = (0..RECORD_BOUND + 4)
        .map(|n| {
            site(
                &format!("k{n:03}"),
                SITE_PNU,
                "1",
                "",
                &format!("2000{:04}", 101 + n),
                "2099-01-02",
            )
        })
        .collect();
    load(&connection, &many)?;
    crate::kind_versions_stage::read_for_test(&REGISTER_MASTERS, &connection, EDITION, "")?;
    REGISTER_MASTERS.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    let rows = built(&connection)?;
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    let records = document["records"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        (records.len(), rows[0].2),
        (RECORD_BOUND as usize, i64::from(RECORD_BOUND + 4))
    );
    assert_eq!(
        records[0]["use_approval_date"],
        format!("2000{:04}", 101 + RECORD_BOUND + 3)
    );
    Ok(())
}

#[test]
fn a_partial_run_names_the_pnus_whose_sites_or_parcels_changed() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &sites())?;
    crate::kind_versions_stage::read_for_test(&REGISTER_MASTERS, &connection, EDITION, "_old")?;
    // s2 moved from the site PNU to the renumbered one; s1 is unchanged.
    let mut moved = sites();
    moved[3] = site(
        "s2",
        "9999910200100010000",
        "30",
        "20210101",
        "",
        "2099-01-03",
    );
    load(&connection, &moved)?;
    crate::kind_versions_stage::read_for_test(&REGISTER_MASTERS, &connection, EDITION, "")?;
    assert_eq!(REGISTER_MASTERS.changed_pnus(&connection)?, 2);
    REGISTER_MASTERS.build(
        &connection,
        false,
        "999990000000000002",
        "2099-01-04T00:00:00Z",
    )?;
    let rows = built(&connection)?;
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].2, rows[1].2), (1, 2));
    // Nothing changed: no PNU to rebuild.
    crate::kind_versions_stage::read_for_test(&REGISTER_MASTERS, &connection, EDITION, "_old")?;
    assert_eq!(REGISTER_MASTERS.changed_pnus(&connection)?, 0);
    Ok(())
}

/// The demolition reports remove duplicates and rank on the provider's text and trim only what the
/// document writes (`building_demolition_silver_to_gold.py`): a report padded with spaces is another
/// report than the same one unpadded, and both are written trimmed.
#[test]
fn demolition_reports_are_distinct_on_the_provider_s_text_and_written_trimmed() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    // The parcels and the code change table; the 총괄표제부 rows are not read here.
    load(&connection, &sites())?;
    let report = |key: &str, kind: &str, loss: &str| {
        format!(
            "('{key}', '{SITE_PNU}', '{kind}', NULL, NULL, '{loss}', '10', '1', '공장', NULL, \
             '202610', TIMESTAMP '2099-01-02', NULL)"
        )
    };
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE silver_building_permit_demolition_loss (mgmt_key VARCHAR, pnu VARCHAR, \
           demolition_kind_name VARCHAR, demolition_start_date VARCHAR, demolition_end_date VARCHAR, \
           demolition_loss_date VARCHAR, total_floor_area_m2 VARCHAR, building_count VARCHAR, \
           main_purpose_name VARCHAR, structure_name VARCHAR, vintage VARCHAR, ingested_at_utc TIMESTAMP, \
           row_state VARCHAR); \
         INSERT INTO silver_building_permit_demolition_loss VALUES {}, {}, {}, {};",
        report("d1", " 철거 ", "20200101"),
        report("d2", "철거", "20200101"),
        // The same report under two keys: one report.
        report("d3", "멸실", "20210101"),
        report("d4", "멸실", "20210101"),
    ))?;
    let read = crate::kind_versions_stage::read_for_test(&DEMOLITIONS, &connection, EDITION, "")?;
    assert_eq!(read.rows, 3);
    DEMOLITIONS.build(
        &connection,
        true,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )?;
    let (document, total): (String, i64) = connection.query_row(
        "SELECT demolitions_json, demolition_total FROM built",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let record = |kind: &str, loss: &str| {
        format!(
            "{{\"demolition_kind_name\":\"{kind}\",\"demolition_start_date\":null,\
             \"demolition_end_date\":null,\"demolition_loss_date\":\"{loss}\",\
             \"total_floor_area_m2\":\"10\",\"building_count\":\"1\",\"main_purpose_name\":\"공장\",\
             \"structure_name\":null}}"
        )
    };
    // The 2021 loss first; of the two 2020 ones the padded text sorts before the plain (a space
    // before any letter), and both are written trimmed.
    assert_eq!(
        document,
        format!(
            "{{\"records\":[{},{},{}]}}",
            record("멸실", "20210101"),
            record("철거", "20200101"),
            record("철거", "20200101")
        )
    );
    assert_eq!(total, 3);
    Ok(())
}
