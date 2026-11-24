//! The registered-factory kind's rules on synthetic inputs (PNUs under `99999`, editions in 2099):
//! the newest edition only, factories by name, a repeat once, blanks null, a renumbered code on the
//! served parcel, an unserved or unplaced row left out, the place id, the bytes Spark writes, the
//! bound, and the partial diff. The national judgement against the Spark job is its takeover
//! verdict.

use duckdb::Connection;
use sha2::{Digest, Sha256};

use super::*;
use crate::kind_versions_stage::{read_for_test, KindVersionsStage};

const EDITION: &str = "synthetic__parcel-209906";
const SITE_PNU: &str = "9999910100100010000";
const MOVED_PNU: &str = "9999910300100010000";
const NEWEST: &str = "20991231";

/// One factory row: edition, loaded at, then the PNU, 회사명, 단지명 and 생산품 as SQL literals.
type Factory<'a> = [&'a str; 6];

fn factory(line: usize, row: Factory<'_>) -> String {
    let [edition, ingested, pnu, company, complex, products] = row;
    format!(
        "({pnu}, {company}, {complex}, {products}, 'synthetic:bronze/{edition}.zip', {line}, \
         '{edition}', TIMESTAMP '{ingested}')"
    )
}

fn load(connection: &Connection, factories: &[Factory<'_>]) -> anyhow::Result<()> {
    let rows = factories
        .iter()
        .enumerate()
        .map(|(line, row)| factory(line + 1, *row))
        .collect::<Vec<_>>()
        .join(", ");
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
         CREATE OR REPLACE TABLE silver_registered_factory (pnu VARCHAR, company_name VARCHAR, \
           complex_name VARCHAR, products VARCHAR, source_record_id VARCHAR, source_line_number INTEGER, \
           source_snapshot_id VARCHAR, ingested_at_utc TIMESTAMP); \
         INSERT INTO silver_registered_factory VALUES {rows};"
    ))?;
    bridge(connection, &[("P-OPEN", SITE_PNU, "2099-01-01", None)])
}

/// The bridge's parcel rows: (place id, code, valid from, valid to), and a code-pair line the build
/// must not read.
fn bridge(
    connection: &Connection,
    periods: &[(&str, &str, &str, Option<&str>)],
) -> anyhow::Result<()> {
    let mut rows: Vec<String> = periods
        .iter()
        .map(|(id, code, from, to)| {
            format!(
                "('parcel', '99', '{id}', '{code}', '{from}', {}, 'synthetic-release', TIMESTAMP '2099-01-01')",
                to.map_or_else(|| "NULL".to_owned(), |to| format!("'{to}'"))
            )
        })
        .collect();
    rows.push(format!(
        "('parcel', '99', 'P-PAIR', '{SITE_PNU}', 'legal-dong-code-change', NULL, 'synthetic-release', \
         TIMESTAMP '2099-01-01')"
    ));
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TABLE gold_place_id_bridge (unit VARCHAR, scope VARCHAR, place_id VARCHAR, \
           code VARCHAR, valid_from VARCHAR, valid_to VARCHAR, release_id VARCHAR, \
           published_at_utc TIMESTAMP); \
         INSERT INTO gold_place_id_bridge VALUES {};",
        rows.join(", ")
    ))?;
    Ok(())
}

fn factories() -> Vec<Factory<'static>> {
    let site = "'9999910100100010000'";
    vec![
        [
            NEWEST,
            "2099-12-31",
            site,
            "'나공업'",
            "' 제1단지 '",
            "'부품'",
        ],
        [NEWEST, "2099-12-31", site, "'가정밀'", "''", "'금형'"],
        // The same factory listed twice (its own 순번): one factory.
        [NEWEST, "2099-12-31", site, "'가정밀'", "''", "' 금형'"],
        // Under the code a reorganization renumbered into the served 10300.
        [
            NEWEST,
            "2099-12-31",
            "'9999910200100010000'",
            "' 다산업 '",
            "NULL",
            "'x'",
        ],
        // A PNU the served edition does not hold, and a row the address placed nowhere.
        [
            NEWEST,
            "2099-12-31",
            "'9999910100100020000'",
            "'라'",
            "NULL",
            "NULL",
        ],
        [NEWEST, "2099-12-31", "NULL", "'마'", "NULL", "NULL"],
        // An older 기준일 loaded later is not the newest edition (root ADR-0328).
        ["20990630", "2100-01-01", site, "'바'", "NULL", "NULL"],
    ]
}

/// A built row: PNU, document, total, place id, digest.
type Built = (String, String, i64, Option<String>, String);

fn built(connection: &Connection) -> anyhow::Result<Vec<Built>> {
    Ok(connection
        .prepare(
            "SELECT pnu, factories_json, factory_total, place_id, row_digest FROM built ORDER BY pnu",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
        })?
        .collect::<Result<Vec<_>, _>>()?)
}

fn build(connection: &Connection, all: bool) -> anyhow::Result<()> {
    FACTORIES.build(
        connection,
        all,
        "999990000000000001",
        "2099-01-03T00:00:00Z",
    )
}

#[test]
fn a_document_is_spark_s_bytes_by_name_with_blanks_null_and_the_place_id() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &factories())?;
    let read = read_for_test(&FACTORIES, &connection, EDITION, "")?;
    assert_eq!((read.served_parcels, read.rows), (2, 3));
    build(&connection, true)?;
    crate::kind_versions_stage::ensure_no_commit_tables(&connection)?;
    let rows = built(&connection)?;
    let pnus: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(pnus, [SITE_PNU, MOVED_PNU]);
    let (_, document, total, place_id, digest) = &rows[0];
    let expected = "{\"records\":[{\"company_name\":\"가정밀\",\"complex_name\":null,\"products\":\"금형\"},\
                    {\"company_name\":\"나공업\",\"complex_name\":\"제1단지\",\"products\":\"부품\"}]}";
    assert_eq!(document, expected);
    assert_eq!(*total, 2);
    // The open period's id, not the closed one's or the code-pair line's.
    assert_eq!(place_id.as_deref(), Some("P-OPEN"));
    // Spark's row_digest: to_json of the content columns, the null attached_via left out.
    let content = format!(
        "{{\"pnu\":\"{SITE_PNU}\",\"factories_json\":{},\"factory_total\":2,\"place_id\":\"P-OPEN\"}}",
        serde_json::to_string(expected)?
    );
    assert_eq!(digest, &format!("{:x}", Sha256::digest(content.as_bytes())));
    // A parcel the bridge does not release yet: no id, and the digest leaves it out.
    let (_, document, total, place_id, digest) = &rows[1];
    assert_eq!(
        (document.as_str(), *total, place_id),
        (
            "{\"records\":[{\"company_name\":\"다산업\",\"complex_name\":null,\"products\":\"x\"}]}",
            1,
            &None
        )
    );
    let content = format!(
        "{{\"pnu\":\"{MOVED_PNU}\",\"factories_json\":{},\"factory_total\":1}}",
        serde_json::to_string(document)?
    );
    assert_eq!(digest, &format!("{:x}", Sha256::digest(content.as_bytes())));
    // The kind's result check: what the build wrote keeps the bound, and a document that counts
    // fewer than it lists breaches, by PNU.
    let checks = FACTORIES.result_checks();
    assert_eq!(checks.len(), 1);
    let check = &checks[0];
    assert!(check.blocking);
    let breaches =
        |connection: &Connection| connection.query_row(&check.sql, [], |row| row.get::<_, i64>(0));
    assert_eq!(breaches(&connection)?, 0);
    connection.execute_batch(&format!(
        "UPDATE built SET factory_total = 1 WHERE pnu = '{SITE_PNU}';"
    ))?;
    assert_eq!(breaches(&connection)?, 1);
    let samples = check.samples_sql.clone().unwrap_or_default();
    let pnu: String = connection.query_row(&samples, [], |row| row.get(0))?;
    assert_eq!(pnu, SITE_PNU);
    Ok(())
}

#[test]
fn the_open_period_wins_else_the_latest_closed_one() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &factories())?;
    bridge(
        &connection,
        &[
            ("P-2098", SITE_PNU, "2097-01-01", Some("2098-01-01")),
            ("P-2099", SITE_PNU, "2098-01-01", Some("2099-01-01")),
            ("P-NOW", MOVED_PNU, "2099-01-01", None),
            ("P-THEN", MOVED_PNU, "2098-01-01", Some("2099-01-01")),
        ],
    )?;
    read_for_test(&FACTORIES, &connection, EDITION, "")?;
    build(&connection, true)?;
    let ids: Vec<Option<String>> = built(&connection)?.into_iter().map(|row| row.3).collect();
    assert_eq!(ids, [Some("P-2099".to_owned()), Some("P-NOW".to_owned())]);
    Ok(())
}

#[test]
fn a_parcel_keeps_the_first_factories_by_name_up_to_the_bound_and_counts_all() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    let names: Vec<String> = (0..RECORD_BOUND + 4)
        .rev()
        .map(|n| format!("'공장{n:03}'"))
        .collect();
    let many: Vec<_> = names
        .iter()
        .map(|name| {
            [
                NEWEST,
                "2099-12-31",
                "'9999910100100010000'",
                name.as_str(),
                "NULL",
                "NULL",
            ]
        })
        .collect();
    load(&connection, &many)?;
    read_for_test(&FACTORIES, &connection, EDITION, "")?;
    build(&connection, true)?;
    let rows = built(&connection)?;
    let document: serde_json::Value = serde_json::from_str(&rows[0].1)?;
    let written: Vec<String> = document["records"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|record| {
            record["company_name"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    assert_eq!(
        (written.len(), rows[0].2),
        (RECORD_BOUND as usize, i64::from(RECORD_BOUND + 4))
    );
    let mut sorted = written.clone();
    sorted.sort();
    assert_eq!(written, sorted);
    assert_eq!(written[0], "공장000");
    Ok(())
}

#[test]
fn a_partial_run_names_the_pnus_whose_factories_parcels_or_place_ids_changed() -> anyhow::Result<()>
{
    let connection = Connection::open_in_memory()?;
    load(&connection, &factories())?;
    read_for_test(&FACTORIES, &connection, EDITION, "_old")?;
    // Nothing changed: no PNU to rebuild.
    read_for_test(&FACTORIES, &connection, EDITION, "")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&FACTORIES, &connection)?,
        0
    );
    // A new edition where 나공업 left the site; the place id of the renumbered parcel is released.
    let mut newer: Vec<_> = factories()
        .into_iter()
        .filter(|row| row[3] != "'나공업'")
        .map(|[_, _, pnu, company, complex, products]| {
            ["20991232", "2100-01-02", pnu, company, complex, products]
        })
        .collect();
    newer.truncate(4);
    load(&connection, &newer)?;
    bridge(
        &connection,
        &[
            ("P-OPEN", SITE_PNU, "2099-01-01", None),
            ("P-NEW", MOVED_PNU, "2099-06-01", None),
        ],
    )?;
    read_for_test(&FACTORIES, &connection, EDITION, "")?;
    assert_eq!(
        crate::kind_versions_stage::changed_for_test(&FACTORIES, &connection)?,
        2
    );
    build(&connection, false)?;
    let rows = built(&connection)?;
    assert_eq!(
        rows.iter()
            .map(|row| (row.2, row.3.clone()))
            .collect::<Vec<_>>(),
        [
            (1, Some("P-OPEN".to_owned())),
            (1, Some("P-NEW".to_owned()))
        ]
    );
    Ok(())
}

/// The contract's SQL rendering of the newest edition picks the rows its row twin picks
/// (`newest_edition_rows`), per group, with a filter that restricts rows only after the choice.
#[test]
fn the_newest_edition_sql_picks_the_rows_the_row_rule_picks() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    load(&connection, &factories())?;
    let contract = lakehouse_domain::SILVER_REGISTERED_FACTORY;
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = connection
        .prepare(
            "SELECT source_snapshot_id, strftime(ingested_at_utc, '%Y-%m-%dT%H:%M:%SZ'), \
             coalesce(company_name, '') FROM silver_registered_factory",
        )?
        .query_map([], |row| {
            let (edition, at, name): (String, String, String) = (row.get(0)?, row.get(1)?, row.get(2)?);
            Ok(serde_json::json!({"source_snapshot_id": edition, "ingested_at_utc": at, "company_name": name}))
        })?
        .map(
            |row| -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
                Ok(row?.as_object().cloned().unwrap_or_default())
            },
        )
        .collect::<anyhow::Result<_>>()?;
    let mut expected: Vec<String> = contract
        .newest_edition_rows(&rows)?
        .iter()
        .map(|row| row["company_name"].as_str().unwrap_or_default().to_owned())
        .collect();
    expected.sort();
    let sql = contract
        .newest_edition_sql(
            "silver_registered_factory",
            &["coalesce(company_name, '') AS company_name"],
            "TRUE",
        )
        .ok_or_else(|| anyhow::anyhow!("the factory file is read by its newest edition"))?;
    let mut picked: Vec<String> = connection
        .prepare(&format!("SELECT company_name FROM ({sql}) ORDER BY 1"))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    picked.sort();
    assert_eq!(picked, expected);
    assert!(!picked.contains(&"바".to_owned()));
    // A filter no row of the newest edition passes reads nothing; it never falls back to an older one.
    let filtered = contract
        .newest_edition_sql(
            "silver_registered_factory",
            &["company_name"],
            "company_name = '바'",
        )
        .ok_or_else(|| anyhow::anyhow!("the factory file is read by its newest edition"))?;
    let count: i64 =
        connection.query_row(&format!("SELECT count(*) FROM ({filtered})"), [], |row| {
            row.get(0)
        })?;
    assert_eq!(count, 0);
    Ok(())
}

/// Each group reads its own newest edition (a court-auction table holds its editions per case).
#[test]
fn each_group_reads_its_own_newest_edition() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE silver_court_auction_item_schedule (court_office_code VARCHAR, case_no VARCHAR, \
           property_seq VARCHAR, source_snapshot_id VARCHAR); \
         INSERT INTO silver_court_auction_item_schedule VALUES \
           ('c', '1', '1', '2099-01-01'), ('c', '1', '1', '2099-01-02'), ('c', '2', '1', '2099-01-01');",
    )?;
    let contract = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
        "silver.court_auction_item_schedule",
    )
    .ok_or_else(|| anyhow::anyhow!("the schedule table has a contract"))?;
    let sql = contract
        .newest_edition_sql(
            "silver_court_auction_item_schedule",
            &["case_no", "source_snapshot_id"],
            "TRUE",
        )
        .ok_or_else(|| anyhow::anyhow!("the schedule is read by its newest edition per case"))?;
    let picked: Vec<(String, String)> = connection
        .prepare(&format!("SELECT * FROM ({sql}) ORDER BY 1"))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    // Each case its own newest edition.
    assert_eq!(
        picked,
        [
            ("1".to_owned(), "2099-01-02".to_owned()),
            ("2".to_owned(), "2099-01-01".to_owned())
        ]
    );
    Ok(())
}
