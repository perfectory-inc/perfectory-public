//! Tests for the parts of the industrial-complex boundary PostGIS publish that need no database.
//!
//! The reprojection, the append-only ledger and the manifest gate are exercised end to end by
//! `tests/industrial_complex_boundary_publication.rs`, which drives the built binary against a
//! disposable PostGIS database. What is here is the file contract: everything the command refuses
//! before it opens a connection, because a run that reaches PostgreSQL with a bad file has already
//! spent an operator's time on a load that must not exist.

use std::{
    fs,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use super::{is_positive_decimal, is_uuid_v5, read_rows, Config};

/// A UUIDv5 in the shape the lakehouse derives, and the v7 shape a locally minted id has.
const LAKEHOUSE_COMPLEX_ID: &str = "7df3859c-1111-51fa-8222-333344445555";
const LOCAL_COMPLEX_ID: &str = "01a0136d-1111-7e61-8222-333344445555";
const OFFICIAL_CODE: &str = "999ZZ0";
const SOURCE_SNAPSHOT_ID: &str = "vworldkr__sandan_boundary-publish-test";
const SOURCE_OBJECT_KEY: &str = "bronze/vworldkr__sandan_boundary/publish-test.zip";
/// A well-formed little-endian WKB polygon is not needed here: nothing in `read_rows` parses WKB,
/// it only hashes the bytes. PostGIS is what refuses a payload that is not a polygon.
const GEOMETRY_WKB_HEX: &str = "0103000000";
/// A well-formed but wrong checksum: 64 lowercase hex characters that are not this geometry's.
const WRONG_GEOMETRY_SHA256: &str =
    "b6e0dfd0a8b2c9c8a0dbc19b3f28c7d5cd80b1b2f6e2ab89ae2f96c3d6f4a1d2";

fn config(root: &Path) -> Config {
    Config {
        database_url: "postgres://unused".to_owned(),
        source_path: root.join("boundaries.jsonl"),
        canonical_snapshot_id: "841361364657368624".to_owned(),
        source_snapshot_id: SOURCE_SNAPSHOT_ID.to_owned(),
        source_record_id: Uuid::new_v4(),
        source_object_key: SOURCE_OBJECT_KEY.to_owned(),
    }
}

/// One export line, with every field the command reads and an optional override of one of them.
fn line(complex_id: &str, code: &str, overrides: &[(&str, &str)]) -> String {
    let mut fields = vec![
        ("complex_id".to_owned(), complex_id.to_owned()),
        ("official_complex_code".to_owned(), code.to_owned()),
        ("boundary_kind".to_owned(), "official".to_owned()),
        ("geometry_wkb_hex".to_owned(), GEOMETRY_WKB_HEX.to_owned()),
        ("geometry_checksum_sha256".to_owned(), checksum().to_owned()),
        ("area_sqm_calculated".to_owned(), "1234.56".to_owned()),
        ("source_record_id".to_owned(), SOURCE_OBJECT_KEY.to_owned()),
        (
            "source_snapshot_id".to_owned(),
            SOURCE_SNAPSHOT_ID.to_owned(),
        ),
    ];
    for (name, value) in overrides {
        for field in &mut fields {
            if field.0 == *name {
                field.1 = (*value).to_owned();
            }
        }
    }
    let mut json = serde_json::Map::new();
    for (name, value) in fields {
        json.insert(name, serde_json::Value::String(value));
    }
    json.insert("geometry_srid".to_owned(), serde_json::Value::from(5186));
    serde_json::Value::Object(json).to_string()
}

/// The checksum the fixture bytes actually hash to, so the fixture cannot drift from itself.
fn checksum() -> &'static str {
    static ONCE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| super::sha256_hex(&super::decode_hex(GEOMETRY_WKB_HEX).expect("hex")))
        .as_str()
}

fn write(root: &Path, body: &str) {
    fs::create_dir_all(root).expect("fixture directory");
    fs::write(root.join("boundaries.jsonl"), body).expect("fixture file");
}

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "complex-boundary-publish-{label}-{}",
        Uuid::new_v4()
    ))
}

#[test]
fn a_well_formed_export_is_read() {
    let root = root("ok");
    write(
        &root,
        &format!("{}\n", line(LAKEHOUSE_COMPLEX_ID, OFFICIAL_CODE, &[])),
    );
    let rows = read_rows(&config(&root)).expect("the export must be readable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].official_complex_code, OFFICIAL_CODE);
    let _ = fs::remove_dir_all(&root);
}

/// ADR-0043's rule, stated where the operator finds out cheaply.
#[test]
fn a_locally_minted_uuid_v7_is_refused() {
    let root = root("v7");
    write(
        &root,
        &format!("{}\n", line(LOCAL_COMPLEX_ID, OFFICIAL_CODE, &[])),
    );
    let error = read_rows(&config(&root)).expect_err("a v7 complex_id must be refused");
    assert!(
        format!("{error:#}").contains("not the UUIDv5"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn a_geometry_whose_bytes_do_not_hash_to_its_checksum_is_refused() {
    let root = root("checksum");
    write(
        &root,
        &format!(
            "{}\n",
            line(
                LAKEHOUSE_COMPLEX_ID,
                OFFICIAL_CODE,
                &[("geometry_checksum_sha256", WRONG_GEOMETRY_SHA256)],
            )
        ),
    );
    let error = read_rows(&config(&root)).expect_err("a mismatched checksum must be refused");
    assert!(
        format!("{error:#}").contains("geometry_checksum_sha256 mismatch"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

/// A row from a different Silver snapshot cannot be published under this one's provenance.
#[test]
fn a_row_from_another_snapshot_is_refused() {
    let root = root("snapshot");
    write(
        &root,
        &format!(
            "{}\n",
            line(
                LAKEHOUSE_COMPLEX_ID,
                OFFICIAL_CODE,
                &[("source_snapshot_id", "vworldkr__sandan_boundary-other")],
            )
        ),
    );
    let error = read_rows(&config(&root)).expect_err("a foreign snapshot must be refused");
    assert!(
        format!("{error:#}").contains("source_snapshot_id mismatch"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

/// The operator names the Bronze object; the export cites the one Silver was built from.
#[test]
fn a_row_citing_another_object_key_is_refused() {
    let root = root("object-key");
    write(
        &root,
        &format!(
            "{}\n",
            line(
                LAKEHOUSE_COMPLEX_ID,
                OFFICIAL_CODE,
                &[("source_record_id", "bronze/other/object.zip")],
            )
        ),
    );
    let error = read_rows(&config(&root)).expect_err("a foreign object key must be refused");
    assert!(
        format!("{error:#}").contains("source_record_id mismatch"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn one_complex_may_not_appear_twice_in_one_load() {
    let root = root("duplicate");
    let body = format!(
        "{}\n{}\n",
        line(LAKEHOUSE_COMPLEX_ID, OFFICIAL_CODE, &[]),
        line(LAKEHOUSE_COMPLEX_ID, OFFICIAL_CODE, &[])
    );
    write(&root, &body);
    let error = read_rows(&config(&root)).expect_err("a repeated complex must be refused");
    assert!(
        format!("{error:#}").contains("appears twice"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

/// Silver refuses a boundary enclosing no area, so a zero here means the file is not Silver's.
#[test]
fn a_non_positive_area_is_refused() {
    let root = root("area");
    write(
        &root,
        &format!(
            "{}\n",
            line(
                LAKEHOUSE_COMPLEX_ID,
                OFFICIAL_CODE,
                &[("area_sqm_calculated", "0.00")],
            )
        ),
    );
    let error = read_rows(&config(&root)).expect_err("a zero area must be refused");
    assert!(
        format!("{error:#}").contains("not a positive decimal"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn an_empty_export_is_refused() {
    let root = root("empty");
    write(&root, "\n");
    let error = read_rows(&config(&root)).expect_err("an empty export must be refused");
    assert!(
        format!("{error:#}").contains("contains no rows"),
        "unexpected error: {error:#}"
    );
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn the_uuid_version_nibble_is_what_decides() {
    assert!(is_uuid_v5(LAKEHOUSE_COMPLEX_ID));
    assert!(!is_uuid_v5(LOCAL_COMPLEX_ID));
    assert!(!is_uuid_v5("7DF3859C-1111-51FA-8222-333344445555"));
    assert!(!is_uuid_v5("7df3859c111151fa8222333344445555"));
    // The variant nibble matters too: RFC 9562 reserves `8`-`b` for it.
    assert!(!is_uuid_v5("7df3859c-1111-51fa-c222-333344445555"));
}

#[test]
fn a_decimal_is_positive_only_when_some_digit_is_not_zero() {
    assert!(is_positive_decimal("1234.56"));
    assert!(is_positive_decimal("0.01"));
    assert!(is_positive_decimal("7"));
    assert!(!is_positive_decimal("0.00"));
    assert!(!is_positive_decimal("-1.00"));
    assert!(!is_positive_decimal("1.0e3"));
    assert!(!is_positive_decimal(""));
}
