//! The compare appends what `silver_release_compare.compared_changes` appended (root ADR-0599 §3).
//! Reserved 99999 codes and 2099 dates only.

use anyhow::Context as _;
use chrono::DateTime;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};

use super::{
    buckets, check_release, compare_bucket, copy_release, release_relation, Bucket, BucketChanges,
    CompareRule, Stamp,
};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const TABLE: &str = "silver.land_use_plan";

fn contract() -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contract_by_table_name(TABLE).context("the land use plan table")
}

fn content() -> Vec<String> {
    ["zone_code", "zone_name", "remark"]
        .map(str::to_owned)
        .to_vec()
}

/// `name` holding `rows` (the key, a zone code, a remark, the edition, epoch seconds, the row
/// state), every other column of the table one fixed text.
fn table(stage: &DuckDbParquetStage, name: &str, rows: &str) -> anyhow::Result<()> {
    let columns = contract()?
        .columns
        .iter()
        .map(|column| match column.name {
            "pnu" | "zone_code" | "remark" | "source_snapshot_id" | "row_state" => {
                format!("v.{}", column.name)
            }
            "ingested_at_utc" => "to_timestamp(v.secs) AS ingested_at_utc".to_owned(),
            other => format!("'x' AS {other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    stage.connection().execute_batch(&format!(
        "CREATE OR REPLACE TABLE {name} AS SELECT {columns} FROM (VALUES {rows}) \
         v(pnu, zone_code, remark, source_snapshot_id, secs, row_state)"
    ))?;
    Ok(())
}

// One synthetic table on each side and what the compare makes of every case.
#[allow(clippy::too_many_lines)]
#[test]
fn a_bucket_appends_the_changed_keys_rows_and_one_marker_per_vanished_key() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = contract()?;
    let rule = CompareRule::of(contract, &content())?;
    assert_eq!(rule.key, ["pnu"]);
    assert_eq!(rule.edition_column, Some("source_snapshot_id"));
    // main: edition e0 then e1.
    table(
        &stage,
        "main_rows",
        "('9999910100100010000', 'a1', 'r', 'e1', 4070908800, NULL),
         ('9999910100100010000', 'a2', 'r', 'e1', 4070908800, NULL),
         ('9999910100100020000', 'b1', 'r', 'e1', 4070908800, NULL),
         ('9999910100100030000', 'c1', NULL, 'e1', 4070908800, NULL),
         ('9999910100100040000', 'd1', 'r', 'e1', 4070908800, NULL),
         ('9999910100100040000', 'd0', 'r', 'e1', 4070908800, NULL),
         ('9999910100100060000', 'f1', 'r', 'e0', 4070900000, NULL),
         ('9999910100100060000', 'f1', 'r', 'e1', 4070908800, 'removed'),
         ('9999910100100070000', 'g0', 'r', 'e0', 4070900000, NULL),
         ('9999910100100070000', 'g1', 'r', 'e1', 4070908800, NULL),
         ('9999920100100010000', 'z1', 'r', 'e1', 4070908800, NULL)",
    )?;
    // The release, edition e2: A as it is, B's row twice, C's null now empty, D gone, E new, F
    // back after its removal, G as its newest version.
    table(
        &stage,
        "release_rows",
        "('9999910100100010000', 'a2', 'r', 'load', 1, NULL),
         ('9999910100100010000', 'a1', 'r', 'load', 1, NULL),
         ('9999910100100020000', 'b1', 'r', 'load', 1, NULL),
         ('9999910100100020000', 'b1', 'r', 'load', 1, NULL),
         ('9999910100100030000', 'c1', '', 'load', 1, NULL),
         ('9999910100100050000', 'e1', 'r', 'load', 1, NULL),
         ('9999910100100060000', 'f1', 'r', 'load', 1, NULL),
         ('9999910100100070000', 'g1', 'r', 'load', 1, NULL)",
    )?;
    let run_at = DateTime::from_timestamp(4_071_000_000, 0).context("an instant")?;
    let stamp = Stamp {
        source_snapshot_id: "e2",
        edition: Some("e2"),
        run_at,
    };
    let bucket = Bucket::Prefix("99999101".to_owned());
    let in_bucket = bucket.predicate("pnu", 8);
    let found = compare_bucket(
        &stage,
        contract,
        &rule,
        &format!("(SELECT * FROM release_rows WHERE {in_bucket})"),
        "main_rows",
        &in_bucket,
        stamp,
    )?;
    assert_eq!(
        found,
        BucketChanges {
            // B, C, E and F; D vanished.
            changed_keys: 4,
            removed_keys: 1
        }
    );
    let rows: Vec<(String, String, Option<String>, String, i64)> = stage
        .connection()
        .prepare(
            "SELECT pnu, zone_code, row_state, source_snapshot_id, epoch_us(ingested_at_utc) // 1000000 \
             FROM rc_changes ORDER BY pnu, zone_code",
        )?
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?
        .collect::<Result<_, _>>()?;
    let live = |pnu: &str, zone: &str| {
        (
            pnu.to_owned(),
            zone.to_owned(),
            None,
            "e2".to_owned(),
            4_071_000_000,
        )
    };
    assert_eq!(
        rows,
        [
            live("9999910100100020000", "b1"),
            live("9999910100100020000", "b1"),
            live("9999910100100030000", "c1"),
            // One marker for the vanished key, made of its first current row by content.
            (
                "9999910100100040000".to_owned(),
                "d0".to_owned(),
                Some("removed".to_owned()),
                "e2".to_owned(),
                4_071_000_000
            ),
            live("9999910100100050000", "e1"),
            live("9999910100100060000", "f1"),
        ]
    );
    // The other bucket's key is not in this bucket's release and is not removed by it.
    let untouched: i64 = stage.connection().query_row(
        "SELECT count(*) FROM rc_changes WHERE starts_with(pnu, '99999201')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(untouched, 0);
    Ok(())
}

#[test]
fn a_release_without_an_edition_is_refused_where_the_table_is_ordered_by_one() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = contract()?;
    let rule = CompareRule::of(contract, &content())?;
    table(
        &stage,
        "main_rows",
        "('9999910100100010000', 'a1', 'r', 'e1', 4070908800, NULL)",
    )?;
    let stamp = Stamp {
        source_snapshot_id: "e2",
        edition: None,
        run_at: DateTime::from_timestamp(4_071_000_000, 0).context("an instant")?,
    };
    assert!(compare_bucket(
        &stage,
        contract,
        &rule,
        "main_rows",
        "main_rows",
        "TRUE",
        stamp
    )
    .is_err());
    Ok(())
}

#[test]
fn the_rule_refuses_content_that_is_a_key_a_load_column_or_no_column() -> anyhow::Result<()> {
    let contract = contract()?;
    for broken in [
        vec![],
        vec!["pnu".to_owned()],
        vec!["zone_code".to_owned(), "ingested_at_utc".to_owned()],
        vec!["zone_code".to_owned(), "row_state".to_owned()],
        vec!["no_such_column".to_owned()],
    ] {
        assert!(CompareRule::of(contract, &broken).is_err(), "{broken:?}");
    }
    // A table of events keeps no newest row per key: there is nothing to compare with.
    let events =
        industrial_complex_lakehouse_contract_by_table_name("silver.parcel_number_change_history")
            .context("the history table")?;
    assert!(CompareRule::of(events, &["old_pnu".to_owned()]).is_err());
    Ok(())
}

#[test]
fn buckets_are_the_key_prefixes_either_side_holds_and_no_value_is_in_two() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let rule = CompareRule::of(contract()?, &content())?;
    stage.connection().execute_batch(
        "CREATE TABLE left_side AS SELECT * FROM (VALUES ('9912'), ('9911'), (NULL), ('9')) t(pnu);
         CREATE TABLE right_side AS SELECT * FROM (VALUES ('9813'), ('9911')) t(pnu);",
    )?;
    let prefix = |text: &str| Bucket::Prefix(text.to_owned());
    assert_eq!(
        buckets(&stage, &rule, &["left_side", "right_side"], 2)?,
        [Bucket::Null, prefix("9"), prefix("98"), prefix("99")]
    );
    assert_eq!(buckets(&stage, &rule, &["left_side"], 0)?, [Bucket::All]);
    assert_eq!(prefix("99").predicate("pnu", 2), "starts_with(pnu, '99')");
    // A value shorter than the bucket length is its own bucket, matched whole.
    assert_eq!(prefix("9").predicate("pnu", 2), "pnu = '9'");
    assert_eq!(Bucket::Null.predicate("pnu", 2), "pnu IS NULL");
    assert_eq!(Bucket::All.predicate("pnu", 0), "TRUE");
    assert_eq!(
        (
            Bucket::Null.label(),
            prefix("").label(),
            Bucket::All.label()
        ),
        ("null".to_owned(), "\"\"".to_owned(), "\"*\"".to_owned())
    );
    Ok(())
}

/// One handoff line of the land use plan table: every text column `x` but the ones given.
fn handoff_line(pnu: Option<&str>, zone: &str, release: &str) -> anyhow::Result<String> {
    let mut row = serde_json::Map::new();
    for column in contract()?.columns {
        let value = match column.name {
            "pnu" => pnu.map_or(serde_json::Value::Null, Into::into),
            "zone_code" => zone.into(),
            "source_snapshot_id" => release.into(),
            "ingested_at_utc" => "2099-01-01T00:00:00Z".into(),
            "row_state" => serde_json::Value::Null,
            _ => "x".into(),
        };
        row.insert(column.name.to_owned(), value);
    }
    Ok(serde_json::Value::Object(row).to_string())
}

fn gzip(lines: &[String], path: &std::path::Path) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut file =
        flate2::write::GzEncoder::new(std::fs::File::create(path)?, flate2::Compression::fast());
    for line in lines {
        writeln!(file, "{line}")?;
    }
    file.finish()?;
    Ok(())
}

/// Root ADR-0599 §1–§2: the handoffs become one typed local copy, and the copy is checked before
/// any row of it can be committed.
#[test]
fn a_release_is_copied_from_its_handoffs_and_checked_before_it_is_compared() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = contract()?;
    // Inside the stage's own directory: the only place this stage may read and write.
    let work = directory.path().join(
        std::fs::read_dir(directory.path())?
            .next()
            .context("the stage directory")??
            .file_name(),
    );
    let (first, second) = (work.join("a.jsonl.gz"), work.join("b.jsonl.gz"));
    gzip(
        &[
            handoff_line(Some("9999910100100010000"), "a1", "e2")?,
            handoff_line(Some("9999910100100020000"), "b1", "e2")?,
        ],
        &first,
    )?;
    gzip(
        &[handoff_line(Some("9999920100100010000"), "z1", "e2")?],
        &second,
    )?;
    let urls = [&first, &second].map(|path| path.to_string_lossy().replace('\\', "/"));
    let copy = work.join("release.parquet");
    assert_eq!(copy_release(&stage, contract, &urls, &copy)?, 3);
    assert!(!copy.with_extension("partial").exists());
    let release = release_relation(&copy)?;
    let instant: i64 = stage.connection().query_row(
        &format!("SELECT epoch_us(max(ingested_at_utc)) // 1000000 FROM {release}"),
        [],
        |row| row.get(0),
    )?;
    assert_eq!(instant, 4_070_908_800, "the instant is read as UTC");
    assert_eq!(check_release(&stage, contract, &release, "e2", Some(3))?, 3);
    // Planted: another count than the export said, another release's rows, no row at all.
    assert!(check_release(&stage, contract, &release, "e2", Some(4)).is_err());
    assert!(check_release(&stage, contract, &release, "e3", None).is_err());
    let none = format!("(SELECT * FROM {release} WHERE false)");
    assert!(check_release(&stage, contract, &none, "e2", None).is_err());
    // Planted: a required column without a value.
    gzip(&[handoff_line(None, "a1", "e2")?], &first)?;
    assert_eq!(copy_release(&stage, contract, &urls[..1], &copy)?, 1);
    let Err(error) = check_release(&stage, contract, &release, "e2", None) else {
        anyhow::bail!("a null key passed the checks");
    };
    assert!(
        format!("{error:#}").contains("pnu must not be null"),
        "{error:#}"
    );
    Ok(())
}
