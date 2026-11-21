//! The compare appends what `silver_release_compare.compared_changes` appended (root ADR-0599 §3).
//! Reserved 99999 codes and 2099 dates only.

use anyhow::Context as _;
use chrono::DateTime;
use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};

use super::{
    buckets, check_pnu_null_share, check_release, compare_bucket, copy_release, edition_text,
    release_relation, tolerate, Bucket, BucketChanges, CompareRule, GeometryTolerance, Stamp,
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
        // As the run names main: a table with a clause after it (`AT (VERSION => n)` there), which
        // takes no alias. 2026-11-21: the first production runs failed on `... g`.
        "main_rows TABLESAMPLE (100 PERCENT)",
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
            // The form the HUB exports wrote until 2026-11-21, on one row: the same instant.
            "ingested_at_utc" if zone == "b1" => "2099-01-01T00:00:00.829434156+00:00".into(),
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
    let instant: (i64, i64) = stage.connection().query_row(
        &format!(
            "SELECT epoch_us(max(ingested_at_utc)) // 1000000, count(DISTINCT ingested_at_utc)              FROM {release}"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(instant, (4_070_908_800, 1), "the instant is read as UTC");
    // Planted: an instant in another zone is not read as if it were UTC.
    let mut line: serde_json::Value =
        serde_json::from_str(&handoff_line(Some("9999910100100010000"), "a1", "e2")?)?;
    line["ingested_at_utc"] = "2099-01-01T00:00:00+09:00".into();
    let other = work.join("c.jsonl.gz");
    gzip(&[line.to_string()], &other)?;
    let other = other.to_string_lossy().replace('\\', "/");
    assert!(copy_release(&stage, contract, &[other], &work.join("other.parquet")).is_err());
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

// ---- the register tables: an instant edition, a Parquet handoff, the PNU null-share gate --------

const TITLES: &str = "silver.building_register_titles";

/// Rows of the titles table: `(key, dong name, edition seconds, load seconds, row state)`, every
/// other column one fixed value of its type.
fn titles(rows: &str) -> anyhow::Result<String> {
    let contract = industrial_complex_lakehouse_contract_by_table_name(TITLES).context("titles")?;
    let columns = contract
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("mgm_bldrgst_pk", _) => "v.k AS mgm_bldrgst_pk".to_owned(),
            ("dong_name_raw", _) => "v.dong AS dong_name_raw".to_owned(),
            ("row_state", _) => "v.state AS row_state".to_owned(),
            ("valid_from_utc", _) => "to_timestamp(v.edition) AS valid_from_utc".to_owned(),
            ("ingested_at_utc", _) => "to_timestamp(v.loaded) AS ingested_at_utc".to_owned(),
            ("source_snapshot_id", _) => "'old' AS source_snapshot_id".to_owned(),
            (name, "double") => format!("CAST(1 AS DOUBLE) AS {name}"),
            (name, "int") => format!("CAST(1 AS INTEGER) AS {name}"),
            (name, "long") => format!("CAST(1 AS BIGINT) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM (VALUES {rows}) v(k, dong, edition, loaded, state))"
    ))
}

#[test]
fn a_table_ordered_by_an_instant_edition_gets_the_releases_instant_on_every_row(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(TITLES).context("titles")?;
    let rule = CompareRule::of(contract, &["dong_name_raw".to_owned()])?;
    assert_eq!(rule.edition_column, Some("valid_from_utc"));
    // 2099-01-01 holds A and B; the 2099-02-01 release changes A and has no B.
    let main = titles(
        "('A', 'one', 4070908800, 4070908800, NULL), ('B', 'two', 4070908800, 4070908800, NULL)",
    )?;
    let release = titles("('A', 'uno', 4073587200, 4073587200, NULL)")?;
    let text: String = stage.connection().query_row(
        &format!(
            "SELECT DISTINCT {} FROM {release}",
            edition_text(contract, "valid_from_utc")?
        ),
        [],
        |row| row.get(0),
    )?;
    assert_eq!(text, "2099-02-01 00:00:00");
    let stamp = Stamp {
        source_snapshot_id: "new",
        edition: Some(&text),
        run_at: DateTime::from_timestamp(4_073_600_000, 0).context("an instant")?,
    };
    let found = compare_bucket(&stage, contract, &rule, &release, &main, "TRUE", stamp)?;
    assert_eq!(
        found,
        BucketChanges {
            changed_keys: 1,
            removed_keys: 1
        }
    );
    let rows: Vec<(String, Option<String>, i64, i64)> = stage
        .connection()
        .prepare(
            "SELECT mgm_bldrgst_pk, row_state, epoch_us(valid_from_utc) // 1000000, \
             epoch_us(ingested_at_utc) // 1000000 FROM rc_changes ORDER BY mgm_bldrgst_pk",
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        [
            ("A".to_owned(), None, 4_073_587_200, 4_073_600_000),
            // The marker carries the release's edition, so it outranks the row it removes.
            (
                "B".to_owned(),
                Some("removed".to_owned()),
                4_073_587_200,
                4_073_600_000
            ),
        ]
    );
    Ok(())
}

#[test]
fn a_parquet_handoff_is_copied_with_the_tables_types() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(TITLES).context("titles")?;
    let work = directory.path().join(
        std::fs::read_dir(directory.path())?
            .next()
            .context("the stage directory")??
            .file_name(),
    );
    let url = work
        .join("part-0000.parquet")
        .to_string_lossy()
        .replace('\\', "/");
    stage.connection().execute_batch(&format!(
        "COPY (SELECT * FROM {}) TO '{url}' (FORMAT parquet)",
        titles(
            "('A', 'one', 4070908800, 4070908800, NULL), ('B', 'two', 4070908800, 4070908800, NULL)"
        )?
    ))?;
    let copy = work.join("release.parquet");
    assert_eq!(copy_release(&stage, contract, &[url], &copy)?, 2);
    let (kind, edition): (String, i64) = stage.connection().query_row(
        &format!(
            "SELECT typeof(valid_from_utc), epoch_us(max(valid_from_utc)) // 1000000 FROM {} GROUP BY 1",
            release_relation(&copy)?
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(
        (kind.as_str(), edition),
        ("TIMESTAMP WITH TIME ZONE", 4_070_908_800)
    );
    Ok(())
}

/// An export writes the columns it derives: the units' Parquet has no `row_state` (2026-11-21, the
/// lane's first run on this path failed on it). A column the table lets be null is read as null;
/// a required one that is missing is refused.
#[test]
fn a_parquet_handoff_without_a_nullable_column_reads_it_as_null() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(TITLES).context("titles")?;
    let work = directory.path().join(
        std::fs::read_dir(directory.path())?
            .next()
            .context("the stage directory")??
            .file_name(),
    );
    let rows = titles("('A', 'one', 4070908800, 4070908800, NULL)")?;
    let write = |name: &str, without: &str| -> anyhow::Result<String> {
        let url = work.join(name).to_string_lossy().replace('\\', "/");
        stage.connection().execute_batch(&format!(
            "COPY (SELECT * EXCLUDE ({without}) FROM {rows}) TO '{url}' (FORMAT parquet)"
        ))?;
        Ok(url)
    };
    let copy = work.join("release.parquet");
    let url = write("no-row-state.parquet", "row_state")?;
    assert_eq!(copy_release(&stage, contract, &[url], &copy)?, 1);
    let (kind, marker): (String, Option<String>) = stage.connection().query_row(
        &format!(
            "SELECT typeof(row_state), row_state FROM {}",
            release_relation(&copy)?
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!((kind.as_str(), marker), ("VARCHAR", None));
    // Planted: the key itself is missing.
    let url = write("no-key.parquet", "mgm_bldrgst_pk")?;
    let Err(error) = copy_release(&stage, contract, &[url], &copy) else {
        anyhow::bail!("a handoff without its key was copied");
    };
    assert!(
        format!("{error:#}").contains("lacks its required column mgm_bldrgst_pk"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn a_release_that_lost_ordinary_land_parcels_is_refused_before_it_is_staged() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(TITLES).context("titles")?;
    // 10,000 rows; `kind` is the 11th character of the register parcel key (0 = ordinary land),
    // `nulls` of them without a PNU.
    let rows = |kind: char, nulls: u32| {
        format!(
            "(SELECT 'k' || i AS mgm_bldrgst_pk, to_timestamp(4070908800) AS valid_from_utc, \
             to_timestamp(4070908800) AS ingested_at_utc, CAST(NULL AS VARCHAR) AS row_state, \
             '9999999999{kind}' || lpad(CAST(i AS VARCHAR), 8, '0') AS register_parcel_key, \
             CASE WHEN i < {nulls} THEN NULL ELSE '99999' || lpad(CAST(i AS VARCHAR), 14, '0') END AS pnu \
             FROM range(10000) t(i))"
        )
    };
    let outcome = |release: &str, main: Option<&str>| {
        check_pnu_null_share(&stage, contract, release, main).map(Option::unwrap_or_default)
    };
    assert!(outcome(&rows('0', 0), None)?.contains("no_baseline"));
    // 4 of 10,000 is under the ceiling (0.0005) and rises by less than the tolerance (0.001).
    assert!(outcome(&rows('0', 4), Some(&rows('0', 0)))?.contains("within_bounds"));
    // Planted: one row in a hundred lost its parcel.
    let Err(error) = outcome(&rows('0', 100), Some(&rows('0', 0))) else {
        anyhow::bail!("a release that lost parcels passed");
    };
    assert!(
        format!("{error:#}").contains("above the contract ceiling"),
        "{error:#}"
    );
    // A block parcel (대지구분 1) has no PNU by nature: it is not counted.
    assert!(outcome(&rows('1', 100), None)?.contains("no_baseline"));
    // A table that declares no gate is not judged.
    assert_eq!(
        check_pnu_null_share(&stage, self::contract()?, &rows('0', 100), None)?,
        None
    );
    Ok(())
}

// ---- the geometry tolerance (the zone areas) ----------------------------------------------------

const ZONES: &str = "silver.land_use_zone_area";

/// Hex of a little-endian WKB `MultiPolygon` of one square of about 100 m at a synthetic place,
/// moved `metres` north.
fn zone_shape(metres: f64) -> String {
    let degree = 1.0 / 111_320.0;
    let (x, y, side) = (99.0, metres.mul_add(degree, 9.0), 100.0 * degree);
    let mut wkb = vec![1_u8];
    wkb.extend(6_u32.to_le_bytes());
    wkb.extend(1_u32.to_le_bytes());
    wkb.push(1);
    wkb.extend(3_u32.to_le_bytes());
    wkb.extend(1_u32.to_le_bytes());
    wkb.extend(5_u32.to_le_bytes());
    for (px, py) in [
        (x, y),
        (x + side, y),
        (x + side, y + side),
        (x, y + side),
        (x, y),
    ] {
        wkb.extend(px.to_le_bytes());
        wkb.extend(py.to_le_bytes());
    }
    wkb.iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .concat()
}

/// Rows of the zone table: `(feature id, zone name, checksum, shape hex, edition, load seconds)`.
fn zones(rows: &[(&str, &str, &str, String, &str, i64)]) -> anyhow::Result<String> {
    let contract = industrial_complex_lakehouse_contract_by_table_name(ZONES).context("zones")?;
    let columns = contract
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("zone_feature_id", _) => "v.id AS zone_feature_id".to_owned(),
            ("zone_name", _) => "v.name AS zone_name".to_owned(),
            ("geometry_checksum_sha256", _) => "v.checksum AS geometry_checksum_sha256".to_owned(),
            ("geometry_wkb", _) => "from_hex(v.shape) AS geometry_wkb".to_owned(),
            ("source_snapshot_id", _) => "v.edition AS source_snapshot_id".to_owned(),
            ("ingested_at_utc", _) => "to_timestamp(v.loaded) AS ingested_at_utc".to_owned(),
            ("row_state", _) => "CAST(NULL AS VARCHAR) AS row_state".to_owned(),
            (name, "double") => format!("CAST(1 AS DOUBLE) AS {name}"),
            (name, "int") => format!("CAST(4326 AS INTEGER) AS {name}"),
            (name, _) => format!("'x' AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let values = rows
        .iter()
        .map(|(id, name, checksum, shape, edition, loaded)| {
            format!("('{id}', '{name}', '{checksum}', '{shape}', '{edition}', {loaded})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(SELECT {columns} FROM (VALUES {values}) v(id, name, checksum, shape, edition, loaded))"
    ))
}

#[test]
fn a_geometry_moved_within_the_tolerance_is_not_a_change() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(directory.path())?;
    let contract = industrial_complex_lakehouse_contract_by_table_name(ZONES).context("zones")?;
    let rule = CompareRule::of(
        contract,
        &[
            "zone_name".to_owned(),
            "geometry_checksum_sha256".to_owned(),
        ],
    )?;
    let tolerance =
        GeometryTolerance::parse("geometry_wkb,geometry_checksum_sha256,0.1", contract, &rule)?;
    // Planted: a tolerance nobody would measure, and one over a content column.
    assert!(
        GeometryTolerance::parse("geometry_wkb,geometry_checksum_sha256,5", contract, &rule)
            .is_err()
    );
    assert!(
        GeometryTolerance::parse("zone_name,geometry_checksum_sha256,0.1", contract, &rule)
            .is_err()
    );
    let main = zones(&[
        ("near", "a", "c1", zone_shape(0.0), "e1", 4_070_908_800),
        ("far", "a", "c1", zone_shape(0.0), "e1", 4_070_908_800),
        ("renamed", "a", "c1", zone_shape(0.0), "e1", 4_070_908_800),
        ("same", "a", "c1", zone_shape(0.0), "e1", 4_070_908_800),
    ])?;
    let release = zones(&[
        // 5 cm: the provider's re-projection. 20 cm: a boundary change.
        ("near", "a", "c2", zone_shape(0.05), "e2", 4_073_587_200),
        ("far", "a", "c2", zone_shape(0.2), "e2", 4_073_587_200),
        // The name changed too: not the checksum alone.
        ("renamed", "b", "c2", zone_shape(0.05), "e2", 4_073_587_200),
        ("same", "a", "c1", zone_shape(0.0), "e2", 4_073_587_200),
    ])?;
    let stamp = Stamp {
        source_snapshot_id: "e2",
        edition: Some("e2"),
        run_at: DateTime::from_timestamp(4_073_600_000, 0).context("an instant")?,
    };
    let found = compare_bucket(&stage, contract, &rule, &release, &main, "TRUE", stamp)?;
    assert_eq!(found.changed_keys, 3);
    assert_eq!(
        tolerate(&stage, contract, &rule, &release, &main, "TRUE", &tolerance)?,
        1
    );
    let appended: Vec<String> = stage
        .connection()
        .prepare("SELECT zone_feature_id FROM rc_changes ORDER BY 1")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(appended, ["far", "renamed"]);
    Ok(())
}
