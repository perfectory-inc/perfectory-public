//! The staged rows are the rows the Spark job built (`build_silver_frame`), and its refusals are kept.
//!
//! `fixtures/export.jsonl` is a synthetic export (codes `99999…`, 2099 dates) of the producer's
//! transport: a plain complex, and one with padded codes and names, a blank address, an impossible
//! date, an unreadable area, a progress with three decimals and a business period. The values the
//! Spark frame gave those inputs are written here by hand from its rules; the engine verdict the
//! move was made on compared the rows with production's.

use std::path::Path;

use anyhow::Context;
use foundation_publisher_core::lakehouse_data_commit::{TableField, TableShape};
use foundation_shared_kernel::ids::ComplexId;
use lakehouse_domain::{LakehouseTableContract, SILVER_INDUSTRIAL_COMPLEXES};
use serde_json::Value;

use super::{checksum_sql, stage_export, StagedComplexes};
use crate::duckdb_parquet_stage::DuckDbParquetStage;

const EXPORT: &str = include_str!("fixtures/export.jsonl");

fn contract() -> &'static LakehouseTableContract {
    &SILVER_INDUSTRIAL_COMPLEXES
}

fn shape() -> anyhow::Result<TableShape> {
    Ok(TableShape {
        location: String::new(),
        fields: contract()
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Ok(TableField {
                    id: i32::try_from(index + 1)?,
                    name: column.name.to_owned(),
                    required: column.required,
                    element_id: None,
                })
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

fn stage_text(
    directory: &Path,
    text: &str,
) -> anyhow::Result<(DuckDbParquetStage, StagedComplexes)> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join("export.jsonl");
    std::fs::write(&path, text)?;
    let stage = DuckDbParquetStage::open(directory)?;
    let staged = stage_export(&stage, &shape()?, contract(), &path, None, "complexes-test")?;
    Ok((stage, staged))
}

/// The rows of `complex_rows`, each as the JSON object `DuckDB` writes, by code.
fn rows(stage: &DuckDbParquetStage) -> anyhow::Result<Vec<Value>> {
    stage
        .json_rows(
            "SELECT * REPLACE (CAST(valid_from_utc AS TIMESTAMP) AS valid_from_utc, \
             CAST(ingested_at_utc AS TIMESTAMP) AS ingested_at_utc, \
             CAST(official_area_sqm AS VARCHAR) AS official_area_sqm, \
             CAST(development_progress_percent AS VARCHAR) AS development_progress_percent, \
             CAST(designated_date AS VARCHAR) AS designated_date, \
             CAST(construction_start_date AS VARCHAR) AS construction_start_date, \
             CAST(completion_date AS VARCHAR) AS completion_date) \
             FROM complex_rows ORDER BY official_complex_code",
        )?
        .iter()
        .map(|row| Ok(serde_json::from_str(row)?))
        .collect()
}

#[test]
fn the_rows_are_read_as_the_spark_frame_read_them() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let (stage, staged) = stage_text(directory.path(), EXPORT)?;
    assert_eq!(
        (staged.rows, staged.changed),
        (2, 2),
        "an empty table: every complex is new"
    );
    assert!(staged.file.is_some());
    let rows = rows(&stage)?;

    let plain = &rows[0];
    assert_eq!(plain["official_complex_code"], "99999A");
    assert_eq!(
        plain["complex_id"],
        ComplexId::for_official_complex_code("99999A").to_string()
    );
    assert_eq!(plain["complex_name_normalized"], "synthetic 시험 산업단지");
    assert_eq!(plain["designated_date"], "2099-01-02");
    assert_eq!(plain["official_area_sqm"], "1234.50");
    assert_eq!(plain["development_progress_percent"], "100.00");
    assert_eq!(plain["sido_code"], Value::Null);
    assert_eq!(plain["valid_to_utc"], Value::Null);
    assert_eq!(plain["valid_from_utc"], "2099-01-01 00:00:00");
    assert_eq!(plain["ingested_at_utc"], "2099-01-03 04:05:06");

    let padded = &rows[1];
    // Spark's `trim` before the id seed: the id is the trimmed code's.
    assert_eq!(padded["official_complex_code"], "99999B");
    assert_eq!(
        padded["complex_id"],
        ComplexId::for_official_complex_code("99999B").to_string()
    );
    assert_eq!(padded["complex_name"], "synthetic   둘째  Complex");
    assert_eq!(padded["complex_name_normalized"], "synthetic 둘째 complex");
    // `trim_to_null` on the three free-text columns only.
    assert_eq!(padded["address_text"], Value::Null);
    assert_eq!(padded["management_agency_name"], "synthetic 관리");
    assert_eq!(
        padded["construction_start_date"],
        Value::Null,
        "month 13 is no date"
    );
    assert_eq!(padded["completion_date"], "2099-02-03");
    assert_eq!(
        padded["official_area_sqm"],
        Value::Null,
        "an unreadable decimal is null"
    );
    assert_eq!(
        padded["development_progress_percent"], "12.35",
        "rounded half up to the scale"
    );
    assert_eq!(padded["business_period_start_month"], "2099-01");
    for row in &rows {
        let checksum = row["row_checksum_sha256"].as_str().context("checksum")?;
        assert!(checksum.len() == 64 && checksum.bytes().all(|b| b.is_ascii_hexdigit()));
    }
    assert_ne!(
        rows[0]["row_checksum_sha256"],
        rows[1]["row_checksum_sha256"]
    );
    Ok(())
}

#[test]
fn the_checksum_is_stable_and_covers_every_other_column_in_name_order() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let (first, _) = stage_text(&directory.path().join("a"), EXPORT)?;
    let (second, _) = stage_text(&directory.path().join("b"), EXPORT)?;
    let checksums = |stage: &DuckDbParquetStage| -> anyhow::Result<Vec<Value>> {
        Ok(rows(stage)?
            .into_iter()
            .map(|row| row["row_checksum_sha256"].clone())
            .collect())
    };
    assert_eq!(checksums(&first)?, checksums(&second)?);

    let sql = checksum_sql(contract())?;
    assert!(!sql.contains("\"row_checksum_sha256\""));
    let positions = contract()
        .columns
        .iter()
        .filter(|column| column.name != "row_checksum_sha256")
        .map(|column| {
            sql.find(&format!("\"{}\" :=", column.name))
                .map(|at| (column.name, at))
                .with_context(|| format!("{} is not hashed", column.name))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut by_position = positions.clone();
    by_position.sort_by_key(|(_, at)| *at);
    let mut by_name = positions;
    by_name.sort_by_key(|(name, _)| *name);
    assert_eq!(by_position, by_name, "keys in name order");
    Ok(())
}

/// Each refusal of the Spark job, on one line changed from the fixture's first.
#[test]
fn the_spark_refusals_hold_before_anything_is_staged() -> anyhow::Result<()> {
    let first: Value = serde_json::from_str(EXPORT.lines().next().context("a line")?)?;
    let with = |changes: &[(&str, Value)]| -> String {
        let mut line = first.clone();
        for (column, value) in changes {
            line[*column] = value.clone();
        }
        format!("{line}\n")
    };
    let cases: Vec<(String, &str)> = vec![
        (
            with(&[("complex_kind", "other".into())]),
            "complex_kind is outside",
        ),
        (with(&[("status", Value::Null)]), "status must not be null"),
        (
            with(&[("sido_code", "".into())]),
            "sido_code must not be empty",
        ),
        (
            with(&[("sido_code", "00".into())]),
            "a region code that is present",
        ),
        (
            with(&[("sigungu_code", "9999".into())]),
            "a region code that is present",
        ),
        (
            with(&[("official_area_sqm", "0".into())]),
            "official_area_sqm must be positive",
        ),
        (
            with(&[("development_progress_percent", "100.5".into())]),
            "between 0 and 100",
        ),
        (
            with(&[("lot_sales_status", "sold".into())]),
            "lot_sales_status is outside",
        ),
        (
            with(&[("development_stage", "dreaming".into())]),
            "development_stage is outside",
        ),
        (
            with(&[("business_period_start_month", "2099-01".into())]),
            "present together",
        ),
        (
            with(&[
                ("business_period_start_month", "2099-1".into()),
                ("business_period_end_month", "2099-12".into()),
            ]),
            "present together",
        ),
        (
            with(&[("complex_id", "not-a-uuid".into())]),
            "complex_id must be a lowercase UUID",
        ),
        (format!("{}{}", with(&[]), with(&[])), "must be unique"),
        ("[1, 2]\n".to_owned(), "is not a JSON object"),
    ];
    for (index, (text, expected)) in cases.iter().enumerate() {
        let directory = tempfile::tempdir()?;
        let error = stage_text(directory.path(), text)
            .err()
            .with_context(|| format!("case {index} ({expected}) was not refused"))?;
        let message = format!("{error:#}");
        assert!(message.contains(expected), "case {index}: {message}");
    }

    let mut missing = first.clone();
    missing
        .as_object_mut()
        .context("an object")?
        .remove("developer_name");
    let directory = tempfile::tempdir()?;
    let error = stage_text(directory.path(), &format!("{missing}\n"))
        .err()
        .context("a line without a transport column was not refused")?;
    assert!(format!("{error:#}").contains("missing the column developer_name"));
    Ok(())
}
