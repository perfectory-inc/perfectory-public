//! Tests for the industrial-complex boundary Silver handoff export.
//!
//! These drive the whole command over a synthetic Bronze zip: a shapefile, its attribute table,
//! and a `.prj`, laid out the way the provider ships them.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use lakehouse_application::GeoPoint;
use serde_json::Value as JsonValue;
use zip::{write::SimpleFileOptions, ZipWriter};

use super::{export_boundary_handoff, BoundarySilverExportConfig};
use crate::{
    dbase_table::test_support::dbase_bytes,
    shapefile_polygon_reader::test_support::{
        clockwise_square, counter_clockwise_square, shapefile_bytes,
    },
};

/// The projection line the real source ships, which the command refuses to run without.
const REAL_PROJECTION: &str = "PROJCS[\"Korea_2000_Korea_Central_Belt_2010\",UNIT[\"Meter\",1.0]]";

/// Synthetic complex codes: six characters like the source's, in a range it does not use.
const CODE_A: &str = "999ZZ0";
const CODE_B: &str = "999ZZ1";

fn temporary_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "foundation-platform-boundary-handoff-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

/// Writes a Bronze zip holding one shapefile, at the layout the Bronze root uses.
fn bronze_zip(
    root: &Path,
    records: &[Vec<Vec<GeoPoint>>],
    codes: &[&str],
    types: &[&str],
    projection: &str,
) -> anyhow::Result<()> {
    let source_dir = root.join("bronze").join("source=vworldkr__sandan_boundary");
    fs::create_dir_all(&source_dir)?;
    let rows = codes
        .iter()
        .zip(types)
        .map(|(code, kind)| vec![code.as_bytes(), kind.as_bytes()])
        .collect::<Vec<_>>();
    let attributes = dbase_bytes(&[("DAN_ID", 6), ("DANJI_TYPE", 1)], &rows);

    let file = fs::File::create(source_dir.join("30137-1.zip"))?;
    let mut zip = ZipWriter::new(file);
    let options = SimpleFileOptions::default();
    zip.start_file("DAM_DAN.shp", options)?;
    zip.write_all(&shapefile_bytes(records))?;
    zip.start_file("DAM_DAN.dbf", options)?;
    zip.write_all(&attributes)?;
    zip.start_file("DAM_DAN.prj", options)?;
    zip.write_all(projection.as_bytes())?;
    zip.finish()?;
    Ok(())
}

fn config(root: &Path) -> BoundarySilverExportConfig {
    BoundarySilverExportConfig {
        bronze_local_object_root: root.to_path_buf(),
        source_slug: "vworldkr__sandan_boundary".to_owned(),
        source_object: None,
        output_path: root.join("out").join("boundaries.jsonl"),
        summary_path: Some(root.join("out").join("summary.json")),
        source_snapshot_id: Some("synthetic-boundary-snapshot-0001".to_owned()),
        valid_from_utc: None,
    }
}

#[test]
fn exports_one_row_per_polygon_with_the_code_from_the_attribute_table() -> anyhow::Result<()> {
    let root = temporary_root("rows");
    bronze_zip(
        &root,
        &[
            vec![clockwise_square(200_000.0, 500_000.0, 300.0)],
            vec![clockwise_square(210_000.0, 500_000.0, 100.0)],
        ],
        &[CODE_A, CODE_B],
        &["2", "4"],
        REAL_PROJECTION,
    )?;
    let config = config(&root);

    let report = export_boundary_handoff(&config)?;

    assert_eq!(report.shapefile_record_count, 2);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.null_shape_count, 0);
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    assert_eq!(report.complex_type_counts.get("2"), Some(&1));
    assert_eq!(report.complex_type_counts.get("4"), Some(&1));

    let jsonl = fs::read_to_string(&config.output_path)?;
    let lines = jsonl.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let first: JsonValue = serde_json::from_str(lines[0])?;
    assert_eq!(
        first
            .get("official_complex_code")
            .and_then(JsonValue::as_str),
        Some(CODE_A)
    );
    assert_eq!(
        first.get("geometry_srid").and_then(JsonValue::as_i64),
        Some(5186)
    );
    assert_eq!(
        first.get("boundary_kind").and_then(JsonValue::as_str),
        Some("official")
    );
    assert_eq!(first.get("complex_id"), Some(&JsonValue::Null));
    assert_eq!(first.get("sido_code"), Some(&JsonValue::Null));
    assert!(
        (first
            .get("area_sqm_calculated")
            .and_then(JsonValue::as_f64)
            .context("area_sqm_calculated must be a number")?
            - 90_000.0)
            .abs()
            < 1e-6
    );

    fs::remove_dir_all(&root)?;
    Ok(())
}

/// The hole reaches the row as a subtraction, which is the whole chain — shapefile winding, ring
/// sorting, area — behaving as one.
#[test]
fn a_hole_in_the_source_polygon_reaches_the_row_as_a_smaller_area() -> anyhow::Result<()> {
    let root = temporary_root("hole");
    bronze_zip(
        &root,
        &[vec![
            clockwise_square(0.0, 0.0, 300.0),
            counter_clockwise_square(100.0, 100.0, 100.0),
        ]],
        &[CODE_A],
        &["2"],
        REAL_PROJECTION,
    )?;
    let config = config(&root);

    export_boundary_handoff(&config)?;

    let jsonl = fs::read_to_string(&config.output_path)?;
    let row: JsonValue = serde_json::from_str(jsonl.trim_end())?;
    let area = row
        .get("area_sqm_calculated")
        .and_then(JsonValue::as_f64)
        .context("area_sqm_calculated must be a number")?;
    assert!((area - 80_000.0).abs() < 1e-6, "90000 - 10000, got {area}");

    fs::remove_dir_all(&root)?;
    Ok(())
}

/// Relabelling another projection as EPSG:5186 puts complexes wherever that projection's numbers
/// happen to land, and nothing downstream can see it. The run stops instead.
#[test]
fn a_source_in_another_projection_is_refused() -> anyhow::Result<()> {
    let root = temporary_root("projection");
    bronze_zip(
        &root,
        &[vec![clockwise_square(0.0, 0.0, 300.0)]],
        &[CODE_A],
        &["2"],
        "PROJCS[\"Korea_2000_Korea_Unified\",UNIT[\"Meter\",1.0]]",
    )?;
    let config = config(&root);

    let error = export_boundary_handoff(&config)
        .map(|_| ())
        .expect_err("a source in another projection must be refused");

    assert!(
        format!("{error:#}").contains("Korea_2000_Korea_Central_Belt_2010"),
        "{error:#}"
    );
    assert!(!config.output_path.exists(), "nothing must be written");

    fs::remove_dir_all(&root)?;
    Ok(())
}

/// Two polygons for one complex would put two active official boundaries on it, which the contract
/// forbids. The source has no duplicates today; the run must not be the thing that discovers it
/// after loading.
#[test]
fn a_duplicated_complex_code_is_refused() -> anyhow::Result<()> {
    let root = temporary_root("duplicate");
    bronze_zip(
        &root,
        &[
            vec![clockwise_square(0.0, 0.0, 300.0)],
            vec![clockwise_square(1_000.0, 0.0, 300.0)],
        ],
        &[CODE_A, CODE_A],
        &["2", "2"],
        REAL_PROJECTION,
    )?;
    let config = config(&root);

    let error = export_boundary_handoff(&config)
        .map(|_| ())
        .expect_err("a duplicated complex code must be refused");

    assert!(format!("{error:#}").contains("appears twice"), "{error:#}");

    fs::remove_dir_all(&root)?;
    Ok(())
}

/// The handoff is evidence of what a Silver load was built from. A rerun that truncated it would
/// destroy the record of the earlier one.
#[test]
fn an_existing_output_is_not_replaced() -> anyhow::Result<()> {
    let root = temporary_root("append-only");
    bronze_zip(
        &root,
        &[vec![clockwise_square(0.0, 0.0, 300.0)]],
        &[CODE_A],
        &["2"],
        REAL_PROJECTION,
    )?;
    let config = config(&root);
    export_boundary_handoff(&config)?;

    let error = export_boundary_handoff(&config)
        .map(|_| ())
        .expect_err("an existing handoff must not be replaced");

    assert!(format!("{error:#}").contains("append-only"), "{error:#}");

    fs::remove_dir_all(&root)?;
    Ok(())
}

/// The summary is where the counts live, including the source complex-type histogram that says why
/// `DANJI_TYPE` is not carried into `boundary_kind`.
#[test]
fn the_summary_records_the_counts_and_what_it_cannot_answer() -> anyhow::Result<()> {
    let root = temporary_root("summary");
    bronze_zip(
        &root,
        &[
            vec![clockwise_square(0.0, 0.0, 300.0)],
            vec![clockwise_square(1_000.0, 0.0, 300.0)],
        ],
        &[CODE_A, CODE_B],
        &["1", "1"],
        REAL_PROJECTION,
    )?;
    let config = config(&root);

    export_boundary_handoff(&config)?;

    let summary_path = config.summary_path.clone().context("summary path")?;
    let summary: JsonValue = serde_json::from_str(&fs::read_to_string(&summary_path)?)?;
    assert_eq!(
        summary
            .pointer("/counts/row_count")
            .and_then(JsonValue::as_u64),
        Some(2)
    );
    assert_eq!(
        summary
            .pointer("/counts/source_complex_type_counts/1")
            .and_then(JsonValue::as_u64),
        Some(2)
    );
    assert_eq!(
        summary
            .pointer("/source/geometry_srid")
            .and_then(JsonValue::as_i64),
        Some(5186)
    );
    let limitations = summary
        .get("evidence_limitations")
        .and_then(JsonValue::as_array)
        .context("evidence_limitations must be an array")?;
    assert!(
        limitations.iter().any(|value| {
            value.as_str() == Some("orphan_and_unbounded_complex_counts_come_from_the_writer_join")
        }),
        "{limitations:?}"
    );

    fs::remove_dir_all(&root)?;
    Ok(())
}
