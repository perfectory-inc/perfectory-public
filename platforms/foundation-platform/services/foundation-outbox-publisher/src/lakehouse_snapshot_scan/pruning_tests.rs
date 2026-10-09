//! A by-PNU scan skips only the data files whose manifest bounds prove they hold none of its
//! prefix, and still accounts for every row of the snapshot (root ADR-0164).
//!
//! Synthetic PNUs only: every value starts with the reserved `99999` region.

use std::{collections::BTreeMap, sync::Arc};

use apache_avro::{types::Value as AvroValue, Schema, Writer};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use lakehouse_domain::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};
use lakehouse_infrastructure::IcebergSnapshotManifestList;
use parquet::arrow::ArrowWriter;

use super::{
    iceberg_scan::{data_files, ColumnStatistics, ScannedDataFile},
    scan_snapshot_rows_kept, KeptPrefix, LakehouseByteReader, ScannedRows,
};

const FIXTURE: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.fixture_panel",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    columns: &[
        LakehouseColumn {
            name: "pnu",
            logical_type: "string",
            required: true,
        },
        LakehouseColumn {
            name: "label",
            logical_type: "string",
            required: false,
        },
    ],
    partition_spec: &[],
    sort_order: &["pnu"],
    write_distribution: LakehouseWriteDistribution::Range,
    quality_gates: &[],
    load: LakehouseLoadUnit::Derived,
};

/// The table schema Iceberg writes into each manifest's Avro header; `pnu` is field 1.
const TABLE_SCHEMA: &str = r#"{"type":"struct","schema-id":0,"fields":[
  {"id":1,"name":"pnu","required":true,"type":"string"},
  {"id":2,"name":"label","required":false,"type":"string"}]}"#;

/// The manifest entry shape Iceberg writes, down to the int-keyed maps as arrays of records.
const MANIFEST_ENTRY_SCHEMA: &str = r#"{
  "type": "record", "name": "manifest_entry", "fields": [
    {"name": "status", "type": "int"},
    {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
      {"name": "content", "type": "int"},
      {"name": "file_path", "type": "string"},
      {"name": "file_format", "type": "string"},
      {"name": "record_count", "type": "long"},
      {"name": "file_size_in_bytes", "type": "long"},
      {"name": "null_value_counts", "type": ["null", {"type": "array", "logicalType": "map",
        "items": {"type": "record", "name": "k121_v122", "fields": [
          {"name": "key", "type": "int"}, {"name": "value", "type": "long"}]}}], "default": null},
      {"name": "lower_bounds", "type": ["null", {"type": "array", "logicalType": "map",
        "items": {"type": "record", "name": "k126_v127", "fields": [
          {"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}], "default": null},
      {"name": "upper_bounds", "type": ["null", {"type": "array", "logicalType": "map",
        "items": {"type": "record", "name": "k129_v130", "fields": [
          {"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}}], "default": null}
    ]}}
  ]}"#;

const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record", "name": "manifest_file", "fields": [
    {"name": "content", "type": "int"},
    {"name": "manifest_path", "type": "string"}
  ]}"#;

const MANIFEST_LIST: &str = "s3://lakehouse/metadata/snap-1.avro";
const MANIFEST: &str = "s3://lakehouse/metadata/manifest-1.avro";

/// One data file of the fixture snapshot: its rows, and what its manifest entry records.
struct FixtureFile {
    path: &'static str,
    pnus: &'static [&'static str],
    /// `(lower, upper, null count)` for `pnu`; None records no statistics at all.
    pnu_statistics: Option<(&'static str, &'static str, i64)>,
}

/// Serves the fixture objects, and fails any read of an object it was not given — a skipped
/// file is left out, so opening it fails the scan.
struct FixtureReader {
    objects: BTreeMap<String, Vec<u8>>,
}

#[async_trait]
impl LakehouseByteReader for FixtureReader {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        self.objects
            .get(location)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unexpected object read: {location}"))
    }
}

fn parquet(pnus: &[&str]) -> anyhow::Result<Vec<u8>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("pnu", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let labels = pnus.iter().map(|_| None::<&str>).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(pnus.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(labels)) as ArrayRef,
        ],
    )?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

fn int_keyed(key: i32, value: AvroValue) -> AvroValue {
    AvroValue::Union(
        1,
        Box::new(AvroValue::Array(vec![AvroValue::Record(vec![
            ("key".to_owned(), AvroValue::Int(key)),
            ("value".to_owned(), value),
        ])])),
    )
}

fn absent() -> AvroValue {
    AvroValue::Union(0, Box::new(AvroValue::Null))
}

fn manifest(files: &[FixtureFile]) -> anyhow::Result<Vec<u8>> {
    let schema = Schema::parse_str(MANIFEST_ENTRY_SCHEMA)?;
    let mut writer = Writer::new(&schema, Vec::new());
    writer.add_user_metadata("schema".to_owned(), TABLE_SCHEMA)?;
    for file in files {
        let (nulls, lower, upper) = match file.pnu_statistics {
            Some((lower, upper, nulls)) => (
                int_keyed(1, AvroValue::Long(nulls)),
                int_keyed(1, AvroValue::Bytes(lower.as_bytes().to_vec())),
                int_keyed(1, AvroValue::Bytes(upper.as_bytes().to_vec())),
            ),
            None => (absent(), absent(), absent()),
        };
        writer.append(AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Int(1)),
            (
                "data_file".to_owned(),
                AvroValue::Record(vec![
                    ("content".to_owned(), AvroValue::Int(0)),
                    (
                        "file_path".to_owned(),
                        AvroValue::String(file.path.to_owned()),
                    ),
                    (
                        "file_format".to_owned(),
                        AvroValue::String("PARQUET".to_owned()),
                    ),
                    (
                        "record_count".to_owned(),
                        AvroValue::Long(i64::try_from(file.pnus.len())?),
                    ),
                    ("file_size_in_bytes".to_owned(), AvroValue::Long(1_024)),
                    ("null_value_counts".to_owned(), nulls),
                    ("lower_bounds".to_owned(), lower),
                    ("upper_bounds".to_owned(), upper),
                ]),
            ),
        ]))?;
    }
    Ok(writer.into_inner()?)
}

fn manifest_list() -> anyhow::Result<Vec<u8>> {
    let schema = Schema::parse_str(MANIFEST_LIST_SCHEMA)?;
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(AvroValue::Record(vec![
        ("content".to_owned(), AvroValue::Int(0)),
        (
            "manifest_path".to_owned(),
            AvroValue::String(MANIFEST.to_owned()),
        ),
    ]))?;
    Ok(writer.into_inner()?)
}

/// The snapshot's objects, with each file in `unreadable` left out so that opening it fails.
fn reader(files: &[FixtureFile], unreadable: &[&str]) -> anyhow::Result<FixtureReader> {
    let mut objects = BTreeMap::from([
        (MANIFEST_LIST.to_owned(), manifest_list()?),
        (MANIFEST.to_owned(), manifest(files)?),
    ]);
    for file in files {
        if !unreadable.contains(&file.path) {
            objects.insert(file.path.to_owned(), parquet(file.pnus)?);
        }
    }
    Ok(FixtureReader { objects })
}

fn snapshot() -> IcebergSnapshotManifestList {
    IcebergSnapshotManifestList {
        table_name: FIXTURE.table_name.to_owned(),
        snapshot_id: 7,
        snapshot_timestamp_ms: 1_777_777_777_000,
        manifest_list_location: MANIFEST_LIST.to_owned(),
        metadata_location: "s3://lakehouse/metadata/00001.json".to_owned(),
    }
}

/// A snapshot written in PNU order: two files with disjoint ranges, a file whose range straddles
/// the shard's lower edge, and a file the writer recorded no statistics for.
const ORDERED: &[FixtureFile] = &[
    FixtureFile {
        path: "s3://lakehouse/gold/fixture/a.parquet",
        pnus: &["9999910100100010000", "9999910100100020000"],
        pnu_statistics: Some(("9999910100100010000", "9999910100100020000", 0)),
    },
    FixtureFile {
        path: "s3://lakehouse/gold/fixture/b.parquet",
        pnus: &["9999911100100010000", "9999911200100010000"],
        pnu_statistics: Some(("9999911100100010000", "9999911200100010000", 0)),
    },
    FixtureFile {
        path: "s3://lakehouse/gold/fixture/c.parquet",
        pnus: &["9999910900100010000", "9999920100100010000"],
        pnu_statistics: Some(("9999910900100010000", "9999920100100010000", 0)),
    },
    FixtureFile {
        path: "s3://lakehouse/gold/fixture/d.parquet",
        pnus: &["9999930100100010000"],
        pnu_statistics: None,
    },
];

fn kept_pnus(scanned: &ScannedRows) -> Vec<&str> {
    scanned
        .rows
        .iter()
        .filter_map(|row| row.get("pnu").and_then(serde_json::Value::as_str))
        .collect()
}

#[tokio::test]
async fn a_shard_skips_only_the_files_whose_bounds_exclude_its_prefix() -> anyhow::Result<()> {
    // Shard 999992: `a` and `b` end below it, so they are left out and reading either fails the
    // scan. `c` straddles the shard's lower edge and `d` has no statistics, so both must be read:
    // skipping `c` would lose the shard's one row, skipping `d` the read count below.
    let reader = reader(ORDERED, &[ORDERED[0].path, ORDERED[1].path])?;

    let scanned = scan_snapshot_rows_kept(
        &FIXTURE,
        &reader,
        &snapshot(),
        |_| true,
        None,
        Some(KeptPrefix::pnu("999992")),
    )
    .await?;

    assert_eq!(kept_pnus(&scanned), vec!["9999920100100010000"]);
    assert_eq!(scanned.data_file_count, 4);
    assert_eq!(scanned.manifest_record_count, 7);
    assert_eq!(scanned.skipped_data_file_count, 2);
    assert_eq!(scanned.skipped_record_count, 4);
    assert_eq!(scanned.read_record_count, 3);
    assert_eq!(scanned.decoded_row_count, 3);
    scanned.ensure_complete()?;
    Ok(())
}

#[tokio::test]
async fn a_file_whose_bounds_overlap_the_prefix_is_read() -> anyhow::Result<()> {
    // Shard 9999910: `a` holds it, `c` starts inside it. Both are served; leaving either
    // unread would lose its row, so a pruning that skipped an overlapping file fails this test.
    let reader = reader(ORDERED, &[ORDERED[1].path])?;

    let scanned = scan_snapshot_rows_kept(
        &FIXTURE,
        &reader,
        &snapshot(),
        |_| true,
        None,
        Some(KeptPrefix::pnu("9999910")),
    )
    .await?;

    assert_eq!(
        kept_pnus(&scanned),
        vec![
            "9999910100100010000",
            "9999910100100020000",
            "9999910900100010000",
        ]
    );
    assert_eq!(scanned.skipped_data_file_count, 1);
    scanned.ensure_complete()?;
    Ok(())
}

#[tokio::test]
async fn without_a_prefix_every_file_is_read() -> anyhow::Result<()> {
    let reader = reader(ORDERED, &[])?;

    let scanned =
        scan_snapshot_rows_kept(&FIXTURE, &reader, &snapshot(), |_| true, None, None).await?;

    assert_eq!(scanned.rows.len(), 7);
    assert_eq!(scanned.skipped_data_file_count, 0);
    scanned.ensure_complete()?;
    Ok(())
}

#[tokio::test]
async fn a_prefix_on_a_column_the_table_lacks_is_refused() -> anyhow::Result<()> {
    let reader = reader(ORDERED, &[])?;
    let refused = scan_snapshot_rows_kept(
        &FIXTURE,
        &reader,
        &snapshot(),
        |_| true,
        None,
        Some(KeptPrefix {
            column: "absent",
            prefix: "9",
        }),
    )
    .await;
    assert!(refused.is_err());
    Ok(())
}

#[test]
fn the_manifest_bounds_are_read_by_column_name() -> anyhow::Result<()> {
    let files = data_files(&manifest(ORDERED)?)?;

    assert_eq!(
        files[0].column_statistics.get("pnu"),
        Some(&ColumnStatistics {
            lower_bound: Some(b"9999910100100010000".to_vec()),
            upper_bound: Some(b"9999910100100020000".to_vec()),
            null_value_count: Some(0),
        })
    );
    assert!(files[3].column_statistics.is_empty());
    Ok(())
}

fn file(lower: Option<&str>, upper: Option<&str>, nulls: Option<u64>) -> ScannedDataFile {
    ScannedDataFile {
        file_path: "s3://lakehouse/gold/fixture/x.parquet".to_owned(),
        record_count: 1,
        file_size_in_bytes: 1,
        column_statistics: BTreeMap::from([(
            "pnu".to_owned(),
            ColumnStatistics {
                lower_bound: lower.map(|value| value.as_bytes().to_vec()),
                upper_bound: upper.map(|value| value.as_bytes().to_vec()),
                null_value_count: nulls,
            },
        )]),
    }
}

#[test]
fn only_bounds_entirely_outside_the_prefix_exclude_a_file() {
    let low = "9999910100100010000";
    let high = "9999910999999999999";
    let cases = [
        // (prefix, excluded)
        ("99999", false),               // the file is inside the prefix
        ("9999910", false),             // equal to the bounds' own prefix
        ("99999105", false),            // strictly inside the range
        ("9999910100100010000", false), // the lower bound itself
        ("9999910999999999999", false), // the upper bound itself
        ("9999909", true),              // wholly below: the lower bound is at its successor
        ("9999911", true),              // wholly above the upper bound
        ("99998", true),
        ("999992", true),
    ];
    for (prefix, excluded) in cases {
        assert_eq!(
            file(Some(low), Some(high), Some(0)).excludes_prefix("pnu", prefix),
            excluded,
            "prefix {prefix}"
        );
    }
}

#[test]
fn a_truncated_bound_still_excludes_only_what_it_proves() {
    // Iceberg truncates string bounds to 16 characters: the lower one is cut, the upper one is
    // cut and rounded up. Both still bound every value.
    let truncated = file(Some("9999910100100010"), Some("9999910999999999:"), Some(0));
    assert!(!truncated.excludes_prefix("pnu", "9999910"));
    assert!(truncated.excludes_prefix("pnu", "9999911"));
    assert!(truncated.excludes_prefix("pnu", "9999909"));
}

#[test]
fn a_file_is_read_unless_every_part_of_the_proof_is_recorded() {
    let low = Some("9999910100100010000");
    let high = Some("9999910999999999999");
    // Each of these would be skipped for prefix 999992 with full statistics.
    assert!(file(low, high, Some(0)).excludes_prefix("pnu", "999992"));
    for unproven in [
        file(low, high, None),
        file(low, high, Some(1)),
        file(None, high, Some(0)),
        file(low, None, Some(0)),
    ] {
        assert!(!unproven.excludes_prefix("pnu", "999992"));
    }
    assert!(!file(low, high, Some(0)).excludes_prefix("other", "999992"));
    assert!(!file(low, high, Some(0)).excludes_prefix("pnu", ""));
}

#[test]
fn a_scan_that_does_not_account_for_every_row_is_refused() {
    let complete = || ScannedRows {
        rows: Vec::new(),
        data_file_count: 3,
        manifest_record_count: 10,
        skipped_data_file_count: 1,
        skipped_record_count: 4,
        read_record_count: 6,
        decoded_row_count: 6,
        keep_limit_exceeded: false,
    };
    assert!(complete().ensure_complete().is_ok());
    let mut short = complete();
    short.decoded_row_count = 5;
    assert!(short.ensure_complete().is_err());
    let mut lost = complete();
    lost.skipped_record_count = 3;
    assert!(lost.ensure_complete().is_err());
    let mut stopped = complete();
    stopped.keep_limit_exceeded = true;
    assert!(stopped.ensure_complete().is_err());
}
