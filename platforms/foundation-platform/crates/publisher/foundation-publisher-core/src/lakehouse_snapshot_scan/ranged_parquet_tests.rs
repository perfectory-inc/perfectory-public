//! A ranged read hands over exactly the rows a whole-file read does, and downloads only the footer
//! and the row groups (or pages) it decodes (root ADR-0319).
//!
//! Synthetic PNUs only: every value starts with the reserved `99999` region.

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use lakehouse_domain::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    file::properties::WriterProperties,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::{
    iceberg_scan::{self, ScannedDataFile},
    ranged_parquet::{self, coalesce, CoalescedRange, RangedDecode, COALESCE_GAP_BYTES},
    LakehouseByteReader,
};

const FIXTURE: LakehouseTableContract = LakehouseTableContract {
    table_name: "gold.fixture_panel",
    layer: LakehouseLayer::Gold,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Projection,
    current_row_predicate: None,
    current_row_latest: None,
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

const LOCATION: &str = "s3://lakehouse/gold/fixture/part-0.parquet";
/// Row groups of the fixture file, one per synthetic 시군구 `999991{group}`.
const ROW_GROUPS: usize = 8;
const ROWS_PER_GROUP: usize = 4_000;
/// Rows to a data page, so the offset index has pages to skip inside a row group.
const ROWS_PER_PAGE: usize = 250;
/// The width of `label`: the payload a document is built from, which the filter on `pnu` keeps
/// from being downloaded for rows outside the shard.
const LABEL_BYTES: usize = 256;

/// A PNU-ordered file of [`ROW_GROUPS`] row groups, row group `g` holding PNUs under `999991{g}`.
fn wide_file() -> anyhow::Result<Vec<u8>> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("pnu", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .set_data_page_row_count_limit(ROWS_PER_PAGE)
        .set_write_batch_size(ROWS_PER_PAGE)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(properties))?;
    for group in 0..ROW_GROUPS {
        let pnus = (0..ROWS_PER_GROUP)
            .map(|row| format!("999991{group}{row:013}"))
            .collect::<Vec<_>>();
        let labels = pnus
            .iter()
            .map(|pnu| format!("{pnu:-<LABEL_BYTES$}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(pnus)) as ArrayRef,
                Arc::new(StringArray::from(labels)) as ArrayRef,
            ],
        )?;
        writer.write(&batch)?;
    }
    Ok(writer.into_inner()?)
}

/// Serves one data file by range only, recording each range.
struct RangeReader {
    bytes: Vec<u8>,
    served: Mutex<Vec<Range<u64>>>,
}

impl RangeReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            served: Mutex::new(Vec::new()),
        }
    }

    fn served(&self) -> Vec<Range<u64>> {
        self.served
            .lock()
            .map(|ranges| ranges.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl LakehouseByteReader for RangeReader {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        anyhow::bail!("a data file was read whole: {location}")
    }

    async fn read_range(&self, location: &str, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(location == LOCATION, "unexpected object read: {location}");
        let bytes = self
            .bytes
            .get(usize::try_from(range.start)?..usize::try_from(range.end)?)
            .ok_or_else(|| anyhow::anyhow!("range {range:?} is past the end of the file"))?
            .to_vec();
        self.served
            .lock()
            .map_err(|_| anyhow::anyhow!("the range log was poisoned"))?
            .push(range);
        Ok(bytes)
    }
}

fn data_file(bytes: &[u8]) -> anyhow::Result<ScannedDataFile> {
    Ok(ScannedDataFile {
        file_path: LOCATION.to_owned(),
        record_count: u64::try_from(ROW_GROUPS * ROWS_PER_GROUP)?,
        file_size_in_bytes: u64::try_from(bytes.len())?,
        column_statistics: BTreeMap::new(),
    })
}

type Rows = Vec<JsonMap<String, JsonValue>>;

/// The rows and counts a whole-file read hands over, taking at most `limit` rows.
fn whole(bytes: &[u8], prefix: Option<&str>, limit: usize) -> anyhow::Result<(Rows, u64, u64)> {
    let mut rows = Vec::new();
    let (decoded, skipped) = iceberg_scan::decode_rows_under(
        &FIXTURE,
        bytes.to_vec(),
        prefix.map(|prefix| ("pnu", prefix)),
        |row| {
            rows.push(row);
            rows.len() < limit
        },
    )?;
    Ok((rows, decoded, skipped))
}

/// The rows and counts a ranged read hands over, taking at most `limit` rows.
async fn ranged(
    reader: &RangeReader,
    prefix: Option<&str>,
    limit: usize,
) -> anyhow::Result<(Rows, RangedDecode)> {
    let mut rows = Vec::new();
    let decode = ranged_parquet::decode_rows_under(
        &FIXTURE,
        reader,
        &data_file(&reader.bytes)?,
        prefix.map(|prefix| ("pnu", prefix)),
        &std::collections::BTreeSet::new(),
        |row| {
            rows.push(row);
            rows.len() < limit
        },
    )
    .await?;
    Ok((rows, decode))
}

#[tokio::test]
async fn a_ranged_read_hands_over_the_rows_a_whole_read_does() -> anyhow::Result<()> {
    let bytes = wide_file()?;
    // No prefix; the whole file under one prefix; one row group; a few pages of one row group; a
    // prefix no row group holds; and every one of them stopped after its first rows.
    let prefixes = [
        None,
        Some("99999"),
        Some("9999913"),
        Some("99999150000000001"),
        Some("999992"),
    ];
    for prefix in prefixes {
        for limit in [usize::MAX, 3] {
            let reader = RangeReader::new(bytes.clone());
            let (expected, decoded, skipped) = whole(&bytes, prefix, limit)?;
            let (rows, decode) = ranged(&reader, prefix, limit).await?;
            assert_eq!(rows, expected, "prefix {prefix:?}, limit {limit}");
            assert_eq!(
                (decode.decoded, decode.skipped),
                (decoded, skipped),
                "prefix {prefix:?}, limit {limit}"
            );
            assert_eq!(
                decode.fetched_bytes,
                reader
                    .served()
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum::<u64>()
            );
        }
    }
    Ok(())
}

/// The byte range of row group `group`'s column chunks.
fn row_group_range(bytes: &[u8], group: usize) -> anyhow::Result<Range<u64>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))?;
    let row_group = builder.metadata().row_group(group);
    let (start, end) = row_group.columns().iter().try_fold(
        (u64::MAX, 0_u64),
        |(start, end), column| -> anyhow::Result<(u64, u64)> {
            let (offset, length) = column.byte_range();
            Ok((start.min(offset), end.max(offset + length)))
        },
    )?;
    Ok(start..end)
}

#[tokio::test]
async fn a_shard_downloads_its_footer_and_its_own_row_group_only() -> anyhow::Result<()> {
    let bytes = wide_file()?;
    let file = u64::try_from(bytes.len())?;
    let tail = file.saturating_sub(ranged_parquet::FOOTER_PREFETCH_BYTES)..file;
    let group = row_group_range(&bytes, 3)?;
    assert!(
        group.end < tail.start,
        "the fixture's row group 3 must lie outside the footer prefetch"
    );

    let reader = RangeReader::new(bytes.clone());
    let (rows, decode) = ranged(&reader, Some("9999913"), usize::MAX).await?;
    assert_eq!(rows.len(), ROWS_PER_GROUP);
    // Every byte read lies in the footer prefetch or in row group 3, and the read is a fraction
    // of the file: the footer prefetch and one row group of eight.
    for range in reader.served() {
        assert!(
            (range.start >= tail.start && range.end <= tail.end)
                || (range.start >= group.start && range.end <= group.end),
            "read {range:?} outside the footer {tail:?} and row group 3 {group:?}"
        );
    }
    assert!(decode.fetched_bytes <= (tail.end - tail.start) + (group.end - group.start));
    assert!(
        decode.fetched_bytes * 4 < file,
        "fetched {} of {file} bytes",
        decode.fetched_bytes
    );
    Ok(())
}

#[tokio::test]
async fn inside_a_row_group_only_the_pages_holding_the_kept_rows_are_downloaded(
) -> anyhow::Result<()> {
    // `99999130000000000` holds rows 0..999 of row group 3's 4000: its `pnu` chunk is read whole to
    // filter, but `label` only for the pages of the rows kept, by the file's offset index.
    let bytes = wide_file()?;
    let file = u64::try_from(bytes.len())?;
    let tail = file.saturating_sub(ranged_parquet::FOOTER_PREFETCH_BYTES)..file;
    let group = row_group_range(&bytes, 3)?;

    let reader = RangeReader::new(bytes.clone());
    let (rows, _) = ranged(&reader, Some("99999130000000000"), usize::MAX).await?;
    assert_eq!(rows.len(), 1_000);
    let in_group = reader
        .served()
        .iter()
        .filter(|range| range.start >= group.start && range.end <= group.end)
        .map(|range| range.end - range.start)
        .sum::<u64>();
    assert!(
        in_group * 2 < group.end - group.start,
        "read {in_group} of row group 3's {} bytes for a quarter of its rows",
        group.end - group.start
    );
    assert!(reader
        .served()
        .iter()
        .all(|range| range.start >= tail.start || range.end <= group.end));
    Ok(())
}

#[test]
fn ranges_within_the_gap_are_one_request_and_far_ones_are_not() {
    let far = 10 * COALESCE_GAP_BYTES;
    let wanted = [far..far + 10, 0..100, 150..200, 90..120, 5..5];
    assert_eq!(
        coalesce(&wanted),
        vec![
            CoalescedRange {
                range: 0..200,
                members: vec![1, 3, 2],
            },
            CoalescedRange {
                range: far..far + 10,
                members: vec![0],
            },
        ]
    );
}
