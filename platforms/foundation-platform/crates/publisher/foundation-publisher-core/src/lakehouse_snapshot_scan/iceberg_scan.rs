//! Row scan of one Iceberg snapshot: manifest list -> manifests -> Parquet data files.
//!
//! The Iceberg REST catalog answers which snapshot is current and where its manifest list lives.
//! Everything below that is object reads, so this module stays byte-oriented: it takes the Avro and
//! Parquet bytes an object reader produced and returns rows shaped by the Lakehouse table contract.
//! Keeping the column set on `LakehouseTableContract` means the projection cannot silently disagree
//! with the canonical table it reads.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{bail, ensure, Context};
use apache_avro::{types::Value as AvroValue, Reader as AvroReader};
use arrow_array::{
    cast::AsArray, Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, SecondsFormat, TimeDelta, Utc};
use lakehouse_domain::LakehouseTableContract;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderBuilder, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection,
    RowSelector,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Iceberg `manifest_file.content` / `data_file.content` value for row data.
const CONTENT_DATA: i64 = 0;
/// Iceberg `manifest_file.content` value for delete files, and `data_file.content` value for
/// position deletes.
const CONTENT_POSITION_DELETES: i64 = 1;
/// Iceberg `manifest_entry.status` value for an entry removed by this snapshot.
const STATUS_DELETED: i64 = 2;
const PARQUET_FILE_FORMAT: &str = "PARQUET";

/// Avro header key under which Iceberg writes the table schema a manifest was written with.
const MANIFEST_SCHEMA_KEY: &str = "schema";

/// One Parquet data file reachable from the scanned snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedDataFile {
    /// Storage location the manifest recorded for the data file.
    pub file_path: String,
    /// Row count the manifest recorded for the data file.
    pub record_count: u64,
    /// Compressed byte size the manifest recorded for the data file.
    pub file_size_in_bytes: u64,
    /// What the manifest recorded per top-level column, by column name. A column the manifest
    /// recorded nothing for, or whose field id the manifest's schema does not name, is absent.
    pub column_statistics: BTreeMap<String, ColumnStatistics>,
}

/// What a manifest recorded about one column of one data file.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnStatistics {
    /// Iceberg single-value bytes no non-null value of the column is below (UTF-8 for a string).
    /// Iceberg may truncate it, which only lowers it.
    pub lower_bound: Option<Vec<u8>>,
    /// Bytes no non-null value of the column is above. A truncated upper bound is rounded up.
    pub upper_bound: Option<Vec<u8>>,
    /// Rows whose value is null; a null has no place between the bounds.
    pub null_value_count: Option<u64>,
}

impl ScannedDataFile {
    /// Whether the manifest proves that no row of this file has a `column` value starting with
    /// `prefix`, so a scan that keeps only such rows may skip the file without opening it.
    ///
    /// The proof needs every part: a recorded null count of zero (a null row lies outside the
    /// bounds, and a scan must still meet it to refuse it), a lower and an upper bound, and the
    /// prefix's whole range — `prefix` up to, not including, its successor — outside them. Any
    /// part missing means the file is read (root ADR-0164).
    ///
    /// The prefix must be ASCII. Iceberg orders strings by UTF-16 code unit and the bounds are
    /// UTF-8 bytes; the two orders agree whenever one side of a comparison is ASCII, which is what
    /// lets the byte comparison below stand in for Iceberg's.
    pub fn excludes_prefix(&self, column: &str, prefix: &str) -> bool {
        if prefix.is_empty() || !prefix.is_ascii() {
            return false;
        }
        let Some(statistics) = self.column_statistics.get(column) else {
            return false;
        };
        bounds_exclude_prefix(
            statistics.null_value_count,
            statistics.lower_bound.as_deref(),
            statistics.upper_bound.as_deref(),
            prefix,
        )
    }
}

/// Whether values with these bounds hold none starting with the ASCII `prefix`: a null count of
/// zero, both bounds, and the prefix's whole range outside them (see
/// [`ScannedDataFile::excludes_prefix`]). A data file's manifest bounds and a Parquet row group's
/// statistics are both read this way.
pub(super) fn bounds_exclude_prefix(
    null_count: Option<u64>,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    prefix: &str,
) -> bool {
    if prefix.is_empty() || !prefix.is_ascii() || null_count != Some(0) {
        return false;
    }
    let (Some(lower), Some(upper)) = (lower, upper) else {
        return false;
    };
    let prefix = prefix.as_bytes();
    // Every value is at most `upper`, and every value starting with `prefix` is at least
    // `prefix`.
    if upper < prefix {
        return true;
    }
    // Every value is at least `lower`, and every value starting with `prefix` is below its
    // successor.
    prefix_successor(prefix).is_some_and(|successor| lower >= successor.as_slice())
}

/// The least byte string above every string that starts with the ASCII `prefix`: the prefix with
/// its last byte raised by one. None when that byte is the last ASCII one.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let (last, head) = prefix.split_last()?;
    (*last < 0x7f).then(|| {
        let mut successor = head.to_vec();
        successor.push(last + 1);
        successor
    })
}

/// The manifests a snapshot's manifest list points at: those of data files, and those of delete
/// files (a merge-on-read commit's, root ADR-0344).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManifestLocations {
    /// Manifests listing data files.
    pub data: Vec<String>,
    /// Manifests listing delete files.
    pub deletes: Vec<String>,
}

/// Returns the manifest locations a snapshot's manifest list points at.
pub fn manifest_locations(manifest_list_avro: &[u8]) -> anyhow::Result<ManifestLocations> {
    let mut locations = ManifestLocations::default();
    for record in
        avro_records(manifest_list_avro).context("failed to read Iceberg manifest list")?
    {
        let path = string_field(&record, "manifest_path")?;
        match optional_integer_field(&record, "content")?.unwrap_or(CONTENT_DATA) {
            CONTENT_DATA => locations.data.push(path),
            CONTENT_POSITION_DELETES => locations.deletes.push(path),
            other => bail!(
                "Iceberg manifest {path} has content {other}, which is neither data nor deletes"
            ),
        }
    }
    Ok(locations)
}

/// One position delete file reachable from the scanned snapshot: each of its rows names a data
/// file and a row position in it that the snapshot no longer holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedDeleteFile {
    /// Storage location the manifest recorded for the delete file.
    pub file_path: String,
    /// Positions the delete file holds, as its manifest recorded.
    pub record_count: u64,
}

/// Returns the live position delete files one delete manifest points at.
///
/// Equality deletes and deletion vectors are refused, never skipped: a scan that passed over them
/// would serve rows the table no longer holds. The Gold tables are format v2 and their only writer
/// of deletes is Spark's merge-on-read `MERGE`, which writes position deletes (root ADR-0344).
pub fn delete_files(manifest_avro: &[u8]) -> anyhow::Result<Vec<ScannedDeleteFile>> {
    let (_, entries) = avro_container(manifest_avro).context("failed to read Iceberg manifest")?;
    let mut delete_files = Vec::new();
    for entry in entries {
        let status = optional_integer_field(&entry, "status")?.unwrap_or(CONTENT_DATA);
        if status == STATUS_DELETED {
            continue;
        }
        let file = resolve(
            field(&entry, "data_file")
                .context("Iceberg manifest entry is missing its data_file record")?,
        );
        let file_path = string_field(file, "file_path")?;
        let content = optional_integer_field(file, "content")?.unwrap_or(CONTENT_DATA);
        ensure!(
            content == CONTENT_POSITION_DELETES,
            "Iceberg delete file {file_path} has content {content}; this scan applies position \
             deletes only (equality deletes are refused, root ADR-0344)"
        );
        let file_format = string_field(file, "file_format")?;
        ensure!(
            file_format.eq_ignore_ascii_case(PARQUET_FILE_FORMAT),
            "Iceberg delete file {file_path} is {file_format}; this scan reads Parquet position \
             deletes only (deletion vectors are refused, root ADR-0344)"
        );
        let record_count = optional_integer_field(file, "record_count")?
            .context("Iceberg delete file is missing its record_count")?;
        delete_files.push(ScannedDeleteFile {
            file_path,
            record_count: u64::try_from(record_count)
                .context("Iceberg delete file record_count must not be negative")?,
        });
    }
    Ok(delete_files)
}

/// Adds the positions one position delete file removes from the data files in `positions` (keyed
/// by data file path) to that file's set. A row naming a data file outside `positions` is dropped:
/// it is a delete of a file the snapshot no longer holds (its rows were rewritten elsewhere).
///
/// A data file is named by its path alone. The Iceberg spec also asks that the data file's
/// sequence number not exceed the delete file's; that only tells apart two files at one path, and
/// Iceberg's writers never reuse a path.
pub fn add_position_deletes(
    parquet_bytes: Vec<u8>,
    positions: &mut HashMap<String, BTreeSet<u64>>,
) -> anyhow::Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
        .context("failed to open an Iceberg position delete file")?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), ["file_path", "pos"]);
    let reader = builder
        .with_projection(mask)
        .build()
        .context("failed to build the position delete reader")?;
    for batch in reader {
        let batch = batch.context("failed to read a position delete record batch")?;
        let paths = batch
            .column_by_name("file_path")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .context("a position delete file has no string file_path column")?;
        let rows = batch
            .column_by_name("pos")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .context("a position delete file has no 64-bit pos column")?;
        ensure!(
            paths.null_count() == 0 && rows.null_count() == 0,
            "a position delete file has a row without its file_path or pos"
        );
        for index in 0..batch.num_rows() {
            if let Some(set) = positions.get_mut(paths.value(index)) {
                set.insert(
                    u64::try_from(rows.value(index))
                        .context("a position delete names a negative position")?,
                );
            }
        }
    }
    Ok(())
}

/// Returns the live Parquet data files one manifest points at.
pub fn data_files(manifest_avro: &[u8]) -> anyhow::Result<Vec<ScannedDataFile>> {
    let (header, entries) =
        avro_container(manifest_avro).context("failed to read Iceberg manifest")?;
    let column_names = manifest_column_names(&header);
    let mut data_files = Vec::new();
    for entry in entries {
        let status = optional_integer_field(&entry, "status")?.unwrap_or(CONTENT_DATA);
        if status == STATUS_DELETED {
            continue;
        }
        let data_file = resolve(
            field(&entry, "data_file")
                .context("Iceberg manifest entry is missing its data_file record")?,
        );
        let content = optional_integer_field(data_file, "content")?.unwrap_or(CONTENT_DATA);
        ensure!(
            content == CONTENT_DATA,
            "Iceberg manifest carries a delete file; this scan reads row data only"
        );
        let file_format = string_field(data_file, "file_format")?;
        ensure!(
            file_format.eq_ignore_ascii_case(PARQUET_FILE_FORMAT),
            "Iceberg data file format {file_format} is not supported by this scan"
        );
        let record_count = optional_integer_field(data_file, "record_count")?
            .context("Iceberg data file is missing its record_count")?;
        let file_size_in_bytes = optional_integer_field(data_file, "file_size_in_bytes")?
            .context("Iceberg data file is missing its file_size_in_bytes")?;
        data_files.push(ScannedDataFile {
            file_path: string_field(data_file, "file_path")?,
            record_count: u64::try_from(record_count)
                .context("Iceberg data file record_count must not be negative")?,
            file_size_in_bytes: u64::try_from(file_size_in_bytes)
                .context("Iceberg data file file_size_in_bytes must not be negative")?,
            column_statistics: column_statistics(data_file, &column_names),
        });
    }
    Ok(data_files)
}

/// Top-level column names by Iceberg field id, from the table schema the manifest's Avro header
/// carries. A header without a readable schema names nothing, and no file is then skipped.
fn manifest_column_names(header: &HashMap<String, Vec<u8>>) -> BTreeMap<i64, String> {
    let Some(schema) = header
        .get(MANIFEST_SCHEMA_KEY)
        .and_then(|bytes| serde_json::from_slice::<JsonValue>(bytes).ok())
    else {
        return BTreeMap::new();
    };
    schema
        .get("fields")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(|field| {
            Some((
                field.get("id")?.as_i64()?,
                field.get("name")?.as_str()?.to_owned(),
            ))
        })
        .collect()
}

/// The bounds and null counts a manifest recorded for one data file, by column name.
///
/// Iceberg writes each of them as an Avro array of `{key: field id, value}` records. A shape this
/// does not recognise records nothing for that column: the file is then read, never skipped.
fn column_statistics(
    data_file: &AvroValue,
    column_names: &BTreeMap<i64, String>,
) -> BTreeMap<String, ColumnStatistics> {
    let mut statistics = BTreeMap::<String, ColumnStatistics>::new();
    for (field_name, set) in [
        (
            "lower_bounds",
            set_lower_bound as fn(&mut ColumnStatistics, &AvroValue),
        ),
        ("upper_bounds", set_upper_bound),
        ("null_value_counts", set_null_value_count),
    ] {
        let Some(AvroValue::Array(entries)) = field(data_file, field_name).map(resolve) else {
            continue;
        };
        for entry in entries {
            let Ok(Some(id)) = optional_integer_field(entry, "key") else {
                continue;
            };
            if let (Some(name), Some(value)) = (column_names.get(&id), field(entry, "value")) {
                set(statistics.entry(name.clone()).or_default(), resolve(value));
            }
        }
    }
    statistics
}

fn set_lower_bound(column: &mut ColumnStatistics, value: &AvroValue) {
    if let AvroValue::Bytes(bytes) = value {
        column.lower_bound = Some(bytes.clone());
    }
}

fn set_upper_bound(column: &mut ColumnStatistics, value: &AvroValue) {
    if let AvroValue::Bytes(bytes) = value {
        column.upper_bound = Some(bytes.clone());
    }
}

fn set_null_value_count(column: &mut ColumnStatistics, value: &AvroValue) {
    column.null_value_count = match value {
        AvroValue::Long(count) => u64::try_from(*count).ok(),
        AvroValue::Int(count) => u64::try_from(*count).ok(),
        _ => None,
    };
}

/// Decodes one Parquet data file into contract-shaped rows, handing each to `visit` as it decodes.
///
/// A row `visit` does not keep is dropped before the next one decodes: what a scan holds is what
/// it keeps plus one record batch, never the whole file. A `gold.building_panel` data file of
/// 900 thousand rows decoded whole took a 47 thousand row patch past its 14G cap six times on
/// 2026-10-11. `visit` answers whether to go on; the decoding stops at the first `false`.
/// Returns the number of rows decoded.
pub fn decode_rows(
    contract: &LakehouseTableContract,
    parquet_bytes: Vec<u8>,
    visit: impl FnMut(JsonMap<String, JsonValue>) -> bool,
) -> anyhow::Result<u64> {
    decode_rows_under(contract, parquet_bytes, None, visit).map(|(decoded, _)| decoded)
}

/// [`decode_rows`] for a scan that keeps only `column` values under `prefix`. A row group whose
/// Parquet statistics prove it holds none is not read, and in the rest only `column` is decoded for
/// every row: the other columns, and the row's JSON, only for the rows under `prefix` (a row with
/// no value is kept for the keep-filter, as [`KeptPrefix`](super::KeptPrefix) does). On 2026-10-14
/// a parcel kind bake ran 775 shards, 72% of them over prefixes with no row, each decoding every
/// row of a 3.6-million-row file into JSON to keep none. Returns the rows handed to `visit` and the
/// rows of the file it never decoded whole; together they are the file's rows (its footer's count)
/// when the decoding ran to the end.
pub(super) fn decode_rows_under(
    contract: &LakehouseTableContract,
    parquet_bytes: Vec<u8>,
    prefix: Option<(&str, &str)>,
    mut visit: impl FnMut(JsonMap<String, JsonValue>) -> bool,
) -> anyhow::Result<(u64, u64)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
        .context("failed to open Iceberg Parquet data file")?;
    let file_rows = file_row_count(builder.metadata())?;
    let (builder, group_skipped) = keep_prefix(builder, prefix, &BTreeSet::new())?;
    let reader = builder
        .build()
        .context("failed to build the Iceberg Parquet reader")?;

    let mut decoded: u64 = 0;
    for batch in reader {
        let batch = batch.context("failed to read an Iceberg Parquet record batch")?;
        if !visit_batch(contract, &batch, &mut decoded, &mut visit)? {
            return Ok((decoded, group_skipped));
        }
    }
    Ok((decoded, rows_not_handed_over(file_rows, decoded)?))
}

/// The row count a Parquet file's footer declares.
pub(super) fn file_row_count(metadata: &ParquetMetaData) -> anyhow::Result<u64> {
    u64::try_from(metadata.file_metadata().num_rows())
        .context("a Parquet file declares a negative row count")
}

/// The rows of a file decoded to its end that were never handed over: skipped by their row group,
/// removed by a position delete, or filtered out under the prefix.
pub(super) fn rows_not_handed_over(file_rows: u64, decoded: u64) -> anyhow::Result<u64> {
    file_rows
        .checked_sub(decoded)
        .context("decoded more rows than the Parquet file declares")
}

/// Narrows a Parquet reader to `column` values under `prefix`: the row groups whose statistics
/// prove they hold none are left out, and the rest are filtered on `column` alone before any other
/// column decodes. The file positions in `deleted` (its position deletes, root ADR-0344) are never
/// decoded. Returns the narrowed builder and the rows of the row groups left out. Shared by the
/// in-memory reader and the ranged one ([`super::ranged_parquet`]), so both keep the same rows
/// whatever bytes they were handed.
pub(super) fn keep_prefix<T>(
    builder: ArrowReaderBuilder<T>,
    prefix: Option<(&str, &str)>,
    deleted: &BTreeSet<u64>,
) -> anyhow::Result<(ArrowReaderBuilder<T>, u64)> {
    let leaf = prefix.and_then(|(column, _)| {
        builder
            .parquet_schema()
            .columns()
            .iter()
            .position(|leaf| leaf.path().parts() == [column])
    });
    let mut group_skipped: u64 = 0;
    let mut read = Vec::new();
    let mut start: u64 = 0;
    for (index, group) in builder.metadata().row_groups().iter().enumerate() {
        let rows = u64::try_from(group.num_rows())?;
        if leaf.is_some_and(|leaf| {
            prefix.is_some_and(|(_, prefix)| row_group_excludes(group.column(leaf), prefix))
        }) {
            group_skipped = group_skipped
                .checked_add(rows)
                .context("skipped row count overflow")?;
        } else {
            read.push((index, start, rows));
        }
        start = start.checked_add(rows).context("row position overflow")?;
    }
    ensure!(
        deleted.last().is_none_or(|&last| last < start),
        "a position delete names row {:?} of a file of {start} rows",
        deleted.last()
    );
    let builder = if deleted.is_empty() {
        builder
    } else {
        builder.with_row_selection(live_rows(&read, deleted)?)
    };
    let builder = builder.with_row_groups(read.iter().map(|&(index, ..)| index).collect());
    let Some((_, prefix)) = prefix else {
        return Ok((builder, group_skipped));
    };
    let builder = match leaf {
        Some(leaf) => {
            let mask = ProjectionMask::leaves(builder.parquet_schema(), [leaf]);
            let prefix = prefix.to_owned();
            let under = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                Ok(starts_with(batch.column(0), &prefix))
            });
            builder.with_row_filter(RowFilter::new(vec![Box::new(under)]))
        }
        None => builder,
    };
    Ok((builder, group_skipped))
}

/// The rows to decode of the row groups `read` (`(index, first row's file position, rows)`, in file
/// order): all but the file positions `deleted`. Arrow applies a selection to the selected row
/// groups' rows only, so positions in groups left out are not in it.
pub(super) fn live_rows(
    read: &[(usize, u64, u64)],
    deleted: &BTreeSet<u64>,
) -> anyhow::Result<RowSelection> {
    let mut selectors = Vec::new();
    for &(_, start, rows) in read {
        let end = start + rows;
        let mut at = start;
        for &position in deleted.range(start..end) {
            if position > at {
                selectors.push(RowSelector::select(usize::try_from(position - at)?));
            }
            selectors.push(RowSelector::skip(1));
            at = position + 1;
        }
        if end > at {
            selectors.push(RowSelector::select(usize::try_from(end - at)?));
        }
    }
    Ok(RowSelection::from(selectors))
}

/// Hands each row of one decoded batch to `visit`, counting it in `decoded`. Returns whether to go
/// on: false once `visit` answered false.
pub(super) fn visit_batch(
    contract: &LakehouseTableContract,
    batch: &RecordBatch,
    decoded: &mut u64,
    visit: &mut impl FnMut(JsonMap<String, JsonValue>) -> bool,
) -> anyhow::Result<bool> {
    assert_batch_matches_contract(contract, batch)?;
    for index in 0..batch.num_rows() {
        let row = decode_row(contract, batch, index)?;
        *decoded = decoded
            .checked_add(1)
            .context("decoded row count overflow")?;
        if !visit(row) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The filter mask of one string column: values starting with `prefix`, and nulls (kept so the
/// contract check still meets them). A column of another type keeps every row.
fn starts_with(values: &dyn Array, prefix: &str) -> BooleanArray {
    let keep = |value: Option<&str>| Some(value.is_none_or(|value| value.starts_with(prefix)));
    if let Some(strings) = values.as_string_opt::<i32>() {
        strings.iter().map(keep).collect()
    } else if let Some(strings) = values.as_string_opt::<i64>() {
        strings.iter().map(keep).collect()
    } else if let Some(strings) = values.as_string_view_opt() {
        strings.iter().map(keep).collect()
    } else {
        BooleanArray::from(vec![true; values.len()])
    }
}

/// Whether one row group's statistics for the column prove none of its values starts with
/// `prefix`. `min_value` and `max_value` bound the values whether or not the writer marked them
/// exact (Parquet format: a truncated minimum is still a lower bound, a truncated maximum still an
/// upper one; parquet-rs reads an unmarked writer's as inexact). The deprecated `min`/`max`, whose
/// byte order older writers got wrong, are not proof.
fn row_group_excludes(column: &parquet::file::metadata::ColumnChunkMetaData, prefix: &str) -> bool {
    let Some(statistics) = column.statistics() else {
        return false;
    };
    if statistics.is_min_max_deprecated() {
        return false;
    }
    bounds_exclude_prefix(
        statistics.null_count_opt(),
        statistics.min_bytes_opt(),
        statistics.max_bytes_opt(),
        prefix,
    )
}

/// Refuses a data file the table contract cannot read: a column the contract does not name, or a
/// required column the file lacks. A column the contract added after the file was written (Iceberg
/// schema evolution: `ALTER TABLE ... ADD COLUMNS`, `evolve_iceberg_table_to_contract`) is absent from
/// the older files and reads as null there, as Iceberg itself reads it. On 2026-10-15 refusing it
/// stopped every VWorld land Silver refresh: `reference.legal_dong_code_snapshot` gained six columns
/// and its files written before kept the first six.
fn assert_batch_matches_contract(
    contract: &LakehouseTableContract,
    batch: &RecordBatch,
) -> anyhow::Result<()> {
    let schema = batch.schema();
    let mut actual = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = contract
        .columns
        .iter()
        .map(|column| column.name.to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    let unknown = actual
        .iter()
        .filter(|name| !expected.contains(name))
        .collect::<Vec<_>>();
    let missing_required = contract
        .columns
        .iter()
        .filter(|column| column.required && schema.field_with_name(column.name).is_err())
        .map(|column| column.name)
        .collect::<Vec<_>>();
    ensure!(
        unknown.is_empty() && missing_required.is_empty(),
        "{} data file columns {actual:?} do not match the table contract {expected:?}: unknown {unknown:?}, required but absent {missing_required:?}",
        contract.table_name
    );
    Ok(())
}

fn decode_row(
    contract: &LakehouseTableContract,
    batch: &RecordBatch,
    index: usize,
) -> anyhow::Result<JsonMap<String, JsonValue>> {
    let mut row = JsonMap::new();
    for column in contract.columns {
        // A column added to the table after this file was written reads as null (see
        // `assert_batch_matches_contract`, which already refused a required one).
        let value = match batch.column_by_name(column.name) {
            Some(array) => decode_value(column.logical_type, column.name, array.as_ref(), index)?,
            None => JsonValue::Null,
        };
        ensure!(
            !(column.required && value.is_null()),
            "{} column {} is required by the table contract but the data file row is null",
            contract.table_name,
            column.name
        );
        row.insert(column.name.to_owned(), value);
    }
    Ok(row)
}

fn decode_value(
    logical_type: &str,
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    if array.is_null(index) {
        return Ok(JsonValue::Null);
    }
    match logical_type {
        "string" => decode_string(column_name, array, index),
        "long" => decode_long(column_name, array, index),
        "date" => decode_date(column_name, array, index),
        "timestamp" => decode_timestamp(column_name, array, index),
        decimal if decimal.starts_with("decimal(") => {
            decode_decimal(decimal, column_name, array, index)
        }
        other => bail!("column {column_name} has unsupported contract type {other}"),
    }
}

fn decode_string(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {column_name} is not a Parquet string column"))?;
    Ok(JsonValue::String(values.value(index).to_owned()))
}

fn decode_long(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {column_name} is not a Parquet 64-bit integer column"))?;
    Ok(JsonValue::Number(values.value(index).into()))
}

/// Renders an Iceberg `date` as an ISO-8601 calendar date.
///
/// Iceberg stores a date as days since the epoch, which is a number a JSON reader would have to
/// know the encoding of to interpret. The scan hands on `YYYY-MM-DD` for the same reason it hands
/// on decimals as exact text: the reader should not need the storage encoding to read the value.
fn decode_date(column_name: &str, array: &dyn Array, index: usize) -> anyhow::Result<JsonValue> {
    match array.data_type() {
        DataType::Date32 => {}
        other => bail!("column {column_name} is a {other} rather than a 32-bit date"),
    }
    let values = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .with_context(|| format!("column {column_name} is not a Parquet date column"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).context("1970-01-01 must parse as a date")?;
    let date = epoch
        .checked_add_signed(TimeDelta::days(i64::from(values.value(index))))
        .with_context(|| format!("column {column_name} carries an out-of-range date"))?;
    Ok(JsonValue::String(date.format("%Y-%m-%d").to_string()))
}

fn decode_timestamp(
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {}
        other => bail!("column {column_name} is a {other} rather than a microsecond timestamp"),
    }
    let values = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .with_context(|| format!("column {column_name} is not a Parquet timestamp column"))?;
    let timestamp: DateTime<Utc> = DateTime::from_timestamp_micros(values.value(index))
        .with_context(|| format!("column {column_name} carries an out-of-range timestamp"))?;
    Ok(JsonValue::String(
        timestamp.to_rfc3339_opts(SecondsFormat::Secs, true),
    ))
}

fn decode_decimal(
    logical_type: &str,
    column_name: &str,
    array: &dyn Array,
    index: usize,
) -> anyhow::Result<JsonValue> {
    let (precision, scale) = parse_decimal_type(logical_type)
        .with_context(|| format!("column {column_name} has an unreadable decimal contract type"))?;
    match *array.data_type() {
        DataType::Decimal128(actual_precision, actual_scale)
            if actual_precision == precision && i64::from(actual_scale) == i64::from(scale) => {}
        ref other => {
            bail!("column {column_name} is a {other} rather than the contract's {logical_type}")
        }
    }
    let values = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .with_context(|| format!("column {column_name} is not a Parquet decimal column"))?;
    Ok(JsonValue::String(format_decimal(
        values.value(index),
        u32::from(scale.unsigned_abs()),
    )))
}

fn parse_decimal_type(logical_type: &str) -> Option<(u8, i8)> {
    let arguments = logical_type
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))?;
    let (precision, scale) = arguments.split_once(',')?;
    Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?))
}

/// Renders a decimal as its exact base-10 text, so no float ever touches a published area.
fn format_decimal(value: i128, scale: u32) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    if scale == 0 {
        return format!("{sign}{magnitude}");
    }
    let divisor = 10_u128.pow(scale);
    let integer = magnitude / divisor;
    let fraction = magnitude % divisor;
    let width = scale as usize;
    format!("{sign}{integer}.{fraction:0width$}")
}

fn avro_records(bytes: &[u8]) -> anyhow::Result<Vec<AvroValue>> {
    Ok(avro_container(bytes)?.1)
}

/// The Avro container's user metadata, and its records.
fn avro_container(bytes: &[u8]) -> anyhow::Result<(HashMap<String, Vec<u8>>, Vec<AvroValue>)> {
    let reader = AvroReader::new(bytes).context("failed to open the Avro container")?;
    let header = reader.user_metadata().clone();
    let records = reader
        .collect::<Result<Vec<_>, _>>()
        .context("failed to decode an Avro record")?;
    Ok((header, records))
}

fn field<'a>(record: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match resolve(record) {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value),
        _ => None,
    }
}

/// Unwraps Avro unions so callers always see the resolved branch.
fn resolve(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => resolve(inner),
        other => other,
    }
}

fn string_field(record: &AvroValue, name: &str) -> anyhow::Result<String> {
    match field(record, name).map(resolve) {
        Some(AvroValue::String(value)) => Ok(value.clone()),
        Some(other) => bail!("Avro field {name} is {other:?} rather than a string"),
        None => bail!("Avro record is missing field {name}"),
    }
}

fn optional_integer_field(record: &AvroValue, name: &str) -> anyhow::Result<Option<i64>> {
    match field(record, name).map(resolve) {
        Some(AvroValue::Int(value)) => Ok(Some(i64::from(*value))),
        Some(AvroValue::Long(value)) => Ok(Some(*value)),
        Some(AvroValue::Null) | None => Ok(None),
        Some(other) => bail!("Avro field {name} is {other:?} rather than an integer"),
    }
}

#[cfg(test)]
mod tests {
    use super::{data_files, delete_files, format_decimal, parse_decimal_type, resolve};
    use apache_avro::{types::Value as AvroValue, Schema, Writer};

    #[test]
    fn data_files_read_manifest_record_count_and_size_without_opening_parquet() -> anyhow::Result<()>
    {
        let schema = Schema::parse_str(
            r#"{
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "long"},
                {"name": "data_file", "type": {
                  "type": "record",
                  "name": "data_file",
                  "fields": [
                    {"name": "content", "type": "long"},
                    {"name": "file_path", "type": "string"},
                    {"name": "file_format", "type": "string"},
                    {"name": "record_count", "type": "long"},
                    {"name": "file_size_in_bytes", "type": "long"}
                  ]
                }}
              ]
            }"#,
        )?;
        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Long(1)),
            (
                "data_file".to_owned(),
                AvroValue::Record(vec![
                    ("content".to_owned(), AvroValue::Long(0)),
                    (
                        "file_path".to_owned(),
                        AvroValue::String("s3://lakehouse/silver/table/part-0.parquet".to_owned()),
                    ),
                    (
                        "file_format".to_owned(),
                        AvroValue::String("PARQUET".to_owned()),
                    ),
                    ("record_count".to_owned(), AvroValue::Long(37)),
                    ("file_size_in_bytes".to_owned(), AvroValue::Long(4_096)),
                ]),
            ),
        ]))?;
        let bytes = writer.into_inner()?;

        let files = data_files(&bytes)?;

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 37);
        assert_eq!(files[0].file_size_in_bytes, 4_096);
        Ok(())
    }

    /// One delete manifest entry of `content` and `format`.
    fn delete_manifest(content: i64, format: &str) -> anyhow::Result<Vec<u8>> {
        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "manifest_entry", "fields": [
                {"name": "status", "type": "long"},
                {"name": "data_file", "type": {"type": "record", "name": "data_file", "fields": [
                  {"name": "content", "type": "long"},
                  {"name": "file_path", "type": "string"},
                  {"name": "file_format", "type": "string"},
                  {"name": "record_count", "type": "long"}]}}]}"#,
        )?;
        let mut writer = Writer::new(&schema, Vec::new());
        writer.append(AvroValue::Record(vec![
            ("status".to_owned(), AvroValue::Long(1)),
            (
                "data_file".to_owned(),
                AvroValue::Record(vec![
                    ("content".to_owned(), AvroValue::Long(content)),
                    (
                        "file_path".to_owned(),
                        AvroValue::String("s3://lakehouse/gold/t/d.parquet".to_owned()),
                    ),
                    (
                        "file_format".to_owned(),
                        AvroValue::String(format.to_owned()),
                    ),
                    ("record_count".to_owned(), AvroValue::Long(3)),
                ]),
            ),
        ]))?;
        Ok(writer.into_inner()?)
    }

    #[test]
    fn only_parquet_position_deletes_are_applied_and_any_other_delete_is_refused(
    ) -> anyhow::Result<()> {
        let files = delete_files(&delete_manifest(1, "PARQUET")?)?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].record_count, 3);
        // An equality delete, and a v3 deletion vector (Puffin): refused, never passed over.
        assert!(delete_files(&delete_manifest(2, "PARQUET")?).is_err());
        assert!(delete_files(&delete_manifest(1, "PUFFIN")?).is_err());
        Ok(())
    }

    #[test]
    fn deleted_positions_are_skipped_only_inside_the_row_groups_read() -> anyhow::Result<()> {
        use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
        // Two groups read: rows 0..4 and 10..13 of the file; 5 lies in a group not read.
        let deleted = std::collections::BTreeSet::from([0, 2, 5, 12]);
        let selection = super::live_rows(&[(0, 0, 4), (2, 10, 3)], &deleted)?;
        assert_eq!(
            selection,
            RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(1),
                RowSelector::skip(1),
                RowSelector::select(3),
                RowSelector::skip(1),
            ])
        );
        Ok(())
    }

    #[test]
    fn decimals_render_as_exact_base_ten_text() {
        assert_eq!(format_decimal(123_456, 2), "1234.56");
        assert_eq!(format_decimal(-5, 2), "-0.05");
        assert_eq!(format_decimal(0, 2), "0.00");
        assert_eq!(format_decimal(700, 0), "700");
    }

    #[test]
    fn decimal_contract_types_are_parsed_into_precision_and_scale() {
        assert_eq!(parse_decimal_type("decimal(18,2)"), Some((18, 2)));
        assert_eq!(parse_decimal_type("decimal(9, 4)"), Some((9, 4)));
        assert_eq!(parse_decimal_type("string"), None);
        assert_eq!(parse_decimal_type("decimal(18)"), None);
    }

    #[test]
    fn unions_resolve_to_their_selected_branch() {
        let value = AvroValue::Union(1, Box::new(AvroValue::Long(7)));
        assert_eq!(resolve(&value), &AvroValue::Long(7));
        assert_eq!(resolve(&AvroValue::Null), &AvroValue::Null);
    }
}
