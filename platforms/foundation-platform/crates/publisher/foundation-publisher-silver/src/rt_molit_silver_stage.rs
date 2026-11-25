//! The 국토교통부 실거래가 Silver load, in `DuckDB` inside the publisher (root ADR-0348, ADR-0349, ADR-0559,
//! ADR-0616 §3-1).
//!
//! It was `rt_molit_real_transaction.py` (Spark). The collector lands one CSV per (dataset, 계약 달),
//! nationwide and whole; each object is one commit of `silver.real_transaction`:
//!
//! - [`transcode`] turns the export's CP949 text into UTF-8 from its header line on (`encoding_rs`;
//!   `DuckDB`'s own reader for CP949 is an extension of 290 MB per platform) and refuses an export
//!   that has no header or names another month in its preamble. Nothing else of the file is read in
//!   Rust: `DuckDB`'s CSV reader reads every record (`records.sql`: quoted cells, line ends inside
//!   them, cell counts). The provider's row numbers are walked in order ([`walk_row_numbers`], the one
//!   rule SQL cannot say without a loop), and `names.sql` gives every row its name (root ADR-0349 §2):
//!   the sha256 of its fields less the row number and the columns filled in after the fact, plus its
//!   rank among rows of the same name. A line that is not a row of this export is rejected with its
//!   Bronze reference (root ADR-0364);
//! - `lots.sql` reads each row's 법정동 from its full address and the source PNU of an unmasked lot;
//!   `real_transaction_placement_stage`'s `resolve.sql` (the one place-identity resolver's rules,
//!   root ADR-0288 §1) carries it to the current parcel of the served edition;
//! - the month is compared with its own current rows (`silver_changes::removal_changes_within`): a
//!   new or changed name is appended, a current name the export no longer holds gets a removal
//!   marker. A cancellation is a changed row, never a removal (root ADR-0349 §3-§4).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;

use crate::duckdb_parquet_stage::{table_at, DuckDbParquetStage};
use crate::place_identity_stage::{CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE};
use crate::real_transaction_placement_stage as placement;
use crate::silver_changes;

/// The table the load appends to.
pub const TABLE: &str = "silver.real_transaction";
/// The lines an export carried that are not loaded, each with its Bronze reference (root ADR-0364).
pub const REJECTED_TABLE: &str = "silver.real_transaction_rejected";
const PLAN_SCHEMA: &str = "foundation-platform.silver_load_objects.v1";
const SOURCE_PREFIX: &str = "rtmolitgokr__";
const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/rt-molit-real-transaction.contract.json");
const LOTS_SQL: &str = include_str!("rt_molit_silver_stage/lots.sql");
const COLUMNS_SQL: &str = include_str!("rt_molit_silver_stage/columns.sql");
const RECORDS_SQL: &str = include_str!("rt_molit_silver_stage/records.sql");
const NAMES_SQL: &str = include_str!("rt_molit_silver_stage/names.sql");
/// The fewest unmasked lots whose share is read (2026-11-05..08: 토지 매매 exports of about 45,000
/// rows, all but 2 lots masked, both without a 법정동, failed every run at a share of 1.0).
pub const MIN_UNMASKED_LOTS: u64 = 50;
/// The reasons a row names no lot, so it is not a lot the link bound counts (root ADR-0469).
pub const NOT_A_LOT: [&str; 4] = ["jibun_masked", "no_jibun", "block_lot", "provisional_lot"];

/// `rt-molit-real-transaction.contract.json`, as far as the load reads it.
#[derive(Debug)]
pub struct RtMolitSource {
    /// The export's text encoding (`cp949`).
    pub encoding: String,
    /// The first cell of the header line (`NO`).
    pub header_first_column: String,
    /// The row number column: never identity.
    pub row_number_column: String,
    /// The columns filled in after the contract: a change in them is a new version of the same row.
    pub mutable_columns: Vec<String>,
    /// The datasets the collector lands.
    pub datasets: BTreeSet<String>,
    /// The share of an export's lines above which it is quarantined whole (root ADR-0364).
    pub max_rejected_share: f64,
    /// The share of unmasked lots without a current parcel above which a load fails after loading.
    pub max_unmasked_unresolved_share: f64,
    /// A dataset's own bound, where the contract measured one (root ADR-0469).
    pub max_unmasked_unresolved_share_by_dataset: BTreeMap<String, f64>,
    /// After this many seconds a run starts no further object.
    pub run_time_budget_seconds: f64,
}

impl RtMolitSource {
    /// The contract compiled into the binary.
    ///
    /// # Errors
    /// Returns an error when it is not JSON or misses a field the load reads.
    pub fn compiled() -> anyhow::Result<Self> {
        let contract: Value = serde_json::from_str(SOURCE_CONTRACT)
            .context("rt-molit-real-transaction.contract.json is not JSON")?;
        let text = |section: &str, name: &str| -> anyhow::Result<String> {
            Ok(contract[section][name]
                .as_str()
                .with_context(|| format!("the 실거래 contract has no {section}.{name}"))?
                .to_owned())
        };
        let number = |name: &str| -> anyhow::Result<f64> {
            contract["silver_load"][name]
                .as_f64()
                .with_context(|| format!("the 실거래 contract has no silver_load.{name}"))
        };
        let by_dataset = contract["silver_load"]["max_unmasked_unresolved_share_by_dataset"]
            .as_object()
            .map(|bounds| {
                bounds
                    .iter()
                    .filter_map(|(dataset, bound)| Some((dataset.clone(), bound.as_f64()?)))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            encoding: text("csv", "encoding")?,
            header_first_column: text("csv", "header_first_column")?,
            row_number_column: text("csv", "row_number_column")?,
            mutable_columns: contract["csv"]["mutable_columns"]
                .as_array()
                .context("the 실거래 contract has no csv.mutable_columns")?
                .iter()
                .filter_map(|name| name.as_str().map(str::to_owned))
                .collect(),
            datasets: contract["datasets"]
                .as_object()
                .context("the 실거래 contract has no datasets")?
                .keys()
                .cloned()
                .collect(),
            max_rejected_share: number("max_rejected_share")?,
            max_unmasked_unresolved_share: number("max_unmasked_unresolved_share")?,
            max_unmasked_unresolved_share_by_dataset: by_dataset,
            run_time_budget_seconds: number("run_time_budget_seconds")?,
        })
    }

    /// The Bronze source slugs the load plans, comma-separated: what the load script asks for.
    #[must_use]
    pub fn source_slugs(&self) -> String {
        self.datasets
            .iter()
            .map(|dataset| format!("{SOURCE_PREFIX}{dataset}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The dataset's own link bound when the contract measured one, else the load's.
    #[must_use]
    pub fn link_bound(&self, dataset: &str) -> f64 {
        self.max_unmasked_unresolved_share_by_dataset
            .get(dataset)
            .copied()
            .unwrap_or(self.max_unmasked_unresolved_share)
    }
}

/// One Bronze object a plan owes: a dataset's export of one 계약 달.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedObject {
    /// The Bronze object key.
    pub object_key: String,
    /// The dataset (`real_transaction_land_trade`).
    pub dataset: String,
    /// The 계약 달, `YYYYMM`.
    pub month: String,
    /// The object's landing time as `YYYYMMDDTHHMMSSZ`: the edition of a content-qualified export.
    pub source_snapshot_id: String,
    /// Where the plan downloaded it.
    pub path: PathBuf,
}

/// The 계약 달 (`YYYYMM`) an object key names (`…/period=YYYY-MM/…`).
fn month_of(object_key: &str) -> anyhow::Result<String> {
    let month = object_key
        .split('/')
        .find_map(|part| part.strip_prefix("period="))
        .filter(|period| {
            let bytes = period.as_bytes();
            bytes.len() == 7
                && bytes.get(4) == Some(&b'-')
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(at, byte)| at == 4 || byte.is_ascii_digit())
        })
        .with_context(|| format!("{object_key} names no whole month (period=YYYY-MM)"))?;
    Ok(month.replace('-', ""))
}

/// The objects of the plans in `plan_dirs`, oldest landing first.
///
/// # Errors
/// Refuses another plan format, a plan of another table, a source this load does not read, an
/// object without its landing time or month, and a local path that leaves the plan directory.
pub fn planned_objects(
    plan_dirs: &[PathBuf],
    source: &RtMolitSource,
) -> anyhow::Result<Vec<PlannedObject>> {
    let mut objects = Vec::new();
    for plan_dir in plan_dirs {
        let plan: Value = serde_json::from_slice(
            &std::fs::read(plan_dir.join("plan.json"))
                .with_context(|| format!("cannot read the plan in {}", plan_dir.display()))?,
        )?;
        ensure!(
            plan["schema_version"] == PLAN_SCHEMA,
            "{}: not a {PLAN_SCHEMA} plan",
            plan_dir.display()
        );
        ensure!(
            plan["table"].is_null() || plan["table"] == TABLE,
            "{}: {} is not the table this load writes",
            plan_dir.display(),
            plan["table"]
        );
        for entry in plan["objects"]
            .as_array()
            .context("the plan lists no objects")?
        {
            let key = entry["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let slug = entry["source_slug"].as_str().unwrap_or_default();
            let dataset = slug
                .strip_prefix(SOURCE_PREFIX)
                .filter(|dataset| source.datasets.contains(*dataset))
                .with_context(|| format!("{key}: {slug} is not a 실거래 source this load reads"))?;
            let landed = entry["last_modified"]
                .as_str()
                .with_context(|| format!("{key}: the plan carries no last_modified"))?;
            let landed = chrono::DateTime::parse_from_rfc3339(landed)
                .with_context(|| format!("{key}: last_modified {landed:?} is not an instant"))?
                .with_timezone(&chrono::Utc);
            let local = entry["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            objects.push(PlannedObject {
                object_key: key.to_owned(),
                dataset: dataset.to_owned(),
                month: month_of(key)?,
                source_snapshot_id: landed.format("%Y%m%dT%H%M%SZ").to_string(),
                path: plan_dir.join(local),
            });
        }
    }
    objects.sort_by(|a, b| {
        (&a.source_snapshot_id, &a.object_key).cmp(&(&b.source_snapshot_id, &b.object_key))
    });
    Ok(objects)
}

/// Why an export is quarantined whole, or `None` when its accepted rows load (root ADR-0364).
///
/// Refused outright, or more of its lines rejected than the contract allows: a mixed export, where
/// what passed the line checks is not trusted either; the collector asks the month again.
#[must_use]
pub fn quarantine_reason(
    rows: u64,
    rejected: u64,
    refused: bool,
    max_share: f64,
) -> Option<String> {
    if refused {
        return Some("export_refused".to_owned());
    }
    let share = rejected_share(rows, rejected);
    (share > max_share).then(|| {
        format!(
            "export_quarantined: {:.4}% of lines rejected",
            share * 100.0
        )
    })
}

/// The share of an export's lines that were rejected.
#[must_use]
#[allow(clippy::cast_precision_loss)] // line counts of one export, far under 2^52
pub const fn rejected_share(rows: u64, rejected: u64) -> f64 {
    if rows + rejected == 0 {
        0.0
    } else {
        rejected as f64 / (rows + rejected) as f64
    }
}

/// Whether enough unmasked lots were read and too many of them found no current parcel.
#[must_use]
#[allow(clippy::cast_precision_loss)] // lot counts of one export
pub const fn link_bound_breached(lots: u64, unresolved: u64, bound: f64) -> bool {
    lots >= MIN_UNMASKED_LOTS && unresolved as f64 / lots as f64 > bound
}

fn plain(text: &str, what: &str) -> anyhow::Result<()> {
    ensure!(
        !text.contains('\'') && !text.contains('\\'),
        "{what} {text:?} holds a quote or a backslash"
    );
    Ok(())
}

fn table_name(prefix: &str, index: usize) -> String {
    format!("{prefix}_{index}")
}

fn sql_type(logical: &str) -> anyhow::Result<&'static str> {
    Ok(match logical {
        "string" => "VARCHAR",
        "int" => "INTEGER",
        "long" => "BIGINT",
        "double" => "DOUBLE",
        "boolean" => "BOOLEAN",
        "date" => "DATE",
        other => bail!("a 실거래 column of type {other} has no reader"),
    })
}

/// The columns `names.sql` fills: the contract's, less the ones placing and the load add.
fn parsed_columns(
    contract: &LakehouseTableContract,
) -> anyhow::Result<Vec<(&'static str, &'static str)>> {
    contract
        .columns
        .iter()
        .filter(|column| !ADDED_COLUMNS.contains(&column.name))
        .map(|column| Ok((column.name, sql_type(column.logical_type)?)))
        .collect()
}

/// Columns the placement and the load write, not the export.
const ADDED_COLUMNS: [&str; 7] = [
    "legal_dong_code",
    "pnu",
    "place_identity_grade",
    "pnu_unresolved_reason",
    "source_snapshot_id",
    "ingested_at_utc",
    "row_state",
];

/// The snapshots a run reads its placing inputs at (each its head when the run starts), and the
/// parcel edition the map serves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pins {
    /// `reference.legal_dong_code_change`.
    pub code_changes: i64,
    /// `reference.legal_dong_code_snapshot`.
    pub dong_names: i64,
    /// `silver.parcel_boundaries`.
    pub parcels: i64,
    /// `silver.parcel_boundaries.source_snapshot_id` of the served edition (root ADR-0291 §5).
    pub served_snapshot_id: String,
}

/// Reads the code changes and the newest 법정동 snapshot at `pins`, and builds the tables the
/// placement reads: `crosswalk`, `ambiguous`, `served_forms` and `address_codes`.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn load_reference(stage: &DuckDbParquetStage, pins: &Pins) -> anyhow::Result<()> {
    let connection = stage.connection();
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TABLE code_changes AS
               SELECT kind, old_code, new_code, level, effective_date, source FROM {changes};
             CREATE OR REPLACE TABLE code_names AS
               SELECT region_cd, full_name, status FROM {names}
               WHERE snapshot_date = (SELECT max(snapshot_date) FROM {names});",
            changes = table_at(CODE_CHANGE_TABLE, pins.code_changes),
            names = table_at(DONG_NAME_TABLE, pins.dong_names),
        ))
        .context("DuckDB failed to read the 법정동 code tables")?;
    reference_from_loaded(stage)
}

/// [`load_reference`] over `code_changes` and `code_names` already on the stage.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn reference_from_loaded(stage: &DuckDbParquetStage) -> anyhow::Result<()> {
    let connection = stage.connection();
    let (remap, ambiguous) =
        placement::crosswalk_tables_from(connection, "code_changes", "code_names")?;
    placement::address_code_table_from(connection, "code_names", &remap, &ambiguous)?;
    connection.execute_batch(placement::ADDRESS_KEY_MACRO)?;
    Ok(())
}

/// An export in UTF-8 from its header line on, for `DuckDB`'s CSV reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transcoded {
    /// The UTF-8 file.
    pub path: PathBuf,
    /// The line of the Bronze object its header is on, from 1.
    pub first_line: u32,
}

/// Writes the export `bytes` as UTF-8 from its header line on to `out`, or says why it is refused.
///
/// An export is refused whole (root ADR-0364) when it is not the contract's encoding, has no header
/// line (a quoted `NO` that starts a line), or has a preamble that does not name `object`'s month.
///
/// Only the preamble is read here (for the month); the records are `DuckDB`'s.
///
/// # Errors
/// Returns an error when the contract names an encoding the load cannot read, or `out` cannot be written.
pub fn transcode(
    bytes: &[u8],
    object: &PlannedObject,
    source: &RtMolitSource,
    out: &Path,
) -> anyhow::Result<Result<Transcoded, String>> {
    ensure!(
        source.encoding.eq_ignore_ascii_case("cp949"),
        "the 실거래 contract names the encoding {:?}; the load reads cp949",
        source.encoding
    );
    // WHATWG's EUC-KR is Microsoft's code page 949 (unified Hangul), the provider's CP949.
    let Some(text) = encoding_rs::EUC_KR.decode_without_bom_handling_and_without_replacement(bytes)
    else {
        return Ok(Err("the export is not CP949 text".to_owned()));
    };
    let header = format!("\"{}\"", source.header_first_column);
    let start = match text.find(&header) {
        Some(at) if at == 0 || text[..at].ends_with(['\r', '\n']) => at,
        _ => {
            return Ok(Err(
                "the export has no header line: not the provider's CSV".to_owned()
            ))
        }
    };
    let preamble = &text[..start];
    let named = regex::Regex::new(MONTH_LINE)?
        .captures(preamble)
        .is_some_and(|dates| {
            format!("{}{}", &dates[1], &dates[2]) == object.month
                && format!("{}{}", &dates[3], &dates[4]) == object.month
        });
    if !named {
        return Ok(Err(format!(
            "the preamble does not name the month {}",
            object.month
        )));
    }
    std::fs::write(out, &text.as_bytes()[start..])
        .with_context(|| format!("cannot write {}", out.display()))?;
    Ok(Ok(Transcoded {
        path: out.to_owned(),
        first_line: u32::try_from(preamble.matches('\n').count() + 1)?,
    }))
}

/// The preamble's line that names the export's dates (`rt_molit_real_transaction.export_lines`).
const MONTH_LINE: &str = r"계약일자 : (\d{4})-(\d{2})-\d{2} ~ (\d{4})-(\d{2})-\d{2}";

/// A record after the header, as the row-number walk reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Its order in the export, the header being 1.
    pub record: i64,
    /// Its cell count.
    pub cells: i64,
    /// Its first cell, stripped: the provider's `NO`.
    pub first_cell: String,
    /// Whether every cell is blank.
    pub blank: bool,
}

/// The records the provider's row numbers refuse, with why (root ADR-0364;
/// `rt_molit_real_transaction.export_lines`).
///
/// The export numbers its rows 1..N. A blank record is skipped. A record whose cell count is not the
/// header's is refused, and still took its number when it carries the one expected (a broken row of
/// this export; another export's row did not). A record that does not carry the number expected is
/// refused: the 2026-10-30 export carried a second export's rows after its own.
#[must_use]
pub fn walk_row_numbers(records: &[Record], header_cells: i64) -> Vec<(i64, String)> {
    let mut expected = 1_i64;
    let mut refused = Vec::new();
    for record in records.iter().filter(|record| !record.blank) {
        let carries_expected = record.first_cell == expected.to_string();
        if record.cells != header_cells {
            refused.push((
                record.record,
                format!("cells_{}_of_{header_cells}", record.cells),
            ));
            expected += i64::from(carries_expected);
        } else if carries_expected {
            expected += 1;
        } else {
            refused.push((
                record.record,
                format!("row_number_{}_where_{expected}", record.first_cell),
            ));
        }
    }
    refused
}

/// Cells the reader declares per record: more than any header holds (the widest export has 22).
const READ_WIDTH: usize = 128;

/// One export read into `rt_parsed_<index>` and `rt_rejected_lines_<index>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Read {
    /// The rows that passed the line checks.
    pub rows: u64,
    /// The lines not loaded.
    pub rejected: u64,
    /// The header's columns no Silver column carries, sorted.
    pub unrepresented: Vec<String>,
}

/// Reads `transcoded` with `DuckDB`'s CSV reader, or says why the export is refused whole.
///
/// It leaves `rt_parsed_<index>` (the rows, named, in the Silver columns the export fills) and
/// `rt_rejected_lines_<index>` (`source_line_number`, `raw_line`, `reason`).
///
/// # Errors
/// Returns a `DuckDB` failure, or a path or load identity that holds a quote.
// `{i}` and the other names in braces are placeholders of the SQL text, not format arguments.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn read_export(
    connection: &duckdb::Connection,
    transcoded: &Transcoded,
    object: &PlannedObject,
    source: &RtMolitSource,
    index: usize,
) -> anyhow::Result<Result<Read, String>> {
    let csv = transcoded.path.to_string_lossy().replace('\\', "/");
    plain(&csv, "the export's path")?;
    plain(&object.object_key, "the object key")?;
    plain(&object.dataset, "the dataset")?;
    plain(&object.month, "the month")?;
    let suffix = index.to_string();
    let columns = format!(
        "{{{}}}",
        (0..READ_WIDTH)
            .map(|cell| format!("'c{cell:03}': 'VARCHAR'"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    connection.execute_batch(COLUMNS_SQL)?;
    connection
        .execute_batch(
            &RECORDS_SQL
                .replace("{csv}", &csv)
                .replace("{columns}", &columns)
                .replace("{first_line}", &transcoded.first_line.to_string())
                .replace("{i}", &suffix),
        )
        .with_context(|| format!("DuckDB failed to read {}", object.object_key))?;
    let header_cells: i64 = connection.query_row(
        &format!("SELECT count(*) FROM rt_header_{suffix}"),
        [],
        |row| row.get(0),
    )?;
    if usize::try_from(header_cells)? >= READ_WIDTH {
        return Ok(Err(format!(
            "the header has {header_cells} cells, as many as the reader reads"
        )));
    }
    let records = connection
        .prepare(&format!(
            "SELECT record, cells, coalesce(first_cell, ''), blank FROM rt_records_{suffix} ORDER BY record"
        ))?
        .query_map([], |row| {
            Ok(Record {
                record: row.get(0)?,
                cells: row.get(1)?,
                first_cell: row.get(2)?,
                blank: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE rt_line_rejects_{suffix} (record BIGINT NOT NULL, reason VARCHAR NOT NULL);"
    ))?;
    {
        let mut appender = connection.appender(&format!("rt_line_rejects_{suffix}"))?;
        for (record, reason) in walk_row_numbers(&records, header_cells) {
            appender.append_row(duckdb::params![record, reason])?;
        }
        appender.flush()?;
    }
    connection
        .execute_batch(&names_sql(object, source, &suffix)?)
        .with_context(|| format!("DuckDB failed to name the rows of {}", object.object_key))?;
    let count = |table: &str| -> anyhow::Result<u64> {
        let rows: i64 = connection.query_row(
            &format!("SELECT count(*) FROM {table}_{suffix}"),
            [],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(rows)?)
    };
    let unrepresented = connection
        .prepare(&format!(
            "SELECT name FROM rt_header_{suffix} \
             WHERE name NOT IN (SELECT header FROM rt_csv_columns) \
               AND name NOT IN (SELECT header FROM rt_known_elsewhere) ORDER BY name"
        ))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(Ok(Read {
        rows: count("rt_parsed")?,
        rejected: count("rt_rejected_lines")?,
        unrepresented,
    }))
}

/// `names.sql` for export `suffix` of `object`.
// `{i}` and the other names in braces are placeholders of the SQL text, not format arguments.
#[allow(clippy::literal_string_with_formatting_args)]
fn names_sql(
    object: &PlannedObject,
    source: &RtMolitSource,
    suffix: &str,
) -> anyhow::Result<String> {
    let list = |names: &[&str]| -> anyhow::Result<String> {
        for name in names {
            plain(name, "a contract column name")?;
        }
        Ok(format!(
            "[{}]",
            names
                .iter()
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    };
    let mutable: Vec<&str> = source.mutable_columns.iter().map(String::as_str).collect();
    let drop: Vec<&str> = std::iter::once(source.row_number_column.as_str())
        .chain(mutable.iter().copied())
        .collect();
    plain(&source.row_number_column, "the row number column")?;
    Ok(NAMES_SQL
        .replace("{drop}", &list(&drop)?)
        .replace("{mutable}", &list(&mutable)?)
        .replace("{row_number_column}", &source.row_number_column)
        .replace("{dataset}", &object.dataset)
        .replace("{month}", &object.month)
        .replace("{object_key}", &object.object_key)
        .replace("{i}", suffix))
}

/// What one export staged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Staged {
    /// The rows that passed the line checks.
    pub rows: u64,
    /// Of them, cancelled deals.
    pub cancelled: u64,
    /// The lines not loaded (with the quarantine marker, when the export is quarantined).
    pub rejected: u64,
    /// The reasons of the rejected lines, up to their first `:`.
    pub rejected_reasons: BTreeSet<String>,
    /// The header's columns no Silver column carries.
    pub unrepresented_fields: Vec<String>,
    /// Whether the export is not the provider's CSV for its month.
    pub refused: bool,
    /// Why nothing of the export loads, when so.
    pub quarantined: Option<String>,
}

/// Stages one planned export as `rt_deals_<index>` and `rt_rejected_<index>`.
///
/// The first holds its rows, each with the 법정동 its address names and the source PNU of its lot;
/// the second the lines not loaded, in the rejected table's columns. The export is transcoded into
/// `work` first ([`transcode`]). A refused export stages no row and one line 0 with its reason; a
/// quarantined one stages no row and gains a marker line 0.
///
/// # Errors
/// Returns a `DuckDB` failure, an export that cannot be read, or a load identity that holds a quote.
#[allow(clippy::too_many_lines)]
// one export, staged table by table
// `{rows}` and `{out}` are placeholders of the SQL text, not format arguments.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn stage_export(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    object: &PlannedObject,
    index: usize,
    load: (&RtMolitSource, i64, &Path),
) -> anyhow::Result<Staged> {
    let (source, ingested_at_epoch_seconds, work) = load;
    plain(&object.object_key, "the object key")?;
    plain(&object.dataset, "the dataset")?;
    plain(&object.month, "the month")?;
    plain(&object.source_snapshot_id, "the snapshot id")?;
    let connection = stage.connection();
    let bytes = std::fs::read(&object.path)
        .with_context(|| format!("cannot read {}", object.path.display()))?;
    let utf8 = work.join(format!("rt-export-{index}.utf8.csv"));
    let read = match transcode(&bytes, object, source, &utf8)? {
        Ok(transcoded) => read_export(connection, &transcoded, object, source, index)?,
        Err(reason) => Err(reason),
    };
    drop(bytes);
    let _ = std::fs::remove_file(&utf8);
    let columns = parsed_columns(contract)?;
    let rows_table = table_name("rt_rows", index);
    let lines = table_name("rt_rejected_lines", index);
    let (rows, unrepresented, refused) = match &read {
        Ok(read) => (read.rows, read.unrepresented.clone(), None),
        Err(reason) => (0, Vec::new(), Some(reason.clone())),
    };
    let rejected_lines = read.as_ref().map_or(0, |read| read.rejected);
    let quarantined = quarantine_reason(
        rows,
        rejected_lines,
        refused.is_some(),
        source.max_rejected_share,
    );
    let projection = columns
        .iter()
        .map(|(name, kind)| format!("CAST({name} AS {kind}) AS {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    if refused.is_some() {
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {rows_table} ({});
             CREATE OR REPLACE TEMP TABLE {lines} (source_line_number INTEGER NOT NULL, raw_line VARCHAR, reason VARCHAR NOT NULL);",
            columns
                .iter()
                .map(|(name, kind)| format!("{name} {kind}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))?;
    } else {
        // A quarantined export stages none of its rows (root ADR-0364).
        let keep = if quarantined.is_some() {
            "WHERE false"
        } else {
            ""
        };
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {rows_table} AS SELECT {projection} FROM rt_parsed_{index} {keep};"
        ))?;
    }
    {
        let mut appender = connection.appender(&lines)?;
        match (&refused, &quarantined) {
            (Some(reason), _) => {
                appender.append_row(duckdb::params![
                    0_i32,
                    "",
                    format!("export_refused: {reason}")
                ])?;
            }
            (None, Some(reason)) => appender.append_row(duckdb::params![0_i32, "", reason])?,
            (None, None) => {}
        }
        appender.flush()?;
    }
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {out} AS
               SELECT '{dataset}' AS dataset_slug, '{month}' AS deal_ym, '{object_key}' AS source_record_id,
                      source_line_number, raw_line, reason, '{snapshot}' AS source_snapshot_id,
                      to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
               FROM {lines};
             {lots}
             DROP TABLE {rows_table};",
            out = table_name("rt_rejected", index),
            dataset = object.dataset,
            month = object.month,
            object_key = object.object_key,
            snapshot = object.source_snapshot_id,
            lots = LOTS_SQL
                .replace("{rows}", &rows_table)
                .replace("{out}", &table_name("rt_deals", index)),
        ))
        .with_context(|| format!("DuckDB failed to stage {}", object.object_key))?;
    let (rejected, reasons): (i64, Option<String>) = connection.query_row(
        &format!(
            "SELECT count(*), string_agg(DISTINCT split_part(reason, ':', 1), '\n') FROM {}",
            table_name("rt_rejected", index)
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let cancelled: i64 = connection.query_row(
        &format!(
            "SELECT count(*) FILTER (WHERE cancelled) FROM {}",
            table_name("rt_deals", index)
        ),
        [],
        |row| row.get(0),
    )?;
    drop_read_tables(connection, index)?;
    Ok(Staged {
        rows: if quarantined.is_some() { 0 } else { rows },
        cancelled: u64::try_from(cancelled)?,
        rejected: u64::try_from(rejected)?,
        rejected_reasons: reasons
            .unwrap_or_default()
            .split('\n')
            .filter(|reason| !reason.is_empty())
            .map(str::to_owned)
            .collect(),
        unrepresented_fields: unrepresented,
        refused: refused.is_some(),
        quarantined,
    })
}

/// Drops what [`read_export`] left for export `index`, once it is staged.
fn drop_read_tables(connection: &duckdb::Connection, index: usize) -> anyhow::Result<()> {
    for table in [
        "rt_cells",
        "rt_header",
        "rt_records",
        "rt_line_rejects",
        "rt_fields",
        "rt_month_rejects",
        "rt_rejected_lines",
        "rt_kept",
        "rt_named",
        "rt_mapped",
        "rt_parsed",
    ] {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {table}_{index};"))?;
    }
    Ok(())
}

/// How many PNUs the run asked the served edition about, and how many it holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParcelsRead {
    /// PNUs asked.
    pub asked: u64,
    /// Of them, served parcels.
    pub held: u64,
}

/// Reads, once a run, the served parcels the staged exports can ask about, as `parcels`.
///
/// The exports are `rt_deals_0..objects`; each source PNU is asked under its current code and
/// under the earlier forms of that code the served edition may still hold (root ADR-0482 §1).
///
/// 2026-10-31: read again per object, the parcels were most of each object's 1-3 minutes and a run
/// of 60 objects outlived its unit's 40 minutes, every hour.
///
/// # Errors
/// Returns a `DuckDB` failure, or the served edition's snapshot id holds a quote.
pub fn read_parcels(
    stage: &DuckDbParquetStage,
    pins: &Pins,
    parcel_contract: &LakehouseTableContract,
    objects: usize,
) -> anyhow::Result<ParcelsRead> {
    read_parcels_from(
        stage,
        &table_at(PARCEL_TABLE, pins.parcels),
        &pins.served_snapshot_id,
        parcel_contract,
        objects,
    )
}

/// [`read_parcels`] over the parcel table reference it is given.
pub(crate) fn read_parcels_from(
    stage: &DuckDbParquetStage,
    parcels: &str,
    served_snapshot_id: &str,
    parcel_contract: &LakehouseTableContract,
    objects: usize,
) -> anyhow::Result<ParcelsRead> {
    plain(served_snapshot_id, "the served edition's snapshot id")?;
    let predicate = parcel_contract
        .current_row_predicate
        .context("silver.parcel_boundaries names its current rows by a predicate")?;
    let sources = if objects == 0 {
        "SELECT NULL::VARCHAR AS pnu WHERE false".to_owned()
    } else {
        (0..objects)
            .map(|index| format!("SELECT pnu FROM {}", table_name("rt_deals", index)))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    let connection = stage.connection();
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE rt_asked AS
             WITH source AS (SELECT DISTINCT pnu FROM ({sources}) WHERE pnu IS NOT NULL)
             SELECT concat(coalesce(x.current_code, substr(s.pnu, 1, 10)), substr(s.pnu, 11, 9)) AS pnu
             FROM source s LEFT JOIN crosswalk x ON x.old_code = substr(s.pnu, 1, 10)
             UNION
             SELECT concat(f._form10, substr(s.pnu, 11, 9))
             FROM source s JOIN served_forms f ON f._code10 = substr(s.pnu, 1, 10);
             CREATE OR REPLACE TABLE parcels AS
             SELECT DISTINCT pnu FROM {parcels}
             WHERE source_snapshot_id = '{served_snapshot_id}' AND ({predicate})
               AND pnu IN (SELECT pnu FROM rt_asked);"
        ))
        .context("DuckDB failed to read the served parcels")?;
    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(value)?)
    };
    Ok(ParcelsRead {
        asked: count("rt_asked")?,
        held: count("parcels")?,
    })
}

/// How one export's rows linked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Linked {
    /// The rows.
    pub rows: u64,
    /// Rows on a current parcel.
    pub pnu: u64,
    /// Rows with a 법정동 and no parcel.
    pub legal_dong_only: u64,
    /// Rows without a parcel, by reason.
    pub unresolved: BTreeMap<String, u64>,
    /// The unmasked lots.
    pub lots: u64,
    /// Of them, those that found no current parcel.
    pub unresolved_lots: u64,
}

/// Places `rt_deals_<index>` on the served parcels (`parcels`) as `rt_staged`, in the columns of
/// `contract`, and counts how its rows linked.
///
/// # Errors
/// Returns a `DuckDB` failure.
#[allow(clippy::too_many_lines)] // the placement, then one count of how it linked
pub fn place_export(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    object: &PlannedObject,
    index: usize,
    ingested_at_epoch_seconds: i64,
) -> anyhow::Result<Linked> {
    plain(&object.source_snapshot_id, "the snapshot id")?;
    let connection = stage.connection();
    let columns = contract
        .columns
        .iter()
        .map(|column| match column.name {
            "legal_dong_code" => "rt_current_code AS legal_dong_code".to_owned(),
            "pnu_unresolved_reason" => {
                "CASE WHEN pnu IS NULL THEN coalesce(rt_reason, pnu_unresolved_reason) END \
                 AS pnu_unresolved_reason"
                    .to_owned()
            }
            "source_snapshot_id" => {
                format!("'{}' AS source_snapshot_id", object.source_snapshot_id)
            }
            "ingested_at_utc" => {
                format!("to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc")
            }
            "row_state" => "NULL::VARCHAR AS row_state".to_owned(),
            name => name.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TABLE deals AS SELECT * FROM {deals};
             {resolve}
             CREATE OR REPLACE TEMP TABLE rt_staged AS SELECT {columns} FROM resolved;
             DROP TABLE resolved;
             DROP TABLE deals;
             DROP TABLE {deals};",
            deals = table_name("rt_deals", index),
            resolve = placement::RESOLVE_SQL,
        ))
        .with_context(|| format!("DuckDB failed to place {}", object.object_key))?;
    let not_a_lot = NOT_A_LOT
        .iter()
        .map(|reason| format!("'{reason}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut grouped = connection.prepare(&format!(
        "SELECT pnu_unresolved_reason, count(*), count(pnu),
                count(*) FILTER (WHERE pnu IS NULL AND legal_dong_code IS NOT NULL),
                count(*) FILTER (WHERE coalesce(pnu_unresolved_reason NOT IN ({not_a_lot}), true)),
                count(*) FILTER (WHERE coalesce(pnu_unresolved_reason NOT IN ({not_a_lot}), true) AND pnu IS NULL)
         FROM rt_staged GROUP BY 1 ORDER BY 1"
    ))?;
    let mut linked = Linked {
        rows: 0,
        pnu: 0,
        legal_dong_only: 0,
        unresolved: BTreeMap::new(),
        lots: 0,
        unresolved_lots: 0,
    };
    let counted = grouped.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for group in counted {
        let (reason, rows, pnu, legal_dong_only, lots, unresolved_lots) = group?;
        linked.rows += u64::try_from(rows)?;
        linked.pnu += u64::try_from(pnu)?;
        linked.legal_dong_only += u64::try_from(legal_dong_only)?;
        linked.lots += u64::try_from(lots)?;
        linked.unresolved_lots += u64::try_from(unresolved_lots)?;
        if let Some(reason) = reason {
            linked.unresolved.insert(reason, u64::try_from(rows)?);
        }
    }
    Ok(linked)
}

/// Builds `rt_changes` from `rt_staged`: what the month's export changes.
///
/// The month's new or changed names and a removal marker per current name the export no longer
/// holds, compared with that month's current rows at `head` only (an export is the whole month,
/// root ADR-0349 §3). Returns (rows, of them markers).
///
/// # Errors
/// Returns a `DuckDB` failure, or a load identity that holds a quote.
pub fn month_changes(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    object: &PlannedObject,
    ingested_at_epoch_seconds: i64,
) -> anyhow::Result<(u64, u64)> {
    plain(&object.dataset, "the dataset")?;
    plain(&object.month, "the month")?;
    silver_changes::removal_changes_within(
        stage,
        contract,
        head,
        "rt_staged",
        "rt_changes",
        &format!(
            "dataset_slug = '{}' AND deal_ym = '{}'",
            object.dataset, object.month
        ),
        (
            &object.object_key,
            &object.source_snapshot_id,
            ingested_at_epoch_seconds,
        ),
    )
}

#[cfg(test)]
mod tests;
