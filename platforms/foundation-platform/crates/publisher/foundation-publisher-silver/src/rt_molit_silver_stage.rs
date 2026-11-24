//! The 국토교통부 실거래가 Silver load, in `DuckDB` inside the publisher (root ADR-0348, ADR-0349, ADR-0559).
//!
//! It was `rt_molit_real_transaction.py` (Spark). The collector lands one CSV per (dataset, 계약 달),
//! nationwide and whole; each object is one commit of `silver.real_transaction`:
//!
//! - [`parse_export`] reads the export as the provider writes it (`export.rs`): CP949 text, a notice
//!   preamble, a header line, one record per deal. Every row gets its name (root ADR-0349 §2): the
//!   sha256 of its fields less the row number and the columns filled in after the fact, plus its
//!   rank among rows of the same name. A line that is not a row of this export is rejected with its
//!   Bronze reference (root ADR-0364);
//! - `lots.sql` reads each row's 법정동 from its full address and the source PNU of an unmasked lot;
//!   `real_transaction_placement_stage`'s `resolve.sql` (the one place-identity resolver's rules,
//!   root ADR-0288 §1) carries it to the current parcel of the served edition;
//! - the month is compared with its own current rows (`silver_changes::removal_changes_within`): a
//!   new or changed name is appended, a current name the export no longer holds gets a removal
//!   marker. A cancellation is a changed row, never a removal (root ADR-0349 §3-§4).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{bail, ensure, Context};
use lakehouse_domain::LakehouseTableContract;
use serde_json::{json, Map, Value};

use crate::duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG};
use crate::place_identity_stage::{CODE_CHANGE_TABLE, DONG_NAME_TABLE, PARCEL_TABLE};
use crate::real_transaction_placement_stage as placement;
use crate::silver_changes;

mod export;

pub use export::{parse_export, Parsed, Rejected, Row};

/// The table the load appends to.
pub const TABLE: &str = "silver.real_transaction";
/// The lines an export carried that are not loaded, each with its Bronze reference (root ADR-0364).
pub const REJECTED_TABLE: &str = "silver.real_transaction_rejected";
const PLAN_SCHEMA: &str = "foundation-platform.silver_load_objects.v1";
const SOURCE_PREFIX: &str = "rtmolitgokr__";
const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/rt-molit-real-transaction.contract.json");
const LOTS_SQL: &str = include_str!("rt_molit_silver_stage/lots.sql");
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

/// The columns [`parse_export`] fills: the contract's, less the ones placing and the load add.
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
               SELECT kind, old_code, new_code, level, effective_date, source
               FROM {LAKEHOUSE_CATALOG}.{CODE_CHANGE_TABLE} AT (VERSION => {changes});
             CREATE OR REPLACE TABLE code_names AS
               SELECT region_cd, full_name, status FROM {LAKEHOUSE_CATALOG}.{DONG_NAME_TABLE} AT (VERSION => {names})
               WHERE snapshot_date = (SELECT max(snapshot_date)
                                      FROM {LAKEHOUSE_CATALOG}.{DONG_NAME_TABLE} AT (VERSION => {names}));",
            changes = pins.code_changes,
            names = pins.dong_names,
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
    let (remap, ambiguous) = placement::crosswalk_tables(connection)?;
    placement::address_code_table(connection, &remap, &ambiguous)?;
    connection.execute_batch(placement::ADDRESS_KEY_MACRO)?;
    Ok(())
}

/// What one export staged.
#[derive(Clone, Debug, PartialEq)]
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

/// Stages one parsed export as `rt_deals_<index>` (its rows, each with the 법정동 its address names
/// and the source PNU of its lot) and `rt_rejected_<index>` (the lines not loaded, in the rejected
/// table's columns).
///
/// A quarantined export stages no row and gains a marker line 0 (unless it was refused, which is
/// its own line 0).
///
/// # Errors
/// Returns a `DuckDB` failure, or a load identity that holds a quote.
#[allow(clippy::too_many_lines)] // one export, staged table by table
pub fn stage_export(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    object: &PlannedObject,
    parsed: &Parsed,
    index: usize,
    load: (&RtMolitSource, i64),
) -> anyhow::Result<Staged> {
    let (source, ingested_at_epoch_seconds) = load;
    plain(&object.object_key, "the object key")?;
    plain(&object.dataset, "the dataset")?;
    plain(&object.month, "the month")?;
    plain(&object.source_snapshot_id, "the snapshot id")?;
    let connection = stage.connection();
    let rows = u64::try_from(parsed.rows.len())?;
    let quarantined = quarantine_reason(
        rows,
        u64::try_from(parsed.rejected.len())?,
        parsed.refused,
        source.max_rejected_share,
    );
    let lines = table_name("rt_lines", index);
    let rejected = table_name("rt_rejected_lines", index);
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {lines} (line VARCHAR NOT NULL);
         CREATE OR REPLACE TEMP TABLE {rejected} (source_line_number INTEGER NOT NULL, raw_line VARCHAR, reason VARCHAR NOT NULL);"
    ))?;
    if quarantined.is_none() {
        let mut appender = connection.appender(&lines)?;
        for row in &parsed.rows {
            appender.append_row(duckdb::params![serde_json::to_string(&row.values)?])?;
        }
        appender.flush()?;
    }
    {
        let mut appender = connection.appender(&rejected)?;
        if let Some(reason) = quarantined.as_deref().filter(|_| !parsed.refused) {
            appender.append_row(duckdb::params![0_i32, "", reason])?;
        }
        for line in &parsed.rejected {
            appender.append_row(duckdb::params![
                i32::try_from(line.line)?,
                line.raw.as_str(),
                line.reason.as_str()
            ])?;
        }
        appender.flush()?;
    }
    let columns = parsed_columns(contract)?;
    let structure = Value::Object(
        columns
            .iter()
            .map(|(name, kind)| ((*name).to_owned(), json!(kind)))
            .collect::<Map<_, _>>(),
    )
    .to_string();
    let names = columns
        .iter()
        .map(|(name, _)| format!("j.{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows_table = table_name("rt_rows", index);
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {rows_table} AS
               SELECT {names} FROM (SELECT from_json(line, '{structure}') AS j FROM {lines});
             DROP TABLE {lines};
             CREATE OR REPLACE TEMP TABLE {out} AS
               SELECT '{dataset}' AS dataset_slug, '{month}' AS deal_ym, '{object_key}' AS source_record_id,
                      source_line_number, raw_line, reason, '{snapshot}' AS source_snapshot_id,
                      to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
               FROM {rejected};
             DROP TABLE {rejected};
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
    let (rejected_lines, reasons): (i64, Option<String>) = connection.query_row(
        &format!(
            "SELECT count(*), string_agg(DISTINCT split_part(reason, ':', 1), '\n') FROM {}",
            table_name("rt_rejected", index)
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(Staged {
        rows: if quarantined.is_some() { 0 } else { rows },
        cancelled: if quarantined.is_some() {
            0
        } else {
            u64::try_from(
                parsed
                    .rows
                    .iter()
                    .filter(|row| row.values.get("cancelled") == Some(&Value::Bool(true)))
                    .count(),
            )?
        },
        rejected: u64::try_from(rejected_lines)?,
        rejected_reasons: reasons
            .unwrap_or_default()
            .split('\n')
            .filter(|reason| !reason.is_empty())
            .map(str::to_owned)
            .collect(),
        unrepresented_fields: parsed.unrepresented.clone(),
        refused: parsed.refused,
        quarantined,
    })
}

/// How many PNUs the run asked the served edition about, and how many it holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParcelsRead {
    /// PNUs asked.
    pub asked: u64,
    /// Of them, served parcels.
    pub held: u64,
}

/// Reads, once a run, the served parcels the staged exports (`rt_deals_0..objects`) can ask about,
/// as `parcels`: each source PNU under its current code and under the earlier forms of that code
/// the served edition may still hold (root ADR-0482 §1).
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
        &format!(
            "{LAKEHOUSE_CATALOG}.{PARCEL_TABLE} AT (VERSION => {})",
            pins.parcels
        ),
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
#[derive(Clone, Debug, PartialEq)]
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

/// Builds `rt_changes` from `rt_staged`: the month's new or changed names and a removal marker per
/// current name the export no longer holds, compared with that month's current rows at `head` only
/// (an export is the whole month, root ADR-0349 §3). Returns (rows, of them markers).
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
