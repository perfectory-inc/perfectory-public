//! The lakehouse side of the 법정동 code-change pairing, on `DuckDB` (root ADR-0143, ADR-0145,
//! ADR-0559).
//!
//! The pairing's rules are plain Python (`legal_dong_code_change_pairs.py`); they read and write
//! the lakehouse through files in the run's work folder. This stage writes the files the rules
//! read (the full-table snapshot, the changes already recorded, the official parcel-number links
//! of the pairing window, the parcels of the editions that bracket a change) and stages the rows
//! the rules decided to append to `reference.legal_dong_code_change`. It was the Spark session of
//! that job. No rule lives here: which parcels are read is the request file's.
//!
//! The file names are the Python module's constants of the same names.

use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufWriter, Write as _},
    path::Path,
};

use anyhow::{ensure, Context};
use chrono::NaiveDate;
use lakehouse_domain::LakehouseTableContract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    data_go_kr_file_data_stage::{column_names, count, literal},
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage, LAKEHOUSE_CATALOG},
};

/// The full-table snapshots the pairing reads one of.
pub const SNAPSHOT_TABLE: &str = "reference.legal_dong_code_snapshot";
/// The one record of code changes: read, and the only table the pairing appends to.
pub const CHANGE_TABLE: &str = "reference.legal_dong_code_change";
/// The official parcel-number history whose links are the official step.
pub const HISTORY_TABLE: &str = "silver.parcel_number_change_history";
/// The parcel editions whose 지번 sets bracket a change.
pub const PARCEL_TABLE: &str = "silver.parcel_boundaries";

/// `snapshot.json`: the rows of one full-table snapshot.
pub const SNAPSHOT_FILE: &str = "snapshot.json";
/// `recorded.json`: every recorded change.
pub const RECORDED_FILE: &str = "recorded.json";
/// `official-links.txt`: `OLD_PNU NEW_PNU YYYYMMDD` per line.
pub const LINKS_FILE: &str = "official-links.txt";
/// `parcel-requests.json`: the parcel reads the rules ask for.
pub const REQUESTS_FILE: &str = "parcel-requests.json";
/// `parcels.json`: the answer to each request, in order.
pub const PARCELS_FILE: &str = "parcels.json";
/// `append.jsonl`: the rows the rules decided to append, in load order.
pub const APPEND_FILE: &str = "append.jsonl";
/// `held.json`: the change table as it stands after the append.
pub const HELD_FILE: &str = "held.json";
/// `appended.json`: what the append did and the snapshot it left.
pub const APPENDED_FILE: &str = "appended.json";

const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/code-go-kr-legal-dong.contract.json");

/// The pairing window's first day (`pairing.floor_date`, `YYYYMMDD`) of the source contract.
///
/// # Errors
/// The contract names no such day.
pub fn pairing_floor() -> anyhow::Result<NaiveDate> {
    let contract: Value = serde_json::from_str(SOURCE_CONTRACT)?;
    let floor = contract["pairing"]["floor_date"]
        .as_str()
        .context("the code.go.kr contract has no pairing.floor_date")?;
    NaiveDate::parse_from_str(floor, "%Y%m%d")
        .with_context(|| format!("pairing.floor_date {floor:?} is not YYYYMMDD"))
}

/// `contract`'s table at the snapshot `head`, with every contract column: the relation the
/// exports read.
///
/// # Errors
/// `DuckDB` cannot describe the table at that snapshot.
pub fn at_head(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: i64,
) -> anyhow::Result<String> {
    let pinned = format!(
        "{LAKEHOUSE_CATALOG}.{} AT (VERSION => {head})",
        contract.table_name
    );
    with_contract_columns(stage.connection(), contract, &pinned)
}

/// Writes the rows of `query` to `path`, one JSON object per row (`to_json`): as one JSON array,
/// or one object per line. Returns how many. The file is written here, not by `DuckDB`: the
/// stage's `DuckDB` reads and writes no file outside its own directory.
fn write_rows(
    stage: &DuckDbParquetStage,
    query: &str,
    path: &Path,
    array: bool,
    mut seen: impl FnMut(&str),
) -> anyhow::Result<u64> {
    let mut file = BufWriter::new(
        File::create(path).with_context(|| format!("cannot write {}", path.display()))?,
    );
    let mut rows = 0_u64;
    if array {
        file.write_all(b"[")?;
    }
    stage.each_json_row(query, |row| {
        if array && rows > 0 {
            file.write_all(b",\n")?;
        }
        file.write_all(row.as_bytes())?;
        if !array {
            file.write_all(b"\n")?;
        }
        seen(&row);
        rows += 1;
        Ok(())
    })?;
    if array {
        file.write_all(b"]\n")?;
    }
    file.flush()
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(rows)
}

/// `snapshot.json`: the six columns the rules read of the snapshot taken on `snapshot_date` from
/// the Bronze object `record`. Returns the row count.
///
/// # Errors
/// `DuckDB` fails the read or the write.
pub fn export_snapshot(
    stage: &DuckDbParquetStage,
    source: &str,
    snapshot_date: NaiveDate,
    record: &str,
    directory: &Path,
) -> anyhow::Result<u64> {
    write_rows(
        stage,
        &format!(
            "SELECT region_cd, full_name, status, parent_cd, created_date, abolished_date FROM {source} \
             WHERE snapshot_date = DATE '{date}' AND source_record_id = {record}",
            date = snapshot_date.format("%Y-%m-%d"),
            record = literal(record),
        ),
        &directory.join(SNAPSHOT_FILE),
        true,
        |_| {},
    )
}

/// Every row of the change table as one JSON array in `file`; returns the row count.
///
/// `recorded.json` before the run, `held.json` after its append. The instant is ISO text. A
/// table nothing was written to (`source` is `None`) is an empty array.
///
/// # Errors
/// `DuckDB` fails the read or the write.
pub fn export_changes(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    source: Option<&str>,
    directory: &Path,
    file: &str,
) -> anyhow::Result<u64> {
    let path = directory.join(file);
    let Some(source) = source else {
        std::fs::write(&path, "[]\n")
            .with_context(|| format!("cannot write {}", path.display()))?;
        return Ok(0);
    };
    let columns = contract
        .columns
        .iter()
        .map(|column| match column.logical_type {
            // Read as its microseconds: no time zone database is loaded in the stage.
            "timestamp" => format!(
                "strftime(make_timestamp(epoch_us({name})), '%Y-%m-%dT%H:%M:%SZ') AS {name}",
                name = column.name
            ),
            _ => column.name.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    write_rows(
        stage,
        &format!("SELECT {columns} FROM {source}"),
        &path,
        true,
        |_| {},
    )
}

/// `official-links.txt`: one `OLD_PNU NEW_PNU YYYYMMDD` line per link; returns the line count.
///
/// The distinct links of every row of the history dated on or after `floor` and not quarantined:
/// the fold of `vworld_parcel_number_change_history.official_links`, the table's one reader. The
/// table is events (root ADR-0318 §8): a load appends only the events it does not hold, so no
/// file is the whole history and none is picked.
///
/// # Errors
/// `DuckDB` fails the read or the write.
pub fn export_official_links(
    stage: &DuckDbParquetStage,
    source: &str,
    floor: NaiveDate,
    directory: &Path,
) -> anyhow::Result<u64> {
    let path = directory.join(LINKS_FILE);
    let mut file = BufWriter::new(
        File::create(&path).with_context(|| format!("cannot write {}", path.display()))?,
    );
    let mut rows = 0_u64;
    stage.each_json_row(
        &format!(
            "SELECT DISTINCT old_pnu || ' ' || new_pnu || ' ' || strftime(changed_on, '%Y%m%d') AS link \
             FROM {source} WHERE changed_on >= DATE '{floor}' AND coalesce(quarantine_reason, '') = '' ORDER BY 1",
            floor = floor.format("%Y-%m-%d"),
        ),
        |row| {
            let row: Value = serde_json::from_str(&row)?;
            let link = row["link"].as_str().context("a link is no text")?;
            writeln!(file, "{link}")?;
            rows += 1;
            Ok(())
        },
    )?;
    file.flush()
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(rows)
}

/// One parcel read the rules ask for (`parcel-requests.json`).
#[derive(Debug, Deserialize)]
pub struct ParcelRequest {
    /// The edition's name, for the answer's file.
    pub edition: String,
    /// `source_snapshot_id` of the edition's rows.
    pub snapshot_id: String,
    /// The 법정동 codes whose parcels are read.
    pub codes: Vec<String>,
    /// The earlier request whose parcels' 지번 bound this one, when it is the later edition.
    pub lots_of: Option<usize>,
}

/// The answer to one request (`parcels.json`).
#[derive(Debug, Serialize)]
pub struct ParcelAnswer {
    /// Whether the parcel table holds any row of the edition: an edition not loaded is not "no
    /// parcels".
    pub present: bool,
    /// The file of its rows (`pnu`, `jibun`, `wkb` as base64 per line), when any were asked for.
    pub file: Option<String>,
    /// How many rows the file holds.
    pub rows: u64,
}

/// Answers every request of `parcel-requests.json` and writes `parcels.json`.
///
/// Each answer holds the edition's parcels with a boundary under the request's codes; for a later
/// edition, only those whose 지번 the earlier request's answer holds (a superset of what the rules
/// keep: they apply their own filter to the file). The 지목 and the area are the rules' to read
/// from `jibun` and the boundary.
///
/// # Errors
/// The request file is not the rules', a request names a later one, or `DuckDB` fails.
pub fn export_parcels(
    stage: &DuckDbParquetStage,
    source: &str,
    directory: &Path,
) -> anyhow::Result<Vec<ParcelAnswer>> {
    let requests: Vec<ParcelRequest> = serde_json::from_slice(
        &std::fs::read(directory.join(REQUESTS_FILE))
            .with_context(|| format!("cannot read {REQUESTS_FILE}"))?,
    )
    .with_context(|| format!("{REQUESTS_FILE} is not the rules' request list"))?;
    let connection = stage.connection();
    let mut answers: Vec<ParcelAnswer> = Vec::with_capacity(requests.len());
    // The requests a later one is bound by: their answers' 지번 are kept as a table.
    let binding = requests
        .iter()
        .filter_map(|request| request.lots_of)
        .collect::<BTreeSet<_>>();
    for (index, request) in requests.iter().enumerate() {
        let edition = literal(&request.snapshot_id);
        let present = connection
            .prepare(&format!(
                "SELECT 1 FROM {source} WHERE source_snapshot_id = {edition} LIMIT 1"
            ))?
            .exists([])?;
        let bound = match request.lots_of {
            None => Some(String::new()),
            Some(earlier) => {
                ensure!(earlier < index, "request {index} is bound by a later one");
                // No file: the earlier edition is not loaded or held nothing, so no 지번 is asked for.
                answers[earlier].file.as_ref().map(|_| {
                    format!(
                        " AND regexp_full_match(pnu, '[0-9]{{19}}') \
                         AND substr(pnu, 11, 9) IN (SELECT lot FROM pair_lots_{earlier})"
                    )
                })
            }
        };
        let (Some(bound), true, false) = (bound, present, request.codes.is_empty()) else {
            answers.push(ParcelAnswer {
                present,
                file: None,
                rows: 0,
            });
            continue;
        };
        connection
            .execute_batch("CREATE OR REPLACE TEMP TABLE pair_codes (code VARCHAR NOT NULL)")?;
        {
            let mut appender = connection.appender("pair_codes")?;
            for code in &request.codes {
                appender.append_row([code])?;
            }
            appender.flush()?;
        }
        let file = format!("parcels-{index}-{}.jsonl", request.edition);
        let mut lots = BTreeSet::new();
        let keep_lots = binding.contains(&index);
        let rows = write_rows(
            stage,
            &format!(
                "SELECT pnu, jibun, to_base64(geometry_wkb) AS wkb FROM {source} \
                 WHERE source_snapshot_id = {edition} AND geometry_wkb IS NOT NULL \
                 AND substr(pnu, 1, 10) IN (SELECT code FROM pair_codes){bound}"
            ),
            &directory.join(&file),
            false,
            |row| {
                if keep_lots {
                    if let Some(lot) = lot_of(row) {
                        lots.insert(lot.to_owned());
                    }
                }
            },
        )
        .with_context(|| format!("cannot export the parcels of {}", request.edition))?;
        if keep_lots {
            connection.execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE pair_lots_{index} (lot VARCHAR NOT NULL)"
            ))?;
            let mut appender = connection.appender(&format!("pair_lots_{index}"))?;
            for lot in &lots {
                appender.append_row([lot])?;
            }
            appender.flush()?;
        }
        answers.push(ParcelAnswer {
            present,
            file: Some(file),
            rows,
        });
    }
    let path = directory.join(PARCELS_FILE);
    std::fs::write(&path, serde_json::to_vec(&answers)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(answers)
}

/// The 지번 (ledger kind, 본번, 부번: the PNU's last nine digits) of an exported parcel row, when
/// its PNU is 19 digits: what bounds a later edition's read.
fn lot_of(row: &str) -> Option<&str> {
    let pnu = row.strip_prefix("{\"pnu\":\"")?.get(..19)?;
    pnu.bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| &pnu[10..])
}

/// One load of `append.jsonl`: a steward decision file, or the run's changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendUnit {
    /// Its `derivation_run_id`: the load unit the table records.
    pub unit: String,
    /// Whether its rows are steward rows (else the run's changes).
    pub steward: bool,
    /// How many rows it holds.
    pub rows: u64,
}

/// `pair_append`: the rows of `append.jsonl` as the table's columns (and `seq`, `what`); returns
/// the load units in the order the rules wrote them.
///
/// # Errors
/// A unit holds both steward rows and changes, or `DuckDB` fails.
pub fn stage_append(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    directory: &Path,
) -> anyhow::Result<Vec<AppendUnit>> {
    let path = directory.join(APPEND_FILE);
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let texts = contract
        .columns
        .iter()
        .filter(|column| column.logical_type != "timestamp")
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let connection = stage.connection();
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE pair_append_read ({}, seq BIGINT NOT NULL, \
             recorded_at_us BIGINT NOT NULL, what VARCHAR NOT NULL)",
            texts
                .iter()
                .map(|name| format!("{name} VARCHAR"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .context("DuckDB failed to make the table of rows to append")?;
    {
        let mut appender = connection.appender("pair_append_read")?;
        for (number, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let row: Value = serde_json::from_str(line)
                .with_context(|| format!("{APPEND_FILE}:{} is not JSON", number + 1))?;
            let mut values: Vec<duckdb::types::Value> = texts
                .iter()
                .map(|name| {
                    row[*name]
                        .as_str()
                        .map_or(duckdb::types::Value::Null, |text| {
                            duckdb::types::Value::Text(text.to_owned())
                        })
                })
                .collect();
            for name in ["seq", "recorded_at_us"] {
                values.push(duckdb::types::Value::BigInt(
                    row[name]
                        .as_i64()
                        .with_context(|| format!("{APPEND_FILE}:{} names no {name}", number + 1))?,
                ));
            }
            values.push(duckdb::types::Value::Text(
                row["what"]
                    .as_str()
                    .with_context(|| format!("{APPEND_FILE}:{} names no what", number + 1))?
                    .to_owned(),
            ));
            appender.append_row(duckdb::appender_params_from_iter(values))?;
        }
        appender.flush()?;
    }
    let columns = contract
        .columns
        .iter()
        .map(|column| match column.logical_type {
            "timestamp" => format!("to_timestamp(recorded_at_us // 1000000) AS {}", column.name),
            _ => column.name.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE pair_append AS SELECT {columns}, seq, what FROM pair_append_read"
        ))
        .context("DuckDB failed to read the rows to append")?;
    append_units(stage)
}

/// The load units of `pair_append`, in the order the rules wrote them.
fn append_units(stage: &DuckDbParquetStage) -> anyhow::Result<Vec<AppendUnit>> {
    let units = stage
        .connection()
        .prepare(
            "SELECT derivation_run_id, min(what), max(what), count(*) FROM pair_append \
             GROUP BY derivation_run_id ORDER BY min(seq)",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    units
        .into_iter()
        .map(|(unit, first, last, rows)| {
            let unit = unit.context("a row to append names no derivation_run_id")?;
            ensure!(
                first == last && matches!(first.as_deref(), Some("steward" | "change")),
                "the load unit {unit} is not steward rows or changes alone"
            );
            Ok(AppendUnit {
                steward: first.as_deref() == Some("steward"),
                rows: u64::try_from(rows)?,
                unit,
            })
        })
        .collect()
}

/// `pair_unit`: the rows of one load unit of `pair_append`, as the table's columns.
///
/// # Errors
/// `DuckDB` fails, or the unit lost rows.
pub fn stage_unit(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    unit: &AppendUnit,
) -> anyhow::Result<()> {
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE pair_unit AS SELECT {columns} FROM pair_append \
             WHERE derivation_run_id = {unit} ORDER BY seq",
            columns = column_names(contract).join(", "),
            unit = literal(&unit.unit),
        ))
        .context("DuckDB failed to stage a load unit")?;
    ensure!(
        count(stage, "pair_unit")? == unit.rows,
        "the load unit {} lost rows",
        unit.unit
    );
    Ok(())
}

#[cfg(test)]
mod tests;
