//! `VWorld` 필지고유번호변동연혁 (MK/30527) for `silver.parcel_number_change_history`, on `DuckDB`
//! (root ADR-0144 §4, ADR-0145, ADR-0559).
//!
//! It was the `load` command of the Spark job `vworld_parcel_number_change_history.py`. The
//! provider's official old PNU → new PNU record: one zip per 시도, each holding one `|`-delimited
//! UTF-8 text file. Its shape is read from `vworld-parcel-number-change-history.contract.json`; a
//! file whose header, member name or row width differs is refused whole and nothing of it is
//! loaded. A row whose date is no date, or whose PNU parts are not the contract's digits, is kept
//! and quarantined; more than the contract's share of such rows refuses the file.
//!
//! A file the provider uploads again repeats nearly every row the table already holds: only the
//! rows whose event the table does not hold yet are appended (compared null-safe), and the object
//! is recorded in the same commit even when none is (root ADR-0318).
//!
//! The stage's tables: `pnch_rows` (one object's rows as the table's columns) and `pnch_fresh`
//! (those the table does not hold).

use std::{collections::HashMap, io::Read as _, path::Path};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    data_go_kr_file_data_stage::{column_names, count, epoch, literal, py_space},
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage},
};

/// The table this load appends to.
pub const TABLE: &str = "silver.parcel_number_change_history";
const CONTRACT: &str = include_str!(
    "../../../../infra/lakehouse/contracts/vworld-parcel-number-change-history.contract.json"
);
/// What a row says, as opposed to where in which upload it was read: a row the table holds with
/// these values is not appended again (root ADR-0318).
const EVENT_COLUMNS: [&str; 9] = [
    "old_pnu",
    "new_pnu",
    "is_dong_level",
    "reason_code",
    "changed_on",
    "changed_on_raw",
    "quarantine_reason",
    "source_sigungu_code",
    "source_region_code",
];

/// The source contract's parts this load reads.
#[derive(Clone, Debug)]
pub struct HistorySource {
    member: regex::Regex,
    region_group: usize,
    encoding: String,
    delimiter: String,
    header: Vec<String>,
    old_parts: Vec<String>,
    new_parts: Vec<String>,
    widths: Vec<usize>,
    max_quarantined_rows: u64,
    max_quarantined_share: f64,
    min_rows: u64,
    key: regex::Regex,
}

fn strings(value: &Value, what: &str) -> anyhow::Result<Vec<String>> {
    value
        .as_array()
        .with_context(|| format!("the contract has no {what}"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .with_context(|| format!("{what} holds a value that is not text"))
        })
        .collect()
}

impl HistorySource {
    /// The contract compiled into the publisher.
    ///
    /// # Errors
    /// The contract is not version 1, or its PNU parts do not add up to 19 digits.
    pub fn compiled() -> anyhow::Result<Self> {
        let contract: Value = serde_json::from_str(CONTRACT)?;
        ensure!(
            contract["version"] == 1,
            "the parcel number change contract's version must be 1"
        );
        let text = |value: &Value, what: &str| -> anyhow::Result<String> {
            value
                .as_str()
                .map(str::to_owned)
                .with_context(|| format!("the contract has no {what}"))
        };
        let files = &contract["files"];
        let parts = &contract["pnu_parts"];
        let widths = parts["widths"]
            .as_array()
            .context("the contract has no pnu_parts.widths")?
            .iter()
            .map(|width| {
                width
                    .as_u64()
                    .and_then(|width| usize::try_from(width).ok())
                    .context("a PNU part width is no number")
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let old_parts = strings(&parts["old"], "pnu_parts.old")?;
        let new_parts = strings(&parts["new"], "pnu_parts.new")?;
        ensure!(
            widths.iter().sum::<usize>() == 19
                && widths.len() == old_parts.len()
                && widths.len() == new_parts.len(),
            "pnu_parts must name fields adding up to 19 digits"
        );
        Ok(Self {
            member: regex::Regex::new(&text(
                &files["member_name_pattern"],
                "member name pattern",
            )?)?,
            region_group: files["member_region_code_group"]
                .as_u64()
                .and_then(|group| usize::try_from(group).ok())
                .context("the contract has no files.member_region_code_group")?,
            encoding: text(&files["encoding"], "files.encoding")?,
            delimiter: text(&files["delimiter"], "files.delimiter")?,
            header: strings(&contract["header"], "header")?,
            old_parts,
            new_parts,
            widths,
            max_quarantined_rows: contract["quarantine"]["max_rows_any_file"]
                .as_u64()
                .context("the contract has no quarantine.max_rows_any_file")?,
            max_quarantined_share: contract["quarantine"]["max_share"]
                .as_f64()
                .context("the contract has no quarantine.max_share")?,
            min_rows: contract["bounds"]["min_rows_per_file"]
                .as_u64()
                .context("the contract has no bounds.min_rows_per_file")?,
            key: regex::Regex::new(&text(
                &contract["bronze_objects"]["key_pattern"],
                "bronze_objects.key_pattern",
            )?)?,
        })
    }

    /// The SHA-256 a Bronze key of file `file_key` names (root ADR-0152).
    ///
    /// The provider reuses file numbers, so only the checksum tells which upload a key holds: a
    /// key that names no checksum, or another file, is refused.
    ///
    /// # Errors
    /// The key is not a content-addressed key of `file_key`.
    pub fn content_key_checksum<'a>(
        &self,
        object_key: &'a str,
        file_key: &str,
    ) -> anyhow::Result<&'a str> {
        let found = self
            .key
            .captures(object_key)
            .filter(|found| found.name("file_key").map(|key| key.as_str()) == Some(file_key))
            .and_then(|found| found.name("sha256"));
        found
            .map(|checksum| checksum.as_str())
            .with_context(|| format!("{object_key:?} is not a content-addressed key of {file_key}"))
    }

    /// The 시도 code a province file's member name carries: the edition group (root ADR-0286).
    ///
    /// # Errors
    /// The name is not the contract's member name.
    pub fn region_code<'a>(&self, member: &'a str) -> anyhow::Result<&'a str> {
        self.member
            .captures(member)
            .and_then(|found| found.get(self.region_group))
            .map(|code| code.as_str())
            .with_context(|| format!("{member} does not match {}", self.member))
    }

    /// (member name, text) of the one text file the zip must hold.
    ///
    /// # Errors
    /// The bytes are not a zip, it holds no member or more than the one the contract names, or the
    /// member is not the contract's encoding.
    pub fn read_member(&self, raw: &[u8]) -> anyhow::Result<(String, String)> {
        ensure!(
            self.encoding == "utf-8",
            "this reader reads UTF-8 members, not {}",
            self.encoding
        );
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(raw))
            .map_err(|_| anyhow::anyhow!("the file is not a zip"))?;
        let mut files = Vec::new();
        for index in 0..archive.len() {
            let entry = archive.by_index(index)?;
            let name = entry.name()?.into_owned();
            if !name.ends_with('/') {
                files.push((index, name));
            }
        }
        ensure!(
            files.len() == 1 && self.member.is_match(&files[0].1),
            "expected one member matching {}, found {:?}",
            self.member,
            files
                .iter()
                .take(3)
                .map(|(_, name)| name)
                .collect::<Vec<_>>()
        );
        let (index, name) = files.remove(0);
        let mut bytes = Vec::new();
        archive.by_index(index)?.read_to_end(&mut bytes)?;
        let text = String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{name} is not utf-8"))?;
        Ok((name, text))
    }

    /// Every distinct data row as a record, in file order (`parse_rows`).
    ///
    /// A row repeated exactly within the file is kept once, at its first line, and
    /// `duplicate_count` says how many times the file held it; rows that differ only in date or
    /// reason are different rows. A date later than the day after `collected_on` is not a change
    /// the provider can have recorded: quarantined.
    ///
    /// # Errors
    /// The header is not the contract's, or a line has another number of fields.
    pub fn parse_rows(
        &self,
        text: &str,
        collected_on: NaiveDate,
    ) -> anyhow::Result<Vec<HistoryRow>> {
        let delimiter = self.delimiter.as_str();
        let mut lines = py_splitlines(text).into_iter();
        let header = lines.next().map(|line| {
            line.trim_start_matches('\u{feff}')
                .split(delimiter)
                .collect::<Vec<_>>()
        });
        ensure!(
            header.as_deref().is_some_and(|found| found == self.header),
            "the header is not {}",
            self.header.join(delimiter)
        );
        let index = |name: &str| self.header.iter().position(|header| header == name);
        let position =
            |name: &str| index(name).with_context(|| format!("the header has no {name}"));
        let side = |names: &[String]| {
            names
                .iter()
                .map(|name| position(name))
                .collect::<anyhow::Result<Vec<_>>>()
        };
        let (old, new) = (side(&self.old_parts)?, side(&self.new_parts)?);
        let (reason_at, date_at, sigungu_at) = (
            position("LAND_MOV_RSN_CD")?,
            position("LAND_MOV_YMD")?,
            position("COL_ADM_SECT_CD")?,
        );
        let strip = |cell: &str| cell.trim_matches(py_space).to_owned();
        let filled = |cell: String| Some(cell).filter(|cell| !cell.is_empty());
        let mut rows: Vec<HistoryRow> = Vec::new();
        let mut seen: HashMap<Vec<String>, usize> = HashMap::new();
        for (offset, line) in lines.enumerate() {
            let line_no = offset + 2;
            if line.trim_matches(py_space).is_empty() {
                continue;
            }
            let cells = line.split(delimiter).collect::<Vec<_>>();
            ensure!(
                cells.len() == self.header.len(),
                "line {line_no} has {} fields, the header {}",
                cells.len(),
                self.header.len()
            );
            let key = cells.iter().map(|cell| strip(cell)).collect::<Vec<_>>();
            if let Some(first) = seen.get(&key) {
                rows[*first].duplicate_count += 1;
                continue;
            }
            let mut malformed = false;
            let mut pnu = |parts: &[usize]| {
                let mut joined = String::with_capacity(19);
                for (at, width) in parts.iter().zip(&self.widths) {
                    let value = &key[*at];
                    malformed |= value.chars().count() != *width
                        || !value.chars().all(|digit| digit.is_ascii_digit());
                    joined.push_str(value);
                }
                joined
            };
            let (old_pnu, new_pnu) = (pnu(&old), pnu(&new));
            let raw_date = key[date_at].clone();
            let changed_on = date_or_none(&raw_date);
            let implausible = changed_on.is_some_and(|day| day > collected_on + Duration::days(1));
            let quarantine_reason = if implausible {
                Some("implausible_land_mov_ymd")
            } else if malformed {
                Some("malformed_pnu")
            } else if changed_on.is_none() {
                Some("unparseable_land_mov_ymd")
            } else {
                None
            };
            seen.insert(key.clone(), rows.len());
            rows.push(HistoryRow {
                is_dong_level: !malformed && is_dong_level(&old_pnu) && is_dong_level(&new_pnu),
                old_pnu,
                new_pnu,
                reason_code: filled(key[reason_at].clone()),
                changed_on,
                changed_on_raw: filled(raw_date),
                quarantine_reason,
                source_sigungu_code: filled(key[sigungu_at].clone()),
                source_line_number: i32::try_from(line_no)?,
                duplicate_count: 1,
            });
        }
        Ok(rows)
    }

    /// Refuses a file below the floor or with more quarantined rows than the contract allows: then
    /// the layout moved, not a few rows.
    ///
    /// # Errors
    /// The file is out of its bounds.
    pub fn check_rows(&self, rows: &[HistoryRow], member: &str) -> anyhow::Result<()> {
        let total = u64::try_from(rows.len())?;
        ensure!(
            total >= self.min_rows,
            "{member}: {total} rows, below the contract's floor"
        );
        let quarantined = u64::try_from(
            rows.iter()
                .filter(|row| row.quarantine_reason.is_some())
                .count(),
        )?;
        #[allow(clippy::cast_precision_loss)]
        let over_share = quarantined as f64 / total as f64 > self.max_quarantined_share;
        ensure!(
            !(quarantined > self.max_quarantined_rows && over_share),
            "{member}: {quarantined} of {total} rows are quarantined (no date or a malformed PNU), above the contract's share; the layout moved"
        );
        Ok(())
    }
}

/// 대장구분 0 and 본번·부번 0000: the row moves a whole 법정동, not one parcel.
fn is_dong_level(pnu: &str) -> bool {
    pnu.len() == 19 && pnu.ends_with("000000000")
}

/// A `YYYYMMDD` that names a day.
fn date_or_none(value: &str) -> Option<NaiveDate> {
    (value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| NaiveDate::parse_from_str(value, "%Y%m%d").ok())
        .flatten()
}

/// The lines of `text` as Python's `str.splitlines` gives them: split at `\n`, `\r\n`, `\r` and
/// the other line boundaries Python knows, without the boundary and without a last empty line.
fn py_splitlines(text: &str) -> Vec<&str> {
    const BOUNDARIES: [char; 9] = [
        '\n', '\r', '\u{0b}', '\u{0c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}',
    ];
    let boundary = |character: char| BOUNDARIES.contains(&character) || character == '\u{2029}';
    let mut lines = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let Some(at) = rest.find(boundary) else {
            lines.push(rest);
            break;
        };
        lines.push(&rest[..at]);
        let after = &rest[at..];
        let width = if after.starts_with("\r\n") {
            2
        } else {
            after.chars().next().map_or(0, char::len_utf8)
        };
        rest = &after[width..];
    }
    lines
}

/// One distinct data row of a file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryRow {
    /// The earlier 19-digit PNU, its parts joined as written.
    pub old_pnu: String,
    /// The later PNU.
    pub new_pnu: String,
    /// Whether the row moves a whole 법정동.
    pub is_dong_level: bool,
    /// 토지이동사유코드, when written.
    pub reason_code: Option<String>,
    /// 토지이동일자 as a day; none when it is no date.
    pub changed_on: Option<NaiveDate>,
    /// 토지이동일자 as written.
    pub changed_on_raw: Option<String>,
    /// Why the row is never evidence, when it is not.
    pub quarantine_reason: Option<&'static str>,
    /// The collecting 시군구 code, when written.
    pub source_sigungu_code: Option<String>,
    /// The row's first line in the file (the header is line 1).
    pub source_line_number: i32,
    /// How many times the file held the row exactly.
    pub duplicate_count: i32,
}

/// One object of a handoff, read and checked.
#[derive(Clone, Debug)]
pub struct HandoffObject {
    /// Its Bronze key: the load unit the table records.
    pub object_key: String,
    /// The member's name.
    pub member: String,
    /// `<file key>@<provider's updated date>`: the edition id.
    pub source_snapshot_id: String,
    /// The day the provider says it updated the file.
    pub provider_updated_on: NaiveDate,
    /// Its rows.
    pub rows: Vec<HistoryRow>,
}

/// Every object of one handoff, each checked again against its checksum (`handoff_rows`).
///
/// # Errors
/// The handoff is not the JSON the collection wrote, a file is not the bytes the handoff checked,
/// a key does not name those bytes, or a file is refused by its shape or its bounds.
pub fn read_handoff(
    source: &HistorySource,
    handoff_dir: &Path,
) -> anyhow::Result<Vec<HandoffObject>> {
    let handoff: Value = serde_json::from_slice(
        &std::fs::read(handoff_dir.join("handoff.json"))
            .with_context(|| format!("cannot read the handoff in {}", handoff_dir.display()))?,
    )?;
    let text = |value: &Value, what: &str| -> anyhow::Result<String> {
        value
            .as_str()
            .map(str::to_owned)
            .with_context(|| format!("the handoff has no {what}"))
    };
    let staged_at = text(&handoff["staged_at_utc"], "staged_at_utc")?;
    // `datetime.fromisoformat(...).date()`: the day as written, whatever its offset.
    let collected_on = NaiveDate::parse_from_str(staged_at.get(..10).unwrap_or(""), "%Y-%m-%d")
        .with_context(|| format!("staged_at_utc {staged_at:?} names no day"))?;
    let mut objects = Vec::new();
    for item in handoff["objects"]
        .as_array()
        .context("the handoff lists no objects")?
    {
        let local = text(&item["local_path"], "local_path")?;
        ensure!(
            !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
            "the local path {local:?} leaves the handoff directory"
        );
        let object_key = text(&item["object_key"], "object_key")?;
        let file_key = text(&item["file_key"], "file_key")?;
        let checksum = text(&item["checksum_sha256"], "checksum_sha256")?;
        let raw = std::fs::read(handoff_dir.join(&local))
            .with_context(|| format!("cannot read {local}"))?;
        let digest = Sha256::digest(&raw)
            .iter()
            .fold(String::new(), |mut hex, byte| {
                use std::fmt::Write as _;
                // Writing to a String cannot fail.
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        ensure!(
            digest == checksum,
            "{local} is not the file the handoff checked"
        );
        if source.content_key_checksum(&object_key, &file_key)? != checksum {
            bail!("{object_key} does not name the bytes the handoff holds");
        }
        let (member, text_body) = source.read_member(&raw)?;
        let rows = source.parse_rows(&text_body, collected_on)?;
        source.check_rows(&rows, &member)?;
        let updated_at = text(&item["updated_at"], "updated_at")?;
        objects.push(HandoffObject {
            provider_updated_on: NaiveDate::parse_from_str(&updated_at, "%Y-%m-%d")
                .with_context(|| format!("updated_at {updated_at:?} is no day"))?,
            source_snapshot_id: format!("{file_key}@{updated_at}"),
            object_key,
            member,
            rows,
        });
    }
    Ok(objects)
}

/// `pnch_rows`: the object's rows as the table's columns.
///
/// # Errors
/// The member's name carries no region code, or `DuckDB` fails.
pub fn stage_object(
    stage: &DuckDbParquetStage,
    source: &HistorySource,
    contract: &LakehouseTableContract,
    object: &HandoffObject,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let region = source.region_code(&object.member)?;
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE pnch_read (old_pnu VARCHAR NOT NULL, new_pnu VARCHAR NOT NULL, \
         is_dong_level BOOLEAN NOT NULL, reason_code VARCHAR, changed_on VARCHAR, changed_on_raw VARCHAR, \
         quarantine_reason VARCHAR, source_sigungu_code VARCHAR, source_line_number INTEGER NOT NULL, \
         duplicate_count INTEGER NOT NULL)",
    )?;
    {
        let mut appender = connection.appender("pnch_read")?;
        for row in &object.rows {
            appender.append_row(duckdb::params![
                row.old_pnu,
                row.new_pnu,
                row.is_dong_level,
                row.reason_code,
                row.changed_on.map(|day| day.format("%Y-%m-%d").to_string()),
                row.changed_on_raw,
                row.quarantine_reason,
                row.source_sigungu_code,
                row.source_line_number,
                row.duplicate_count
            ])?;
        }
        appender.flush()?;
    }
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE pnch_rows AS SELECT {columns} FROM (\
             SELECT * REPLACE (CAST(changed_on AS DATE) AS changed_on), {member} AS source_file_name, \
                    {region} AS source_region_code, {record} AS source_record_id, {snapshot} AS source_snapshot_id, \
                    DATE '{updated}' AS provider_updated_on, {at} AS ingested_at_utc FROM pnch_read)",
            columns = column_names(contract).join(", "),
            member = literal(&object.member),
            region = literal(region),
            record = literal(&object.object_key),
            snapshot = literal(&object.source_snapshot_id),
            updated = object.provider_updated_on.format("%Y-%m-%d"),
            at = epoch(ingested_at),
        ))
        .context("DuckDB failed to stage the object's rows")?;
    ensure!(
        count(stage, "pnch_rows")? == u64::try_from(object.rows.len())?,
        "the staged object lost rows"
    );
    Ok(())
}

/// `pnch_fresh`: the rows of `pnch_rows` whose event the table at `head` does not hold, compared
/// null-safe (a quarantined row has no date and an ordinary one no reason). Returns how many.
///
/// # Errors
/// `DuckDB` fails the read.
pub fn unheld(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
) -> anyhow::Result<u64> {
    let table = match head {
        Some(head) => {
            let pinned = crate::duckdb_parquet_stage::table_at(contract.table_name, head);
            with_contract_columns(stage.connection(), contract, &pinned)?
        }
        None => "(SELECT * FROM pnch_rows WHERE false)".to_owned(),
    };
    unheld_in(stage, &table)
}

/// [`unheld`] against `held`, a relation of the table's columns.
fn unheld_in(stage: &DuckDbParquetStage, held: &str) -> anyhow::Result<u64> {
    let events = EVENT_COLUMNS.join(", ");
    let same = EVENT_COLUMNS
        .iter()
        .map(|column| format!("h.{column} IS NOT DISTINCT FROM r.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE pnch_held AS SELECT DISTINCT {events} FROM {held};
             CREATE OR REPLACE TEMP TABLE pnch_fresh AS SELECT r.* FROM pnch_rows r \
             WHERE NOT EXISTS (SELECT 1 FROM pnch_held h WHERE {same});"
        ))
        .context("DuckDB failed to compare the rows with the table's")?;
    count(stage, "pnch_fresh")
}

#[cfg(test)]
mod tests;
