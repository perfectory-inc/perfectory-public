//! One snapshot of the official 법정동 code list for `reference.legal_dong_code_snapshot`, on
//! `DuckDB` (root ADR-0143 §2, ADR-0559).
//!
//! It was the Spark job `legal_dong_code_snapshot_to_reference.py` and the full-table reader of
//! `code_go_kr_legal_dong.py`. The daily collector lands the code.go.kr 전체 표 response in Bronze;
//! this reads the one table whose header row is the contract's, a row per code, and refuses any
//! other shape: a reordered or renamed column, a malformed row, a repeated code, a table under the
//! contract's floor or shrunk from the previous code.go.kr snapshot the table holds. Nothing is
//! skipped: a dropped code later reads as an abolished one.
//!
//! The stage's table: `legal_dong_snapshot` (the snapshot as the table's columns).

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;

use crate::{
    code_go_kr_legal_dong_contract::CONTRACT_JSON,
    data_go_kr_file_data_stage::{column_names, count, epoch, literal, py_space},
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage},
};

/// The table this load appends to.
pub const TABLE: &str = "reference.legal_dong_code_snapshot";
/// The `status` of a code in force, as the snapshot table writes it (`code_go_kr_legal_dong.EXISTS`).
pub const EXISTS: &str = "존재";
/// The Bronze source of the collector's full-table objects.
pub const CODE_GO_KR_TABLE_SOURCE: &str = "codegokr__legal_dong_code";
/// The Bronze prefix of the collector's full-table objects. The shrink bound compares a
/// code.go.kr snapshot only with earlier code.go.kr snapshots: another source lists another set.
pub const CODE_GO_KR_TABLE_PREFIX: &str = "bronze/source=codegokr__legal_dong_code/";

/// The prefixes a code.go.kr snapshot's `source_record_id` starts with: the source's, and those of
/// the ids it had before the Bronze epoch renamed it (`codegokr__legal_dong_code_table`, root
/// ADR-0626 §4), which the rows loaded before the epoch keep until the table is rebuilt from Bronze.
#[must_use]
pub fn code_go_kr_table_prefixes() -> Vec<String> {
    std::iter::once(CODE_GO_KR_TABLE_SOURCE)
        .chain(collection_domain::former_source_slugs(
            CODE_GO_KR_TABLE_SOURCE,
        ))
        .map(|slug| format!("bronze/source={slug}/"))
        .collect()
}
/// The fields of a full-table row, one per contract header in the same order.
const FULL_TABLE_FIELDS: usize = 11;

// --- HTML tables ---------------------------------------------------------------------------------

/// One `<table>` of a page: its header texts (the last row of its `<thead>`) and its rows of
/// cell texts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HtmlTable {
    /// The header row, when the table closed a `<thead>` that held one.
    pub head: Option<Vec<String>>,
    /// The rows outside the `<thead>`.
    pub rows: Vec<Vec<String>>,
    row: Option<Vec<String>>,
    cell: Option<String>,
    in_head: bool,
    head_rows: Vec<Vec<String>>,
}

impl HtmlTable {
    fn end_cell(&mut self) -> anyhow::Result<()> {
        let Some(cell) = self.cell.take() else {
            return Ok(());
        };
        // `" ".join("".join(cell).split())`: the references read, the white space folded.
        let text = unescape(&cell)?
            .split(py_space)
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        self.row.get_or_insert_with(Vec::new).push(text);
        Ok(())
    }

    fn end_row(&mut self) -> anyhow::Result<()> {
        self.end_cell()?;
        if let Some(row) = self.row.take().filter(|row| !row.is_empty()) {
            if self.in_head {
                self.head_rows.push(row);
            } else {
                self.rows.push(row);
            }
        }
        Ok(())
    }
}

/// The character references of a cell's text, as `html.unescape` reads the ones the site writes.
///
/// Numeric references and `nbsp`, `amp`, `lt`, `gt`, `quot` (with or without the semicolon, as
/// HTML allows for those) and `apos;`. Another name ending in a semicolon is refused: this reader
/// has no table of every HTML name, and a cell read wrong is a code read wrong.
fn unescape(text: &str) -> anyhow::Result<String> {
    const LEGACY: [(&str, char); 5] = [
        ("nbsp", '\u{a0}'),
        ("amp", '&'),
        ("lt", '<'),
        ("gt", '>'),
        ("quot", '"'),
    ];
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        if let Some(number) = after.strip_prefix('#') {
            let (radix, digits_from) = match number.as_bytes().first() {
                Some(b'x' | b'X') => (16, 1),
                _ => (10, 0),
            };
            let digits = number[digits_from..]
                .bytes()
                .take_while(|byte| char::from(*byte).is_digit(radix))
                .count();
            if digits == 0 {
                out.push('&');
                rest = after;
                continue;
            }
            let value = &number[digits_from..digits_from + digits];
            let character = u32::from_str_radix(value, radix)
                .ok()
                .and_then(char::from_u32)
                .with_context(|| format!("&#{value} is no character"))?;
            out.push(character);
            let end = digits_from + digits;
            let after_digits = &number[end..];
            rest = after_digits.strip_prefix(';').unwrap_or(after_digits);
            continue;
        }
        if let Some((name, character)) = LEGACY.iter().find(|(name, _)| after.starts_with(name)) {
            out.push(*character);
            let end = &after[name.len()..];
            rest = end.strip_prefix(';').unwrap_or(end);
            continue;
        }
        if let Some(end) = after.strip_prefix("apos;") {
            out.push('\'');
            rest = end;
            continue;
        }
        let name = after.bytes().take_while(u8::is_ascii_alphanumeric).count();
        ensure!(
            name == 0 || !after[name..].starts_with(';'),
            "a cell holds &{}; and this reader does not know that name",
            &after[..name]
        );
        out.push('&');
        rest = after;
    }
    out.push_str(rest);
    Ok(out)
}

/// The end of the tag that opens at `text[0] == '<'`: past its `>`, quotes respected.
fn tag_end(text: &str) -> Option<usize> {
    let mut quote = None;
    for (at, byte) in text.bytes().enumerate() {
        match (quote, byte) {
            (None, b'"' | b'\'') => quote = Some(byte),
            (Some(open), _) if open == byte => quote = None,
            (None, b'>') => return Some(at + 1),
            _ => {}
        }
    }
    None
}

/// Every `<table>` of a page.
///
/// The site leaves `<tr>` unclosed and nests layout tables, so a row ends at the next `<tr>` or at
/// the end of its own table, and a cell belongs to the innermost open table only. Comments,
/// declarations and the text of `<script>` and `<style>` are not read.
///
/// # Errors
/// A cell holds a character reference this reader does not know.
pub fn html_tables(text: &str) -> anyhow::Result<Vec<HtmlTable>> {
    let mut open: Vec<HtmlTable> = Vec::new();
    let mut done = Vec::new();
    let mut rest = text;
    let data = |open: &mut Vec<HtmlTable>, data: &str| {
        if let Some(cell) = open.last_mut().and_then(|table| table.cell.as_mut()) {
            cell.push_str(data);
        }
    };
    while let Some(at) = rest.find('<') {
        data(&mut open, &rest[..at]);
        rest = &rest[at..];
        if let Some(comment) = rest.strip_prefix("<!--") {
            rest = comment.find("-->").map_or("", |end| &comment[end + 3..]);
            continue;
        }
        let closing = rest[1..].starts_with('/');
        let name_from = if closing { 2 } else { 1 };
        let name_len = rest[name_from..]
            .bytes()
            .take_while(u8::is_ascii_alphanumeric)
            .count();
        let starts_a_name = rest[name_from..]
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic());
        if !starts_a_name {
            if rest[1..].starts_with(['!', '?']) || closing {
                // A declaration, a processing instruction or a bogus end tag: not text.
                rest = rest.find('>').map_or("", |end| &rest[end + 1..]);
            } else {
                // A lone `<` is text.
                data(&mut open, "<");
                rest = &rest[1..];
            }
            continue;
        }
        let name = rest[name_from..name_from + name_len].to_ascii_lowercase();
        let Some(end) = tag_end(rest) else {
            // An unfinished tag at the end of the page.
            break;
        };
        rest = &rest[end..];
        if closing {
            end_tag(&mut open, &mut done, &name)?;
            continue;
        }
        if name == "script" || name == "style" {
            // Raw text up to its own end tag.
            let close = format!("</{name}");
            let lower = rest.to_ascii_lowercase();
            rest = lower.find(&close).map_or("", |found| &rest[found..]);
            continue;
        }
        start_tag(&mut open, &name)?;
    }
    data(&mut open, rest);
    while let Some(mut table) = open.pop() {
        table.end_row()?;
        done.push(table);
    }
    Ok(done)
}

fn start_tag(open: &mut Vec<HtmlTable>, name: &str) -> anyhow::Result<()> {
    if name == "table" {
        open.push(HtmlTable::default());
        return Ok(());
    }
    let Some(table) = open.last_mut() else {
        return Ok(());
    };
    match name {
        "thead" => {
            table.end_row()?;
            table.in_head = true;
        }
        "tr" => {
            table.end_row()?;
            table.row = Some(Vec::new());
        }
        "td" | "th" => {
            table.end_cell()?;
            table.row.get_or_insert_with(Vec::new);
            table.cell = Some(String::new());
        }
        _ => {}
    }
    Ok(())
}

fn end_tag(open: &mut Vec<HtmlTable>, done: &mut Vec<HtmlTable>, name: &str) -> anyhow::Result<()> {
    let Some(table) = open.last_mut() else {
        return Ok(());
    };
    match name {
        "td" | "th" => table.end_cell()?,
        "tr" => table.end_row()?,
        "thead" => {
            table.end_row()?;
            table.in_head = false;
            if let Some(last) = table.head_rows.last() {
                table.head = Some(last.clone());
            }
        }
        "table" => {
            table.end_row()?;
            done.extend(open.pop());
        }
        _ => {}
    }
    Ok(())
}

// --- the full table ------------------------------------------------------------------------------

/// One code of the 법정동 전체 표.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeRow {
    /// The 10-digit 법정동코드.
    pub region_cd: String,
    /// The full name.
    pub full_name: String,
    /// 존재 or 폐지, the words the snapshot table uses.
    pub status: String,
    /// The parent's code; empty on most long-abolished codes.
    pub parent_cd: String,
    /// 생성일, `YYYYMMDD` or empty.
    pub created_date: String,
    /// 폐지일, `YYYYMMDD` or empty.
    pub abolished_date: String,
    /// The lowest name.
    pub lowest_name: String,
    /// The 주민 code.
    pub jumin_cd: String,
    /// The 지적 code.
    pub jijuk_cd: String,
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.chars().all(|character| character.is_ascii_digit())
}

/// `YYYYMMDD` naming a real day.
fn real_day(text: &str) -> bool {
    text.len() == 8 && digits(text) && NaiveDate::parse_from_str(text, "%Y%m%d").is_ok()
}

/// The source contract's full-table part: its headers, status words, empty-result text and bounds.
#[derive(Clone, Debug)]
pub struct FullTableContract {
    headers: Vec<String>,
    status_words: serde_json::Map<String, Value>,
    empty_text: String,
    min_rows: u64,
    max_shrink_share: f64,
}

impl FullTableContract {
    /// The contract compiled into the publisher.
    ///
    /// # Errors
    /// The contract is not the JSON this reads.
    pub fn compiled() -> anyhow::Result<Self> {
        let contract: Value = serde_json::from_str(CONTRACT_JSON)?;
        let spec = &contract["full_table"];
        let headers = spec["headers"]
            .as_array()
            .context("the contract has no full_table.headers")?
            .iter()
            .map(|header| header.as_str().map(str::to_owned).context("a header"))
            .collect::<anyhow::Result<Vec<_>>>()?;
        ensure!(
            headers.len() == FULL_TABLE_FIELDS,
            "the contract's full_table.headers must name {FULL_TABLE_FIELDS} columns"
        );
        Ok(Self {
            headers,
            status_words: spec["status_words"]
                .as_object()
                .context("the contract has no full_table.status_words")?
                .clone(),
            empty_text: spec["empty_result"]["text"]
                .as_str()
                .context("the contract has no full_table.empty_result.text")?
                .to_owned(),
            min_rows: spec["bounds"]["min_rows"]
                .as_u64()
                .context("the contract has no full_table.bounds.min_rows")?,
            max_shrink_share: spec["bounds"]["max_shrink_share"]
                .as_f64()
                .context("the contract has no full_table.bounds.max_shrink_share")?,
        })
    }

    /// The 법정동 전체 표 as one row per code, in the order the site lists them.
    ///
    /// # Errors
    /// The page holds no table (or several) headed as the contract says, its columns changed, a
    /// row lacks a column, a code is not 10 digits, a parent or a 주민/지적 code is not digits, a
    /// status is no word of the contract, a date is no real day, or a code appears twice.
    pub fn parse(&self, text: &str) -> anyhow::Result<Vec<CodeRow>> {
        let first = &self.headers[0];
        let mut found = html_tables(text)?
            .into_iter()
            .filter(|table| table.head.as_ref().and_then(|head| head.first()) == Some(first))
            .collect::<Vec<_>>();
        ensure!(
            !found.is_empty(),
            "the full table page holds no table headed {first:?}"
        );
        ensure!(
            found.len() == 1,
            "the full table page holds {} tables headed {first:?}",
            found.len()
        );
        let table = found.remove(0);
        ensure!(
            table.head.as_ref() == Some(&self.headers),
            "the full table columns changed: expected {:?}, got {:?}",
            self.headers,
            table.head
        );
        // A query that matches no code lists the site's one-cell notice.
        if table.rows.len() == 1 && table.rows[0] == [self.empty_text.clone()] {
            return Ok(Vec::new());
        }
        let mut rows = Vec::with_capacity(table.rows.len());
        let mut seen = std::collections::HashSet::with_capacity(table.rows.len());
        for (index, cells) in table.rows.into_iter().enumerate() {
            let line = index + 1;
            let cells: [String; FULL_TABLE_FIELDS] =
                cells.try_into().map_err(|cells: Vec<_>| {
                    anyhow::anyhow!(
                        "row {line}: {} cells, expected {FULL_TABLE_FIELDS}",
                        cells.len()
                    )
                })?;
            // The contract's header order: code, name, status, parent, rank, created, abolished,
            // last work, lowest name, 주민 code, 지적 code.
            let [region_cd, full_name, status, parent_cd, _rank, created_date, abolished_date, last_work_date, lowest_name, jumin_cd, jijuk_cd] =
                cells;
            ensure!(
                region_cd.len() == 10 && digits(&region_cd),
                "row {line}: {region_cd:?} is not a 10-digit 법정동코드"
            );
            ensure!(
                parent_cd.is_empty() || digits(&parent_cd),
                "row {line}: parent {parent_cd:?} is not a code"
            );
            let Some(status) = self.status_words.get(&status).and_then(Value::as_str) else {
                bail!("row {line}: status {status:?} is none of the contract's words");
            };
            for (name, value) in [
                ("created_date", &created_date),
                ("abolished_date", &abolished_date),
                ("last_work_date", &last_work_date),
            ] {
                ensure!(
                    value.is_empty() || real_day(value),
                    "row {line}: {name} {value:?} is not a YYYYMMDD date"
                );
            }
            for (name, value) in [("jumin_cd", &jumin_cd), ("jijuk_cd", &jijuk_cd)] {
                ensure!(
                    value.is_empty() || digits(value),
                    "row {line}: {name} {value:?} is not a code"
                );
            }
            ensure!(
                seen.insert(region_cd.clone()),
                "row {line}: {region_cd} appears twice"
            );
            rows.push(CodeRow {
                region_cd,
                full_name,
                status: status.to_owned(),
                parent_cd,
                created_date,
                abolished_date,
                lowest_name,
                jumin_cd,
                jijuk_cd,
            });
        }
        Ok(rows)
    }

    /// Refuses a full table below the contract's floor, or shrunk from the previous one.
    ///
    /// The table lists abolished codes too, so it only grows: a smaller one is a broken response.
    ///
    /// # Errors
    /// The table is out of its bounds.
    pub fn check_table_size(&self, rows: u64, previous: Option<u64>) -> anyhow::Result<()> {
        ensure!(
            rows >= self.min_rows,
            "the full table has {rows} rows, below the floor of {}",
            self.min_rows
        );
        if let Some(previous) = previous.filter(|previous| *previous > 0) {
            #[allow(clippy::cast_precision_loss)]
            let shrunk = (rows as f64) < (previous as f64) * (1.0 - self.max_shrink_share);
            ensure!(
                !shrunk,
                "the full table has {rows} rows, {} fewer than the previous {previous} (max shrink share {})",
                previous.saturating_sub(rows),
                self.max_shrink_share
            );
        }
        Ok(())
    }
}

// --- staging -------------------------------------------------------------------------------------

/// `legal_dong_snapshot`: `rows` as the table's columns, sorted by code; an empty optional field
/// is null. Returns the rows per status.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn stage_snapshot(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    rows: &[CodeRow],
    snapshot_date: NaiveDate,
    source_record_id: &str,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<std::collections::BTreeMap<String, u64>> {
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE legal_dong_read (region_cd VARCHAR NOT NULL, full_name VARCHAR NOT NULL, \
         status VARCHAR NOT NULL, parent_cd VARCHAR, created_date VARCHAR, abolished_date VARCHAR, \
         lowest_name VARCHAR, jumin_cd VARCHAR, jijuk_cd VARCHAR)",
    )?;
    let mut by_status = std::collections::BTreeMap::new();
    {
        let mut appender = connection.appender("legal_dong_read")?;
        let optional = |value: &str| (!value.is_empty()).then(|| value.to_owned());
        for row in rows {
            *by_status.entry(row.status.clone()).or_insert(0_u64) += 1;
            appender.append_row(duckdb::params![
                row.region_cd,
                row.full_name,
                row.status,
                optional(&row.parent_cd),
                optional(&row.created_date),
                optional(&row.abolished_date),
                optional(&row.lowest_name),
                optional(&row.jumin_cd),
                optional(&row.jijuk_cd)
            ])?;
        }
        appender.flush()?;
    }
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE legal_dong_snapshot AS SELECT {columns} FROM (\
             SELECT *, DATE '{date}' AS snapshot_date, {record} AS source_record_id, {at} AS ingested_at_utc \
             FROM legal_dong_read) ORDER BY region_cd",
            columns = column_names(contract).join(", "),
            date = snapshot_date.format("%Y-%m-%d"),
            record = literal(source_record_id),
            at = epoch(ingested_at),
        ))
        .context("DuckDB failed to stage the snapshot")?;
    ensure!(
        count(stage, "legal_dong_snapshot")? == u64::try_from(rows.len())?,
        "the staged snapshot lost rows"
    );
    Ok(by_status)
}

/// Rows of the latest code.go.kr snapshot the table holds at `head` that is earlier than
/// `snapshot_date`, or none.
///
/// One load is one table: two tables taken the same day are two snapshots, and counting every row
/// of that day would double the baseline. The latest is the newest date, then the newest load on
/// it, then the greatest record id (the collector's keys carry the second they were taken).
///
/// # Errors
/// `DuckDB` fails the read.
pub fn previous_code_go_kr_row_count(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    snapshot_date: NaiveDate,
) -> anyhow::Result<Option<u64>> {
    let Some(head) = head else {
        return Ok(None);
    };
    let pinned = crate::duckdb_parquet_stage::table_at(contract.table_name, head);
    let source = with_contract_columns(stage.connection(), contract, &pinned)?;
    previous_row_count_in(stage, &source, snapshot_date)
}

/// [`previous_code_go_kr_row_count`] over `source`, a relation of the table's columns.
fn previous_row_count_in(
    stage: &DuckDbParquetStage,
    source: &str,
    snapshot_date: NaiveDate,
) -> anyhow::Result<Option<u64>> {
    let of_the_source = code_go_kr_table_prefixes()
        .iter()
        .map(|prefix| format!("starts_with(source_record_id, {})", literal(prefix)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let mut found = stage.connection().prepare(&format!(
        "SELECT count(*) FROM {source} WHERE ({of_the_source}) AND snapshot_date < DATE '{date}' \
         GROUP BY snapshot_date, source_record_id \
         ORDER BY snapshot_date DESC, max(ingested_at_utc) DESC, source_record_id DESC LIMIT 1",
        date = snapshot_date.format("%Y-%m-%d"),
    ))?;
    let rows = found
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()
        .context("DuckDB failed to read the earlier snapshots")?;
    rows.first()
        .map(|rows| u64::try_from(*rows).context("a row count"))
        .transpose()
}

#[cfg(test)]
mod tests;
