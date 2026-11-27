//! The data.go.kr bulk files as Silver rows, in `DuckDB` inside the publisher (root ADR-0401 §3–4,
//! ADR-0559).
//!
//! It was the Spark job `data_go_kr_file_data.py`. One Bronze file is one edition of one table:
//!
//! - [`read_file`] reads its data (the CSV, every zip member the contract's name pattern matches, or
//!   a 표준데이터 pass of gzip JSON lines) strictly in the contract's encoding, refuses a header that
//!   is not the contract's, and puts every data row on the stage with its line number;
//! - [`keyed`] gives each row its `row_key` (the provider key's values joined by U+001F, or the
//!   SHA-256 of the row's cells joined so) and `provider_fields_json` (every cell under its header
//!   name, written as Spark's `to_json` wrote it);
//! - [`link`] links each row by the contract's `place_link`: an address or the provider's lot code
//!   through `place_identity_stage`, 도·시군구·읍면동 names to a 법정동 code, or a 행정동 code kept as it is;
//! - [`changes`] keeps the keys that are new or changed and one removal marker per key the edition
//!   no longer holds (`changes.sql`).
//!
//! A table whose links a 법정동 code change can move records the change table's snapshot its links
//! were made against ([`follow_record`]); [`changed_prefixes`] and [`affected`] select the current
//! rows a later change can reach, and they are linked again.

use std::{
    io::Read as _,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;

use crate::{
    duckdb_parquet_stage::{with_contract_columns, DuckDbParquetStage},
    place_identity_stage::{self, PlacePins, CODE_CHANGE_TABLE},
    silver_changes::{self, REMOVED},
    silver_load_plan::PLAN_SCHEMA,
};

pub(crate) const CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/data-go-kr-file-data.contract.json");
const CHANGES_SQL: &str = include_str!("data_go_kr_file_data_stage/changes.sql");
const SEPARATOR: char = '\u{1f}';
const PARCEL_LINK_COLUMNS: [&str; 4] = [
    "pnu",
    "pnu_basis",
    "pnu_unresolved_reason",
    "legal_dong_code",
];
const AREA_LINK_COLUMNS: [&str; 3] = [
    "legal_dong_code",
    "administrative_dong_code",
    "legal_dong_unresolved_reason",
];

/// How a file's rows are linked to a place (`place_link.kind`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaceLink {
    /// An address read as a lot, then as a road (`address_pnu`).
    AddressPnu,
    /// The provider's 19-digit lot code through the place-identity resolver.
    PlaceIdentity,
    /// 도·시군구·읍면동 names to a 법정동 code.
    LegalDongByNames,
    /// A 행정동 code kept for the linkage table to bridge.
    AdmDongViaLinkage,
    /// The 행정동 ↔ 법정동 bridge itself.
    IsTheBridge,
}

impl PlaceLink {
    /// Whether the link is a parcel (`pnu` and how), not an area code.
    #[must_use]
    pub const fn parcel(self) -> bool {
        matches!(self, Self::AddressPnu | Self::PlaceIdentity)
    }

    /// Whether a 법정동 code change can move the link; the 행정동 ones are bridged at read time.
    #[must_use]
    pub const fn followed(self) -> bool {
        matches!(
            self,
            Self::AddressPnu | Self::PlaceIdentity | Self::LegalDongByNames
        )
    }

    /// The table's link columns, in the contract's order.
    #[must_use]
    pub const fn columns(self) -> &'static [&'static str] {
        if self.parcel() {
            &PARCEL_LINK_COLUMNS
        } else {
            &AREA_LINK_COLUMNS
        }
    }
}

/// One file of the contract: a dataset and its Silver table.
#[derive(Clone, Debug)]
pub struct FileEntry {
    /// The dataset slug: the table is `silver.<slug>`, the Bronze source `datagokr__<slug>`.
    pub slug: String,
    entry: Value,
}

/// The files this load reads, in the contract's order.
#[derive(Clone, Debug)]
pub struct FileDataSource {
    /// Every file that names no `own_job` (the registered factories and the workplace registers have
    /// their own jobs, root ADR-0604).
    pub files: Vec<FileEntry>,
}

impl FileDataSource {
    /// The contract compiled into the publisher.
    ///
    /// # Errors
    /// The contract is not the JSON this reads.
    pub fn compiled() -> anyhow::Result<Self> {
        let contract: serde_json::Map<String, Value> = serde_json::from_str(CONTRACT)?;
        let files = contract
            .get("files")
            .and_then(Value::as_object)
            .context("the file data contract lists no files")?
            .iter()
            .filter(|(_, entry)| entry.get("own_job").is_none())
            .map(|(slug, entry)| FileEntry {
                slug: slug.clone(),
                entry: entry.clone(),
            })
            .collect();
        Ok(Self { files })
    }

    /// The file of `slug`.
    ///
    /// # Errors
    /// No file of the contract has the slug.
    pub fn file(&self, slug: &str) -> anyhow::Result<&FileEntry> {
        self.files
            .iter()
            .find(|file| file.slug == slug)
            .with_context(|| format!("{slug} is not a file this load reads"))
    }
}

fn text<'a>(value: &'a Value, what: &str) -> anyhow::Result<&'a str> {
    value
        .as_str()
        .with_context(|| format!("the file data contract has no {what}"))
}

fn texts(value: &Value, what: &str) -> anyhow::Result<Vec<String>> {
    value
        .as_array()
        .with_context(|| format!("the file data contract has no {what}"))?
        .iter()
        .map(|item| text(item, what).map(str::to_owned))
        .collect()
}

/// `'text'` as a SQL literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

impl FileEntry {
    /// `silver.<slug>`.
    #[must_use]
    pub fn table(&self) -> String {
        format!("silver.{}", self.slug)
    }

    /// The Bronze source (root ADR-0014 `{provider}__{dataset}`).
    #[must_use]
    pub fn source_slug(&self) -> String {
        format!("datagokr__{}", self.slug)
    }

    fn format(&self) -> anyhow::Result<&str> {
        text(&self.entry["format"], "format")
    }

    /// Where the encoding, header and bounds are: the zip's member rule, or the entry itself.
    fn spec(&self) -> anyhow::Result<&Value> {
        Ok(if self.format()? == "zip" {
            &self.entry["member"]
        } else {
            &self.entry
        })
    }

    /// The contract's header: the data columns, in file order.
    ///
    /// # Errors
    /// The contract names none, or repeats a name.
    pub fn header(&self) -> anyhow::Result<Vec<String>> {
        let header = texts(&self.spec()?["header"], "header")?;
        let distinct = header.iter().collect::<std::collections::BTreeSet<_>>();
        ensure!(
            distinct.len() == header.len(),
            "{}: the contract header repeats a name",
            self.slug
        );
        Ok(header)
    }

    /// How the rows are linked.
    ///
    /// # Errors
    /// The contract names a link this load does not know.
    pub fn place_link(&self) -> anyhow::Result<PlaceLink> {
        Ok(
            match text(&self.entry["place_link"]["kind"], "place_link.kind")? {
                "address_pnu" => PlaceLink::AddressPnu,
                "place_identity" => PlaceLink::PlaceIdentity,
                "legal_dong_by_names" => PlaceLink::LegalDongByNames,
                "adm_dong_via_linkage" => PlaceLink::AdmDongViaLinkage,
                "is_the_bridge" => PlaceLink::IsTheBridge,
                other => bail!("{}: unknown place_link kind {other:?}", self.slug),
            },
        )
    }

    /// The column an edition is one period of (`row_key.period_column`), when it is.
    #[must_use]
    pub fn period_column(&self) -> Option<&str> {
        self.entry["row_key"]["period_column"].as_str()
    }

    /// The provider key's columns; `None` for a row keyed by its own SHA-256.
    fn key_columns(&self) -> anyhow::Result<Option<Vec<String>>> {
        let rule = &self.entry["row_key"];
        Ok(match text(&rule["kind"], "row_key.kind")? {
            "sha256_of_row" => None,
            "columns" => Some(texts(&rule["columns"], "row_key.columns")?),
            other => bail!("{}: unknown row_key kind {other:?}", self.slug),
        })
    }

    /// What a change is: the raw row and its link (a re-link that moves a PNU is a change).
    ///
    /// # Errors
    /// The contract names a link this load does not know.
    pub fn content_columns(&self) -> anyhow::Result<Vec<&'static str>> {
        let mut columns = vec!["provider_fields_json"];
        columns.extend(self.place_link()?.columns());
        Ok(columns)
    }

    /// Refuses an edition under the row floor, or shrunk past its bound against `previous` current
    /// rows (none for a table with no current row).
    ///
    /// # Errors
    /// The edition is out of its bounds.
    pub fn check_bounds(&self, rows: u64, previous: u64) -> anyhow::Result<()> {
        let bounds = &self.spec()?["bounds"];
        let floor = bounds["min_rows"]
            .as_u64()
            .context("the file data contract has no bounds.min_rows")?;
        let share = bounds["max_shrink_share"]
            .as_f64()
            .context("the file data contract has no bounds.max_shrink_share")?;
        ensure!(
            rows >= floor,
            "{}: {rows} rows is under the floor of {floor}",
            self.slug
        );
        #[allow(clippy::cast_precision_loss)]
        let shrunk = previous > 0 && (rows as f64) < (previous as f64) * (1.0 - share);
        ensure!(
            !shrunk,
            "{}: {rows} rows shrank past {:.0}% of the {previous} current rows",
            self.slug,
            share * 100.0
        );
        Ok(())
    }
}

/// One Bronze file a plan fetched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedFile {
    /// Its key in the lakehouse bucket: the batch identity the table records.
    pub object_key: String,
    /// The dataset it is an edition of.
    pub slug: String,
    /// Where the plan put it.
    pub path: PathBuf,
}

impl PlannedFile {
    /// The edition id: the Bronze leaf without its extension (the dataset and its bytes' SHA-256, root
    /// ADR-0604; the dataset and the portal file id for the editions landed before it).
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        let leaf = self
            .object_key
            .rsplit_once('/')
            .map_or(self.object_key.as_str(), |(_, leaf)| leaf);
        leaf.rsplit_once('.').map_or(leaf, |(stem, _)| stem)
    }
}

/// Every file the plans in `plan_dirs` fetched, in plan order (each plan's oldest first).
///
/// # Errors
/// Refuses another plan format, and a file of a source this load does not read.
pub fn planned_files(
    plan_dirs: &[PathBuf],
    source: &FileDataSource,
) -> anyhow::Result<Vec<PlannedFile>> {
    let mut files = Vec::new();
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
        for item in plan["objects"]
            .as_array()
            .context("the plan lists no objects")?
        {
            let key = item["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let found = item["source_slug"].as_str().unwrap_or_default();
            let file = source
                .files
                .iter()
                .find(|file| file.source_slug() == found)
                .with_context(|| format!("{key}: {found} is not a source this load reads"))?;
            let local = item["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            files.push(PlannedFile {
                object_key: key.to_owned(),
                slug: file.slug.clone(),
                path: plan_dir.join(local),
            });
        }
    }
    Ok(files)
}

// --- reading -------------------------------------------------------------------------------------

/// Python's `str.isspace`: Unicode white space and the four information separators.
pub(crate) const fn py_space(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{1c}'..='\u{1f}')
}

pub(crate) fn py_strip(text: &str) -> &str {
    text.trim_matches(py_space)
}

/// `bytes` read strictly in `encoding`, as Python's codec of that name reads them.
pub(crate) fn decode(bytes: &[u8], encoding: &str) -> anyhow::Result<String> {
    let strict_utf8 = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .context("a byte is not UTF-8")
    };
    match encoding {
        "cp949" => encoding_rs::EUC_KR
            .decode_without_bom_handling_and_without_replacement(bytes)
            .map(std::borrow::Cow::into_owned)
            .context("a byte is not cp949"),
        "utf-8" => strict_utf8(bytes),
        "utf-8-sig" => strict_utf8(bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes)),
        other => bail!("no reader of the encoding {other:?}"),
    }
}

/// The first line of `text` with its end (`\n`, `\r\n` or `\r`), and the rest: a text file read
/// with `newline=""`.
fn split_line(text: &str) -> (&str, &str) {
    let Some(at) = text.find(['\n', '\r']) else {
        return (text, "");
    };
    let end = if text[at..].starts_with("\r\n") {
        at + 2
    } else {
        at + 1
    };
    text.split_at(end)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CsvState {
    StartRecord,
    StartField,
    InField,
    InQuotedField,
    QuoteInQuotedField,
    EatLineEnd,
}

/// The records of a CSV text as Python's `csv.reader` reads them (the excel dialect, not strict):
/// a quote opens a quoted field only at a field's start, a doubled quote inside one is a quote, a
/// character after a closing quote is kept, and a blank line is a record of no fields.
pub(crate) struct PyCsvRecords<'a> {
    pub(crate) rest: &'a str,
}

impl Iterator for PyCsvRecords<'_> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Vec<String>> {
        if self.rest.is_empty() {
            return None;
        }
        let mut state = CsvState::StartRecord;
        let mut fields = Vec::new();
        let mut field = String::new();
        loop {
            if self.rest.is_empty() {
                // The text ended inside a quoted field: what was read is the last record.
                fields.push(std::mem::take(&mut field));
                return Some(fields);
            }
            let (line, rest) = split_line(self.rest);
            self.rest = rest;
            for character in line.chars() {
                let line_end = matches!(character, '\n' | '\r');
                state = match state {
                    CsvState::StartRecord | CsvState::StartField if line_end => {
                        if state == CsvState::StartField {
                            fields.push(std::mem::take(&mut field));
                        }
                        CsvState::EatLineEnd
                    }
                    CsvState::StartRecord | CsvState::StartField => match character {
                        '"' => CsvState::InQuotedField,
                        ',' => {
                            fields.push(std::mem::take(&mut field));
                            CsvState::StartField
                        }
                        other => {
                            field.push(other);
                            CsvState::InField
                        }
                    },
                    CsvState::InField | CsvState::QuoteInQuotedField if line_end => {
                        fields.push(std::mem::take(&mut field));
                        CsvState::EatLineEnd
                    }
                    CsvState::InField => {
                        if character == ',' {
                            fields.push(std::mem::take(&mut field));
                            CsvState::StartField
                        } else {
                            field.push(character);
                            CsvState::InField
                        }
                    }
                    CsvState::InQuotedField => {
                        if character == '"' {
                            CsvState::QuoteInQuotedField
                        } else {
                            field.push(character);
                            CsvState::InQuotedField
                        }
                    }
                    CsvState::QuoteInQuotedField => match character {
                        '"' => {
                            field.push('"');
                            CsvState::InQuotedField
                        }
                        ',' => {
                            fields.push(std::mem::take(&mut field));
                            CsvState::StartField
                        }
                        other => {
                            field.push(other);
                            CsvState::InField
                        }
                    },
                    CsvState::EatLineEnd => CsvState::EatLineEnd,
                };
            }
            // The end of the line.
            match state {
                CsvState::InQuotedField => {}
                CsvState::StartField | CsvState::InField | CsvState::QuoteInQuotedField => {
                    fields.push(std::mem::take(&mut field));
                    return Some(fields);
                }
                CsvState::StartRecord | CsvState::EatLineEnd => return Some(fields),
            }
        }
    }
}

/// The rows of one file going onto the stage as `file_rows(_row, source_line_number, cells)`:
/// `cells` is the row's cells as a JSON array of strings.
struct RowSink<'a> {
    appender: duckdb::Appender<'a>,
    rows: i64,
}

impl RowSink<'_> {
    fn push(&mut self, line: usize, cells: &[impl AsRef<str>]) -> anyhow::Result<()> {
        let cells = serde_json::to_string(&cells.iter().map(AsRef::as_ref).collect::<Vec<_>>())?;
        self.rows += 1;
        self.appender
            .append_row(duckdb::params![self.rows, i32::try_from(line)?, cells])?;
        Ok(())
    }
}

/// One data part (a CSV text): refused unless its header is `header` exactly and every row has
/// the header's width. A row's line number is its record's place in the part (the first data
/// record is 2); blank rows are left out and still counted.
fn push_csv(
    sink: &mut RowSink<'_>,
    text: &str,
    header: &[String],
    name: &str,
) -> anyhow::Result<()> {
    let (first, rest) = split_line(text);
    let found = PyCsvRecords { rest: first }
        .next()
        .unwrap_or_default()
        .iter()
        .map(|cell| py_strip(cell).trim_start_matches('\u{feff}').to_owned())
        .collect::<Vec<_>>();
    ensure!(found == header, "{name}: the header is not the contract's");
    for (index, row) in (PyCsvRecords { rest }).enumerate() {
        let line = index + 2;
        if row.iter().all(|cell| py_strip(cell).is_empty()) {
            continue;
        }
        ensure!(
            row.len() == header.len(),
            "{name}: row {line} has {} cells, not {}",
            row.len(),
            header.len()
        );
        sink.push(line, &row)?;
    }
    Ok(())
}

/// A zip member's name as written: zips from Windows tools store UTF-8 names without the flag.
pub(crate) fn member_name(
    member: &zip::read::ZipFile<'_, std::fs::File>,
) -> anyhow::Result<String> {
    match std::str::from_utf8(member.name_raw()) {
        Ok(name) => Ok(name.to_owned()),
        Err(_) => Ok(member.name()?.into_owned()),
    }
}

/// What Python's `str()` makes of a 표준데이터 field.
fn page_cell(value: Option<&Value>) -> anyhow::Result<String> {
    Ok(match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) if number.is_i64() || number.is_u64() => number.to_string(),
        Some(other) => bail!("a field is neither text nor a whole number: {other}"),
    })
}

/// One 표준데이터 pass: the collector's raw lines, one `columns` answer (`totalCount`), then each
/// `rows` answer (a JSON array of rows keyed by column code). A row's fields go in header order
/// (an absent or null field is empty); row n is numbered n + 1, as a CSV line would be.
fn push_pages(sink: &mut RowSink<'_>, path: &Path, header: &[String]) -> anyhow::Result<()> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(path)?)
        .read_to_string(&mut text)
        .context("the pass is not gzip UTF-8 text")?;
    let mut total = None;
    let mut rows = 0_usize;
    for line in text.lines() {
        if py_strip(line).is_empty() {
            continue;
        }
        let answer: Value = serde_json::from_str(line)?;
        let body: Value = serde_json::from_str(
            answer["body"]
                .as_str()
                .context("an answer has no body text")?,
        )?;
        if answer["kind"] == "columns" {
            total = Some(match &body["totalCount"] {
                Value::String(count) => count.trim().parse::<usize>()?,
                count => usize::try_from(count.as_u64().context("totalCount is no count")?)?,
            });
            continue;
        }
        let Some(items) = body.as_array().filter(|_| answer["kind"] == "rows") else {
            bail!("a line is neither the column list nor a page of rows");
        };
        for item in items {
            let item = item.as_object().context("a row is not an object")?;
            let extra = item
                .keys()
                .filter(|name| !header.contains(name))
                .collect::<std::collections::BTreeSet<_>>();
            ensure!(
                extra.is_empty(),
                "the answer has fields the contract header does not name: {extra:?}"
            );
            rows += 1;
            let cells = header
                .iter()
                .map(|name| page_cell(item.get(name)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            sink.push(rows + 1, &cells)?;
        }
    }
    ensure!(
        Some(rows) == total,
        "{rows} rows, but the column list reports {total:?}"
    );
    Ok(())
}

/// Reads the file at `path` as `file_rows`; returns how many data rows it holds.
///
/// # Errors
/// The file does not decode strictly in the contract's encoding, a header is not the contract's,
/// a row has another width, a zip holds no member (or more than the one) the contract names, or a
/// pass's rows are not the total it reports.
pub fn read_file(stage: &DuckDbParquetStage, file: &FileEntry, path: &Path) -> anyhow::Result<u64> {
    let header = file.header()?;
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE file_rows (_row BIGINT NOT NULL, source_line_number INTEGER NOT NULL, cells VARCHAR NOT NULL)",
    )?;
    let mut sink = RowSink {
        appender: connection.appender("file_rows")?,
        rows: 0,
    };
    let slug = &file.slug;
    match file.format()? {
        "standard_pages" => {
            push_pages(&mut sink, path, &header).with_context(|| slug.clone())?;
        }
        "csv" => {
            let encoding = text(&file.entry["encoding"], "encoding")?;
            let text = decode(&std::fs::read(path)?, encoding).with_context(|| slug.clone())?;
            push_csv(&mut sink, &text, &header, slug)?;
        }
        "zip" => {
            let member = &file.entry["member"];
            let pattern = text(&member["name_pattern"], "member.name_pattern")?;
            let encoding = text(&member["encoding"], "member.encoding")?;
            let every = member["every_match"].as_bool().unwrap_or(false);
            let pattern = regex::Regex::new(pattern)?;
            let mut archive = zip::ZipArchive::new(std::fs::File::open(path)?)?;
            let mut found = 0_usize;
            for index in 0..archive.len() {
                let mut entry = archive.by_index(index)?;
                let name = member_name(&entry)?;
                if !pattern.is_match(&name) {
                    continue;
                }
                found += 1;
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes)?;
                let text = decode(&bytes, encoding).with_context(|| format!("{slug} {name}"))?;
                drop(bytes);
                push_csv(&mut sink, &text, &header, &format!("{slug} {name}"))?;
            }
            ensure!(
                found > 0 && (found == 1 || every),
                "{slug}: expected {} matching {pattern}, found {found}",
                if every { "members" } else { "one member" }
            );
        }
        other => bail!("{slug}: no reader of the format {other:?}"),
    }
    sink.appender.flush()?;
    Ok(u64::try_from(sink.rows)?)
}

pub(crate) fn count(stage: &DuckDbParquetStage, query: &str) -> anyhow::Result<u64> {
    let value: i64 =
        stage
            .connection()
            .query_row(&format!("SELECT count(*) FROM {query}"), [], |row| {
                row.get(0)
            })?;
    Ok(u64::try_from(value)?)
}

/// `file_keyed(_row, row_key, provider_fields_json, source_line_number)` of `file_rows`.
///
/// # Errors
/// A provider key is empty or repeats, the key names a column the header lacks, or `DuckDB` fails.
pub fn keyed(stage: &DuckDbParquetStage, file: &FileEntry) -> anyhow::Result<()> {
    let header = file.header()?;
    let cell = |name: &str| -> anyhow::Result<String> {
        let at = header
            .iter()
            .position(|title| title == name)
            .with_context(|| format!("{}: the header has no {name}", file.slug))?;
        Ok(format!("c[{}]", at + 1))
    };
    let key_columns = file.key_columns()?;
    let key = match &key_columns {
        None => format!("sha256(array_to_string(c, chr({})))", u32::from(SEPARATOR)),
        Some(columns) => format!(
            "concat_ws(chr({}), {})",
            u32::from(SEPARATOR),
            columns
                .iter()
                .map(|column| cell(column))
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(", ")
        ),
    };
    // Spark's to_json of a struct of strings: the names in order, each value as Jackson writes a
    // string. DuckDB's to_json writes a string the same way (the short escapes, then \u00XX).
    let fields = header
        .iter()
        .enumerate()
        .map(|(index, title)| {
            Ok(format!(
                "{} || CAST(to_json(c[{}]) AS VARCHAR)",
                literal(&format!("{}:", serde_json::to_string(title)?)),
                index + 1
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE file_keyed AS \
             SELECT _row, {key} AS row_key, '{{' || concat_ws(',', {fields}) || '}}' AS provider_fields_json, source_line_number \
             FROM (SELECT _row, source_line_number, from_json(cells, '[\"VARCHAR\"]') AS c FROM file_rows)"
        ))
        .with_context(|| format!("DuckDB failed to key the rows of {}", file.slug))?;
    if let Some(columns) = key_columns {
        ensure!(
            count(stage, "file_keyed WHERE row_key = ''")? == 0,
            "{}: a row has an empty provider key {columns:?}",
            file.slug
        );
        ensure!(
            count(stage, "(SELECT DISTINCT row_key FROM file_keyed)")?
                == count(stage, "file_keyed")?,
            "{}: the provider key {columns:?} repeats",
            file.slug
        );
    }
    Ok(())
}

/// The field `name` of a row's `provider_fields_json`.
fn field(name: &str) -> anyhow::Result<String> {
    ensure!(
        !name.contains(['"', '\\', '\'']),
        "the column name {name:?} holds a quote"
    );
    Ok(format!(
        "json_extract_string(provider_fields_json, '$.\"{name}\"')"
    ))
}

/// The table's current rows at `head` as `file_current` (every column of the contract); for a file
/// that is one period, only the rows of the periods `file_keyed` holds.
///
/// # Errors
/// The table keeps no versions per key, or `DuckDB` fails the read.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn current(
    stage: &DuckDbParquetStage,
    file: &FileEntry,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    filter: &str,
) -> anyhow::Result<u64> {
    let columns = column_names(contract).join(", ");
    let connection = stage.connection();
    let query = match head {
        None => {
            // A table with no snapshot has no rows: the columns, typed as the contract's.
            let typed = contract
                .columns
                .iter()
                .map(|column| {
                    let sql_type = match column.logical_type {
                        "int" => "INTEGER",
                        "timestamp" => "TIMESTAMPTZ",
                        _ => "VARCHAR",
                    };
                    format!("CAST(NULL AS {sql_type}) AS {}", column.name)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT {typed} WHERE false")
        }
        Some(head) => {
            let pinned = crate::duckdb_parquet_stage::table_at(contract.table_name, head);
            contract
                .current_rows_sql_template()
                .with_context(|| format!("{} keeps no versions per key", contract.table_name))?
                .replace("{columns}", &columns)
                .replace(
                    "{source}",
                    &with_contract_columns(connection, contract, &pinned)?,
                )
                .replace("{filter}", "TRUE")
        }
    };
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE file_current AS SELECT * FROM ({query}) c WHERE {filter}"
        ))
        .with_context(|| format!("DuckDB failed to read the current rows of {}", file.slug))?;
    count(stage, "file_current")
}

/// The filter of [`current`] for an edition: the periods the edition holds, or every row.
///
/// # Errors
/// The period column's name holds a quote.
pub fn edition_filter(file: &FileEntry) -> anyhow::Result<String> {
    Ok(match file.period_column() {
        // One period per edition: earlier periods are history, not keys this edition dropped.
        Some(period) => format!(
            "{0} IN (SELECT DISTINCT {0} FROM file_keyed)",
            field(period)?
        ),
        None => "TRUE".to_owned(),
    })
}

pub(crate) fn column_names(contract: &LakehouseTableContract) -> Vec<&'static str> {
    contract.columns.iter().map(|column| column.name).collect()
}

/// What the parcel links read: the pins of the run and the register titles' contract.
#[derive(Clone, Copy, Debug)]
pub struct PlaceInputs<'a> {
    /// The snapshots the place-identity inputs are read at.
    pub pins: &'a PlacePins,
    /// `silver.building_register_titles`.
    pub title_contract: &'a LakehouseTableContract,
}

/// The address link: one offer per row of its address columns, placed on the served parcels.
///
/// Returns the statement that makes `file_link` of the placed rows.
fn address_link(
    stage: &DuckDbParquetStage,
    file: &FileEntry,
    input: &str,
    place: PlaceInputs<'_>,
) -> anyhow::Result<String> {
    let rule = &file.entry["place_link"];
    let connection = stage.connection();
    let columns = match rule["address_columns"].as_array() {
        Some(_) => texts(&rule["address_columns"], "place_link.address_columns")?,
        None => vec![text(&rule["address_column"], "place_link.address_column")?.to_owned()],
    };
    ensure!(!columns.is_empty(), "{}: no address column", file.slug);
    let width = columns.len();
    // One place row per address column, the columns of a row side by side.
    let offered = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Ok(format!(
                "SELECT _row * {width} + {index} AS _row, NULL::VARCHAR AS pnu, {} AS address FROM {input}",
                field(column)?
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(" UNION ALL ");
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE place_rows AS {offered}"
    ))?;
    place_identity_stage::offers(stage)?;
    // address_offer, 지번 first: the first column's jibun PNU and the first column's road key;
    // when no column offers either, the first reason that is not an empty column.
    connection.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE file_column_offers AS SELECT * FROM place_offers;
         CREATE OR REPLACE TEMP TABLE file_offers AS
         SELECT _row // {width} AS _row,
                arg_min(jibun_pnu, _row) FILTER (WHERE jibun_pnu IS NOT NULL) AS jibun_pnu,
                arg_min(jibun_code, _row) FILTER (WHERE jibun_pnu IS NOT NULL) AS legal_dong_code,
                min(_row) FILTER (WHERE road_name IS NOT NULL) AS road_at,
                coalesce(arg_min(reason, _row) FILTER (WHERE reason IS NOT NULL AND reason <> 'no_address'),
                         arg_min(reason, _row) FILTER (WHERE reason IS NOT NULL)) AS reason
         FROM file_column_offers GROUP BY 1;
         CREATE OR REPLACE TEMP TABLE place_offers AS
         SELECT f._row, f.jibun_pnu, f.legal_dong_code AS jibun_code,
                CASE WHEN f.jibun_pnu IS NULL AND f.road_at IS NULL THEN f.reason END AS reason,
                r.road_sido, r.road_sigungu, r.road_name, r.road_main_no, r.road_sub_no
         FROM file_offers f LEFT JOIN file_column_offers r ON r._row = f.road_at;
         CREATE OR REPLACE TEMP TABLE place_rows AS SELECT _row, NULL::VARCHAR AS pnu, NULL::VARCHAR AS address FROM {input};"
    ))?;
    place_identity_stage::read_served(stage, place.pins, place.title_contract)?;
    place_identity_stage::place_offered(stage)?;
    let link = "CREATE OR REPLACE TEMP TABLE file_link AS \
     SELECT a._row, a.pnu, a.pnu_basis, a.reason AS pnu_unresolved_reason, \
            coalesce(f.legal_dong_code, left(a.pnu, 10)) AS legal_dong_code \
     FROM place_address_result a JOIN file_offers f USING (_row)"
        .to_owned();
    Ok(link)
}

/// `file_link(_row, <the link columns>)` of the rows of `input`, by the file's `place_link`.
///
/// `input` holds `_row` and `provider_fields_json`. The place-identity reference tables are on the
/// stage (`place_identity_stage::load_reference`) for every link a code change can move.
///
/// # Errors
/// The contract's link names no column, or `DuckDB` fails.
pub fn link(
    stage: &DuckDbParquetStage,
    file: &FileEntry,
    input: &str,
    place: PlaceInputs<'_>,
) -> anyhow::Result<()> {
    let rule = &file.entry["place_link"];
    let connection = stage.connection();
    let sql = match file.place_link()? {
        PlaceLink::AdmDongViaLinkage => format!(
            "CREATE OR REPLACE TEMP TABLE file_link AS SELECT _row, NULL::VARCHAR AS legal_dong_code, \
             {} AS administrative_dong_code, NULL::VARCHAR AS legal_dong_unresolved_reason FROM {input}",
            field(text(&rule["code_column"], "place_link.code_column")?)?
        ),
        PlaceLink::IsTheBridge => format!(
            "CREATE OR REPLACE TEMP TABLE file_link AS SELECT _row, {} AS legal_dong_code, \
             {} AS administrative_dong_code, NULL::VARCHAR AS legal_dong_unresolved_reason FROM {input}",
            field("법정동코드")?,
            field("행정동코드")?
        ),
        PlaceLink::LegalDongByNames => {
            let names = texts(&rule["names"], "place_link.names")?
                .iter()
                .map(|name| field(name))
                .collect::<anyhow::Result<Vec<_>>>()?
                .join(", ");
            // names_code: the words that say something; under two is no address; the first is the
            // 시도 (the longest prefix wins), the rest the 법정동's name.
            format!(
                "CREATE OR REPLACE TEMP TABLE file_link AS \
                 WITH n AS (SELECT _row, list_filter(list_transform([{names}], lambda w: trim(coalesce(w, ''), py_ws())), lambda w: w <> '') AS words FROM {input}), \
                 s AS (SELECT n._row, arg_max(p.sido, length(p.prefix)) AS sido FROM n JOIN place_sido_prefixes p \
                       ON len(n.words) >= 2 AND starts_with(n.words[1], p.prefix) GROUP BY n._row) \
                 SELECT n._row, d.code AS legal_dong_code, NULL::VARCHAR AS administrative_dong_code, \
                        CASE WHEN len(n.words) < 2 THEN 'no_address' WHEN s.sido IS NULL THEN 'unparsed' \
                             WHEN d.code IS NULL THEN 'no_legal_dong' END AS legal_dong_unresolved_reason \
                 FROM n LEFT JOIN s USING (_row) \
                 LEFT JOIN place_dongs d ON d.sido = s.sido AND d.place = array_to_string(n.words[2:], ' ')"
            )
        }
        PlaceLink::PlaceIdentity => {
            let code = field(text(
                &rule["provider_pnu_column"],
                "place_link.provider_pnu_column",
            )?)?;
            // The provider's code is offered only when it is 19 digits; never copied.
            connection.execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE place_rows AS \
                 SELECT _row, CASE WHEN regexp_full_match(code, '[0-9]{{19}}') THEN code END AS pnu, NULL::VARCHAR AS address \
                 FROM (SELECT _row, {code} AS code FROM {input})"
            ))?;
            place_identity_stage::offers(stage)?;
            place_identity_stage::read_served(stage, place.pins, place.title_contract)?;
            place_identity_stage::place_offered(stage)?;
            "CREATE OR REPLACE TEMP TABLE file_link AS \
             SELECT _row, pnu, CASE WHEN pnu IS NOT NULL THEN grade END AS pnu_basis, pnu_unresolved_reason, \
                    left(pnu, 10) AS legal_dong_code FROM place_code_result"
                .to_owned()
        }
        PlaceLink::AddressPnu => address_link(stage, file, input, place)?,
    };
    connection
        .execute_batch(&sql)
        .with_context(|| format!("DuckDB failed to link the rows of {}", file.slug))
}

pub(crate) fn epoch(instant: DateTime<Utc>) -> String {
    format!("to_timestamp({})", instant.timestamp())
}

/// `, NULL::VARCHAR AS <removal column>` of a staged row, which is never a removal marker; nothing
/// for a table that keeps no markers.
fn unremoved(contract: &LakehouseTableContract) -> String {
    silver_changes::removal_column(contract)
        .map(|column| format!(", NULL::VARCHAR AS {column}"))
        .unwrap_or_default()
}

/// `file_staged`: the edition's rows (`file_keyed` and their `file_link`) as the table's columns.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn staged_edition(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    planned: &PlannedFile,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    stage
        .connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE file_staged AS SELECT {columns} FROM (\
             SELECT k.row_key, k.provider_fields_json, l.* EXCLUDE (_row), {object} AS source_record_id, \
                    k.source_line_number, {snapshot} AS source_snapshot_id, {at} AS ingested_at_utc{unremoved} \
             FROM file_keyed k JOIN file_link l USING (_row))",
            columns = column_names(contract).join(", "),
            object = literal(&planned.object_key),
            snapshot = literal(planned.snapshot_id()),
            at = epoch(ingested_at),
            unremoved = unremoved(contract),
        ))
        .with_context(|| format!("DuckDB failed to stage {}", planned.object_key))
}

/// What a comparison appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Changes {
    /// Rows to append: every row of a new or changed key, and the removal markers.
    pub appended: u64,
    /// Of them, removal markers.
    pub removed: u64,
}

/// `file_changes`: what `file_staged` appends against `file_current` (`changes.sql`).
///
/// The rows of every key that is new or whose rows differ, and one removal marker per key with
/// current rows and none staged. A marker carries `source_snapshot_id` and this run's instant.
///
/// # Errors
/// Returns a `DuckDB` failure.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn changes(
    stage: &DuckDbParquetStage,
    file: &FileEntry,
    contract: &LakehouseTableContract,
    source_snapshot_id: &str,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<Changes> {
    let content = file.content_columns()?;
    let columns = column_names(contract);
    let removal = silver_changes::removal_column(contract);
    let markers = columns
        .iter()
        .map(|column| match *column {
            name if removal == Some(name) => format!("'{REMOVED}' AS {name}"),
            "source_snapshot_id" => {
                format!("{} AS source_snapshot_id", literal(source_snapshot_id))
            }
            "ingested_at_utc" => format!("{} AS ingested_at_utc", epoch(ingested_at)),
            other => format!("c.{other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let same = content
        .iter()
        .map(|column| format!("c.{column} IS NOT DISTINCT FROM s.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    stage
        .connection()
        .execute_batch(
            &CHANGES_SQL
                .replace("{content}", &content.join(", "))
                .replace("{same}", &same)
                .replace(
                    "{content_order}",
                    &content
                        .iter()
                        .map(|column| format!("c.{column}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .replace("{columns}", &columns.join(", "))
                .replace("{marker_columns}", &markers),
        )
        .with_context(|| format!("DuckDB failed to compare the rows of {}", file.slug))?;
    Ok(Changes {
        appended: count(stage, "file_changes")?,
        removed: count(
            stage,
            &format!("file_changes WHERE {}", silver_changes::removed(contract)),
        )?,
    })
}

/// How many rows of `file_staged` are linked (a parcel file's `pnu`, an area file's `legal_dong_code`).
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn linked(stage: &DuckDbParquetStage, link: PlaceLink) -> anyhow::Result<u64> {
    let column = if link.parcel() {
        "pnu"
    } else {
        "legal_dong_code"
    };
    count(stage, &format!("file_staged WHERE {column} IS NOT NULL"))
}

// --- following code changes ----------------------------------------------------------------------

/// The record of one linking against a change-table snapshot; the instant orders the records
/// (snapshot ids are not ordered).
#[must_use]
pub fn follow_record(snapshot: i64, at: DateTime<Utc>) -> String {
    format!(
        "follow:{CODE_CHANGE_TABLE}@{snapshot}@{}",
        at.format("%Y%m%dT%H%M%SZ")
    )
}

/// The change-table snapshot a table's links were last made against, from its commit records.
#[must_use]
pub fn last_followed<'a>(recorded: impl IntoIterator<Item = &'a str>) -> Option<i64> {
    let prefix = format!("follow:{CODE_CHANGE_TABLE}@");
    recorded
        .into_iter()
        .filter_map(|key| {
            let (snapshot, at) = key.strip_prefix(&prefix)?.split_once('@')?;
            let digits =
                |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
            let stamped = at.len() == 16
                && digits(&at[..8])
                && at.as_bytes()[8] == b'T'
                && digits(&at[9..15])
                && at.as_bytes()[15] == b'Z';
            if !(digits(snapshot) && stamped) {
                return None;
            }
            Some((at, snapshot.parse::<i64>().ok()?))
        })
        .max()
        .map(|(_, snapshot)| snapshot)
}

/// `file_prefixes(prefix)`: the PNU prefixes a change of the code change table can reach.
///
/// The prefix under both codes of every row that differs between the two snapshots (by kind, codes
/// and level). Returns how many, or `None` when the earlier snapshot can no longer be read (then
/// every row is linked again).
///
/// # Errors
/// `DuckDB` fails to read the change table at `after`.
pub fn changed_prefixes(
    stage: &DuckDbParquetStage,
    before: i64,
    after: i64,
) -> anyhow::Result<Option<u64>> {
    let read = |name: &str, snapshot: i64| {
        stage.connection().execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {name} AS \
             SELECT DISTINCT coalesce(CAST(kind AS VARCHAR), '') AS kind, coalesce(old_code, '') AS old_code, \
                    coalesce(new_code, '') AS new_code, coalesce(CAST(level AS VARCHAR), '') AS level \
             FROM {pinned}",
            pinned = crate::duckdb_parquet_stage::table_at(CODE_CHANGE_TABLE, snapshot)
        ))
    };
    read("file_changes_after", after).context("DuckDB failed to read the code change table")?;
    if let Err(error) = read("file_changes_before", before) {
        // An expired snapshot is a full re-link, never a skipped one.
        tracing::warn!(
            before,
            %error,
            "the code change table's earlier snapshot is unreadable; linking every row again"
        );
        return Ok(None);
    }
    stage.connection().execute_batch(
        "CREATE OR REPLACE TEMP TABLE file_prefixes AS
         WITH moved AS ((SELECT * FROM file_changes_before EXCEPT SELECT * FROM file_changes_after)
                        UNION (SELECT * FROM file_changes_after EXCEPT SELECT * FROM file_changes_before)),
              codes AS (SELECT old_code AS code FROM moved WHERE length(old_code) = 10
                        UNION SELECT new_code FROM moved WHERE length(new_code) = 10)
         SELECT DISTINCT left(code, CASE WHEN substr(code, 3) = '00000000' THEN 2 WHEN substr(code, 6) = '00000' THEN 5
                                         WHEN substr(code, 9) = '00' THEN 8 ELSE 10 END) AS prefix
         FROM codes",
    )?;
    count(stage, "file_prefixes").map(Some)
}

/// The filter of [`current`] for a follow: the rows a code change can move, under a changed code
/// (`file_prefixes`) or not linked at all; every row when the prefixes are unknown.
#[must_use]
pub fn affected_filter(link: PlaceLink, prefixes_known: bool) -> String {
    if !prefixes_known {
        return "TRUE".to_owned();
    }
    let under = |column: &str| {
        format!("EXISTS (SELECT 1 FROM file_prefixes p WHERE starts_with(c.{column}, p.prefix))")
    };
    let mut filter = format!("c.legal_dong_code IS NULL OR {}", under("legal_dong_code"));
    if link.parcel() {
        filter = format!("{filter} OR {}", under("pnu"));
    }
    filter
}

/// `file_staged`: the rows of `file_current` linked again (`file_follow_in`, then [`link`]), with
/// this run's instant.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn relinked(
    stage: &DuckDbParquetStage,
    file: &FileEntry,
    contract: &LakehouseTableContract,
    place: PlaceInputs<'_>,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let connection = stage.connection();
    connection.execute_batch(
        "CREATE OR REPLACE TEMP TABLE file_follow_in AS SELECT row_number() OVER () AS _row, * FROM file_current",
    )?;
    link(stage, file, "file_follow_in", place)?;
    // The link's columns, the instant and the removal column are this run's.
    let mut replaced = file.place_link()?.columns().to_vec();
    replaced.push("ingested_at_utc");
    replaced.extend(silver_changes::removal_column(contract));
    connection
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE file_staged AS SELECT {columns} FROM (\
             SELECT i.* EXCLUDE (_row, {replaced}), l.* EXCLUDE (_row), \
                    {at} AS ingested_at_utc{unremoved} \
             FROM file_follow_in i JOIN file_link l USING (_row))",
            columns = column_names(contract).join(", "),
            replaced = replaced.join(", "),
            at = epoch(ingested_at),
            unremoved = unremoved(contract),
        ))
        .with_context(|| format!("DuckDB failed to link the rows of {} again", file.slug))
}

#[cfg(test)]
mod tests;
