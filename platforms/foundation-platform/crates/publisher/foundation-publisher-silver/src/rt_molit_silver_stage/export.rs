//! One export as the provider writes it, read into named rows (root ADR-0349 §2, ADR-0364).
//!
//! `rt_molit_real_transaction.py`'s `export_lines`, `rows_of` and `build_row`, rule for rule: the
//! text is CSV from the header line on (a quoted field may span lines; only CR and LF end a line),
//! a record is rejected when its cell count is not the header's, its `NO` does not continue the
//! export's 1..N (the 2026-10-30 export carried a second export's rows after its own) or it names
//! another month, and an export without its header or whose preamble names another month is
//! refused whole.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::NaiveDate;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{PlannedObject, RtMolitSource};

/// A line of an export that is not loaded, with the line that finds it again in the Bronze object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rejected {
    /// The line the record starts on, from 1; 0 for an export refused whole.
    pub line: u32,
    /// The record's text.
    pub raw: String,
    /// Why it is not loaded.
    pub reason: String,
}

/// A row of an export: its line and the Silver columns the export fills, by name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    /// The line the record starts on, from 1.
    pub line: u32,
    /// The columns, as JSON values `DuckDB` reads by the contract's types.
    pub values: Map<String, Value>,
}

/// One export, parsed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Parsed {
    /// The rows that passed the line checks, in the export's order.
    pub rows: Vec<Row>,
    /// The lines not loaded.
    pub rejected: Vec<Rejected>,
    /// The header's columns no Silver column carries, sorted.
    pub unrepresented: Vec<String>,
    /// Whether the export is not the provider's CSV for its month (one rejected line 0, no rows).
    pub refused: bool,
}

#[derive(Clone, Copy)]
enum Reader {
    Text,
    /// A whole number; 만원 as the provider writes amounts.
    Whole,
    Day,
}

/// CSV column → (Silver column, reader).
const COLUMNS: [(&str, &str, Reader); 36] = [
    ("시군구", "address_raw", Reader::Text),
    ("번지", "jibun_raw", Reader::Text),
    ("지번", "jibun_raw", Reader::Text),
    ("본번", "bonbun", Reader::Text),
    ("부번", "bubun", Reader::Text),
    ("단지명", "building_name", Reader::Text),
    ("건물명", "building_name", Reader::Text),
    ("유형", "building_type", Reader::Text),
    ("주택유형", "house_type", Reader::Text),
    ("건축물주용도", "building_use", Reader::Text),
    ("용도지역", "land_use", Reader::Text),
    ("지목", "jimok", Reader::Text),
    ("도로조건", "road_condition", Reader::Text),
    ("도로명", "road_address", Reader::Text),
    ("거래금액(만원)", "deal_amount_10k_krw", Reader::Whole),
    ("보증금(만원)", "deposit_10k_krw", Reader::Whole),
    ("월세금(만원)", "monthly_rent_10k_krw", Reader::Whole),
    (
        "종전계약 보증금(만원)",
        "pre_deposit_10k_krw",
        Reader::Whole,
    ),
    (
        "종전계약 월세(만원)",
        "pre_monthly_rent_10k_krw",
        Reader::Whole,
    ),
    ("전월세구분", "rent_kind", Reader::Text),
    ("계약기간", "contract_term", Reader::Text),
    ("계약구분", "contract_type", Reader::Text),
    ("갱신요구권 사용", "use_rr_right", Reader::Text),
    ("동", "apt_dong", Reader::Text),
    ("층", "floor", Reader::Text),
    ("건축년도", "build_year", Reader::Whole),
    ("매수자", "buyer_gbn", Reader::Text),
    ("매수", "buyer_gbn", Reader::Text),
    ("매도자", "seller_gbn", Reader::Text),
    ("매도", "seller_gbn", Reader::Text),
    ("거래유형", "dealing_gbn", Reader::Text),
    ("중개사소재지", "estate_agent_sgg_nm", Reader::Text),
    ("지분구분", "share_dealing_type", Reader::Text),
    ("분/입주권", "ownership_gbn", Reader::Text),
    ("해제사유발생일", "cancel_date", Reader::Day),
    ("등기일자", "registration_date", Reader::Day),
];
/// Areas: CSV column → (column, kind column, kind). 계약면적 is the land's in the 토지 export
/// (`계약면적`) and the house's in the 단독다가구 전월세 export (`계약면적(㎡)`).
const AREAS: [(&str, &str, &str, &str); 7] = [
    (
        "전용면적(㎡)",
        "building_area_m2",
        "building_area_kind",
        "exclusive",
    ),
    (
        "전용/연면적(㎡)",
        "building_area_m2",
        "building_area_kind",
        "exclusive_or_total_floor",
    ),
    (
        "연면적(㎡)",
        "building_area_m2",
        "building_area_kind",
        "total_floor",
    ),
    (
        "계약면적(㎡)",
        "building_area_m2",
        "building_area_kind",
        "contract",
    ),
    ("대지면적(㎡)", "land_area_m2", "land_area_kind", "plottage"),
    (
        "대지권면적(㎡)",
        "land_area_m2",
        "land_area_kind",
        "land_right",
    ),
    ("계약면적", "land_area_m2", "land_area_kind", "deal"),
];
/// Header columns read elsewhere than [`COLUMNS`] and [`AREAS`].
const KNOWN_ELSEWHERE: [&str; 3] = ["NO", "계약년월", "계약일"];
const DEAL_MONTH: &str = "계약년월";
const DEAL_DAY: &str = "계약일";

/// A cell's text, trimmed; `None` for blank or `-`.
fn text_or_none(value: &str) -> Option<&str> {
    let text = value.trim();
    (!text.is_empty() && text != "-").then_some(text)
}

/// A whole number with its thousands commas removed; `None` for anything else.
fn whole(value: &str) -> Option<i64> {
    let text = text_or_none(value)?.replace(',', "");
    let digits = text.strip_prefix('-').unwrap_or(&text);
    (!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// A decimal with its thousands commas removed; `None` for anything else.
fn decimal(value: &str) -> Option<f64> {
    text_or_none(value)?
        .replace(',', "")
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
}

/// `20261028` (or `26.10.28`, `2026.10.28`, `2026-10-28`) as a date; `None` for anything else.
fn day(value: &str) -> Option<NaiveDate> {
    let text = text_or_none(value)?;
    ["%Y%m%d", "%y.%m.%d", "%Y.%m.%d", "%Y-%m-%d"]
        .iter()
        .find_map(|format| NaiveDate::parse_from_str(text, format).ok())
}

fn json_or_null<T: Into<Value>>(value: Option<T>) -> Value {
    value.map_or(Value::Null, Into::into)
}

/// One CSV record: its cells and its text, less the line end.
struct Record {
    cells: Vec<String>,
    raw: String,
}

/// The records of `text`, as Python's `csv.reader` reads them with its default dialect: a `"` opens
/// a quoted field only at a field's start, `""` inside one is a quote, a quoted field may hold line
/// ends, and CR, LF or CRLF outside quotes ends the record. A blank line is a record of no cell.
fn records(text: &str) -> Vec<Record> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        RecordStart,
        FieldStart,
        Field,
        Quoted,
        QuoteInQuoted,
    }
    let mut found = Vec::new();
    let mut cells: Vec<String> = Vec::new();
    let mut cell = String::new();
    let mut raw = String::new();
    let mut state = State::RecordStart;
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        let line_end = matches!(character, '\r' | '\n');
        if line_end && state != State::Quoted {
            if character == '\r' && chars.peek() == Some(&'\n') {
                chars.next();
            }
            if state != State::RecordStart {
                cells.push(std::mem::take(&mut cell));
            }
            found.push(Record {
                cells: std::mem::take(&mut cells),
                raw: std::mem::take(&mut raw),
            });
            state = State::RecordStart;
            continue;
        }
        raw.push(character);
        state = match (state, character) {
            (State::RecordStart | State::FieldStart, '"') => State::Quoted,
            (State::RecordStart | State::FieldStart | State::Field | State::QuoteInQuoted, ',') => {
                cells.push(std::mem::take(&mut cell));
                State::FieldStart
            }
            (State::Quoted, '"') => State::QuoteInQuoted,
            (State::Quoted, _) => {
                cell.push(character);
                State::Quoted
            }
            (State::QuoteInQuoted, '"') => {
                cell.push('"');
                State::Quoted
            }
            (State::RecordStart | State::FieldStart | State::Field | State::QuoteInQuoted, _) => {
                cell.push(character);
                State::Field
            }
        };
    }
    if state != State::RecordStart {
        cells.push(cell);
        found.push(Record { cells, raw });
    }
    found
}

/// The export's text, or why it is not the provider's.
fn decode(data: &[u8], encoding: &str) -> Result<String, String> {
    // `cp949` is Python's name for the table the Encoding Standard calls EUC-KR (windows-949).
    let encoding = if encoding.eq_ignore_ascii_case("cp949") {
        encoding_rs::EUC_KR
    } else {
        encoding_rs::Encoding::for_label(encoding.as_bytes())
            .ok_or_else(|| format!("the contract's encoding {encoding} is not known"))?
    };
    encoding
        .decode_without_bom_handling_and_without_replacement(data)
        .map(std::borrow::Cow::into_owned)
        .ok_or_else(|| format!("the export is not {} text", encoding.name()))
}

/// The month (`YYYYMM`) the preamble's `계약일자 : YYYY-MM-DD ~ YYYY-MM-DD` names at both ends.
fn preamble_month(preamble: &str) -> Option<(String, String)> {
    let range =
        regex::Regex::new(r"계약일자 : (\d{4})-(\d{2})-\d{2} ~ (\d{4})-(\d{2})-\d{2}").ok()?;
    let found = range.captures(preamble)?;
    let part = |index: usize| found.get(index).map_or("", |m| m.as_str());
    Some((
        format!("{}{}", part(1), part(2)),
        format!("{}{}", part(3), part(4)),
    ))
}

/// A row's fields in the header's order; a repeated name keeps its first place and its last value.
fn fields_of<'a>(header: &'a [String], cells: &'a [String]) -> Vec<(&'a str, &'a str)> {
    let mut fields: Vec<(&str, &str)> = Vec::with_capacity(header.len());
    for (name, value) in header.iter().zip(cells) {
        if let Some(found) = fields.iter_mut().find(|(known, _)| *known == name.as_str()) {
            found.1 = value.as_str();
        } else {
            fields.push((name.as_str(), value.as_str()));
        }
    }
    fields
}

/// `fields` as the JSON Python's `json.dumps(…, ensure_ascii=False, sort_keys=True,
/// separators=(",", ":"))` writes: keys in code point order, no space.
fn sorted_json<'a>(fields: impl Iterator<Item = (&'a str, &'a str)>) -> String {
    let sorted: BTreeMap<&str, &str> = fields.collect();
    serde_json::to_string(&sorted).unwrap_or_default()
}

/// The sha256 (first 32 hex) of a row's bucket and its fields less the row number and mutable ones.
fn identity(dataset: &str, month: &str, fields: &[(&str, &str)], drop: &[&str]) -> String {
    let stable = sorted_json(
        fields
            .iter()
            .copied()
            .filter(|(name, _)| !drop.contains(name)),
    );
    let digest = Sha256::digest(format!("{dataset}|{month}|{stable}").as_bytes());
    digest.iter().take(16).fold(String::new(), |mut hex, byte| {
        // Writing to a String cannot fail.
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// The Silver columns one row's fields fill (`fields` without the row number column).
fn build_row(dataset: &str, month: &str, fields: &[(&str, &str)]) -> Map<String, Value> {
    let mut row = Map::new();
    row.insert("dataset_slug".to_owned(), dataset.into());
    row.insert("deal_ym".to_owned(), month.into());
    for (name, value) in fields {
        if let Some((_, column, reader)) = COLUMNS.iter().find(|(known, _, _)| known == name) {
            let read = match reader {
                Reader::Text => json_or_null(text_or_none(value)),
                Reader::Whole => json_or_null(whole(value)),
                Reader::Day => json_or_null(day(value).map(|date| date.to_string())),
            };
            row.insert((*column).to_owned(), read);
        }
        if let Some((_, column, kind_column, kind)) =
            AREAS.iter().find(|(known, _, _, _)| known == name)
        {
            let area = decimal(value);
            row.insert((*kind_column).to_owned(), json_or_null(area.map(|_| *kind)));
            row.insert((*column).to_owned(), json_or_null(area));
        }
    }
    let deal_day = fields
        .iter()
        .find(|(name, _)| *name == DEAL_DAY)
        .and_then(|(_, value)| value.trim().parse::<u32>().ok());
    let deal_date = deal_day.and_then(|day_of_month| {
        NaiveDate::from_ymd_opt(
            month.get(..4)?.parse().ok()?,
            month.get(4..)?.parse().ok()?,
            day_of_month,
        )
    });
    row.insert(
        "deal_date".to_owned(),
        json_or_null(deal_date.map(|date| date.to_string())),
    );
    let cancelled = row.get("cancel_date").is_some_and(|date| !date.is_null());
    row.insert("cancelled".to_owned(), cancelled.into());
    let masked = row
        .get("jibun_raw")
        .and_then(Value::as_str)
        .is_some_and(|lot| lot.contains('*'));
    row.insert("jibun_masked".to_owned(), masked.into());
    row.insert(
        "provider_fields_json".to_owned(),
        sorted_json(fields.iter().copied()).into(),
    );
    row
}

fn refused(reason: &str) -> Parsed {
    Parsed {
        rejected: vec![Rejected {
            line: 0,
            raw: String::new(),
            reason: format!("export_refused: {reason}"),
        }],
        refused: true,
        ..Parsed::default()
    }
}

/// Reads one export's bytes into its named rows and its rejected lines.
///
/// Rows of the same name are ranked by the columns filled in after the fact, then by line, so a
/// rank is stable across re-pulls; `row_key` is `<name>-<rank>`.
#[must_use]
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)] // one pass over the export, rule by rule
pub fn parse_export(data: &[u8], object: &PlannedObject, source: &RtMolitSource) -> Parsed {
    let text = match decode(data, &source.encoding) {
        Ok(text) => text,
        Err(reason) => return refused(&reason),
    };
    let (dataset, month) = (object.dataset.as_str(), object.month.as_str());
    let marker = format!("\"{}\"", source.header_first_column);
    let Some(start) = text.find(&marker).filter(|start| {
        *start == 0
            || text
                .get(..*start)
                .and_then(|before| before.chars().next_back())
                .is_some_and(|before| matches!(before, '\r' | '\n'))
    }) else {
        return refused("the export has no header line: not the provider's CSV");
    };
    let (preamble, body) = text.split_at(start);
    if preamble_month(preamble).is_none_or(|(from, to)| from != month || to != month) {
        return refused(&format!("the preamble does not name the month {month}"));
    }
    let mut lines = records(body).into_iter();
    let Some(header) = lines.next().map(|record| record.cells) else {
        return refused("the export has no header line: not the provider's CSV");
    };
    let number_column = source.row_number_column.as_str();
    let mutable: Vec<&str> = source.mutable_columns.iter().map(String::as_str).collect();
    let drop: Vec<&str> = std::iter::once(number_column)
        .chain(mutable.iter().copied())
        .collect();
    let mut parsed = Parsed {
        unrepresented: {
            let mut unknown: Vec<String> = header
                .iter()
                .filter(|name| {
                    let name = name.as_str();
                    !COLUMNS.iter().any(|(known, _, _)| *known == name)
                        && !AREAS.iter().any(|(known, _, _, _)| *known == name)
                        && !KNOWN_ELSEWHERE.contains(&name)
                })
                .cloned()
                .collect();
            unknown.sort();
            unknown.dedup();
            unknown
        },
        ..Parsed::default()
    };
    // (name, the mutable fields, the row), in the export's order.
    let mut named: Vec<(String, Vec<String>, Row)> = Vec::new();
    let mut expected = 1_u64;
    let mut line = u32::try_from(preamble.matches('\n').count())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    for record in lines {
        let number = line.saturating_add(1);
        line = number
            .saturating_add(u32::try_from(record.raw.matches('\n').count()).unwrap_or(u32::MAX));
        let cells = record.cells;
        if cells.iter().all(|cell| cell.trim().is_empty()) {
            continue;
        }
        let first = cells.first().map_or("", |cell| cell.trim());
        if cells.len() != header.len() {
            // A broken row of this export still took its number; another export's row did not.
            if first == expected.to_string() {
                expected += 1;
            }
            parsed.rejected.push(Rejected {
                line: number,
                raw: record.raw,
                reason: format!("cells_{}_of_{}", cells.len(), header.len()),
            });
            continue;
        }
        if first != expected.to_string() {
            parsed.rejected.push(Rejected {
                line: number,
                raw: record.raw,
                reason: format!("row_number_{first}_where_{expected}"),
            });
            continue;
        }
        expected += 1;
        let fields = fields_of(&header, &cells);
        let named_month = fields
            .iter()
            .find(|(name, _)| *name == DEAL_MONTH)
            .map_or(month, |(_, value)| value.trim());
        if named_month != month {
            parsed.rejected.push(Rejected {
                line: number,
                raw: cells
                    .iter()
                    .map(|cell| format!("\"{cell}\""))
                    .collect::<Vec<_>>()
                    .join(","),
                reason: "deal_month_outside_export".to_owned(),
            });
            continue;
        }
        let name = identity(dataset, month, &fields, &drop);
        let sort = mutable
            .iter()
            .map(|column| {
                fields
                    .iter()
                    .find(|(name, _)| name == column)
                    .map_or("", |(_, value)| *value)
                    .to_owned()
            })
            .collect();
        let without_number: Vec<(&str, &str)> = fields
            .iter()
            .copied()
            .filter(|(name, _)| *name != number_column)
            .collect();
        let mut values = build_row(dataset, month, &without_number);
        values.insert(
            "source_record_id".to_owned(),
            object.object_key.as_str().into(),
        );
        values.insert("source_line_number".to_owned(), number.into());
        named.push((
            name,
            sort,
            Row {
                line: number,
                values,
            },
        ));
    }
    // Same name: ranked by the mutable fields (a stable sort, so then by line).
    let mut order: Vec<usize> = (0..named.len()).collect();
    order.sort_by(|a, b| {
        let (left, right) = (named.get(*a), named.get(*b));
        left.map(|row| (&row.0, &row.1))
            .cmp(&right.map(|row| (&row.0, &row.1)))
    });
    let mut ranks = vec![0_u64; named.len()];
    let mut previous: Option<&str> = None;
    let mut rank = 0_u64;
    for index in order {
        let Some((name, _, _)) = named.get(index) else {
            continue;
        };
        rank = if previous == Some(name.as_str()) {
            rank + 1
        } else {
            1
        };
        previous = Some(name.as_str());
        if let Some(slot) = ranks.get_mut(index) {
            *slot = rank;
        }
    }
    parsed.rows = named
        .into_iter()
        .zip(ranks)
        .map(|((name, _, mut row), rank)| {
            row.values
                .insert("row_key".to_owned(), format!("{name}-{rank}").into());
            row
        })
        .collect();
    parsed
}

#[cfg(test)]
pub(super) fn records_for_test(text: &str) -> Vec<(Vec<String>, String)> {
    records(text)
        .into_iter()
        .map(|record| (record.cells, record.raw))
        .collect()
}
