//! `TB_IRSTT_BASS_HIST.xlsx` decoding for the industrial-complex profile Bronze object.
//!
//! Columns are located by header name, never by position, so a provider that inserts a column does
//! not silently shift every field by one.

use std::{collections::BTreeMap, io::Cursor};

use anyhow::{bail, Context as _};
use calamine::{open_workbook_from_rs, Data, Reader, Xlsx};
use lakehouse_application::IndustrialComplexBronzeSourceRecord;

const HEADER_SNAPSHOT_PERIOD: &str = "sta_ym";
const HEADER_COMPLEX_CODE: &str = "krihs_irstt_code";
const HEADER_COMPLEX_NAME: &str = "krihs_irstt_nm";
const HEADER_COMPLEX_KIND: &str = "lrstt_ty";
const HEADER_STATUS: &str = "make_sttus_nm";
const HEADER_MANAGEMENT_AGENCY: &str = "manage_instt_nm";
/// The provider spells the developing-operator column `bsms_`; the typo is theirs and is load
/// bearing, so it is matched verbatim.
const HEADER_DEVELOPER: &str = "bsms_opertn_entrps_nm";
const HEADER_DESIGNATED_DATE: &str = "appn_de";
const HEADER_COMPLETION_DATE: &str = "compet_cnfm_de";
const HEADER_AREA: &str = "appn_ar";

const REQUIRED_HEADERS: &[&str] = &[
    HEADER_SNAPSHOT_PERIOD,
    HEADER_COMPLEX_CODE,
    HEADER_COMPLEX_NAME,
    HEADER_COMPLEX_KIND,
    HEADER_STATUS,
    HEADER_MANAGEMENT_AGENCY,
    HEADER_DEVELOPER,
    HEADER_DESIGNATED_DATE,
    HEADER_COMPLETION_DATE,
    HEADER_AREA,
];

/// Decodes the profile worksheet into source records.
pub(super) fn decode_profile_rows(
    workbook_bytes: Vec<u8>,
    sheet_name: Option<&str>,
    max_rows: Option<usize>,
) -> anyhow::Result<Vec<IndustrialComplexBronzeSourceRecord>> {
    let mut workbook: Xlsx<_> = open_workbook_from_rs(Cursor::new(workbook_bytes))
        .context("failed to open the industrial-complex profile workbook")?;
    let sheet_name = select_sheet(&workbook, sheet_name)?;
    let range = workbook
        .worksheet_range(sheet_name.as_str())
        .with_context(|| format!("failed to read worksheet {sheet_name}"))?;

    let mut rows = range.rows();
    let header_row = rows
        .next()
        .context("the profile worksheet has no header row")?;
    let headers = header_index(header_row)?;

    let mut records = Vec::new();
    for (offset, row) in rows.enumerate() {
        if matches!(max_rows, Some(limit) if records.len() >= limit) {
            break;
        }
        if row.iter().all(|cell| cell_text(cell).is_none()) {
            continue;
        }
        // Row 1 is the header, so the first data row is row 2.
        let source_row_number = u64::try_from(offset + 2).context("row number exceeded u64")?;
        records.push(IndustrialComplexBronzeSourceRecord {
            official_complex_code: required_cell(
                row,
                &headers,
                HEADER_COMPLEX_CODE,
                source_row_number,
            )?,
            complex_name: required_cell(row, &headers, HEADER_COMPLEX_NAME, source_row_number)?,
            complex_kind_label: required_cell(
                row,
                &headers,
                HEADER_COMPLEX_KIND,
                source_row_number,
            )?,
            status_label: required_cell(row, &headers, HEADER_STATUS, source_row_number)?,
            management_agency_name: optional_cell(row, &headers, HEADER_MANAGEMENT_AGENCY),
            developer_name: optional_cell(row, &headers, HEADER_DEVELOPER),
            designated_date_raw: optional_cell(row, &headers, HEADER_DESIGNATED_DATE),
            completion_date_raw: optional_cell(row, &headers, HEADER_COMPLETION_DATE),
            official_area_sqm_raw: optional_cell(row, &headers, HEADER_AREA),
            snapshot_period: required_cell(
                row,
                &headers,
                HEADER_SNAPSHOT_PERIOD,
                source_row_number,
            )?,
            source_row_number,
        });
    }
    if records.is_empty() {
        bail!("the profile worksheet {sheet_name} has no data rows");
    }
    Ok(records)
}

fn select_sheet<RS>(workbook: &Xlsx<RS>, pinned: Option<&str>) -> anyhow::Result<String>
where
    RS: std::io::Read + std::io::Seek,
{
    let names = workbook.sheet_names();
    if let Some(pinned) = pinned {
        if !names.iter().any(|name| name == pinned) {
            bail!("pinned worksheet {pinned} not found; workbook holds {names:?}");
        }
        return Ok(pinned.to_owned());
    }
    match names.as_slice() {
        [only] => Ok(only.clone()),
        [] => bail!("the profile workbook holds no worksheet"),
        _ => bail!(
            "the profile workbook holds {} worksheets {names:?}; pin the exact worksheet",
            names.len()
        ),
    }
}

fn header_index(header_row: &[Data]) -> anyhow::Result<BTreeMap<String, usize>> {
    let mut headers = BTreeMap::new();
    for (index, cell) in header_row.iter().enumerate() {
        if let Some(name) = cell_text(cell) {
            headers.entry(name.to_lowercase()).or_insert(index);
        }
    }
    let missing = REQUIRED_HEADERS
        .iter()
        .filter(|header| !headers.contains_key(**header))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let found = headers.keys().cloned().collect::<Vec<_>>();
        bail!("the profile worksheet is missing columns {missing:?}; it holds {found:?}");
    }
    Ok(headers)
}

fn required_cell(
    row: &[Data],
    headers: &BTreeMap<String, usize>,
    header: &str,
    source_row_number: u64,
) -> anyhow::Result<String> {
    optional_cell(row, headers, header)
        .with_context(|| format!("row {source_row_number}: required column {header} is blank"))
}

fn optional_cell(row: &[Data], headers: &BTreeMap<String, usize>, header: &str) -> Option<String> {
    headers
        .get(header)
        .and_then(|index| row.get(*index))
        .and_then(cell_text)
}

/// Renders one cell as trimmed text, treating a blank cell as absent.
fn cell_text(cell: &Data) -> Option<String> {
    let text = match cell {
        Data::Empty => return None,
        Data::String(value) => value.trim().to_owned(),
        Data::Int(value) => value.to_string(),
        Data::Float(value) => format_float(*value),
        Data::Bool(value) => value.to_string(),
        Data::DateTime(value) => value.as_datetime().map_or_else(
            || format_float(value.as_f64()),
            |value| value.format("%Y%m%d").to_string(),
        ),
        Data::DateTimeIso(value) | Data::DurationIso(value) => value.trim().to_owned(),
        Data::Error(_) => return None,
    };
    (!text.is_empty()).then_some(text)
}

/// Renders a numeric cell without the trailing `.0` an integral f64 would otherwise carry, which
/// would turn `19640415` into `19640415.0` and fail date validation.
fn format_float(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        return format!("{value:.0}");
    }
    value.to_string()
}

#[cfg(test)]
pub(super) mod tests_support {
    use std::io::Write as _;

    use zip::{write::SimpleFileOptions, ZipWriter};

    /// Builds a minimal single-sheet xlsx whose first row is the header row.
    ///
    /// Cells that parse as a number are written as numeric cells and everything else as an inline
    /// string, so the decoder is exercised against both `Data::Float` and `Data::String`.
    pub(in crate::industrial_complex_bronze_raw_jsonl_export) fn build_profile_workbook(
        rows: &[&[&str]],
    ) -> anyhow::Result<Vec<u8>> {
        let mut sheet = String::from(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>"#,
        );
        for (row_index, row) in rows.iter().enumerate() {
            sheet.push_str(&format!(r#"<row r="{}">"#, row_index + 1));
            for (column_index, value) in row.iter().enumerate() {
                let reference = format!("{}{}", column_name(column_index), row_index + 1);
                if value.is_empty() {
                    continue;
                }
                if row_index > 0 && value.parse::<f64>().is_ok() {
                    sheet.push_str(&format!(r#"<c r="{reference}"><v>{value}</v></c>"#));
                } else {
                    sheet.push_str(&format!(
                        r#"<c r="{reference}" t="inlineStr"><is><t>{}</t></is></c>"#,
                        escape_xml(value)
                    ));
                }
            }
            sheet.push_str("</row>");
        }
        sheet.push_str("</sheetData></worksheet>");

        let mut buffer = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut buffer);
            let mut writer = ZipWriter::new(cursor);
            let options = SimpleFileOptions::default();
            for (name, body) in [
                ("[Content_Types].xml", CONTENT_TYPES),
                ("_rels/.rels", ROOT_RELS),
                ("xl/workbook.xml", WORKBOOK),
                ("xl/_rels/workbook.xml.rels", WORKBOOK_RELS),
            ] {
                writer.start_file(name, options)?;
                writer.write_all(body.as_bytes())?;
            }
            writer.start_file("xl/worksheets/sheet1.xml", options)?;
            writer.write_all(sheet.as_bytes())?;
            writer.finish()?;
        }
        Ok(buffer)
    }

    fn column_name(index: usize) -> String {
        let mut index = index;
        let mut name = String::new();
        loop {
            name.insert(0, char::from(b'A' + u8::try_from(index % 26).unwrap_or(0)));
            if index < 26 {
                return name;
            }
            index = index / 26 - 1;
        }
    }

    fn escape_xml(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    const CONTENT_TYPES: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;

    const ROOT_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;

    const WORKBOOK: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="TB_IRSTT_BASS_HIST" sheetId="1" r:id="rId1"/></sheets></workbook>"#;

    const WORKBOOK_RELS: &str = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;
}

#[cfg(test)]
mod tests {
    use super::{decode_profile_rows, tests_support::build_profile_workbook};

    fn header() -> &'static [&'static str] {
        &[
            "sta_ym",
            "krihs_irstt_code",
            "krihs_irstt_nm",
            "lrstt_ty",
            "make_sttus_nm",
            "manage_instt_nm",
            "bsms_opertn_entrps_nm",
            "appn_de",
            "compet_cnfm_de",
            "appn_ar",
        ]
    }

    #[test]
    fn decodes_numeric_cells_without_a_trailing_decimal_point() -> anyhow::Result<()> {
        let workbook = build_profile_workbook(&[
            header(),
            &[
                "202506",
                "111010",
                "구로디지털단지",
                "국가",
                "조성완료",
                "한국산업단지공단",
                "",
                "19640415",
                "",
                "1925368.7",
            ],
        ])?;

        let records = decode_profile_rows(workbook, None, None)?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].snapshot_period, "202506");
        assert_eq!(records[0].official_complex_code, "111010");
        assert_eq!(records[0].complex_name, "구로디지털단지");
        assert_eq!(records[0].designated_date_raw.as_deref(), Some("19640415"));
        assert_eq!(records[0].completion_date_raw, None);
        assert_eq!(records[0].developer_name, None);
        assert_eq!(
            records[0].official_area_sqm_raw.as_deref(),
            Some("1925368.7")
        );
        assert_eq!(records[0].source_row_number, 2);
        Ok(())
    }

    #[test]
    fn columns_are_located_by_header_name_not_position() -> anyhow::Result<()> {
        let mut shuffled = header().to_vec();
        shuffled.reverse();
        shuffled.insert(0, "unrelated_new_provider_column");
        let mut values = vec![
            "1925368.7",
            "",
            "19640415",
            "",
            "한국산업단지공단",
            "조성완료",
            "국가",
            "구로디지털단지",
            "111010",
            "202506",
        ];
        values.insert(0, "noise");

        let records =
            decode_profile_rows(build_profile_workbook(&[&shuffled, &values])?, None, None)?;

        assert_eq!(records[0].official_complex_code, "111010");
        assert_eq!(records[0].snapshot_period, "202506");
        assert_eq!(
            records[0].official_area_sqm_raw.as_deref(),
            Some("1925368.7")
        );
        Ok(())
    }

    #[test]
    fn a_missing_provider_column_names_what_is_missing() -> anyhow::Result<()> {
        let mut incomplete = header().to_vec();
        incomplete.retain(|name| *name != "appn_ar");

        let error = decode_profile_rows(
            build_profile_workbook(&[&incomplete, &["202506"]])?,
            None,
            None,
        )
        .expect_err("a missing column must fail");

        assert!(format!("{error:#}").contains("appn_ar"), "{error:#}");
        Ok(())
    }

    #[test]
    fn a_blank_required_cell_fails_with_its_row_number() -> anyhow::Result<()> {
        let error = decode_profile_rows(
            build_profile_workbook(&[
                header(),
                &["202506", "", "구로", "국가", "조성완료", "", "", "", "", ""],
            ])?,
            None,
            None,
        )
        .expect_err("a blank identity cell must fail");

        assert!(format!("{error:#}").contains("row 2"), "{error:#}");
        Ok(())
    }

    #[test]
    fn max_rows_bounds_a_smoke_decode() -> anyhow::Result<()> {
        let row_a = &[
            "202506",
            "111010",
            "가",
            "국가",
            "조성완료",
            "",
            "",
            "",
            "",
            "1",
        ];
        let row_b = &[
            "202506",
            "111020",
            "나",
            "국가",
            "조성완료",
            "",
            "",
            "",
            "",
            "2",
        ];
        let records = decode_profile_rows(
            build_profile_workbook(&[header(), row_a, row_b])?,
            None,
            Some(1),
        )?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].official_complex_code, "111010");
        Ok(())
    }
}
