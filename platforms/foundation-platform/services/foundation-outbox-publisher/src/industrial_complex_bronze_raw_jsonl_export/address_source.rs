//! Injected address resolution for the industrial-complex Bronze JSONL producer.
//!
//! The profile workbook has no administrative location column, so the location has to come from a
//! separate source. This reader is the only way one enters the producer, and every field is
//! validated by `IndustrialComplexAddress` before it can be stored.

use std::{fs, path::Path};

use anyhow::Context as _;
use lakehouse_application::{IndustrialComplexAddress, IndustrialComplexAddressBook};
use serde::Deserialize;

/// One line of the address resolution JSONL.
#[derive(Debug, Deserialize)]
struct RawAddressResolution {
    official_complex_code: String,
    primary_bjdong_code: String,
    address_text: String,
    address_source_dataset: String,
    address_source_record_id: String,
}

/// Reads the injected address resolution file into an address book.
pub(super) fn read_address_book(path: &Path) -> anyhow::Result<IndustrialComplexAddressBook> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read the industrial-complex address resolution {}; an industrial complex \
             without a sourced address is not representable, so this export cannot proceed",
            path.display()
        )
    })?;
    parse_address_book(raw.as_str())
        .with_context(|| format!("invalid address resolution {}", path.display()))
}

fn parse_address_book(raw: &str) -> anyhow::Result<IndustrialComplexAddressBook> {
    let mut book = IndustrialComplexAddressBook::new();
    // Windows editors and PowerShell's `Set-Content -Encoding utf8` prefix a BOM. Left in place it
    // reaches serde_json as `expected value at line 1 column 1`, which reads like malformed JSON
    // rather than an encoding artifact; the first real run of this command hit exactly that.
    let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    for (index, line) in raw.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let line_number = index + 1;
        let resolution = serde_json::from_str::<RawAddressResolution>(line)
            .with_context(|| format!("invalid JSONL at address resolution line {line_number}"))?;
        let address = IndustrialComplexAddress::try_new(
            resolution.primary_bjdong_code.as_str(),
            resolution.address_text.as_str(),
            resolution.address_source_dataset.as_str(),
            resolution.address_source_record_id.as_str(),
        )
        .with_context(|| format!("invalid address at resolution line {line_number}"))?;
        book.insert(resolution.official_complex_code.as_str(), address)
            .with_context(|| format!("invalid address at resolution line {line_number}"))?;
    }
    if book.is_empty() {
        anyhow::bail!(
            "the address resolution is empty; an industrial complex without a sourced address is \
             not representable, so this export cannot proceed"
        );
    }
    Ok(book)
}

#[cfg(test)]
mod tests {
    use super::parse_address_book;

    const VALID_LINE: &str = concat!(
        r#"{"official_complex_code":"111010","primary_bjdong_code":"1153010200","#,
        r#""address_text":"서울특별시 구로구 구로동","#,
        r#""address_source_dataset":"ilis__industrial_complex_detail","#,
        r#""address_source_record_id":"ilis:111010"}"#
    );

    #[test]
    fn reads_one_resolution_per_line() -> anyhow::Result<()> {
        let book = parse_address_book(&format!("\n{VALID_LINE}\n\n"))?;

        assert_eq!(book.len(), 1);
        let address = book
            .get("111010")
            .ok_or_else(|| anyhow::anyhow!("missing resolution"))?;
        assert_eq!(address.sido_code(), "11");
        assert_eq!(address.sigungu_code(), "11530");
        Ok(())
    }

    #[test]
    fn rejects_a_resolution_that_leaves_the_location_blank() {
        for line in [
            r#"{"official_complex_code":"111010","primary_bjdong_code":"","address_text":"주소","address_source_dataset":"d","address_source_record_id":"r"}"#,
            r#"{"official_complex_code":"111010","primary_bjdong_code":"1153010200","address_text":"","address_source_dataset":"d","address_source_record_id":"r"}"#,
            r#"{"official_complex_code":"111010","primary_bjdong_code":"1153010200","address_text":"주소","address_source_dataset":"","address_source_record_id":"r"}"#,
            r#"{"official_complex_code":"","primary_bjdong_code":"1153010200","address_text":"주소","address_source_dataset":"d","address_source_record_id":"r"}"#,
        ] {
            let error = parse_address_book(line).expect_err("a blank location must be rejected");
            assert!(format!("{error:#}").contains("line 1"), "{error:#}");
        }
    }

    #[test]
    fn rejects_a_resolution_missing_its_provenance_fields() {
        let error = parse_address_book(
            r#"{"official_complex_code":"111010","primary_bjdong_code":"1153010200","address_text":"주소"}"#,
        )
        .expect_err("a resolution without provenance must be rejected");

        assert!(format!("{error:#}").contains("invalid JSONL"), "{error:#}");
    }

    #[test]
    fn reads_a_resolution_written_with_a_utf8_bom() -> anyhow::Result<()> {
        let book = parse_address_book(&format!("\u{feff}{VALID_LINE}\n"))?;

        assert_eq!(book.len(), 1);
        assert!(book.get("111010").is_some());
        Ok(())
    }

    #[test]
    fn rejects_an_empty_resolution_file() {
        let error = parse_address_book("\n\n").expect_err("an empty resolution must be rejected");

        assert!(format!("{error:#}").contains("is empty"), "{error:#}");
    }

    #[test]
    fn rejects_two_resolutions_for_the_same_complex() {
        let error = parse_address_book(&format!("{VALID_LINE}\n{VALID_LINE}"))
            .expect_err("a repeated resolution must be rejected");

        assert!(format!("{error:#}").contains("duplicate"), "{error:#}");
    }
}
