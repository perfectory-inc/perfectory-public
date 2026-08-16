//! Injected address resolution for the industrial-complex Bronze JSONL producer.
//!
//! The profile workbook has no administrative location column, so the location has to come from a
//! separate source. This reader is the only way one enters the producer, and every field is
//! validated by `IndustrialComplexAddress` before it can be stored.
//!
//! Each line also declares how it was resolved (`resolution_tier`). Two of the three tiers are
//! exact and one is a heuristic, so "which city is this complex in?" and "how do we know?" have to
//! be answerable from the same artifact — the export summary carries the per-tier counts forward
//! (root ADR-0034).

use std::{collections::BTreeMap, fs, path::Path};

use anyhow::Context as _;
// The tier vocabulary belongs to the derivation that writes it, not to this reader: one definition
// means the producer and the consumer cannot spell the same tier differently.
use lakehouse_application::{
    IndustrialComplexAddress, IndustrialComplexAddressBook, IndustrialComplexAddressGranularity,
    ResolutionTier, NOTICE_DATASET_SLUG_SUFFIX,
};
use serde::Deserialize;

/// One line of the address resolution JSONL.
#[derive(Debug, Deserialize)]
struct RawAddressResolution {
    official_complex_code: String,
    administrative_code: String,
    administrative_code_granularity: String,
    address_text: String,
    address_source_dataset: String,
    address_source_record_id: String,
    resolution_tier: String,
}

/// An address book plus the evidence about how it was built.
#[derive(Debug)]
pub(super) struct AddressResolution {
    pub(super) book: IndustrialComplexAddressBook,
    /// Number of resolutions produced by each tier, keyed by wire name.
    pub(super) tier_counts: BTreeMap<&'static str, u64>,
    /// Number of resolutions at each administrative granularity, keyed by wire name.
    pub(super) granularity_counts: BTreeMap<&'static str, u64>,
}

/// Reads the injected address resolution file into an address book.
pub(super) fn read_address_book(path: &Path) -> anyhow::Result<AddressResolution> {
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

fn parse_address_book(raw: &str) -> anyhow::Result<AddressResolution> {
    let mut book = IndustrialComplexAddressBook::new();
    let mut tier_counts = BTreeMap::<&'static str, u64>::new();
    let mut granularity_counts = BTreeMap::<&'static str, u64>::new();
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
        let tier = ResolutionTier::from_wire(resolution.resolution_tier.as_str())
            .with_context(|| format!("invalid address at resolution line {line_number}"))?;
        if !tier.admits_source_dataset(resolution.address_source_dataset.trim()) {
            anyhow::bail!(
                "invalid address at resolution line {line_number}: resolution_tier {:?} cannot be \
                 sourced from {:?}; only {:?} reads a code out of a \
                 {NOTICE_DATASET_SLUG_SUFFIX} dataset",
                tier.wire_name(),
                resolution.address_source_dataset.trim(),
                ResolutionTier::ModalNoticeCode.wire_name()
            );
        }
        let granularity = IndustrialComplexAddressGranularity::from_wire(
            resolution.administrative_code_granularity.as_str(),
        )
        .with_context(|| format!("invalid address at resolution line {line_number}"))?;
        let address = IndustrialComplexAddress::try_new(
            resolution.administrative_code.as_str(),
            granularity,
            resolution.address_text.as_str(),
            resolution.address_source_dataset.as_str(),
            resolution.address_source_record_id.as_str(),
        )
        .with_context(|| format!("invalid address at resolution line {line_number}"))?;
        book.insert(resolution.official_complex_code.as_str(), address)
            .with_context(|| format!("invalid address at resolution line {line_number}"))?;
        *tier_counts.entry(tier.wire_name()).or_insert(0) += 1;
        *granularity_counts
            .entry(granularity.wire_name())
            .or_insert(0) += 1;
    }
    if book.is_empty() {
        anyhow::bail!(
            "the address resolution is empty; an industrial complex without a sourced address is \
             not representable, so this export cannot proceed"
        );
    }
    Ok(AddressResolution {
        book,
        tier_counts,
        granularity_counts,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_address_book;

    const VALID_LINE: &str = concat!(
        r#"{"official_complex_code":"111010","administrative_code":"1153000000","#,
        r#""administrative_code_granularity":"sigungu","#,
        r#""address_text":"서울특별시 구로구 구로동","#,
        r#""address_source_dataset":"industrylandorkr__industrial_complex_list","#,
        r#""address_source_record_id":"industrylandorkr:danji_cd=111010","#,
        r#""resolution_tier":"source_code_in_authority"}"#
    );

    const VALID_LEGAL_DONG_LINE: &str = concat!(
        r#"{"official_complex_code":"222020","administrative_code":"1153010200","#,
        r#""administrative_code_granularity":"legal_dong","#,
        r#""address_text":"서울특별시 구로구 구로동","#,
        r#""address_source_dataset":"industrylandorkr__industrial_complex_list","#,
        r#""address_source_record_id":"industrylandorkr:danji_cd=222020","#,
        r#""resolution_tier":"source_code_in_authority"}"#
    );

    #[test]
    fn reads_one_resolution_per_line() -> anyhow::Result<()> {
        let resolution = parse_address_book(&format!("\n{VALID_LINE}\n\n"))?;

        assert_eq!(resolution.book.len(), 1);
        let address = resolution
            .book
            .get("111010")
            .ok_or_else(|| anyhow::anyhow!("missing resolution"))?;
        assert_eq!(address.sido_code(), "11");
        assert_eq!(address.sigungu_code(), "11530");
        // The source named a district, so there is no legal dong to report.
        assert_eq!(address.primary_bjdong_code(), None);
        assert_eq!(
            resolution.tier_counts.get("source_code_in_authority"),
            Some(&1)
        );
        assert_eq!(resolution.granularity_counts.get("sigungu"), Some(&1));
        Ok(())
    }

    #[test]
    fn a_legal_dong_resolution_reports_its_dong_code() -> anyhow::Result<()> {
        let resolution = parse_address_book(VALID_LEGAL_DONG_LINE)?;

        let address = resolution
            .book
            .get("222020")
            .ok_or_else(|| anyhow::anyhow!("missing resolution"))?;
        assert_eq!(address.primary_bjdong_code(), Some("1153010200"));
        assert_eq!(resolution.granularity_counts.get("legal_dong"), Some(&1));
        Ok(())
    }

    /// A sigungu code declared `legal_dong` would put five zeros in the dong position and read
    /// downstream as a real dong. The type refuses it, which is the whole point of carrying the
    /// granularity.
    #[test]
    fn rejects_a_sigungu_code_declared_as_a_legal_dong() {
        let line = VALID_LINE.replace(r#""sigungu""#, r#""legal_dong""#);

        let error = parse_address_book(&line)
            .expect_err("a sigungu code must not be accepted as a legal dong");

        assert!(format!("{error:#}").contains("granularity"), "{error:#}");
    }

    #[test]
    fn rejects_a_legal_dong_code_declared_as_a_sigungu() {
        let line = VALID_LEGAL_DONG_LINE.replace(r#""legal_dong""#, r#""sigungu""#);

        let error = parse_address_book(&line)
            .expect_err("a legal-dong code must not be accepted as a district");

        assert!(format!("{error:#}").contains("granularity"), "{error:#}");
    }

    #[test]
    fn rejects_a_resolution_that_does_not_say_how_precise_its_code_is() {
        let line = VALID_LINE.replace(r#""administrative_code_granularity":"sigungu","#, "");

        let error =
            parse_address_book(&line).expect_err("an undeclared granularity must be rejected");

        assert!(format!("{error:#}").contains("invalid JSONL"), "{error:#}");
    }

    #[test]
    fn rejects_a_resolution_that_does_not_say_how_it_was_resolved() {
        let line = VALID_LINE.replace(r#","resolution_tier":"source_code_in_authority""#, "");

        let error = parse_address_book(&line).expect_err("an unstated tier must be rejected");

        assert!(format!("{error:#}").contains("invalid JSONL"), "{error:#}");
    }

    /// The tier is a claim about where the value came from, so it has to agree with the dataset the
    /// line names. A notice-derived code attributed to the complex list would make the heuristic
    /// tier invisible in the evidence, and a list-derived code attributed to the notices would
    /// invent a notice that decided nothing.
    #[test]
    fn rejects_a_tier_that_contradicts_the_dataset_it_names() {
        for line in [
            VALID_LINE.replace(
                r#""resolution_tier":"source_code_in_authority""#,
                r#""resolution_tier":"modal_notice_code""#,
            ),
            VALID_LINE.replace(
                r#""address_source_dataset":"industrylandorkr__industrial_complex_list""#,
                r#""address_source_dataset":"industrylandorkr__industrial_complex_notice""#,
            ),
        ] {
            let error =
                parse_address_book(&line).expect_err("a contradicting tier must be rejected");
            assert!(
                format!("{error:#}").contains("industrial_complex_notice"),
                "{error:#}"
            );
        }
    }

    /// The four complexes the bulk list omits come from the per-complex detail endpoint. Their
    /// provenance names that dataset, and the tier check must admit it rather than pin one slug.
    #[test]
    fn a_complex_sourced_from_the_detail_endpoint_is_accepted() -> anyhow::Result<()> {
        let line = VALID_LINE.replace(
            "industrylandorkr__industrial_complex_list",
            "industrylandorkr__industrial_complex_detail",
        );

        let resolution = parse_address_book(&line)?;

        assert_eq!(resolution.book.len(), 1);
        Ok(())
    }

    #[test]
    fn rejects_a_resolution_that_leaves_the_location_blank() {
        for line in [
            VALID_LINE.replace(
                r#""administrative_code":"1153000000""#,
                r#""administrative_code":"""#,
            ),
            VALID_LINE.replace(
                r#""address_text":"서울특별시 구로구 구로동""#,
                r#""address_text":"""#,
            ),
            VALID_LINE.replace(
                r#""address_source_dataset":"industrylandorkr__industrial_complex_list""#,
                r#""address_source_dataset":"""#,
            ),
            VALID_LINE.replace(
                r#""official_complex_code":"111010""#,
                r#""official_complex_code":"""#,
            ),
        ] {
            let error = parse_address_book(&line).expect_err("a blank location must be rejected");
            assert!(format!("{error:#}").contains("line 1"), "{error:#}");
        }
    }

    #[test]
    fn rejects_a_resolution_missing_its_provenance_fields() {
        let error = parse_address_book(
            r#"{"official_complex_code":"111010","administrative_code":"1153000000","administrative_code_granularity":"sigungu","address_text":"주소"}"#,
        )
        .expect_err("a resolution without provenance must be rejected");

        assert!(format!("{error:#}").contains("invalid JSONL"), "{error:#}");
    }

    #[test]
    fn reads_a_resolution_written_with_a_utf8_bom() -> anyhow::Result<()> {
        let resolution = parse_address_book(&format!("\u{feff}{VALID_LINE}\n"))?;

        assert_eq!(resolution.book.len(), 1);
        assert!(resolution.book.get("111010").is_some());
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
