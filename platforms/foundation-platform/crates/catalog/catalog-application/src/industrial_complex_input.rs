//! Shared validation helpers for industrial-complex application inputs.

use catalog_domain::CatalogError;

pub fn validate_clean_required(label: &'static str, value: &str) -> Result<(), CatalogError> {
    if value.trim() == value && !value.is_empty() {
        return Ok(());
    }
    Err(CatalogError::InvalidIndustrialComplexInput(format!(
        "{label} must be non-empty text without surrounding whitespace"
    )))
}

pub fn validate_source_official_complex_code(value: &str) -> Result<(), CatalogError> {
    if !value.starts_with("foundation-platform:") {
        return Ok(());
    }
    Err(CatalogError::InvalidIndustrialComplexInput(
        "official_complex_code must be source-side, not a foundation-platform migration placeholder"
            .to_owned(),
    ))
}

/// Validates a legal-dong code that the caller may not have.
///
/// Absence is accepted, malformation is not (root ADR-0040): a complex whose source resolved only
/// to sigungu granularity carries `None`, and the shape rule still holds for every code that is
/// actually present.
pub fn validate_optional_primary_bjdong_code(value: Option<&str>) -> Result<(), CatalogError> {
    value.map_or(Ok(()), validate_primary_bjdong_code)
}

pub fn validate_primary_bjdong_code(value: &str) -> Result<(), CatalogError> {
    validate_administrative_code("primary_bjdong_code", 10, value)
}

/// Validates a province-level code the caller may not have.
pub fn validate_optional_sido_code(value: Option<&str>) -> Result<(), CatalogError> {
    value.map_or(Ok(()), |code| {
        validate_administrative_code("sido_code", 2, code)
    })
}

/// Validates a city/county/district code the caller may not have.
pub fn validate_optional_sigungu_code(value: Option<&str>) -> Result<(), CatalogError> {
    value.map_or(Ok(()), |code| {
        validate_administrative_code("sigungu_code", 5, code)
    })
}

fn validate_administrative_code(
    label: &'static str,
    digits: usize,
    value: &str,
) -> Result<(), CatalogError> {
    if value.len() == digits && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    Err(CatalogError::InvalidIndustrialComplexInput(format!(
        "{label} must be exactly {digits} ASCII digits: {value}"
    )))
}

/// Validates optional free text: absent is fine, present-but-blank is not.
///
/// `Some("")` and `Some("   ")` claim the source stated a value while carrying none. The two facts
/// this column has to keep apart are "the source said nothing" and "the source said this", and an
/// empty string is the spelling that erases the difference.
pub fn validate_optional_clean_text(
    label: &'static str,
    value: Option<&str>,
) -> Result<(), CatalogError> {
    value.map_or(Ok(()), |text| validate_clean_required(label, text))
}
