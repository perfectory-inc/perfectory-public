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
    match value {
        None => Ok(()),
        Some(code) => validate_primary_bjdong_code(code),
    }
}

pub fn validate_primary_bjdong_code(value: &str) -> Result<(), CatalogError> {
    if value.len() == 10 && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    Err(CatalogError::InvalidIndustrialComplexInput(format!(
        "primary_bjdong_code must be exactly 10 ASCII digits: {value}"
    )))
}
