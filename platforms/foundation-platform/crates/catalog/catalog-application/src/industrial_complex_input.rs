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

/// Validates a site-formation progress percentage the caller may not have.
///
/// Exact decimal text between `0` and `100`, because the canonical column is `numeric(5,2)` and a
/// value outside that range is not a proportion of one complex's own site works. Parsing is what
/// makes the check possible without a decimal type: the text has to be a number before Postgres is
/// asked to store one, and the `f64` here is used to compare, never to carry the value.
pub fn validate_optional_progress_percent(value: Option<&str>) -> Result<(), CatalogError> {
    let Some(value) = value else { return Ok(()) };
    let invalid = || {
        CatalogError::InvalidIndustrialComplexInput(format!(
            "development_progress_percent must be a number between 0 and 100: {value}"
        ))
    };
    let parsed = value.trim().parse::<f64>().map_err(|_| invalid())?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(invalid());
    }
    Ok(())
}

/// Validates the two derived business-period months.
///
/// They answer together or not at all. One month without the other describes a period with an
/// invented boundary, and the producer that derives them never emits that shape — this is the
/// second place that says so, at the boundary where rows enter the canonical table.
pub fn validate_optional_business_period_months(
    start_month: Option<&str>,
    end_month: Option<&str>,
) -> Result<(), CatalogError> {
    match (start_month, end_month) {
        (None, None) => return Ok(()),
        (Some(start), Some(end)) => {
            validate_month("business_period_start_month", start)?;
            validate_month("business_period_end_month", end)?;
        }
        _ => {
            return Err(CatalogError::InvalidIndustrialComplexInput(
                "business_period_start_month and business_period_end_month must be present \
                 together: a period with one boundary states one the source did not"
                    .to_owned(),
            ))
        }
    }
    Ok(())
}

fn validate_month(label: &'static str, value: &str) -> Result<(), CatalogError> {
    let shaped = value.len() == 7
        && value.as_bytes()[4] == b'-'
        && value[0..4].bytes().all(|byte| byte.is_ascii_digit())
        && value[5..7].bytes().all(|byte| byte.is_ascii_digit())
        && matches!(value[5..7].parse::<u32>(), Ok(month) if (1..=12).contains(&month));
    if shaped {
        return Ok(());
    }
    Err(CatalogError::InvalidIndustrialComplexInput(format!(
        "{label} must be yyyy-MM with a month between 01 and 12: {value}"
    )))
}
