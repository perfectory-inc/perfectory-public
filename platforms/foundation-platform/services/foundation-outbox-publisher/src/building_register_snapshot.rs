//! 건축물 원문 파일명과 재시도 시각의 공통 계약.
use anyhow::{bail, Context};
use chrono::NaiveDate;

/// 로컬 exporter와 원격 runner가 공유하는 Bronze ZIP 파일명·스냅샷 날짜 계약.
/// # Errors
/// 안전한 파일명 또는 유효한 날짜 형식이 아니면 실패한다.
// Exact lowercase .zip is part of the Bronze object-name contract.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn object_date(name: &str) -> anyhow::Result<NaiveDate> {
    if name.contains('/') || name.contains('\\') {
        bail!("{name} must be a Bronze object file name, not a path");
    }
    if !name.ends_with(".zip")
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
    {
        bail!("{name} must be a safe .zip object file name");
    }
    let date = name
        .strip_prefix("OPN")
        .and_then(|suffix| suffix.get(..8))
        .filter(|date| date.bytes().all(|character| character.is_ascii_digit()))
        .with_context(|| format!("{name} must embed YYYYMMDD after the OPN prefix"))?;
    NaiveDate::parse_from_str(date, "%Y%m%d")
        .with_context(|| format!("{name} must embed a valid YYYYMMDD date after the OPN prefix"))
}

/// 원문 파일명의 날짜가 선택한 스냅샷 날짜와 일치하는지 검사한다.
/// # Errors
/// 파일명이 잘못되었거나 날짜가 다르면 실패한다.
pub fn validate_object_name(name: &str, expected_date: NaiveDate) -> anyhow::Result<()> {
    if object_date(name)? != expected_date {
        bail!(
            "{name} must embed snapshot date {} after the OPN prefix",
            expected_date.format("%Y%m%d")
        );
    }
    Ok(())
}

/// Full typed exports keep row timestamps fixed across attempts; manual exports default to now.
/// # Errors
/// 지정한 환경변수가 유효한 UTF-8 또는 RFC 3339 시각이 아니면 실패한다.
pub fn ingested_at_utc() -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    match std::env::var("FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC") {
        Ok(raw) => Ok(chrono::DateTime::parse_from_rfc3339(&raw)?.with_timezone(&chrono::Utc)),
        Err(std::env::VarError::NotPresent) => Ok(chrono::Utc::now()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_snapshot_date_from_safe_opn_zip() -> anyhow::Result<()> {
        let expected = NaiveDate::from_ymd_opt(2099, 2, 28).context("valid date")?;
        assert_eq!(object_date("OPN20990228SYNTHETIC-BASIS.zip")?, expected);
        validate_object_name("OPN20990228SYNTHETIC-BASIS.zip", expected)?;
        Ok(())
    }

    #[test]
    fn rejects_invalid_calendar_dates_and_missing_opn_dates() {
        for name in [
            "OPN20990229BASIS.zip",
            "OPN20991301BASIS.zip",
            "OPN20990100BASIS.zip",
            "OPN2099BASIS.zip",
            "OPN2099-01-01BASIS.zip",
            "BASIS20990101.zip",
            "OPN.zip",
        ] {
            assert!(object_date(name).is_err(), "accepted invalid date: {name}");
        }
    }

    #[test]
    fn rejects_unsafe_names_before_reading_the_date() {
        for name in [
            "../OPN20990101BASIS.zip",
            "nested/OPN20990101BASIS.zip",
            "nested\\OPN20990101BASIS.zip",
            "OPN20990101BASIS'.zip",
            "OPN20990101BASIS.ZIP",
            "OPN20990101BASIS.json",
            "OPN20990101기본개요.zip",
            "",
        ] {
            assert!(object_date(name).is_err(), "accepted unsafe name: {name}");
        }
    }

    #[test]
    fn rejects_another_snapshot_date() -> anyhow::Result<()> {
        let expected = NaiveDate::from_ymd_opt(2099, 1, 2).context("valid date")?;
        let error = validate_object_name("OPN20990101SYNTHETIC-BASIS.zip", expected)
            .err()
            .context("mixed snapshots must fail")?;
        assert!(error
            .to_string()
            .contains("must embed snapshot date 20990102"));
        Ok(())
    }
}
