use anyhow::{bail, Context};
use chrono::NaiveDate;

/// 로컬 exporter와 원격 runner가 공유하는 Bronze ZIP 파일명·스냅샷 날짜 계약.
pub(crate) fn object_date(name: &str) -> anyhow::Result<NaiveDate> {
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

pub(crate) fn validate_object_name(name: &str, expected_date: NaiveDate) -> anyhow::Result<()> {
    if object_date(name)? != expected_date {
        bail!(
            "{name} must embed snapshot date {} after the OPN prefix",
            expected_date.format("%Y%m%d")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_snapshot_date_from_safe_opn_zip() -> anyhow::Result<()> {
        let expected = NaiveDate::from_ymd_opt(2099, 2, 28).expect("valid date");
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
    fn rejects_another_snapshot_date() {
        let expected = NaiveDate::from_ymd_opt(2099, 1, 2).expect("valid date");
        let error = validate_object_name("OPN20990101SYNTHETIC-BASIS.zip", expected)
            .expect_err("mixed snapshots must fail");
        assert!(error
            .to_string()
            .contains("must embed snapshot date 20990102"));
    }
}
