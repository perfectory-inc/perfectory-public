//! The one definition of how a hub.go.kr Bronze file belongs to a provider release.
//!
//! hub.go.kr publishes each register once a month. The Bronze ledger files a monthly ZIP under the
//! first of its provider month (`snapshot_date`, granularity month, basis provider file period),
//! and the file name `OPN<YYYYMMDD>...zip` carries the day the provider exported it. From those two
//! facts every reader derives the same four things, and only from here:
//!
//! - a file's **provider day**: the `YYYYMMDD` in its name ([`provider_day`]);
//! - its **provider month**: the month of that day ([`ProviderMonth::of_file`]);
//! - **one release**: files of one provider month that the provider exported on one day
//!   ([`ProviderRelease`]). Root ADR-0169 §5② states both conditions for the Silver lanes ("파일
//!   이름은 그 월 안이어야 하고, 여러 역할은 같은 내보내기 날이어야 한다") and calls it FLOOR's rule
//!   (root ADR-0128, `CommittedInputs::from_objects`). The same export day is what ties a unit to
//!   the titles and basis outlines it is checked against (unit parent evidence, root ADR-0125): two
//!   exports of one month are two different states of the register, and mixing them would judge one
//!   state's units against another state's parents;
//! - a release's **validity**: rows are valid from the first of the provider month, 00:00 UTC
//!   (root ADR-0128 §2, [`ProviderMonth::valid_from_utc`]), never from the export day or the run.
//!
//! The 2026-10-10 units refusal came from a second copy of this rule that compared the export day
//! with `valid_from` (the first of the month). Callers do not re-derive any of it.
use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, SecondsFormat, Utc};

/// The day the provider exported a hub.go.kr monthly ZIP, from its Bronze file name.
///
/// The name must be a bare, safe `.zip` file name (no path) that embeds a valid calendar date as
/// `YYYYMMDD` right after the `OPN` prefix.
///
/// # Errors
/// Refuses a path, an unsafe or non-`.zip` name, or a name without a valid date after `OPN`.
// Exact lowercase .zip is part of the Bronze object-name contract.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn provider_day(name: &str) -> anyhow::Result<NaiveDate> {
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

/// A hub.go.kr provider month, held as its first day (the ledger's `snapshot_date`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderMonth {
    first: NaiveDate,
}

impl ProviderMonth {
    /// The month a day falls in.
    #[must_use]
    pub fn containing(day: NaiveDate) -> Self {
        Self {
            first: day - chrono::Days::new(u64::from(day.day0())),
        }
    }

    /// The provider month of a monthly ZIP: the month of its provider day.
    ///
    /// # Errors
    /// Refuses a name [`provider_day`] refuses.
    pub fn of_file(name: &str) -> anyhow::Result<Self> {
        provider_day(name).map(Self::containing)
    }

    /// The month the ledger names by its first day (`snapshot_date`).
    ///
    /// # Errors
    /// Refuses a date that is not the first of a month.
    pub fn from_first_day(first: NaiveDate) -> anyhow::Result<Self> {
        ensure!(
            first.day() == 1,
            "the hub provider month {first} is not the first of a month"
        );
        Ok(Self { first })
    }

    /// The month whose rows are valid from `valid_from` ([`Self::valid_from_utc`]).
    ///
    /// # Errors
    /// Refuses any instant other than the first of a month at 00:00 UTC.
    pub fn from_valid_from(valid_from: DateTime<Utc>) -> anyhow::Result<Self> {
        let month = Self::containing(valid_from.date_naive());
        ensure!(
            valid_from == month.valid_from_utc(),
            "valid_from_utc {} is not the start of a provider month; hub rows are valid from the \
             first of their provider month, {} (root ADR-0128)",
            valid_from.to_rfc3339_opts(SecondsFormat::Secs, true),
            month.valid_from_rfc3339()
        );
        Ok(month)
    }

    /// The first day of the month, as the ledger's `snapshot_date` holds it.
    #[must_use]
    pub const fn first_day(self) -> NaiveDate {
        self.first
    }

    /// `YYYYMM`: the hub tables' `vintage` and the month in a Silver run identity.
    #[must_use]
    pub fn vintage(self) -> String {
        self.first.format("%Y%m").to_string()
    }

    /// `YYYY-MM`: the ledger's `snapshot_period`.
    #[must_use]
    pub fn ledger_period(self) -> String {
        self.first.format("%Y-%m").to_string()
    }

    /// The instant the month's rows are valid from: its first day at 00:00 UTC (root ADR-0128).
    #[must_use]
    pub const fn valid_from_utc(self) -> DateTime<Utc> {
        self.first.and_time(NaiveTime::MIN).and_utc()
    }

    /// [`Self::valid_from_utc`] as the exporters' `*_VALID_FROM_UTC` text, `YYYY-MM-01T00:00:00Z`.
    #[must_use]
    pub fn valid_from_rfc3339(self) -> String {
        self.valid_from_utc()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }
}

impl std::fmt::Display for ProviderMonth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.ledger_period())
    }
}

/// Files that are one hub.go.kr release: one provider month, exported on one day.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRelease {
    month: ProviderMonth,
    export_day: NaiveDate,
}

impl ProviderRelease {
    /// The release a set of monthly ZIPs forms. Every name must be valid and all of them must
    /// carry one provider export day; the release's month is that day's.
    ///
    /// # Errors
    /// Refuses no names, a name [`provider_day`] refuses, or files of different export days.
    pub fn of_files<I>(names: I) -> anyhow::Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut export_day: Option<NaiveDate> = None;
        for name in names {
            let name = name.as_ref();
            let day = provider_day(name)?;
            match export_day {
                None => export_day = Some(day),
                Some(first) => ensure!(
                    day == first,
                    "{name} was exported on {day}, but the release's other files on {first}; one \
                     hub release is one provider month exported on one day (root ADR-0169)"
                ),
            }
        }
        let export_day = export_day.context("a hub release has at least one file")?;
        Ok(Self {
            month: ProviderMonth::containing(export_day),
            export_day,
        })
    }

    /// [`Self::of_files`] for a release whose month is already decided (by the ledger's
    /// `snapshot_date` or the run's `valid_from`): the files must also be of that month.
    ///
    /// # Errors
    /// Refuses what [`Self::of_files`] refuses, or files of another month.
    pub fn of_files_in<I>(month: ProviderMonth, names: I) -> anyhow::Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let release = Self::of_files(names)?;
        ensure!(
            release.month == month,
            "the files were exported on {}, outside the provider month {month}",
            release.export_day
        );
        Ok(release)
    }

    /// The release's provider month.
    #[must_use]
    pub const fn month(self) -> ProviderMonth {
        self.month
    }

    /// The day the provider exported every file of the release.
    #[must_use]
    pub const fn export_day(self) -> NaiveDate {
        self.export_day
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(year: i32, month: u32, day: u32) -> anyhow::Result<NaiveDate> {
        NaiveDate::from_ymd_opt(year, month, day).context("valid date")
    }

    #[test]
    fn reads_the_provider_day_from_a_safe_opn_zip() -> anyhow::Result<()> {
        assert_eq!(
            provider_day("OPN20990228SYNTHETIC-BASIS.zip")?,
            date(2099, 2, 28)?
        );
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
            assert!(provider_day(name).is_err(), "accepted invalid date: {name}");
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
            assert!(provider_day(name).is_err(), "accepted unsafe name: {name}");
        }
    }

    #[test]
    fn a_file_belongs_to_the_month_of_its_export_day() -> anyhow::Result<()> {
        let month = ProviderMonth::of_file("OPN20990920SYNTHETIC-UNIT.zip")?;
        assert_eq!(month.first_day(), date(2099, 9, 1)?);
        assert_eq!(month.vintage(), "209909");
        assert_eq!(month.ledger_period(), "2099-09");
        assert_eq!(
            ProviderMonth::of_file("OPN20991231SYNTHETIC-UNIT.zip")?.vintage(),
            "209912"
        );
        Ok(())
    }

    #[test]
    fn a_month_is_valid_from_its_first_day_at_midnight_utc() -> anyhow::Result<()> {
        let month = ProviderMonth::from_first_day(date(2099, 7, 1)?)?;
        assert_eq!(month.valid_from_rfc3339(), "2099-07-01T00:00:00Z");
        assert_eq!(
            ProviderMonth::from_valid_from(month.valid_from_utc())?,
            month
        );
        Ok(())
    }

    #[test]
    fn a_month_refuses_anything_but_its_start() -> anyhow::Result<()> {
        assert!(ProviderMonth::from_first_day(date(2099, 7, 2)?).is_err());
        for refused in ["2099-07-20T00:00:00Z", "2099-07-01T09:00:00Z"] {
            let instant = DateTime::parse_from_rfc3339(refused)?.to_utc();
            let error = ProviderMonth::from_valid_from(instant)
                .err()
                .context("a mid-month valid_from must fail")?;
            assert!(
                error.to_string().contains("2099-07-01T00:00:00Z"),
                "{error}"
            );
        }
        Ok(())
    }

    #[test]
    fn one_release_is_one_month_exported_on_one_day() -> anyhow::Result<()> {
        let month = ProviderMonth::from_first_day(date(2099, 9, 1)?)?;
        let release = ProviderRelease::of_files_in(
            month,
            [
                "OPN20990920SYNTHETIC-UNIT.zip",
                "OPN20990920SYNTHETIC-TITLE.zip",
                "OPN20990920SYNTHETIC-BASIS.zip",
            ],
        )?;
        assert_eq!(release.month(), month);
        assert_eq!(release.export_day(), date(2099, 9, 20)?);
        // A file exported on the first of the month is still that month's.
        ProviderRelease::of_files_in(month, ["OPN20990901SYNTHETIC-UNIT.zip"])?;
        Ok(())
    }

    #[test]
    fn a_release_refuses_another_export_day_or_month() -> anyhow::Result<()> {
        let month = ProviderMonth::from_first_day(date(2099, 9, 1)?)?;
        let error = ProviderRelease::of_files([
            "OPN20990920SYNTHETIC-FLOOR.zip",
            "OPN20990921SYNTHETIC-TITLE.zip",
        ])
        .err()
        .context("two export days of one month must fail")?;
        assert!(
            error.to_string().contains("exported on 2099-09-21"),
            "{error}"
        );
        for name in [
            "OPN20990831SYNTHETIC-UNIT.zip",
            "OPN20991001SYNTHETIC-UNIT.zip",
        ] {
            assert!(
                ProviderRelease::of_files_in(month, [name]).is_err(),
                "accepted another month: {name}"
            );
        }
        assert!(ProviderRelease::of_files(Vec::<String>::new()).is_err());
        Ok(())
    }
}
