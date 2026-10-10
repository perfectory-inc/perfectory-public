//! The newest complete release of a lane, read from the Bronze ledger (root ADR-0169 §2).
//!
//! A hub.go.kr release is one provider month: the ledger records each monthly ZIP with
//! `snapshot_date` on the first of that month (granularity month, basis provider file period), and
//! the file name `OPN<YYYYMMDD>...zip` carries the day the provider exported it. FLOOR picks its
//! pair the same way (`latest_complete_bronze_month_candidates`, root ADR-0128).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context};
use chrono::{Datelike, NaiveDate};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

/// A read bound: the hub sources hold a few dozen monthly objects each.
const MAX_LEDGER_ROWS: i64 = 10_000;

/// One committed Bronze object of one of the lane's sources, as the ledger records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct LedgerObject {
    pub slug: String,
    pub object_key: String,
    pub snapshot_date: NaiveDate,
    pub provider_updated_at: Option<NaiveDate>,
    pub size_bytes: u64,
    pub checksum_sha256: String,
}

impl LedgerObject {
    /// The provider file name: the last segment of the Bronze key.
    pub(in crate::remote_lakehouse_job) fn file_name(&self) -> &str {
        self.object_key
            .rsplit('/')
            .next()
            .unwrap_or(self.object_key.as_str())
    }

    fn provider_day(&self) -> anyhow::Result<NaiveDate> {
        foundation_outbox_publisher::building_register_snapshot::object_date(self.file_name())
    }
}

/// The release a run loads: one object per role, all of one provider month.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct Release {
    /// The provider month (the first of it, as the ledger keeps it).
    pub month: NaiveDate,
    /// Role name to the object that role loads.
    pub objects: BTreeMap<String, LedgerObject>,
    /// Newer months that lacked a role, for the log; they are not loaded.
    pub skipped_incomplete: Vec<NaiveDate>,
}

impl Release {
    /// `YYYYMM`, the hub tables' `vintage`.
    pub(in crate::remote_lakehouse_job) fn vintage(&self) -> String {
        self.month.format("%Y%m").to_string()
    }

    /// The rows' validity: the first of the provider month, as FLOOR states it.
    pub(in crate::remote_lakehouse_job) fn valid_from_utc(&self) -> String {
        format!("{}T00:00:00Z", self.month)
    }

    /// The load identity of a one-run lane: the lane, the month, and a digest of exactly the
    /// committed bytes it reads, so a re-collected file is a new release and a re-run is not.
    pub(in crate::remote_lakehouse_job) fn run_identity(&self, lane: &str) -> String {
        let mut digest = Sha256::new();
        for (role, object) in &self.objects {
            digest.update(format!(
                "{role}\n{}\n{}\n{}\n",
                object.object_key, object.size_bytes, object.checksum_sha256
            ));
        }
        let hex = format!("{:x}", digest.finalize());
        format!(
            "{}{}-{}",
            run_identity_prefix(lane),
            self.month.format("%Y%m"),
            &hex[..16]
        )
    }

    pub(in crate::remote_lakehouse_job) fn object(
        &self,
        role: &str,
    ) -> anyhow::Result<&LedgerObject> {
        self.objects
            .get(role)
            .with_context(|| format!("the release has no {role} object"))
    }
}

/// What every run identity of `lane` starts with; the month follows it.
pub(in crate::remote_lakehouse_job) fn run_identity_prefix(lane: &str) -> String {
    format!("silver-refresh-{lane}-")
}

/// The provider month a recorded identity names, when it is one this refresh wrote for `lane`.
pub(in crate::remote_lakehouse_job) fn run_identity_month(
    lane: &str,
    identity: &str,
) -> Option<String> {
    let rest = identity.strip_prefix(&run_identity_prefix(lane))?;
    let (month, digest) = rest.split_once('-')?;
    (month.len() == 6
        && month.bytes().all(|b| b.is_ascii_digit())
        && digest.len() == 16
        && digest.bytes().all(|b| b.is_ascii_hexdigit()))
    .then(|| month.to_owned())
}

/// Picks the newest complete release.
///
/// Months are tried newest first. A month in which some role has no object is skipped (named in
/// `skipped_incomplete`). In the first complete month, a role with two objects keeps the one the
/// provider updated later; two with the same or no update date are refused, never decided by
/// collection time or key order. Every chosen object must be a hub monthly ZIP of that month, and
/// all of a release's files must come from one provider export day.
pub(in crate::remote_lakehouse_job) fn select(
    roles: &BTreeMap<String, String>,
    ledger: &[LedgerObject],
) -> anyhow::Result<Release> {
    ensure!(!roles.is_empty(), "a lane reads at least one source");
    let months: BTreeSet<NaiveDate> = ledger
        .iter()
        .filter(|object| roles.values().any(|slug| *slug == object.slug))
        .map(|object| object.snapshot_date)
        .collect();
    let mut skipped_incomplete = Vec::new();
    for month in months.into_iter().rev() {
        let mut objects = BTreeMap::new();
        let mut complete = true;
        for (role, slug) in roles {
            let candidates: Vec<&LedgerObject> = ledger
                .iter()
                .filter(|object| object.slug == *slug && object.snapshot_date == month)
                .collect();
            match newest(&candidates)? {
                Some(object) => {
                    objects.insert(role.clone(), object.clone());
                }
                None => complete = false,
            }
        }
        if !complete {
            skipped_incomplete.push(month);
            continue;
        }
        let release = Release {
            month,
            objects,
            skipped_incomplete,
        };
        validate(roles, &release)?;
        return Ok(release);
    }
    bail!(
        "the ledger holds no complete release of {} (incomplete months: {})",
        roles.values().cloned().collect::<Vec<_>>().join(", "),
        skipped_incomplete
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Of several objects that would fill one place in a release, the one the provider updated
/// later; the same or no update date is refused (the land lanes use the same rule, `land.rs`).
pub(in crate::remote_lakehouse_job) fn newest<'a>(
    candidates: &[&'a LedgerObject],
) -> anyhow::Result<Option<&'a LedgerObject>> {
    let Some(latest) = candidates
        .iter()
        .map(|object| object.provider_updated_at)
        .max()
    else {
        return Ok(None);
    };
    let winners: Vec<&&LedgerObject> = candidates
        .iter()
        .filter(|object| object.provider_updated_at == latest)
        .collect();
    match winners.as_slice() {
        [only] => Ok(Some(**only)),
        _ => bail!(
            "{} objects of {} for {} share the latest provider update date {:?}; refusing to choose: {}",
            winners.len(),
            winners[0].slug,
            winners[0].snapshot_date,
            latest,
            winners
                .iter()
                .map(|object| object.object_key.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn validate(roles: &BTreeMap<String, String>, release: &Release) -> anyhow::Result<()> {
    ensure!(
        release.month.day() == 1,
        "the ledger's hub month {} is not the first of a month",
        release.month
    );
    let mut days = BTreeSet::new();
    for (role, object) in &release.objects {
        let slug = roles
            .get(role)
            .with_context(|| format!("the release names a role {role} the lane does not read"))?;
        let file = object
            .object_key
            .strip_prefix(&format!("bronze/source={slug}/"))
            .filter(|name| !name.contains('/'))
            .with_context(|| format!("{} is not a {slug} Bronze object", object.object_key))?;
        let day = object.provider_day()?;
        ensure!(
            day.year() == release.month.year() && day.month() == release.month.month(),
            "{file} names {day}, outside the ledger's month {}",
            release.month
        );
        ensure!(
            object.size_bytes > 0
                && object.checksum_sha256.len() == 64
                && object
                    .checksum_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "{file} has no byte evidence in the ledger"
        );
        days.insert(day);
    }
    ensure!(
        days.len() == 1,
        "the release's files come from different provider export days: {days:?}"
    );
    Ok(())
}

/// Reads every committed object of the lane's sources (bounded), newest month first.
pub(in crate::remote_lakehouse_job) async fn read_ledger(
    pool: &PgPool,
    roles: &BTreeMap<String, String>,
) -> anyhow::Result<Vec<LedgerObject>> {
    let slugs: Vec<String> = roles.values().cloned().collect();
    let rows = sqlx::query(
        "SELECT s.slug, b.object_key, b.snapshot_date, b.provider_updated_at, b.size_bytes,
                b.checksum_sha256
         FROM catalog.bronze_object b
         JOIN catalog.source_catalog s ON s.id = b.source_catalog_id
         WHERE s.slug = ANY($1)
         ORDER BY b.snapshot_date DESC, b.object_key
         LIMIT $2",
    )
    .bind(&slugs)
    .bind(MAX_LEDGER_ROWS + 1)
    .fetch_all(pool)
    .await
    .context("cannot read the Bronze ledger")?;
    ensure!(
        i64::try_from(rows.len())? <= MAX_LEDGER_ROWS,
        "the lane's sources hold more than {MAX_LEDGER_ROWS} ledger rows"
    );
    rows.iter()
        .map(|row| {
            Ok(LedgerObject {
                slug: row.try_get("slug")?,
                object_key: row.try_get("object_key")?,
                snapshot_date: row.try_get("snapshot_date")?,
                provider_updated_at: row.try_get("provider_updated_at")?,
                size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?)?,
                checksum_sha256: row.try_get("checksum_sha256")?,
            })
        })
        .collect()
}
