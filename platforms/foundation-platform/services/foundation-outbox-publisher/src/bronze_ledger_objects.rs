//! The Bronze objects of a source as the ledger records them, the one place a Silver plan learns what
//! there is to load (root ADR-0280, amended 2026-11-28 by root ADR-0626 §4).
//!
//! A Bronze write commits its ledger row (`catalog.bronze_object`, root ADR-0016), so the ledger
//! holds every object, when its collector's run collected it (`collected_at`) and when it landed
//! (the row's `created_at`). Neither is read from the key: a key names a provider file and its
//! bytes and carries no run stamp (root ADR-0626 rule 5), and a copy would reset R2's own
//! modification time. A key may change in place while the object's row and bytes stay (the Bronze
//! epoch, root ADR-0626 §8 N2); the version trigger keeps each replaced row
//! (`catalog.bronze_object_version`, ADR-0264), so a plan learns the keys an object had before.
//! A table that recorded one of them has loaded the object.

use std::collections::BTreeSet;

use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row as _};

use crate::public_data_control_support::required_env_value;

/// One Bronze object of the ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LedgerObject {
    pub(crate) source_slug: String,
    pub(crate) key: String,
    pub(crate) size_bytes: i64,
    /// When the collector's run collected it: the object's `source_snapshot_id`.
    pub(crate) collected_at: DateTime<Utc>,
    /// When its row was written, which a re-key keeps.
    pub(crate) landed_at: DateTime<Utc>,
    /// The keys it had before, with the same bytes, oldest first.
    pub(crate) former_keys: Vec<String>,
}

impl LedgerObject {
    /// Whether a table that records `recorded` has loaded this object: under its key, or under a
    /// key it had before with the same bytes.
    pub(crate) fn is_recorded_in(&self, recorded: &BTreeSet<String>) -> bool {
        recorded.contains(&self.key) || self.former_keys.iter().any(|key| recorded.contains(key))
    }
}

/// The ledger's objects of `slugs`, oldest landing first.
///
/// # Errors
/// The ledger cannot be read.
pub(crate) async fn objects_of(
    pool: &PgPool,
    slugs: &[String],
) -> anyhow::Result<Vec<LedgerObject>> {
    let rows = sqlx::query(
        "SELECT sc.slug, bo.object_key, bo.size_bytes, bo.collected_at, bo.created_at, \
                coalesce(array_agg(v.prior_row ->> 'object_key' ORDER BY v.id) \
                         FILTER (WHERE v.prior_row ->> 'object_key' <> bo.object_key \
                                   AND v.prior_row ->> 'checksum_sha256' = bo.checksum_sha256), \
                         '{}'::text[]) AS former_keys \
         FROM catalog.bronze_object bo \
         JOIN catalog.source_catalog sc ON sc.id = bo.source_catalog_id \
         LEFT JOIN catalog.bronze_object_version v ON v.bronze_object_id = bo.id \
         WHERE sc.slug = ANY($1) \
         GROUP BY bo.id, sc.slug \
         ORDER BY bo.created_at, bo.object_key",
    )
    .bind(slugs)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to read the Bronze ledger of {}", slugs.join(", ")))?;
    rows.iter()
        .map(|row| {
            Ok(LedgerObject {
                source_slug: row.try_get("slug")?,
                key: row.try_get("object_key")?,
                size_bytes: row.try_get("size_bytes")?,
                collected_at: row.try_get("collected_at")?,
                landed_at: row.try_get("created_at")?,
                former_keys: row.try_get("former_keys")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .context("the Bronze ledger answered a row of another shape")
}

/// The ledger, from `DATABASE_URL` (the scripts build it from the release's recovery env, as every
/// collector does).
///
/// # Errors
/// `DATABASE_URL` is unset or the database refuses the connection.
pub(crate) async fn ledger_from_env() -> anyhow::Result<PgPool> {
    PgPool::connect(&required_env_value("DATABASE_URL")?)
        .await
        .context("failed to connect to the Bronze ledger (DATABASE_URL)")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::LedgerObject;

    #[test]
    fn an_object_a_table_recorded_under_a_former_key_is_loaded() {
        let at = chrono::DateTime::UNIX_EPOCH;
        let object = LedgerObject {
            source_slug: "courtauctiongokr__item".to_owned(),
            key: "bronze/source=courtauctiongokr__item/item--sha256-00.gz".to_owned(),
            size_bytes: 1,
            collected_at: at,
            landed_at: at,
            former_keys: vec!["bronze/source=courtauctiongokr__auction_item/a.gz".to_owned()],
        };
        assert!(object.is_recorded_in(&BTreeSet::from([object.key.clone()])));
        assert!(object.is_recorded_in(&BTreeSet::from([object.former_keys[0].clone()])));
        assert!(!object.is_recorded_in(&BTreeSet::from(["bronze/source=x/y.gz".to_owned()])));
    }
}
