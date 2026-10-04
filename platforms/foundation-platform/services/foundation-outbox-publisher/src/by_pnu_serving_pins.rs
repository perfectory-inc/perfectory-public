//! Pins the Gold snapshot a published by-PNU manifest reflects (root ADR-0146 §2).
//!
//! The next change set is computed against `reflected_gold_iceberg_snapshot_id` (root ADR-0141
//! §3). When Iceberg snapshot expiry removes that snapshot, the change set is unknown and the
//! lane can only be re-based (a full bake, or the verified re-base of ADR-0146 §1). So every
//! publish tags the snapshot its manifest reflects, and Iceberg's expiry keeps every snapshot a
//! live tag names.
//!
//! - The tag is named after the manifest it pins: `served-{unit}-{published_at}-{snapshot}`. A
//!   name always means one snapshot and a tag is never moved, so a failed publish cannot unpin
//!   the live manifest.
//! - The tag is created before the manifest is written. When the write fails, the new tag is
//!   released again; when it succeeds, every other `served-{unit}-` tag is released. A failed
//!   release leaves an extra pin, which is the safe direction; the next publish releases it.
//! - The production pins are tags in the Iceberg catalog. A local rehearsal store keeps the same
//!   tags in a file beside its objects, so a rehearsal never touches the catalog.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};

use crate::by_pnu_gateway_contract::ByPnuLane;
use crate::industrial_complex_gold_profile_store::ProfileStoreConfig;

/// File a local rehearsal store keeps its tags in, at the store root.
pub(crate) const LOCAL_TAGS_FILE: &str = "gold-snapshot-tags.json";

/// Where a lane's pins live.
pub(crate) enum SnapshotPins {
    /// Tags in the Iceberg catalog: the production pins.
    Catalog(Box<IcebergRestCatalog>),
    /// `{tag: snapshot}` per table in a JSON file beside a local rehearsal store.
    LocalFile(PathBuf),
}

/// The prefix every pin of `lane` carries; nothing else is ever released by a publish.
pub(crate) fn tag_prefix(lane: ByPnuLane) -> String {
    format!("served-{}-", lane.unit())
}

/// The pin of the manifest published at `published_at_utc` (RFC 3339) reflecting `snapshot`.
/// Two manifests published in the same second share a name only when they pin the same snapshot.
///
/// # Errors
/// Refuses a timestamp that is not RFC 3339.
pub(crate) fn tag_name(
    lane: ByPnuLane,
    published_at_utc: &str,
    snapshot: i64,
) -> anyhow::Result<String> {
    let published = DateTime::parse_from_rfc3339(published_at_utc)
        .with_context(|| format!("published_at_utc {published_at_utc:?} is not RFC 3339"))?
        .with_timezone(&Utc)
        .format("%Y%m%dT%H%M%SZ");
    Ok(format!("{}{published}-{snapshot}", tag_prefix(lane)))
}

impl SnapshotPins {
    /// The pins that go with the output store: the catalog for R2, a file for a local root.
    ///
    /// # Errors
    /// Returns an error when the catalog is not configured.
    pub(crate) fn for_output(output: &ProfileStoreConfig) -> anyhow::Result<Self> {
        Ok(match output {
            ProfileStoreConfig::Local { root } => Self::LocalFile(root.join(LOCAL_TAGS_FILE)),
            ProfileStoreConfig::R2 => Self::Catalog(Box::new(
                IcebergRestCatalog::new(
                    LakehouseCatalogConfig::from_env()
                        .context("the publish pins the reflected Gold snapshot in the catalog")?,
                )
                .context("failed to build the Iceberg catalog client")?,
            )),
        })
    }

    /// Every tag of `table` whose name starts with `prefix`, with the snapshot it names.
    ///
    /// # Errors
    /// Returns an error when the tags cannot be read.
    pub(crate) async fn tags(
        &self,
        table: &str,
        prefix: &str,
    ) -> anyhow::Result<BTreeMap<String, i64>> {
        let all = match self {
            Self::Catalog(catalog) => catalog
                .load_snapshot_refs(table)
                .await?
                .with_context(|| format!("{table} does not exist"))?
                .refs
                .into_iter()
                .filter(|(_, reference)| reference.kind == "tag")
                .map(|(name, reference)| (name, reference.snapshot_id))
                .collect(),
            Self::LocalFile(path) => read_local(path)?.remove(table).unwrap_or_default(),
        };
        Ok(all
            .into_iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .collect())
    }

    /// Creates tag `name` on `snapshot` of `table`; the same tag on the same snapshot is a re-run.
    ///
    /// # Errors
    /// Refuses a name another snapshot holds, or a snapshot the catalog no longer has.
    pub(crate) async fn create(
        &self,
        table: &str,
        name: &str,
        snapshot: i64,
    ) -> anyhow::Result<()> {
        match self {
            Self::Catalog(catalog) => Ok(catalog.create_tag(table, name, snapshot).await?),
            Self::LocalFile(path) => {
                let mut all = read_local(path)?;
                let tags = all.entry(table.to_owned()).or_default();
                if let Some(existing) = tags.get(name) {
                    ensure!(
                        *existing == snapshot,
                        "{table} already has a tag named {name} on snapshot {existing}; a pin is \
                         never moved"
                    );
                    return Ok(());
                }
                tags.insert(name.to_owned(), snapshot);
                write_local(path, &all)
            }
        }
    }

    /// Removes tag `name` of `table` while it still names `snapshot`; an absent tag is removed.
    ///
    /// # Errors
    /// Refuses a tag that names another snapshot.
    pub(crate) async fn remove(
        &self,
        table: &str,
        name: &str,
        snapshot: i64,
    ) -> anyhow::Result<()> {
        match self {
            Self::Catalog(catalog) => Ok(catalog.remove_tag(table, name, snapshot).await?),
            Self::LocalFile(path) => {
                let mut all = read_local(path)?;
                let Some(tags) = all.get_mut(table) else {
                    return Ok(());
                };
                match tags.get(name) {
                    None => return Ok(()),
                    Some(existing) => ensure!(
                        *existing == snapshot,
                        "{table}'s tag {name} names snapshot {existing}, not {snapshot}"
                    ),
                }
                tags.remove(name);
                write_local(path, &all)
            }
        }
    }
}

/// Pins `snapshot` of `table` for the manifest published at `published_at_utc`; returns the tag.
///
/// # Errors
/// Refuses a snapshot id that is not a number and any refusal of [`SnapshotPins::create`]; the
/// manifest must then not be written.
pub(crate) async fn pin(
    pins: &SnapshotPins,
    lane: ByPnuLane,
    table: &str,
    snapshot: &str,
    published_at_utc: &str,
) -> anyhow::Result<String> {
    let snapshot = parse_snapshot(snapshot)?;
    let tag = tag_name(lane, published_at_utc, snapshot)?;
    pins.create(table, &tag, snapshot).await.with_context(|| {
        format!("could not pin Gold snapshot {snapshot} of {table} as {tag}; the manifest stays")
    })?;
    tracing::info!(table, tag = %tag, snapshot, "reflected Gold snapshot pinned");
    Ok(tag)
}

/// Releases every pin of `lane` on `table` except `keep`, once the manifest `keep` pins is live.
/// Returns the released tags. A tag that cannot be released stays; it only keeps a snapshot alive
/// longer, and the next publish releases it.
pub(crate) async fn release_others(
    pins: &SnapshotPins,
    lane: ByPnuLane,
    table: &str,
    keep: &str,
) -> Vec<String> {
    let tags = match pins.tags(table, &tag_prefix(lane)).await {
        Ok(tags) => tags,
        Err(error) => {
            tracing::warn!(table, error = %format!("{error:#}"), "older pins not read; they stay until the next publish");
            return Vec::new();
        }
    };
    let mut released = Vec::new();
    for (name, snapshot) in tags.into_iter().filter(|(name, _)| name != keep) {
        match pins.remove(table, &name, snapshot).await {
            Ok(()) => released.push(name),
            Err(error) => tracing::warn!(
                table,
                tag = %name,
                error = %format!("{error:#}"),
                "older pin not released; it stays until the next publish"
            ),
        }
    }
    released
}

/// An Iceberg snapshot id as the manifest stores it (a decimal string).
///
/// # Errors
/// Refuses anything but a decimal 64-bit integer.
pub(crate) fn parse_snapshot(snapshot: &str) -> anyhow::Result<i64> {
    snapshot
        .parse::<i64>()
        .with_context(|| format!("Gold snapshot id {snapshot:?} is not an Iceberg snapshot id"))
}

type LocalTags = BTreeMap<String, BTreeMap<String, i64>>;

fn read_local(path: &Path) -> anyhow::Result<LocalTags> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("{} does not hold local pins", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(LocalTags::new()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn write_local(path: &Path, tags: &LocalTags) -> anyhow::Result<()> {
    let body = serde_json::to_vec_pretty(tags).context("failed to serialize local pins")?;
    std::fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{pin, release_others, tag_name, SnapshotPins};
    use crate::by_pnu_gateway_contract::ByPnuLane;

    const TABLE: &str = "gold.parcel_panel";

    fn pins(label: &str) -> (SnapshotPins, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "foundation-platform-by-pnu-pins-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        let _ = std::fs::create_dir_all(&root);
        (SnapshotPins::LocalFile(root.join("tags.json")), root)
    }

    #[test]
    fn a_tag_names_its_lane_and_the_manifest_it_pins() -> anyhow::Result<()> {
        assert_eq!(
            tag_name(ByPnuLane::Parcel, "2026-01-02T03:04:05Z", 7)?,
            "served-parcel-by-pnu-20260102T030405Z-7"
        );
        assert!(tag_name(ByPnuLane::Parcel, "yesterday", 7).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn a_local_pin_is_never_moved_and_release_keeps_the_live_one() -> anyhow::Result<()> {
        let (pins, root) = pins("local");
        let first = pin(
            &pins,
            ByPnuLane::Parcel,
            TABLE,
            "999990000000000001",
            "2026-01-01T00:00:00Z",
        )
        .await?;
        // A re-run is the same pin; another snapshot under a taken name is refused.
        pin(
            &pins,
            ByPnuLane::Parcel,
            TABLE,
            "999990000000000001",
            "2026-01-01T00:00:00Z",
        )
        .await?;
        assert!(pins
            .create(TABLE, &first, 999_990_000_000_000_002)
            .await
            .is_err());
        let second = pin(
            &pins,
            ByPnuLane::Parcel,
            TABLE,
            "999990000000000002",
            "2026-01-02T00:00:00Z",
        )
        .await?;
        // Another lane's pin and a hand-made tag are not this lane's to release.
        pins.create(TABLE, "served-building-by-pnu-20260101T000000Z", 7)
            .await?;
        pins.create(TABLE, "audit-2026", 7).await?;

        let released = release_others(&pins, ByPnuLane::Parcel, TABLE, &second).await;
        let left = pins.tags(TABLE, "").await?;
        std::fs::remove_dir_all(&root)?;

        assert_eq!(released, vec![first]);
        assert_eq!(
            left.keys().cloned().collect::<Vec<_>>(),
            vec![
                "audit-2026".to_owned(),
                "served-building-by-pnu-20260101T000000Z".to_owned(),
                second,
            ]
        );
        Ok(())
    }
}
